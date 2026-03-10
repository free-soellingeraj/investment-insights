#!/usr/bin/env python3
"""Migrate existing cache files to the unified file-per-item layout.

Converts old-style cache files:
  data/raw/news/{TICKER}.json          → sources/{TICKER}/news/{YYYY}/{MM}/{uuid}.json
  data/raw/github/{TICKER}.json        → sources/{TICKER}/github/{YYYY}/{MM}/{uuid}.json
  data/raw/analysts/{TICKER}.json      → sources/{TICKER}/analyst/{YYYY}/{MM}/{uuid}.json
  data/raw/web_enrichment/{TICKER}.json → sources/{TICKER}/web_{type}/{YYYY}/{MM}/{uuid}.json
  data/raw/filings/{TICKER}/*.txt      → sources/{TICKER}/filing/{YYYY}/{MM}/{uuid}.json

Also converts extraction caches:
  data/raw/extracted_filings/{TICKER}.json → extracted/{TICKER}/filing/{YYYY}/{MM}/{uuid}.json
  data/raw/extracted_news/{TICKER}.json    → extracted/{TICKER}/news/{YYYY}/{MM}/{uuid}.json

This is a one-time migration. Not strictly required — old caches will be
re-collected on next run — but saves API calls and LLM costs.

Usage:
    python scripts/migrate_caches.py
    python scripts/migrate_caches.py --dry-run
    python scripts/migrate_caches.py --tickers AAPL MSFT
"""

import argparse
import json
import logging
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_opportunity_index.config import EXTRACTED_DIR, RAW_DIR, SOURCES_DIR
from ai_opportunity_index.domains import (
    CollectedItem,
    ExtractedItem,
    ExtractedPassage,
    SourceAuthority,
    SourceType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _write_item(base_dir: Path, ticker: str, source_type: str, d: date | None, payload: str) -> Path:
    """Write a JSON payload to the file-per-item layout."""
    if d:
        target_dir = base_dir / ticker.upper() / source_type / str(d.year) / f"{d.month:02d}"
    else:
        target_dir = base_dir / ticker.upper() / source_type / "undated"
    target_dir.mkdir(parents=True, exist_ok=True)
    filepath = target_dir / f"{uuid.uuid4()}.json"
    filepath.write_text(payload)
    return filepath


def migrate_news(ticker: str, dry_run: bool = False) -> int:
    """Migrate news cache to CollectedItem files."""
    cache_path = RAW_DIR / "news" / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return 0

    data = json.loads(cache_path.read_text())
    articles = data.get("articles", [])
    count = 0

    for a in articles:
        pub = a.get("published_at", "")
        source_date = None
        if pub:
            try:
                source_date = date.fromisoformat(pub[:10])
            except (ValueError, TypeError):
                pass

        source_name = a.get("source", "")
        item = CollectedItem(
            item_id=a.get("url") or f"{ticker}_{a.get('title', '')[:50]}",
            title=a.get("title", ""),
            content=f"{a.get('title', '')}\n{a.get('description', '')}".strip(),
            author=source_name or None,
            publisher=source_name or None,
            url=a.get("url"),
            source_date=source_date,
            access_date=date.today(),
            authority=SourceAuthority.THIRD_PARTY_JOURNALISM,
            metadata={"raw_title": a.get("title", ""), "description": a.get("description", "")},
        )

        if not dry_run:
            _write_item(SOURCES_DIR, ticker, SourceType.NEWS.value, source_date, item.model_dump_json(indent=2))
        count += 1

    return count


def migrate_github(ticker: str, dry_run: bool = False) -> int:
    """Migrate github cache to CollectedItem file."""
    cache_path = RAW_DIR / "github" / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return 0

    from ai_opportunity_index.data.github_signals import github_dict_to_collected_item

    data = json.loads(cache_path.read_text())
    item = github_dict_to_collected_item(data)

    if not dry_run:
        _write_item(SOURCES_DIR, ticker, SourceType.GITHUB.value, item.source_date, item.model_dump_json(indent=2))
    return 1


def migrate_analysts(ticker: str, dry_run: bool = False) -> int:
    """Migrate analyst cache to CollectedItem file."""
    cache_path = RAW_DIR / "analysts" / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return 0

    from ai_opportunity_index.data.analyst_data import analyst_dict_to_collected_item

    data = json.loads(cache_path.read_text())
    item = analyst_dict_to_collected_item(data)

    if not dry_run:
        _write_item(SOURCES_DIR, ticker, SourceType.ANALYST.value, item.source_date, item.model_dump_json(indent=2))
    return 1


def migrate_extracted_filings(ticker: str, dry_run: bool = False) -> int:
    """Migrate extracted filings to ExtractedItem files."""
    cache_path = RAW_DIR / "extracted_filings" / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return 0

    data = json.loads(cache_path.read_text())
    filings = data.get("filings", [])
    count = 0

    for filing in filings:
        filename = filing.get("filename", "")
        filing_date_str = filing.get("filing_date", "")
        passages = filing.get("passages", [])
        if not passages:
            continue

        source_date = None
        if filing_date_str:
            try:
                source_date = date.fromisoformat(filing_date_str[:10])
            except (ValueError, TypeError):
                pass

        item = ExtractedItem(
            item_id=filename or f"{ticker}_filing_{filing_date_str}",
            title=filename,
            author=data.get("company_name"),
            publisher="SEC EDGAR",
            source_date=source_date,
            authority=SourceAuthority.FIRST_PARTY_DISCLOSURE,
            passages=[
                ExtractedPassage(
                    passage_text=p.get("passage_text", ""),
                    target_dimension=p.get("target_dimension", "general"),
                    capture_stage=p.get("capture_stage", "invested"),
                    confidence=p.get("confidence", 0.0),
                    reasoning=p.get("reasoning", ""),
                )
                for p in passages
                if p.get("passage_text")
            ],
        )

        if not dry_run:
            _write_item(EXTRACTED_DIR, ticker, SourceType.FILING.value, source_date, item.model_dump_json(indent=2))
        count += 1

    return count


def migrate_extracted_news(ticker: str, dry_run: bool = False) -> int:
    """Migrate extracted news to ExtractedItem files."""
    cache_path = RAW_DIR / "extracted_news" / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return 0

    data = json.loads(cache_path.read_text())
    articles = data.get("articles", [])
    count = 0

    for article in articles:
        passages = article.get("passages", [])
        if not passages:
            continue

        pub = article.get("published_at", "")
        source_date = None
        if pub:
            try:
                source_date = date.fromisoformat(pub[:10])
            except (ValueError, TypeError):
                pass

        item = ExtractedItem(
            item_id=article.get("url") or article.get("title", "")[:50],
            title=article.get("title"),
            url=article.get("url"),
            publisher=article.get("source"),
            source_date=source_date,
            authority=SourceAuthority.THIRD_PARTY_JOURNALISM,
            passages=[
                ExtractedPassage(
                    passage_text=p.get("passage_text", ""),
                    target_dimension=p.get("target_dimension", "general"),
                    capture_stage=p.get("capture_stage", "invested"),
                    confidence=p.get("confidence", 0.0),
                    reasoning=p.get("reasoning", ""),
                )
                for p in passages
                if p.get("passage_text")
            ],
        )

        if not dry_run:
            _write_item(EXTRACTED_DIR, ticker, SourceType.NEWS.value, source_date, item.model_dump_json(indent=2))
        count += 1

    return count


def get_all_tickers() -> list[str]:
    """Get all tickers from existing cache directories."""
    tickers = set()
    for cache_dir in ["news", "github", "analysts", "extracted_filings", "extracted_news"]:
        d = RAW_DIR / cache_dir
        if d.exists():
            for p in d.glob("*.json"):
                tickers.add(p.stem)
    return sorted(tickers)


def main():
    parser = argparse.ArgumentParser(description="Migrate caches to unified file-per-item layout")
    parser.add_argument("--dry-run", action="store_true", help="Count items without writing")
    parser.add_argument("--tickers", nargs="*", help="Specific tickers to migrate")
    args = parser.parse_args()

    tickers = [t.upper() for t in args.tickers] if args.tickers else get_all_tickers()
    logger.info("Migrating %d tickers%s", len(tickers), " (dry run)" if args.dry_run else "")

    totals = {
        "news": 0,
        "github": 0,
        "analysts": 0,
        "extracted_filings": 0,
        "extracted_news": 0,
    }

    for ticker in tickers:
        totals["news"] += migrate_news(ticker, args.dry_run)
        totals["github"] += migrate_github(ticker, args.dry_run)
        totals["analysts"] += migrate_analysts(ticker, args.dry_run)
        totals["extracted_filings"] += migrate_extracted_filings(ticker, args.dry_run)
        totals["extracted_news"] += migrate_extracted_news(ticker, args.dry_run)

    logger.info("Migration complete:")
    for source, count in totals.items():
        logger.info("  %s: %d items", source, count)


if __name__ == "__main__":
    main()
