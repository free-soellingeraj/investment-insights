#!/usr/bin/env python3
"""Collect evidence data from external APIs and cache locally.

This script handles ALL external API calls. The scoring pipeline should
never contact external services — it only reads locally cached data.

Evidence sources:
- News articles (Google News RSS, GNews API, SEC EDGAR EFTS)
- Patent data (USPTO PatentsView API)
- Job postings (Adzuna API)
- GitHub organization signals
- Analyst consensus data (Yahoo Finance)
- Web enrichment (careers, IR, blog pages)

Collected data is stored in two layouts:
1. Legacy: data/raw/{source_type}/{TICKER}.json (backward compat)
2. Unified: data/raw/sources/{TICKER}/{source_type}/{YYYY}/{MM}/{uuid}.json
"""

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_opportunity_index.cache import cache_is_fresh, stamp_cache
from ai_opportunity_index.config import (
    GITHUB_RATE_LIMIT_SECONDS,
    PATENTSVIEW_API_KEY,
    PRODUCT_NEWS_LOOKBACK_DAYS,
    RAW_DIR,
    SOURCES_DIR,
)
from ai_opportunity_index.domains import (
    CollectedItem,
    CollectionManifest,
    PipelineRun,
    PipelineSubtask,
    PipelineTask,
    SourceType,
)
from ai_opportunity_index.storage.db import (
    complete_pipeline_run,
    create_pipeline_run,
    get_session,
    init_db,
)
from ai_opportunity_index.storage.models import CompanyModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

NEWS_CACHE_DIR = RAW_DIR / "news"
PATENT_CACHE_DIR = RAW_DIR / "patents"
JOB_CACHE_DIR = RAW_DIR / "jobs"
GITHUB_CACHE_DIR = RAW_DIR / "github"
ANALYST_CACHE_DIR = RAW_DIR / "analysts"
WEB_ENRICHMENT_CACHE_DIR = RAW_DIR / "web_enrichment"


# ── Unified file-per-item helpers ─────────────────────────────────────────


def _item_dir(ticker: str, source_type: SourceType, item: CollectedItem) -> Path:
    """Compute the directory path for a CollectedItem."""
    d = item.source_date or item.access_date
    if d:
        return SOURCES_DIR / ticker.upper() / source_type.value / str(d.year) / f"{d.month:02d}"
    return SOURCES_DIR / ticker.upper() / source_type.value / "undated"


def _existing_item_ids(directory: Path) -> set[str]:
    """Read item_ids from existing JSON files in a directory."""
    ids: set[str] = set()
    if not directory.exists():
        return ids
    for p in directory.glob("*.json"):
        if p.name.startswith("_"):
            continue
        try:
            data = json.loads(p.read_text())
            if "item_id" in data:
                ids.add(data["item_id"])
        except Exception:
            pass
    return ids


def write_collected_items(
    ticker: str,
    source_type: SourceType,
    items: list[CollectedItem],
    company_name: str | None = None,
    since_date: datetime | None = None,
) -> int:
    """Write CollectedItem objects to the unified file layout.

    Deduplicates by item_id against existing files in the target directories.
    Returns count of new items written.
    """
    if not items:
        return 0

    # Collect all unique target directories
    dir_cache: dict[str, set[str]] = {}
    written = 0
    new_item_ids: list[str] = []

    for item in items:
        target_dir = _item_dir(ticker, source_type, item)
        dir_key = str(target_dir)
        if dir_key not in dir_cache:
            dir_cache[dir_key] = _existing_item_ids(target_dir)

        if item.item_id in dir_cache[dir_key]:
            continue

        target_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{uuid.uuid4()}.json"
        filepath = target_dir / filename
        filepath.write_text(item.model_dump_json(indent=2))
        dir_cache[dir_key].add(item.item_id)
        new_item_ids.append(item.item_id)
        written += 1

    # Write manifest
    if written > 0:
        # Pick the most common directory for the manifest
        manifest_dir = SOURCES_DIR / ticker.upper() / source_type.value
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest = CollectionManifest(
            ticker=ticker,
            company_name=company_name,
            source_type=source_type,
            collected_at=datetime.utcnow(),
            since_date=since_date,
            items_found=written,
            item_ids=new_item_ids,
        )
        manifest_path = manifest_dir / "_manifest.json"
        manifest_path.write_text(manifest.model_dump_json(indent=2))

    return written


# ── Collectors (write to both legacy + unified layout) ────────────────────


def collect_news(
    companies: list[CompanyModel],
    days_back: int = PRODUCT_NEWS_LOOKBACK_DAYS,
    news_api_key: str | None = None,
    force: bool = False,
    since_date: datetime | None = None,
):
    """Fetch and cache news articles for each company."""
    from ai_opportunity_index.data.news_signals import search_company_news

    NEWS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    collected = 0
    skipped = 0
    failed = 0

    for i, company in enumerate(companies):
        ticker = company.ticker or company.slug
        cache_path = NEWS_CACHE_DIR / f"{ticker.upper()}.json"

        if not force and cache_is_fresh(cache_path, "collect_news", force=force):
            skipped += 1
            continue

        name = company.company_name or ticker
        try:
            items = search_company_news(
                name, ticker, days_back=days_back, api_key=news_api_key,
                since_date=since_date,
            )
            # Write unified layout
            write_collected_items(ticker, SourceType.NEWS, items, company_name=name, since_date=since_date)

            # Legacy layout (backward compat)
            articles = [
                {
                    "title": item.title,
                    "description": item.metadata.get("description", ""),
                    "url": item.url,
                    "published_at": item.source_date.isoformat() if item.source_date else "",
                    "source": item.publisher or "",
                }
                for item in items
            ]
            data = stamp_cache({
                "ticker": ticker,
                "company_name": name,
                "collected_at": datetime.utcnow().isoformat(),
                "days_back": days_back,
                "article_count": len(articles),
                "articles": articles,
            }, "collect_news")
            cache_path.write_text(json.dumps(data, indent=2))
            collected += 1
        except Exception as e:
            logger.warning("News collection failed for %s: %s", ticker, e)
            failed += 1

        if (i + 1) % 100 == 0:
            logger.info(
                "News progress: %d/%d (collected: %d, skipped: %d, failed: %d)",
                i + 1, len(companies), collected, skipped, failed,
            )

    logger.info(
        "News collection complete: %d collected, %d skipped, %d failed",
        collected, skipped, failed,
    )


def collect_patents(
    companies: list[CompanyModel],
    years_back: int = 5,
    force: bool = False,
):
    """Fetch and cache patent data for each company."""
    import re
    from datetime import timedelta

    import requests

    if not PATENTSVIEW_API_KEY:
        logger.warning("PATENTSVIEW_API_KEY not set; skipping patent collection")
        return

    PATENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    PATENTSVIEW_API_URL = "https://search.patentsview.org/api/v1/patent/"
    cutoff_date = (datetime.now() - timedelta(days=years_back * 365)).strftime("%Y-%m-%d")
    headers = {"X-Api-Key": PATENTSVIEW_API_KEY}

    collected = 0
    skipped = 0
    failed = 0

    for i, company in enumerate(companies):
        ticker = company.ticker or company.slug
        cache_path = PATENT_CACHE_DIR / f"{ticker.upper()}.json"

        if cache_is_fresh(cache_path, "collect_news", force=force):
            skipped += 1
            continue

        name = company.company_name or ticker
        clean_name = re.sub(r"\b(inc|corp|ltd|llc|co|plc)\b\.?", "", name.lower()).strip()
        clean_name = re.sub(r"[^a-z0-9 ]", "", clean_name).strip()

        if not clean_name:
            skipped += 1
            continue

        query = {
            "_and": [
                {"_contains": {"assignees.assignee_organization": clean_name}},
                {"_gte": {"patent_date": cutoff_date}},
            ]
        }
        params = {
            "q": json.dumps(query),
            "f": json.dumps(["patent_id", "patent_title", "patent_date", "patent_abstract"]),
            "o": json.dumps({"size": 100}),
        }

        try:
            resp = requests.get(
                PATENTSVIEW_API_URL, params=params, headers=headers, timeout=30
            )
            if resp.status_code != 200:
                logger.debug("PatentsView returned %d for %s", resp.status_code, ticker)
                failed += 1
                continue

            data = resp.json()
            patents = data.get("patents", []) or []

            patent_data = stamp_cache({
                "ticker": ticker,
                "company_name": name,
                "collected_at": datetime.utcnow().isoformat(),
                "years_back": years_back,
                "total_hits": data.get("total_hits", len(patents)),
                "patent_count": len(patents),
                "patents": patents,
            }, "collect_news")
            cache_path.write_text(json.dumps(patent_data, indent=2))
            collected += 1

            # Rate limit
            time.sleep(0.2)

        except Exception as e:
            logger.warning("Patent collection failed for %s: %s", ticker, e)
            failed += 1

        if (i + 1) % 100 == 0:
            logger.info(
                "Patent progress: %d/%d (collected: %d, skipped: %d, failed: %d)",
                i + 1, len(companies), collected, skipped, failed,
            )

    logger.info(
        "Patent collection complete: %d collected, %d skipped, %d failed",
        collected, skipped, failed,
    )


def collect_jobs(
    companies: list[CompanyModel],
    adzuna_app_id: str | None = None,
    adzuna_api_key: str | None = None,
    force: bool = False,
):
    """Fetch and cache job posting data for each company."""
    import requests

    if not adzuna_app_id or not adzuna_api_key:
        logger.warning("Adzuna credentials not set; skipping job collection")
        return

    JOB_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    ADZUNA_API_URL = "https://api.adzuna.com/v1/api/jobs/us/search/1"

    collected = 0
    skipped = 0
    failed = 0

    for i, company in enumerate(companies):
        ticker = company.ticker or company.slug
        cache_path = JOB_CACHE_DIR / f"{ticker.upper()}.json"

        if cache_is_fresh(cache_path, "collect_news", force=force):
            skipped += 1
            continue

        name = company.company_name or ticker

        try:
            # Total jobs
            total_resp = requests.get(
                ADZUNA_API_URL,
                params={"app_id": adzuna_app_id, "app_key": adzuna_api_key,
                        "what": name, "results_per_page": 0},
                timeout=15,
            )
            total_resp.raise_for_status()
            total_count = total_resp.json().get("count", 0)

            # AI jobs
            ai_resp = requests.get(
                ADZUNA_API_URL,
                params={"app_id": adzuna_app_id, "app_key": adzuna_api_key,
                        "what": f"{name} machine learning OR artificial intelligence OR AI engineer",
                        "results_per_page": 0},
                timeout=15,
            )
            ai_resp.raise_for_status()
            ai_count = ai_resp.json().get("count", 0)

            job_data = stamp_cache({
                "ticker": ticker,
                "company_name": name,
                "collected_at": datetime.utcnow().isoformat(),
                "total_jobs": total_count,
                "ai_jobs": ai_count,
                "ratio": ai_count / total_count if total_count > 0 else 0.0,
                "source": "adzuna",
            }, "collect_news")
            cache_path.write_text(json.dumps(job_data, indent=2))
            collected += 1

            time.sleep(0.3)

        except Exception as e:
            logger.warning("Job collection failed for %s: %s", ticker, e)
            failed += 1

        if (i + 1) % 100 == 0:
            logger.info(
                "Job progress: %d/%d (collected: %d, skipped: %d, failed: %d)",
                i + 1, len(companies), collected, skipped, failed,
            )

    logger.info(
        "Job collection complete: %d collected, %d skipped, %d failed",
        collected, skipped, failed,
    )


def collect_github(
    companies: list[CompanyModel],
    force: bool = False,
    since_date: datetime | None = None,
):
    """Fetch and cache GitHub signals for each company."""
    from ai_opportunity_index.data.github_signals import search_company_github, github_dict_to_collected_item

    GITHUB_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    collected = 0
    skipped = 0
    failed = 0

    for i, company in enumerate(companies):
        ticker = company.ticker or company.slug
        cache_path = GITHUB_CACHE_DIR / f"{ticker.upper()}.json"

        if cache_is_fresh(cache_path, "collect_github", force=force):
            skipped += 1
            continue

        name = company.company_name or ticker
        try:
            data = search_company_github(name, ticker, since_date=since_date)
            stamp_cache(data, "collect_github")
            cache_path.write_text(json.dumps(data, indent=2))

            # Write unified layout
            item = github_dict_to_collected_item(data)
            write_collected_items(ticker, SourceType.GITHUB, [item], company_name=name, since_date=since_date)

            collected += 1
        except Exception as e:
            logger.warning("GitHub collection failed for %s: %s", ticker, e)
            failed += 1

        if (i + 1) % 100 == 0:
            logger.info(
                "GitHub progress: %d/%d (collected: %d, skipped: %d, failed: %d)",
                i + 1, len(companies), collected, skipped, failed,
            )

    logger.info(
        "GitHub collection complete: %d collected, %d skipped, %d failed",
        collected, skipped, failed,
    )


def collect_analysts(
    companies: list[CompanyModel],
    force: bool = False,
    since_date: datetime | None = None,
):
    """Fetch and cache analyst consensus data for each company."""
    from ai_opportunity_index.data.analyst_data import fetch_analyst_data, analyst_dict_to_collected_item

    ANALYST_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    collected = 0
    skipped = 0
    failed = 0

    for i, company in enumerate(companies):
        ticker = company.ticker or company.slug
        cache_path = ANALYST_CACHE_DIR / f"{ticker.upper()}.json"

        if cache_is_fresh(cache_path, "collect_analysts", force=force):
            skipped += 1
            continue

        try:
            data = fetch_analyst_data(ticker)
            stamp_cache(data, "collect_analysts")
            cache_path.write_text(json.dumps(data, indent=2))

            # Write unified layout
            item = analyst_dict_to_collected_item(data)
            write_collected_items(ticker, SourceType.ANALYST, [item], company_name=company.company_name, since_date=since_date)

            collected += 1
        except Exception as e:
            logger.warning("Analyst collection failed for %s: %s", ticker, e)
            failed += 1

        if (i + 1) % 100 == 0:
            logger.info(
                "Analyst progress: %d/%d (collected: %d, skipped: %d, failed: %d)",
                i + 1, len(companies), collected, skipped, failed,
            )

    logger.info(
        "Analyst collection complete: %d collected, %d skipped, %d failed",
        collected, skipped, failed,
    )


def collect_web_enrichment(
    companies: list[CompanyModel],
    force: bool = False,
):
    """Fetch and cache web enrichment data for each company.

    Uses requests + BeautifulSoup + Gemini Flash to scrape and extract
    AI signals from careers, IR, and blog pages.

    Reads URLs from the database (careers_url, ir_url, blog_url).
    Skips companies with no URLs.
    """
    from ai_opportunity_index.data.web_enrichment import (
        fetch_web_enrichment,
        web_page_to_collected_item,
    )

    WEB_ENRICHMENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    collected = 0
    skipped = 0
    failed = 0
    no_urls = 0

    for i, company in enumerate(companies):
        ticker = company.ticker or company.slug
        cache_path = WEB_ENRICHMENT_CACHE_DIR / f"{ticker.upper()}.json"

        if cache_is_fresh(cache_path, "collect_web_enrichment", force=force):
            skipped += 1
            continue

        # Read URLs from DB
        careers_url = company.careers_url
        ir_url = company.ir_url
        blog_url = company.blog_url

        # Skip companies with no URLs at all
        if not careers_url and not ir_url and not blog_url:
            no_urls += 1
            continue

        name = company.company_name or ticker
        try:
            data = fetch_web_enrichment(
                ticker, name,
                careers_url=careers_url,
                ir_url=ir_url,
                blog_url=blog_url,
            )
            stamp_cache(data, "collect_web_enrichment")
            cache_path.write_text(json.dumps(data, indent=2))

            # Write unified layout — one CollectedItem per page type
            page_map = {
                "careers": (careers_url, SourceType.WEB_CAREERS),
                "ir": (ir_url, SourceType.WEB_IR),
                "blog": (blog_url, SourceType.WEB_BLOG),
            }
            for page_type, (url, st) in page_map.items():
                if url:
                    item = web_page_to_collected_item(url, page_type, ticker, name)
                    if item:
                        write_collected_items(ticker, st, [item], company_name=name)

            collected += 1
        except Exception as e:
            logger.warning("Web enrichment collection failed for %s: %s", ticker, e)
            failed += 1

        if (i + 1) % 50 == 0:
            logger.info(
                "Web enrichment progress: %d/%d (collected: %d, skipped: %d, no_urls: %d, failed: %d)",
                i + 1, len(companies), collected, skipped, no_urls, failed,
            )

    logger.info(
        "Web enrichment collection complete: %d collected, %d skipped, %d no_urls, %d failed",
        collected, skipped, no_urls, failed,
    )


async def collect_filing_extraction(
    companies: list[CompanyModel],
    force: bool = False,
    semaphore=None,
):
    """Run LLM extraction on cached filings and store results.

    Reads data/raw/filings/{TICKER}/*.txt, extracts AI signals via Gemini Flash,
    and caches to data/raw/extracted_filings/{TICKER}.json.
    """
    from ai_opportunity_index.data.filing_extraction import extract_and_cache_filings

    collected = 0
    skipped = 0
    failed = 0

    for i, company in enumerate(companies):
        ticker = company.ticker or company.slug
        cache_path = RAW_DIR / "extracted_filings" / f"{ticker.upper()}.json"

        if cache_is_fresh(cache_path, "extract_filings", force=force):
            skipped += 1
            continue

        name = company.company_name or ticker
        try:
            await extract_and_cache_filings(
                ticker, company_name=name,
                sector=company.sector or "",
                force=force,
                semaphore=semaphore,
            )
            collected += 1
        except Exception as e:
            logger.warning("Filing extraction failed for %s: %s", ticker, e)
            failed += 1

        if (i + 1) % 50 == 0:
            logger.info(
                "Filing extraction progress: %d/%d (collected: %d, skipped: %d, failed: %d)",
                i + 1, len(companies), collected, skipped, failed,
            )

    logger.info(
        "Filing extraction complete: %d collected, %d skipped, %d failed",
        collected, skipped, failed,
    )


async def collect_news_extraction(
    companies: list[CompanyModel],
    force: bool = False,
    semaphore=None,
):
    """Run LLM extraction on cached news articles and store results.

    Reads data/raw/news/{TICKER}.json, extracts AI signals via Gemini Flash,
    and caches to data/raw/extracted_news/{TICKER}.json.
    """
    from ai_opportunity_index.data.news_extraction import extract_and_cache_news

    collected = 0
    skipped = 0
    failed = 0

    for i, company in enumerate(companies):
        ticker = company.ticker or company.slug
        cache_path = RAW_DIR / "extracted_news" / f"{ticker.upper()}.json"

        if cache_is_fresh(cache_path, "extract_news", force=force):
            skipped += 1
            continue

        name = company.company_name or ticker
        try:
            await extract_and_cache_news(
                ticker, company_name=name,
                force=force,
                semaphore=semaphore,
            )
            collected += 1
        except Exception as e:
            logger.warning("News extraction failed for %s: %s", ticker, e)
            failed += 1

        if (i + 1) % 50 == 0:
            logger.info(
                "News extraction progress: %d/%d (collected: %d, skipped: %d, failed: %d)",
                i + 1, len(companies), collected, skipped, failed,
            )

    logger.info(
        "News extraction complete: %d collected, %d skipped, %d failed",
        collected, skipped, failed,
    )


def main():
    parser = argparse.ArgumentParser(description="Collect evidence data from external APIs")
    parser.add_argument("--sources", nargs="*",
                        default=["news", "github", "analysts", "web"],
                        choices=["news", "github", "analysts", "web"],
                        help="Which evidence sources to collect")
    parser.add_argument("--tickers", nargs="*", help="Specific tickers to collect")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of companies")
    parser.add_argument("--force", action="store_true", help="Re-collect even if cache exists")
    parser.add_argument("--news-api-key", type=str, default=None, help="GNews API key")
    args = parser.parse_args()

    init_db()
    session = get_session()

    # Get companies
    query = session.query(CompanyModel)
    if args.tickers:
        query = query.filter(CompanyModel.ticker.in_([t.upper() for t in args.tickers]))
    if args.limit:
        query = query.limit(args.limit)
    companies = query.all()
    ticker_list = [c.ticker for c in companies]
    logger.info("Collecting evidence for %d companies", len(companies))

    run_type = "partial" if args.tickers else "full"

    # Map source names to subtask enums and collection functions
    source_map = {
        "news": (PipelineSubtask.NEWS, lambda: collect_news(
            companies, news_api_key=args.news_api_key, force=args.force)),
        "github": (PipelineSubtask.GITHUB, lambda: collect_github(
            companies, force=args.force)),
        "analysts": (PipelineSubtask.ANALYSTS, lambda: collect_analysts(
            companies, force=args.force)),
        "web": (PipelineSubtask.WEB_ENRICHMENT, lambda: collect_web_enrichment(
            companies, force=args.force)),
    }

    try:
        for source_name in args.sources:
            subtask_enum, collect_fn = source_map[source_name]
            run_id = str(uuid.uuid4())
            create_pipeline_run(PipelineRun(
                run_id=run_id,
                task=PipelineTask.COLLECT,
                subtask=subtask_enum,
                run_type=run_type,
                status="running",
                tickers_requested=ticker_list,
                parameters={"force": args.force},
            ))
            try:
                logger.info("=== Collecting %s ===", source_name)
                collect_fn()
                complete_pipeline_run(
                    run_id=run_id,
                    status="completed",
                    tickers_succeeded=len(companies),
                )
            except Exception as exc:
                complete_pipeline_run(
                    run_id=run_id,
                    status="failed",
                    error_message=str(exc),
                )
                raise

        logger.info("Evidence collection complete.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
