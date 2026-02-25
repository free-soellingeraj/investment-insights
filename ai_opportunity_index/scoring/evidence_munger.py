"""Stage 1: Evidence Munger — group extracted passages into evidence groups.

Reads extraction caches (filing + news) and groups related passages by
target_dimension + fuzzy text similarity. Pure Python, no LLM calls.

Output: EvidenceGroup domain objects ready for valuation.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path

from ai_opportunity_index.config import RAW_DIR
from ai_opportunity_index.domains import EvidenceGroup, EvidenceGroupPassage

logger = logging.getLogger(__name__)

EXTRACTED_FILINGS_DIR = RAW_DIR / "extracted_filings"
EXTRACTED_NEWS_DIR = RAW_DIR / "extracted_news"

# Passages with similarity above this threshold are grouped together
SIMILARITY_THRESHOLD = 0.55

# Maximum passages per group before splitting
MAX_GROUP_SIZE = 8


def _text_similarity(a: str, b: str) -> float:
    """Fast approximate text similarity using SequenceMatcher."""
    # Truncate to first 300 chars for speed
    return SequenceMatcher(None, a[:300].lower(), b[:300].lower()).ratio()


def _parse_date(s: str | None) -> date | None:
    """Parse a date string, returning None on failure."""
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


def _load_filing_passages(ticker: str) -> list[EvidenceGroupPassage]:
    """Load passages from extracted filings cache."""
    cache_path = EXTRACTED_FILINGS_DIR / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return []

    try:
        data = json.loads(cache_path.read_text())
    except Exception:
        return []

    passages = []
    for filing in data.get("filings", []):
        filename = filing.get("filename", "")
        # Try filing_date field first, then parse from filename
        filing_date = _parse_date(filing.get("filing_date"))
        if not filing_date and filename:
            import re as _re
            m = _re.search(r"(\d{4}-\d{2}-\d{2})", filename)
            if m:
                filing_date = _parse_date(m.group(1))
        for p in filing.get("passages", []):
            text = p.get("passage_text", "")
            if not text or len(text) < 20:
                continue
            passages.append(EvidenceGroupPassage(
                passage_text=text,
                source_type="filing",
                source_filename=filename,
                source_date=filing_date,
                confidence=max(0.0, min(1.0, p.get("confidence", 0.0))),
                reasoning=p.get("reasoning", ""),
                target_dimension=p.get("target_dimension", "general"),
                capture_stage=p.get("capture_stage", "invested"),
            ))

    return passages


def _load_news_passages(ticker: str) -> list[EvidenceGroupPassage]:
    """Load passages from extracted news cache."""
    cache_path = EXTRACTED_NEWS_DIR / f"{ticker.upper()}.json"
    if not cache_path.exists():
        return []

    try:
        data = json.loads(cache_path.read_text())
    except Exception:
        return []

    passages = []
    for article in data.get("articles", []):
        title = article.get("title", "")
        pub_date = _parse_date(
            article.get("published_at") or article.get("published_date") or article.get("date")
        )
        article_url = article.get("url", "") or ""
        article_source = article.get("source", "") or ""
        for p in article.get("passages", []):
            text = p.get("passage_text", "")
            if not text or len(text) < 20:
                continue
            passages.append(EvidenceGroupPassage(
                passage_text=text,
                source_type="news",
                source_filename=title[:255],
                source_date=pub_date,
                confidence=max(0.0, min(1.0, p.get("confidence", 0.0))),
                reasoning=p.get("reasoning", ""),
                target_dimension=p.get("target_dimension", "general"),
                capture_stage=p.get("capture_stage", "invested"),
                source_url=article_url or None,
                source_author=article_source or None,
            ))

    return passages


def _group_passages(
    passages: list[EvidenceGroupPassage],
) -> list[list[EvidenceGroupPassage]]:
    """Group passages by text similarity using greedy clustering."""
    if not passages:
        return []

    groups: list[list[EvidenceGroupPassage]] = []
    assigned = [False] * len(passages)

    for i, p in enumerate(passages):
        if assigned[i]:
            continue

        group = [p]
        assigned[i] = True

        for j in range(i + 1, len(passages)):
            if assigned[j]:
                continue
            if len(group) >= MAX_GROUP_SIZE:
                break

            # Check similarity against the group representative (first passage)
            sim = _text_similarity(p.passage_text, passages[j].passage_text)
            if sim >= SIMILARITY_THRESHOLD:
                group.append(passages[j])
                assigned[j] = True

        groups.append(group)

    return groups


def munge_evidence(
    ticker: str,
    company_id: int,
    pipeline_run_id: int | None = None,
) -> list[EvidenceGroup]:
    """Stage 1: Load extraction caches and group passages into evidence groups.

    Groups passages by (target_dimension) + text similarity.
    Returns EvidenceGroup domain objects (without DB ids — caller saves them).
    """
    # Load all passages from extraction caches
    filing_passages = _load_filing_passages(ticker)
    news_passages = _load_news_passages(ticker)
    all_passages = filing_passages + news_passages

    if not all_passages:
        logger.info("[%s] No extracted passages found for munging", ticker)
        return []

    logger.info(
        "[%s] Munging %d passages (%d filing, %d news)",
        ticker, len(all_passages), len(filing_passages), len(news_passages),
    )

    # Split by target_dimension first, then cluster within each dimension
    by_dimension: dict[str, list[EvidenceGroupPassage]] = {}
    for p in all_passages:
        dim = p.target_dimension or "general"
        by_dimension.setdefault(dim, []).append(p)

    evidence_groups: list[EvidenceGroup] = []

    for dimension, dim_passages in by_dimension.items():
        clustered = _group_passages(dim_passages)

        for cluster in clustered:
            dates = [p.source_date for p in cluster if p.source_date]
            confidences = [p.confidence for p in cluster if p.confidence is not None]
            source_types = list({p.source_type for p in cluster if p.source_type})

            # Pick the highest-confidence passage as representative
            representative = max(cluster, key=lambda p: p.confidence or 0.0)

            group = EvidenceGroup(
                company_id=company_id,
                pipeline_run_id=pipeline_run_id,
                target_dimension=dimension,
                passage_count=len(cluster),
                source_types=source_types,
                date_earliest=min(dates) if dates else None,
                date_latest=max(dates) if dates else None,
                mean_confidence=sum(confidences) / len(confidences) if confidences else None,
                max_confidence=max(confidences) if confidences else None,
                representative_text=representative.passage_text[:500],
                passages=cluster,
            )
            evidence_groups.append(group)

    logger.info(
        "[%s] Munged into %d evidence groups across %d dimensions",
        ticker, len(evidence_groups), len(by_dimension),
    )

    return evidence_groups
