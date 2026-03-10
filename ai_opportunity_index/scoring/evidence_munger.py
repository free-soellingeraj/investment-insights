"""Stage 1: Evidence Munger — group extracted passages into evidence groups.

Reads extraction caches (unified + legacy) and groups related passages by
target_dimension + fuzzy text similarity. Pure Python, no LLM calls.

Output: EvidenceGroup domain objects ready for valuation.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path

from ai_opportunity_index.config import EXTRACTED_DIR, RAW_DIR
from ai_opportunity_index.domains import (
    CaptureStage,
    EvidenceGroup,
    EvidenceGroupPassage,
    ExtractedItem,
    SourceType,
    TargetDimension,
)

logger = logging.getLogger(__name__)


# ── Enum normalization helpers ────────────────────────────────────────────
# LLMs sometimes return non-standard casing or values for target_dimension
# and capture_stage (e.g. "Cost", "REVENUE", "investing").  Normalise to
# valid enum members or fall back to a safe default so that Pydantic
# validation never rejects a passage.

_TD_LOOKUP: dict[str, TargetDimension] = {
    v.value: v for v in TargetDimension
}
_CS_LOOKUP: dict[str, CaptureStage] = {
    v.value: v for v in CaptureStage
}


def _normalise_target_dimension(raw: str | TargetDimension | None) -> TargetDimension:
    """Normalise a raw target_dimension value to a valid TargetDimension enum."""
    if isinstance(raw, TargetDimension):
        return raw
    if raw is None:
        return TargetDimension.GENERAL
    return _TD_LOOKUP.get(str(raw).lower().strip(), TargetDimension.GENERAL)


def _normalise_capture_stage(raw: str | CaptureStage | None) -> CaptureStage:
    """Normalise a raw capture_stage value to a valid CaptureStage enum."""
    if isinstance(raw, CaptureStage):
        return raw
    if raw is None:
        return CaptureStage.INVESTED
    return _CS_LOOKUP.get(str(raw).lower().strip(), CaptureStage.INVESTED)

# Legacy paths (backward compat)
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


# ── Unified loader (file-per-item layout) ─────────────────────────────────


def _load_passages(
    ticker: str,
    source_types: list[SourceType] | None = None,
) -> list[EvidenceGroupPassage]:
    """Load passages from all (or specified) extracted item files.

    Reads from the unified extracted/{TICKER}/{source_type}/**/*.json layout.
    """
    if source_types is None:
        source_types = list(SourceType)

    passages = []
    for st in source_types:
        type_dir = EXTRACTED_DIR / ticker.upper() / st.value
        if not type_dir.is_dir():
            continue
        for item_path in type_dir.rglob("*.json"):
            if item_path.name.startswith("_"):
                continue
            try:
                item = ExtractedItem.model_validate_json(item_path.read_text())
            except Exception:
                logger.debug("Failed to parse extracted item %s", item_path)
                continue
            for p in item.passages:
                text = p.passage_text
                if not text or len(text) < 20:
                    continue
                passages.append(EvidenceGroupPassage(
                    passage_text=text,
                    source_type=st.value,
                    source_filename=item.title,
                    source_date=item.source_date,
                    confidence=max(0.0, min(1.0, p.confidence)),
                    reasoning=p.reasoning,
                    target_dimension=_normalise_target_dimension(p.target_dimension),
                    capture_stage=_normalise_capture_stage(p.capture_stage),
                    source_url=item.url,
                    source_author=item.author,
                    source_author_role=item.author_role,
                    source_author_affiliation=item.author_affiliation,
                    source_publisher=item.publisher,
                    source_access_date=item.access_date,
                    source_authority=item.authority,
                ))
    return passages


# ── Legacy loaders (backward compat) ─────────────────────────────────────


_cik_cache: dict[str, int | None] = {}


def _lookup_cik(ticker: str) -> int | None:
    """Look up CIK for a ticker from the database, with caching."""
    upper = ticker.upper()
    if upper in _cik_cache:
        return _cik_cache[upper]
    try:
        from ai_opportunity_index.storage.db import get_session
        session = get_session()
        try:
            from ai_opportunity_index.storage.models import CompanyModel
            row = session.query(CompanyModel.cik).filter(CompanyModel.ticker == upper).first()
            cik = row[0] if row else None
        finally:
            session.close()
    except Exception:
        cik = None
    _cik_cache[upper] = cik
    return cik


def _load_filing_passages(ticker: str) -> list[EvidenceGroupPassage]:
    """Load passages from extracted filings cache (legacy layout)."""
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
        # Build SEC EDGAR URL from filing metadata
        filing_url = filing.get("url")
        if not filing_url:
            cik = filing.get("cik") or data.get("cik")
            accession = filing.get("accession_number") or filing.get("accession")
            if cik and accession:
                acc_clean = accession.replace("-", "")
                filing_url = (
                    f"https://www.sec.gov/Archives/edgar/data/{cik}/"
                    f"{acc_clean}/{filename}"
                )
        # Fallback: look up CIK from database and build EDGAR viewer URL
        if not filing_url:
            cik = _lookup_cik(ticker)
            if cik:
                # Extract form type from filename (e.g., "10-K_2024-11-01.txt" -> "10-K")
                form_type = filename.split("_")[0] if "_" in filename else "10-K"
                filing_url = (
                    f"https://www.sec.gov/cgi-bin/browse-edgar"
                    f"?action=getcompany&CIK={cik:010d}&type={form_type}&dateb=&owner=include&count=10"
                )

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
                target_dimension=_normalise_target_dimension(p.get("target_dimension", "general")),
                capture_stage=_normalise_capture_stage(p.get("capture_stage", "invested")),
                source_url=filing_url,
                source_publisher="SEC EDGAR",
                source_authority="first_party_disclosure",
            ))

    return passages


def _load_news_passages(ticker: str) -> list[EvidenceGroupPassage]:
    """Load passages from extracted news cache (legacy layout)."""
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
                target_dimension=_normalise_target_dimension(p.get("target_dimension", "general")),
                capture_stage=_normalise_capture_stage(p.get("capture_stage", "invested")),
                source_url=article_url or None,
                source_author=article_source or None,
                source_publisher=article_source or None,
                source_authority="third_party_journalism",
            ))

    return passages


# ── Grouping ─────────────────────────────────────────────────────────────


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


def _get_child_tickers(company_id: int) -> list[str]:
    """Look up child share-class tickers for a company from the DB."""
    try:
        from ai_opportunity_index.storage.db import get_session
        from ai_opportunity_index.storage.models import CompanyModel
        session = get_session()
        try:
            company = session.get(CompanyModel, company_id)
            if not company or not company.child_ticker_refs:
                return []
            tickers = []
            for child_id in company.child_ticker_refs:
                child = session.get(CompanyModel, child_id)
                if child:
                    tickers.append(child.ticker)
            return tickers
        finally:
            session.close()
    except Exception:
        logger.debug("[%s] Could not look up child tickers", company_id)
        return []


def munge_evidence(
    ticker: str,
    company_id: int,
    pipeline_run_id: int | None = None,
) -> list[EvidenceGroup]:
    """Stage 1: Load extraction caches and group passages into evidence groups.

    Groups passages by (target_dimension) + text similarity.
    Returns EvidenceGroup domain objects (without DB ids — caller saves them).

    Loads from both unified (extracted/) and legacy (extracted_filings/, extracted_news/)
    layouts, deduplicating by passage text.

    Automatically includes extraction caches from child share-class tickers
    (e.g. GOOG for GOOGL) if the company has child_ticker_refs set.
    """
    # Collect all tickers to load caches from (parent + children)
    all_tickers = [ticker]
    child_tickers = _get_child_tickers(company_id)
    all_tickers.extend(child_tickers)
    if child_tickers:
        logger.info("[%s] Including child ticker caches: %s", ticker, child_tickers)

    # Load all passages from both layouts across all tickers
    all_passages: list[EvidenceGroupPassage] = []
    seen_texts: set[str] = set()

    for t in all_tickers:
        # Unified layout (all source types)
        unified_passages = _load_passages(t)
        for p in unified_passages:
            key = p.passage_text[:200]
            if key not in seen_texts:
                seen_texts.add(key)
                all_passages.append(p)

        # Legacy layout (filing + news only)
        for p in _load_filing_passages(t):
            key = p.passage_text[:200]
            if key not in seen_texts:
                seen_texts.add(key)
                all_passages.append(p)
        for p in _load_news_passages(t):
            key = p.passage_text[:200]
            if key not in seen_texts:
                seen_texts.add(key)
                all_passages.append(p)

    if not all_passages:
        logger.info("[%s] No extracted passages found for munging", ticker)
        return []

    # Count by source type for logging
    type_counts: dict[str, int] = {}
    for p in all_passages:
        st = p.source_type or "unknown"
        type_counts[st] = type_counts.get(st, 0) + 1

    logger.info(
        "[%s] Munging %d passages (%s)",
        ticker, len(all_passages),
        ", ".join(f"{k}={v}" for k, v in sorted(type_counts.items())),
    )

    # Split by target_dimension first, then cluster within each dimension
    by_dimension: dict[TargetDimension, list[EvidenceGroupPassage]] = {}
    for p in all_passages:
        dim = _normalise_target_dimension(p.target_dimension)
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
