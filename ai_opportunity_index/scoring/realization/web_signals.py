"""Sub-scorer E: Web enrichment signals (careers, IR, blog).

Reads pre-collected web enrichment data from local cache
(data/raw/web_enrichment/{TICKER}.json). Collection is handled by
scripts/collect_evidence.py --sources web.

Returns ClassifiedScorerOutput with cost/revenue/general classification.
"""

import json
import logging

from ai_opportunity_index.config import RAW_DIR
from ai_opportunity_index.scoring.evidence_classification import (
    CaptureStage,
    ClassifiedEvidence,
    ClassifiedScorerOutput,
    TargetDimension,
)

logger = logging.getLogger(__name__)

WEB_ENRICHMENT_CACHE_DIR = RAW_DIR / "web_enrichment"

_TARGET_MAP = {
    "cost": TargetDimension.COST,
    "revenue": TargetDimension.REVENUE,
    "general": TargetDimension.GENERAL,
}

_STAGE_MAP = {
    "planned": CaptureStage.PLANNED,
    "invested": CaptureStage.INVESTED,
    "realized": CaptureStage.REALIZED,
}


def score_web_enrichment_classified(
    company_name: str,
    ticker: str,
) -> ClassifiedScorerOutput | None:
    """Score web enrichment evidence for a company.

    Reads data/raw/web_enrichment/{TICKER}.json, aggregates evidence items
    across all 3 page types (careers, IR, blog), and returns a
    ClassifiedScorerOutput.
    """
    cache_file = WEB_ENRICHMENT_CACHE_DIR / f"{ticker.upper()}.json"
    if not cache_file.exists():
        logger.debug("No web enrichment cache for %s", ticker)
        return None

    try:
        data = json.loads(cache_file.read_text())
    except Exception as e:
        logger.warning("Failed to read web enrichment cache for %s: %s", ticker, e)
        return None

    evidence_items: list[ClassifiedEvidence] = []
    page_types = ["careers", "investor_relations", "blog"]

    for page_type in page_types:
        section = data.get(page_type)
        if not section:
            continue

        items = section.get("evidence_items", [])
        for item in items:
            target_str = item.get("target_dimension", "general")
            stage_str = item.get("capture_stage", "invested")
            confidence = item.get("confidence", 0.5)

            target = _TARGET_MAP.get(target_str, TargetDimension.GENERAL)
            stage = _STAGE_MAP.get(stage_str, CaptureStage.INVESTED)

            evidence_items.append(ClassifiedEvidence(
                source_type="web_enrichment",
                target=target,
                stage=stage,
                raw_score=confidence,
                weight=1.0,
                description=item.get("reasoning", ""),
                source_excerpt=item.get("passage_text", ""),
                metadata={
                    "page_type": page_type,
                    "url": section.get("url", ""),
                },
            ))

    if not evidence_items:
        return None

    # Compute dimension scores: average confidence of evidence in each dimension
    cost_scores = [e.raw_score for e in evidence_items if e.target == TargetDimension.COST]
    revenue_scores = [e.raw_score for e in evidence_items if e.target == TargetDimension.REVENUE]
    general_scores = [e.raw_score for e in evidence_items if e.target == TargetDimension.GENERAL]

    cost_capture = _compute_dimension_score(cost_scores)
    revenue_capture = _compute_dimension_score(revenue_scores)
    general_investment = _compute_dimension_score(general_scores)

    # Overall is a weighted blend of all dimensions
    all_scores = [e.raw_score for e in evidence_items]
    overall = _compute_dimension_score(all_scores)

    return ClassifiedScorerOutput(
        overall_score=overall,
        cost_capture_score=cost_capture,
        revenue_capture_score=revenue_capture,
        general_investment_score=general_investment,
        evidence_items=evidence_items,
        raw_details={
            "num_evidence_items": len(evidence_items),
            "num_cost": len(cost_scores),
            "num_revenue": len(revenue_scores),
            "num_general": len(general_scores),
            "pages_with_data": [pt for pt in page_types if data.get(pt)],
        },
    )


def _compute_dimension_score(scores: list[float]) -> float:
    """Compute a 0-1 score from a list of confidence values.

    Uses count-boosted average: more evidence items increase the score,
    capped at 1.0.
    """
    if not scores:
        return 0.0
    avg = sum(scores) / len(scores)
    # Boost for having multiple items (diminishing returns)
    count_factor = min(len(scores) / 5.0, 1.0)  # 5+ items = full boost
    return min(avg * (0.5 + 0.5 * count_factor), 1.0)
