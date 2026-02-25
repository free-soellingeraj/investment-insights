"""Dimension 2: AI Realization Score — ensemble of sub-scorers.

Combines independent signals about how much a company is actually
capturing its AI opportunity. Delegates to ai_capture.py for
the 4-value framework; this module provides backward-compatible
interfaces.
"""

import logging

from ai_opportunity_index.config import DISCREPANCY_THRESHOLD, REALIZATION_WEIGHTS

logger = logging.getLogger(__name__)


def compute_realization_score(
    filing_nlp_score: float | None = None,
    product_score: float | None = None,
) -> dict:
    """Compute the composite AI Realization score from sub-scorer outputs.

    Each sub-score should be 0.0-1.0 (or None if unavailable).
    Missing sub-scores are excluded from the weighted average.

    Returns dict with individual scores and composite.
    """
    scores = {
        "filing_nlp": filing_nlp_score,
        "product_analysis": product_score,
    }

    # Compute weighted average, excluding None values
    weighted_sum = 0.0
    total_weight = 0.0

    for key, score in scores.items():
        if score is not None:
            weight = REALIZATION_WEIGHTS.get(key, 0.25)
            weighted_sum += weight * score
            total_weight += weight

    if total_weight > 0:
        composite = weighted_sum / total_weight
    else:
        composite = 0.0

    return {
        "filing_nlp_score": filing_nlp_score,
        "product_score": product_score,
        "composite_realization": round(composite, 4),
    }


def flag_discrepancies(scores: dict, threshold: float = DISCREPANCY_THRESHOLD) -> list[str]:
    """Identify discrepancies between sub-scorers.

    Returns list of human-readable flags.
    """
    flags = []
    filing = scores.get("filing_nlp_score")
    product = scores.get("product_score")

    if filing is not None and product is not None:
        if filing - product > threshold:
            flags.append("High AI discussion in filings but low product evidence — possible AI-washing")
        if product - filing > threshold:
            flags.append("Strong AI products but limited filing discussion — under-reported AI strategy")

    return flags
