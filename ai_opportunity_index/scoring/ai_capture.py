"""Dimension 2: AI Capture Score — 4-value framework aggregation.

Replaces ai_realization.py as the aggregation layer. Aggregates
ClassifiedScorerOutput from each sub-scorer into cost_capture,
revenue_capture, and general_investment scores.
"""

import logging

from ai_opportunity_index.config import CAPTURE_WEIGHTS, DISCREPANCY_THRESHOLD
from ai_opportunity_index.scoring.evidence_classification import (
    ClassifiedScorerOutput,
    TargetDimension,
)

logger = logging.getLogger(__name__)


def compute_capture_scores(
    filing: ClassifiedScorerOutput | None = None,
    product: ClassifiedScorerOutput | None = None,
    web: ClassifiedScorerOutput | None = None,
    github: ClassifiedScorerOutput | None = None,
    analyst: ClassifiedScorerOutput | None = None,
) -> dict:
    """Compute cost_capture, revenue_capture, and general_investment from classified sub-scorers.

    Aggregates ONLY cost-targeted evidence into cost_capture,
    ONLY revenue-targeted into revenue_capture, ONLY general into general_investment.

    Also returns legacy composite_realization for backward compat.
    """
    scorers = {
        "filing_nlp": filing,
        "product_analysis": product,
        "web_enrichment": web,
        "github": github,
        "analyst": analyst,
    }

    # Weighted aggregation per dimension
    cost_weighted = 0.0
    cost_total_weight = 0.0
    revenue_weighted = 0.0
    revenue_total_weight = 0.0
    general_weighted = 0.0
    general_total_weight = 0.0
    overall_weighted = 0.0
    overall_total_weight = 0.0

    for key, scorer in scorers.items():
        if scorer is None:
            continue
        weight = CAPTURE_WEIGHTS.get(key, 0.25)

        cost_weighted += weight * scorer.cost_capture_score
        cost_total_weight += weight

        revenue_weighted += weight * scorer.revenue_capture_score
        revenue_total_weight += weight

        general_weighted += weight * scorer.general_investment_score
        general_total_weight += weight

        overall_weighted += weight * scorer.overall_score
        overall_total_weight += weight

    cost_capture = cost_weighted / cost_total_weight if cost_total_weight > 0 else 0.0
    revenue_capture = revenue_weighted / revenue_total_weight if revenue_total_weight > 0 else 0.0
    general_investment = general_weighted / general_total_weight if general_total_weight > 0 else 0.0
    composite_realization = overall_weighted / overall_total_weight if overall_total_weight > 0 else 0.0

    return {
        "cost_capture": round(cost_capture, 4),
        "revenue_capture": round(revenue_capture, 4),
        "general_investment": round(general_investment, 4),
        # Legacy fields
        "filing_nlp_score": filing.overall_score if filing else None,
        "product_score": product.overall_score if product else None,
        "web_score": web.overall_score if web else None,
        "github_score": github.overall_score if github else None,
        "analyst_score": analyst.overall_score if analyst else None,
        "composite_realization": round(composite_realization, 4),
    }


def flag_capture_discrepancies(
    cost_opportunity: float,
    revenue_opportunity: float,
    cost_capture: float,
    revenue_capture: float,
    general_investment: float,
    threshold: float = DISCREPANCY_THRESHOLD,
) -> list[str]:
    """Identify discrepancies between opportunity and capture dimensions.

    Returns list of human-readable flags.
    """
    flags = []

    if cost_opportunity - cost_capture > threshold:
        flags.append("Significant AI cost savings potential unrealized")

    if revenue_opportunity - revenue_capture > threshold:
        flags.append("Significant AI revenue opportunity untapped")

    if revenue_capture > 0 and revenue_capture - revenue_opportunity > threshold:
        flags.append("AI revenue overweighted vs industry baseline")

    if cost_capture > 0 and cost_capture - cost_opportunity > threshold:
        flags.append("AI cost investment exceeds estimated opportunity")

    if general_investment > threshold and (cost_capture + revenue_capture) < threshold:
        flags.append("AI investment not translating to targeted outcomes")

    return flags
