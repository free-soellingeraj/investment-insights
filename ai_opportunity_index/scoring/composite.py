"""Combine opportunity + realization into the final AI Opportunity Index.

Supports both the legacy 2-value system and the new 4-value framework
(cost/revenue x opportunity/capture) with ROI metrics.
"""

import logging

import pandas as pd

from ai_opportunity_index.config import (
    LEGACY_COMPOSITE_COST_WEIGHT,
    LEGACY_COMPOSITE_REVENUE_WEIGHT,
    QUADRANT_LABELS,
    QUADRANT_OPP_THRESHOLD,
    QUADRANT_REAL_THRESHOLD,
    ROI_CAP,
    ROI_MIN_DENOMINATOR,
    ROI_WEIGHTS,
)

logger = logging.getLogger(__name__)


def compute_index(
    opportunity_score: float,
    realization_score: float,
    opp_threshold: float = QUADRANT_OPP_THRESHOLD,
    real_threshold: float = QUADRANT_REAL_THRESHOLD,
) -> dict:
    """Compute the final index value and quadrant assignment. Legacy interface.

    Args:
        opportunity_score: AI Opportunity composite score (0-1).
        realization_score: AI Realization composite score (0-1).
        opp_threshold: Threshold for high/low opportunity.
        real_threshold: Threshold for high/low realization.

    Returns dict with: opportunity, realization, quadrant, quadrant_label.
    """
    if opportunity_score >= opp_threshold:
        if realization_score >= real_threshold:
            quadrant = "high_opp_high_real"
        else:
            quadrant = "high_opp_low_real"
    else:
        if realization_score >= real_threshold:
            quadrant = "low_opp_high_real"
        else:
            quadrant = "low_opp_low_real"

    return {
        "opportunity": round(opportunity_score, 4),
        "realization": round(realization_score, 4),
        "quadrant": quadrant,
        "quadrant_label": QUADRANT_LABELS[quadrant],
    }


def compute_index_4v(
    cost_opportunity: float,
    revenue_opportunity: float,
    cost_capture: float,
    revenue_capture: float,
    general_investment: float,
    opp_threshold: float = QUADRANT_OPP_THRESHOLD,
    real_threshold: float = QUADRANT_REAL_THRESHOLD,
) -> dict:
    """Compute the 4-value index with ROI metrics.

    Args:
        cost_opportunity: AI cost-savings opportunity (0-1).
        revenue_opportunity: AI revenue opportunity (0-1).
        cost_capture: Realized/invested AI cost savings (0-1).
        revenue_capture: Realized/invested AI revenue gains (0-1).
        general_investment: Unspecified AI investment (0-1).
        opp_threshold: Threshold for high/low opportunity.
        real_threshold: Threshold for high/low realization.

    Returns dict with 4 values, ROI, and legacy compatibility fields.
    """
    # ROI metrics (general excluded from ROI, it's tracked separately)
    cost_roi = min(ROI_CAP, cost_capture / max(cost_opportunity, ROI_MIN_DENOMINATOR))
    revenue_roi = min(ROI_CAP, revenue_capture / max(revenue_opportunity, ROI_MIN_DENOMINATOR))
    combined_roi = (
        ROI_WEIGHTS["cost"] * cost_roi
        + ROI_WEIGHTS["revenue"] * revenue_roi
    )

    # Legacy composites for backward compat
    opportunity = LEGACY_COMPOSITE_COST_WEIGHT * cost_opportunity + LEGACY_COMPOSITE_REVENUE_WEIGHT * revenue_opportunity
    realization = LEGACY_COMPOSITE_COST_WEIGHT * cost_capture + LEGACY_COMPOSITE_REVENUE_WEIGHT * revenue_capture

    # Quadrant assignment (same as legacy)
    if opportunity >= opp_threshold:
        if realization >= real_threshold:
            quadrant = "high_opp_high_real"
        else:
            quadrant = "high_opp_low_real"
    else:
        if realization >= real_threshold:
            quadrant = "low_opp_high_real"
        else:
            quadrant = "low_opp_low_real"

    return {
        # 4-value scores
        "cost_opportunity": round(cost_opportunity, 4),
        "revenue_opportunity": round(revenue_opportunity, 4),
        "cost_capture": round(cost_capture, 4),
        "revenue_capture": round(revenue_capture, 4),
        "general_investment": round(general_investment, 4),
        # ROI metrics
        "cost_roi": round(cost_roi, 4),
        "revenue_roi": round(revenue_roi, 4),
        "combined_roi": round(combined_roi, 4),
        # Legacy compatibility
        "opportunity": round(opportunity, 4),
        "realization": round(realization, 4),
        "quadrant": quadrant,
        "quadrant_label": QUADRANT_LABELS[quadrant],
    }


def compute_index_bulk(df: pd.DataFrame) -> pd.DataFrame:
    """Compute index values for an entire DataFrame of companies.

    Expects columns: composite_opportunity, composite_realization.
    Adds columns: quadrant, quadrant_label.
    """
    # Use median as thresholds for relative positioning
    opp_median = df["composite_opportunity"].median()
    real_median = df["composite_realization"].median()

    results = []
    for _, row in df.iterrows():
        idx = compute_index(
            row["composite_opportunity"],
            row["composite_realization"],
            opp_threshold=opp_median,
            real_threshold=real_median,
        )
        results.append(idx)

    result_df = pd.DataFrame(results)
    return pd.concat([df.reset_index(drop=True), result_df], axis=1)


def rank_companies(df: pd.DataFrame) -> pd.DataFrame:
    """Rank companies within the index.

    Adds rank columns for opportunity, realization, and a combined rank.
    """
    df = df.copy()
    df["opportunity_rank"] = df["composite_opportunity"].rank(ascending=False, method="min").astype(int)
    df["realization_rank"] = df["composite_realization"].rank(ascending=False, method="min").astype(int)

    # Combined rank: average of the two ranks
    df["combined_rank"] = ((df["opportunity_rank"] + df["realization_rank"]) / 2).rank(method="min").astype(int)

    return df.sort_values("combined_rank")
