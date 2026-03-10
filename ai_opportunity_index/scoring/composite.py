"""Combine opportunity + realization into the final AI Opportunity Index.

Supports both the legacy 2-value system and the new 4-value framework
(cost/revenue x opportunity/capture) with ROI metrics.

The AI Index computes an expected dollar value of future AI opportunity
capture: AI_Index = P(capture | evidence) × Dollar_potential.
"""

import logging
import math

import pandas as pd

from ai_opportunity_index.domains import Quadrant
from ai_opportunity_index.config import (
    AI_INDEX_P_BASE,
    AI_INDEX_SIGMOID_K,
    AI_INDEX_STAGE_WEIGHTS,
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
            quadrant = Quadrant.HIGH_OPP_HIGH_REAL
        else:
            quadrant = Quadrant.HIGH_OPP_LOW_REAL
    else:
        if realization_score >= real_threshold:
            quadrant = Quadrant.LOW_OPP_HIGH_REAL
        else:
            quadrant = Quadrant.LOW_OPP_LOW_REAL

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
            quadrant = Quadrant.HIGH_OPP_HIGH_REAL
        else:
            quadrant = Quadrant.HIGH_OPP_LOW_REAL
    else:
        if realization >= real_threshold:
            quadrant = Quadrant.LOW_OPP_HIGH_REAL
        else:
            quadrant = Quadrant.LOW_OPP_LOW_REAL

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


def compute_ai_index(
    plan_dollars: float,
    investment_dollars: float,
    capture_dollars: float,
    opportunity_usd: float | None = None,
) -> dict:
    """Compute the AI Index as expected value of future AI opportunity capture.

    The model treats realization evidence (plans, investments, captures) as
    increasing the probability that the company will achieve its full AI
    dollar potential.  More mature evidence (actual capture > investment > plan)
    contributes more to probability.

    Formula:
        weighted_progress = Σ(stage_weight_i × dollars_i) / total_dollars
        P(capture) = P_BASE + (1 - P_BASE) × sigmoid(weighted_progress)
        AI_Index   = P(capture) × opportunity_usd   (when available)
                   = P(capture) × total_evidence_dollars  (fallback)

    Args:
        plan_dollars: Sum of dollar_mid for plan-type valuations.
        investment_dollars: Sum of dollar_mid for investment-type valuations.
        capture_dollars: Sum of dollar_mid for capture-type valuations.
        opportunity_usd: Structural AI opportunity in dollars (cost_opp_usd +
            revenue_opp_usd). When provided, used as the dollar base instead
            of total evidence dollars.

    Returns dict with: ai_index_usd, capture_probability, dollar_potential,
        opportunity_usd, evidence_dollars, plan_dollars, investment_dollars,
        capture_dollars.
    """
    evidence_total = plan_dollars + investment_dollars + capture_dollars

    if evidence_total <= 0 and not opportunity_usd:
        return {
            "ai_index_usd": 0.0,
            "capture_probability": AI_INDEX_P_BASE,
            "dollar_potential": 0.0,
            "opportunity_usd": opportunity_usd or 0.0,
            "evidence_dollars": 0.0,
            "plan_dollars": 0.0,
            "investment_dollars": 0.0,
            "capture_dollars": 0.0,
        }

    # Weighted progress: how mature is the evidence?
    if evidence_total > 0:
        weighted = (
            AI_INDEX_STAGE_WEIGHTS["plan"] * plan_dollars
            + AI_INDEX_STAGE_WEIGHTS["investment"] * investment_dollars
            + AI_INDEX_STAGE_WEIGHTS["capture"] * capture_dollars
        ) / evidence_total
    else:
        weighted = 0.0

    # Map to probability via logistic sigmoid centered at 0.35
    # (midpoint chosen so that pure-plan evidence ≈ P_BASE, pure-capture ≈ 0.85)
    midpoint = 0.35
    raw_sigmoid = 1.0 / (1.0 + math.exp(-AI_INDEX_SIGMOID_K * (weighted - midpoint)))

    p_capture = AI_INDEX_P_BASE + (1.0 - AI_INDEX_P_BASE) * raw_sigmoid

    # Dollar base: prefer structural opportunity when available
    dollar_base = opportunity_usd if opportunity_usd is not None else evidence_total
    ai_index = p_capture * dollar_base

    return {
        "ai_index_usd": round(ai_index, 2),
        "capture_probability": round(p_capture, 4),
        "dollar_potential": round(dollar_base, 2),
        "opportunity_usd": round(opportunity_usd, 2) if opportunity_usd is not None else None,
        "evidence_dollars": round(evidence_total, 2),
        "plan_dollars": round(plan_dollars, 2),
        "investment_dollars": round(investment_dollars, 2),
        "capture_dollars": round(capture_dollars, 2),
    }


def compute_subsidiary_attribution(
    parent_opportunity: float,
    parent_realization: float,
    subsidiaries: list[dict],
) -> dict:
    """Add weighted subsidiary scores as attribution to a parent company.

    Each subsidiary dict should have: ownership_pct, opportunity, realization.
    Returns adjusted opportunity/realization with subsidiary bonus and breakdown.
    """
    sub_opp_bonus = 0.0
    sub_real_bonus = 0.0
    breakdown = []
    for sub in subsidiaries:
        pct = sub.get("ownership_pct") or 0.0
        opp = sub.get("opportunity")
        real = sub.get("realization")
        if opp is None and real is None:
            continue
        weighted_opp = (opp or 0.0) * pct
        weighted_real = (real or 0.0) * pct
        sub_opp_bonus += weighted_opp
        sub_real_bonus += weighted_real
        breakdown.append({
            "company_name": sub.get("company_name", ""),
            "slug": sub.get("slug", ""),
            "ownership_pct": pct,
            "opportunity": opp,
            "realization": real,
            "weighted_opportunity": round(weighted_opp, 4),
            "weighted_realization": round(weighted_real, 4),
        })

    # Blend: parent gets up to 20% boost from subsidiaries (capped)
    max_boost = 0.2
    opp_boost = min(sub_opp_bonus, max_boost)
    real_boost = min(sub_real_bonus, max_boost)
    adjusted_opportunity = min(1.0, parent_opportunity + opp_boost)
    adjusted_realization = min(1.0, parent_realization + real_boost)

    return {
        "adjusted_opportunity": round(adjusted_opportunity, 4),
        "adjusted_realization": round(adjusted_realization, 4),
        "opportunity_boost": round(opp_boost, 4),
        "realization_boost": round(real_boost, 4),
        "subsidiary_breakdown": breakdown,
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
