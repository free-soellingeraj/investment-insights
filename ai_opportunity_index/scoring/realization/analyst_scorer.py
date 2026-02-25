"""Sub-scorer F: Analyst consensus signals.

Reads pre-collected analyst data from local cache (data/raw/analysts/{TICKER}.json).
Collection is handled by scripts/collect_evidence.py --sources analysts.

Uses heuristic scoring based on analyst ratings and growth estimates.
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

ANALYST_CACHE_DIR = RAW_DIR / "analysts"


def score_analyst_classified(
    company_name: str,
    ticker: str,
) -> ClassifiedScorerOutput | None:
    """Score analyst consensus signals for a company.

    Reads data/raw/analysts/{TICKER}.json, applies heuristics to compute
    cost/revenue/general scores based on:
    - Recommendation (buy/sell) → general AI investment sentiment
    - Revenue growth estimates → revenue signal
    - Earnings growth → cost efficiency signal
    - Number of analysts covering → confidence weighting

    Returns ClassifiedScorerOutput or None if no data.
    """
    cache_file = ANALYST_CACHE_DIR / f"{ticker.upper()}.json"
    if not cache_file.exists():
        logger.debug("No analyst cache for %s", ticker)
        return None

    try:
        data = json.loads(cache_file.read_text())
    except Exception as e:
        logger.warning("Failed to read analyst cache for %s: %s", ticker, e)
        return None

    rec_mean = data.get("recommendation_mean")  # 1=strong buy, 5=strong sell
    rec_key = data.get("recommendation_key")
    target_mean = data.get("target_mean_price")
    current_price = data.get("current_price")
    num_analysts = data.get("number_of_analysts") or 0
    revenue_growth = data.get("revenue_estimate_next_quarter")
    earnings_growth = data.get("earnings_estimate_next_quarter")

    # Need at least recommendation or price target data
    if rec_mean is None and target_mean is None:
        return None

    evidence_items: list[ClassifiedEvidence] = []

    # Analyst coverage count → confidence weight (more analysts = more reliable)
    coverage_confidence = min(num_analysts / 20.0, 1.0) if num_analysts else 0.3

    # Recommendation → general investment signal
    # Strong buy (1) = 1.0, Hold (3) = 0.3, Strong sell (5) = 0.0
    if rec_mean is not None:
        rec_score = max(0.0, min(1.0, (5.0 - rec_mean) / 4.0)) * coverage_confidence
        if rec_score > 0.1:
            stage = CaptureStage.REALIZED if rec_key in ("buy", "strongBuy") else CaptureStage.INVESTED
            evidence_items.append(ClassifiedEvidence(
                source_type="analyst",
                target=TargetDimension.GENERAL,
                stage=stage,
                raw_score=round(rec_score, 4),
                description=f"Analyst consensus: {rec_key or 'N/A'} (mean={rec_mean:.1f}, n={num_analysts})",
                metadata={
                    "recommendation_mean": rec_mean,
                    "recommendation_key": rec_key,
                    "number_of_analysts": num_analysts,
                },
            ))

    # Price target upside → revenue opportunity signal
    if target_mean is not None and current_price is not None and current_price > 0:
        upside = (target_mean - current_price) / current_price
        # >20% upside = strong signal, 0% = neutral, negative = low
        upside_score = max(0.0, min(1.0, upside / 0.4)) * coverage_confidence
        if upside_score > 0.1:
            evidence_items.append(ClassifiedEvidence(
                source_type="analyst",
                target=TargetDimension.REVENUE,
                stage=CaptureStage.PLANNED,
                raw_score=round(upside_score, 4),
                description=f"Price target upside: {upside:.0%} (${current_price:.0f} → ${target_mean:.0f})",
                metadata={
                    "target_mean_price": target_mean,
                    "current_price": current_price,
                    "upside_pct": round(upside, 4),
                },
            ))

    # Revenue growth → revenue capture signal
    if revenue_growth is not None and revenue_growth > 0:
        rev_score = min(1.0, revenue_growth / 0.3) * coverage_confidence  # 30%+ growth = max
        if rev_score > 0.1:
            evidence_items.append(ClassifiedEvidence(
                source_type="analyst",
                target=TargetDimension.REVENUE,
                stage=CaptureStage.REALIZED,
                raw_score=round(rev_score, 4),
                description=f"Revenue growth: {revenue_growth:.0%}",
                metadata={"revenue_growth": revenue_growth},
            ))

    # Earnings growth → cost efficiency / profitability signal
    if earnings_growth is not None and earnings_growth > 0:
        earn_score = min(1.0, earnings_growth / 0.3) * coverage_confidence
        if earn_score > 0.1:
            evidence_items.append(ClassifiedEvidence(
                source_type="analyst",
                target=TargetDimension.COST,
                stage=CaptureStage.REALIZED,
                raw_score=round(earn_score, 4),
                description=f"Earnings growth: {earnings_growth:.0%}",
                metadata={"earnings_growth": earnings_growth},
            ))

    if not evidence_items:
        return None

    # Aggregate per-dimension scores
    cost_scores = [e.raw_score for e in evidence_items if e.target == TargetDimension.COST]
    revenue_scores = [e.raw_score for e in evidence_items if e.target == TargetDimension.REVENUE]
    general_scores = [e.raw_score for e in evidence_items if e.target == TargetDimension.GENERAL]

    cost_capture = round(min(1.0, sum(cost_scores)), 4)
    revenue_capture = round(min(1.0, sum(revenue_scores)), 4)
    general_investment = round(min(1.0, sum(general_scores)), 4)
    overall = round(min(1.0, cost_capture + revenue_capture + general_investment), 4)

    return ClassifiedScorerOutput(
        overall_score=overall,
        cost_capture_score=cost_capture,
        revenue_capture_score=revenue_capture,
        general_investment_score=general_investment,
        evidence_items=evidence_items,
        raw_details={
            "recommendation_mean": rec_mean,
            "recommendation_key": rec_key,
            "num_analysts": num_analysts,
            "revenue_growth": revenue_growth,
            "earnings_growth": earnings_growth,
        },
    )
