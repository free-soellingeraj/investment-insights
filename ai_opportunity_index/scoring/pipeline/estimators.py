"""Formula-based dollar estimators (deterministic, no LLM required).

Estimates dollar impact of evidence passages using company financials,
BLS salary data, and configurable formulas.
"""

from __future__ import annotations

import csv
import logging
from functools import lru_cache
from pathlib import Path

from ai_opportunity_index.config import (
    BLS_SALARY_DATA_PATH,
    DOLLAR_COST_PER_PATENT,
    DOLLAR_HORIZON_INVESTED,
    DOLLAR_HORIZON_PLANNED,
    DOLLAR_HORIZON_REALIZED,
    DOLLAR_PRODUCT_REVENUE_BRACKETS,
    DOLLAR_PRODUCTIVITY_GAIN_PCT,
    DOLLAR_REVENUE_PENETRATION_RATE,
    OPP_DEFAULT_SOC_SCORE,
)
from ai_opportunity_index.scoring.evidence_classification import (
    CaptureStage,
    TargetDimension,
)
from ai_opportunity_index.scoring.pipeline.base import DollarEstimator, HorizonEstimator
from ai_opportunity_index.scoring.pipeline.models import (
    EvidencePassage,
    ValuedEvidence,
)

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def load_bls_salaries() -> dict[str, float]:
    """Load SOC group → median annual salary mapping from BLS data."""
    salaries: dict[str, float] = {}
    path = BLS_SALARY_DATA_PATH
    if not path.exists():
        logger.warning("BLS salary data not found at %s; using defaults", path)
        return salaries
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            soc = row.get("soc_group", "").strip()
            salary_str = row.get("median_annual_salary", "0").strip()
            try:
                salaries[soc] = float(salary_str)
            except ValueError:
                continue
    return salaries


def _get_sector_avg_salary(sector: str | None, soc_groups: list[str] | None = None) -> float:
    """Get average salary for a sector/SOC group mix."""
    salaries = load_bls_salaries()
    if soc_groups:
        soc_salaries = [salaries.get(soc, 50_000) for soc in soc_groups]
        if soc_salaries:
            return sum(soc_salaries) / len(soc_salaries)
    # Fallback by sector
    sector_defaults = {
        "Technology": 100_000,
        "Financial Services": 85_000,
        "Healthcare": 70_000,
        "Communication Services": 90_000,
        "Consumer Cyclical": 50_000,
        "Consumer Defensive": 45_000,
        "Industrials": 55_000,
        "Energy": 60_000,
        "Basic Materials": 50_000,
        "Real Estate": 55_000,
        "Utilities": 55_000,
    }
    return sector_defaults.get(sector or "", 55_000)


def _get_ai_applicability(soc_groups: list[str] | None = None) -> float:
    """Get average AI applicability for a set of SOC groups."""
    if not soc_groups:
        return OPP_DEFAULT_SOC_SCORE

    # Use the fallback scores from industry_mappings
    from ai_opportunity_index.data.industry_mappings import load_ai_applicability_scores

    try:
        df = load_ai_applicability_scores()
        score_col = None
        for c in ["ai_applicability_score", "score", "applicability"]:
            if c in df.columns:
                score_col = c
                break
        if score_col is None:
            score_col = df.columns[-1]

        soc_col = None
        for c in ["soc_group", "soc_code", "soc"]:
            if c in df.columns:
                soc_col = c
                break
        if soc_col is None:
            soc_col = df.columns[0]

        df["soc_major"] = df[soc_col].astype(str).str[:2] + "-0000"
        lookup = df.groupby("soc_major")[score_col].mean().to_dict()
        scores = [lookup.get(g, OPP_DEFAULT_SOC_SCORE) for g in soc_groups]
        return sum(scores) / len(scores) if scores else OPP_DEFAULT_SOC_SCORE
    except Exception:
        return OPP_DEFAULT_SOC_SCORE


def _product_revenue_estimate(revenue: float) -> float:
    """Estimate revenue per AI product based on company revenue bracket."""
    for threshold in sorted(DOLLAR_PRODUCT_REVENUE_BRACKETS.keys(), reverse=True):
        if revenue >= threshold:
            return DOLLAR_PRODUCT_REVENUE_BRACKETS[threshold]
    return 50_000


class FormulaHorizonEstimator(HorizonEstimator):
    """Estimate 3-year horizon using stage-based multipliers."""

    def estimate_horizon(
        self,
        passage: EvidencePassage,
        base_annual_value: float,
    ) -> tuple[float, float, float, str]:
        if passage.stage == CaptureStage.REALIZED:
            mult = DOLLAR_HORIZON_REALIZED
            shape = "flat"
        elif passage.stage == CaptureStage.INVESTED:
            mult = DOLLAR_HORIZON_INVESTED
            shape = "linear_ramp"
        else:  # PLANNED
            mult = DOLLAR_HORIZON_PLANNED
            shape = "back_loaded"

        y1 = base_annual_value * mult[0]
        y2 = base_annual_value * mult[1]
        y3 = base_annual_value * mult[2]
        return y1, y2, y3, shape


class FormulaDollarEstimator(DollarEstimator):
    """Estimate dollar impact using company financials + configurable formulas.

    Cost capture = employees * avg_salary * ai_applicability * productivity_gain
    Revenue capture = revenue * penetration_rate (or per-product bracket)
    """

    def __init__(self) -> None:
        self.horizon_estimator = FormulaHorizonEstimator()

    def estimate(
        self,
        passage: EvidencePassage,
        company_financials: dict,
    ) -> ValuedEvidence:
        if passage.target == TargetDimension.COST:
            return self._estimate_cost_capture(passage, company_financials)
        elif passage.target == TargetDimension.REVENUE:
            return self._estimate_revenue_capture(passage, company_financials)
        else:
            return self._estimate_general(passage, company_financials)

    def _estimate_cost_capture(
        self,
        passage: EvidencePassage,
        financials: dict,
    ) -> ValuedEvidence:
        """Cost capture = employees * avg_salary * ai_applicability * productivity_gain."""
        employees = financials.get("employees", 0) or 0
        sector = financials.get("sector")
        soc_groups = financials.get("soc_groups", [])

        avg_salary = _get_sector_avg_salary(sector, soc_groups)
        ai_applicability = _get_ai_applicability(soc_groups)

        # Base annual cost savings potential
        base_annual = employees * avg_salary * ai_applicability * DOLLAR_PRODUCTIVITY_GAIN_PCT

        # Scale by evidence confidence and a per-evidence fraction
        # (one evidence item represents a slice of the total opportunity)
        evidence_fraction = passage.confidence * self._stage_multiplier(passage.stage)
        annual = base_annual * evidence_fraction

        y1, y2, y3, shape = self.horizon_estimator.estimate_horizon(passage, annual)
        total = y1 + y2 + y3

        rationale = (
            f"Cost capture: {employees:,} employees × ${avg_salary:,.0f} avg salary "
            f"× {ai_applicability:.2f} AI applicability × {DOLLAR_PRODUCTIVITY_GAIN_PCT:.0%} gain "
            f"× {evidence_fraction:.2f} evidence factor = ${annual:,.0f}/yr"
        )

        return ValuedEvidence(
            passage=passage,
            dollar_year_1=round(y1, 2),
            dollar_year_2=round(y2, 2),
            dollar_year_3=round(y3, 2),
            total_3yr=round(total, 2),
            horizon_shape=shape,
            valuation_method="formula",
            valuation_rationale=rationale,
        )

    def _estimate_revenue_capture(
        self,
        passage: EvidencePassage,
        financials: dict,
    ) -> ValuedEvidence:
        """Revenue capture = revenue * penetration_rate, scaled by evidence."""
        revenue = financials.get("revenue", 0) or 0

        # Choose estimation method based on evidence type
        if passage.metadata.get("is_product") or passage.source_type == "news":
            # Per-product estimate
            base_annual = _product_revenue_estimate(revenue)
        else:
            # General revenue penetration
            base_annual = revenue * DOLLAR_REVENUE_PENETRATION_RATE

        evidence_fraction = passage.confidence * self._stage_multiplier(passage.stage)
        annual = base_annual * evidence_fraction

        y1, y2, y3, shape = self.horizon_estimator.estimate_horizon(passage, annual)
        total = y1 + y2 + y3

        rationale = (
            f"Revenue capture: ${revenue:,.0f} revenue base "
            f"× {evidence_fraction:.2f} evidence factor = ${annual:,.0f}/yr"
        )

        return ValuedEvidence(
            passage=passage,
            dollar_year_1=round(y1, 2),
            dollar_year_2=round(y2, 2),
            dollar_year_3=round(y3, 2),
            total_3yr=round(total, 2),
            horizon_shape=shape,
            valuation_method="formula",
            valuation_rationale=rationale,
        )

    def _estimate_general(
        self,
        passage: EvidencePassage,
        financials: dict,
    ) -> ValuedEvidence:
        """General AI investment — split between cost and revenue heuristically."""
        employees = financials.get("employees", 0) or 0
        revenue = financials.get("revenue", 0) or 0
        sector = financials.get("sector")
        soc_groups = financials.get("soc_groups", [])

        # Use a blend of cost and revenue estimation
        avg_salary = _get_sector_avg_salary(sector, soc_groups)
        ai_applicability = _get_ai_applicability(soc_groups)

        cost_component = employees * avg_salary * ai_applicability * DOLLAR_PRODUCTIVITY_GAIN_PCT * 0.5
        revenue_component = revenue * DOLLAR_REVENUE_PENETRATION_RATE * 0.5
        base_annual = cost_component + revenue_component

        evidence_fraction = passage.confidence * self._stage_multiplier(passage.stage)
        annual = base_annual * evidence_fraction

        y1, y2, y3, shape = self.horizon_estimator.estimate_horizon(passage, annual)
        total = y1 + y2 + y3

        rationale = (
            f"General AI investment: cost component ${cost_component:,.0f} + "
            f"revenue component ${revenue_component:,.0f} "
            f"× {evidence_fraction:.2f} = ${annual:,.0f}/yr"
        )

        return ValuedEvidence(
            passage=passage,
            dollar_year_1=round(y1, 2),
            dollar_year_2=round(y2, 2),
            dollar_year_3=round(y3, 2),
            total_3yr=round(total, 2),
            horizon_shape=shape,
            valuation_method="formula",
            valuation_rationale=rationale,
        )

    @staticmethod
    def _stage_multiplier(stage: CaptureStage) -> float:
        """Confidence multiplier based on capture stage."""
        if stage == CaptureStage.REALIZED:
            return 1.0
        elif stage == CaptureStage.INVESTED:
            return 0.7
        else:  # PLANNED
            return 0.3
