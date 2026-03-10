"""Tests for the formula-based dollar estimation system.

Tests FormulaDollarEstimator, FormulaHorizonEstimator, _product_revenue_estimate,
get_sector_avg_salary, and get_ai_applicability from the estimators module.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from ai_opportunity_index.config import (
    DOLLAR_HORIZON_INVESTED,
    DOLLAR_HORIZON_PLANNED,
    DOLLAR_HORIZON_REALIZED,
    DOLLAR_PRODUCTIVITY_GAIN_PCT,
    DOLLAR_REVENUE_PENETRATION_RATE,
    OPP_DEFAULT_SOC_SCORE,
)
from ai_opportunity_index.scoring.evidence_classification import (
    CaptureStage,
    TargetDimension,
)
from ai_opportunity_index.scoring.pipeline.estimators import (
    FormulaDollarEstimator,
    FormulaHorizonEstimator,
    _product_revenue_estimate,
    get_ai_applicability,
    get_sector_avg_salary,
    load_bls_salaries,
)
from ai_opportunity_index.scoring.pipeline.models import EvidencePassage

# ── Load test fixtures ──────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parent / "fixtures"

with open(FIXTURES_DIR / "dollar_estimation_cases.json") as _f:
    CASES = json.load(_f)


# ── Helpers ─────────────────────────────────────────────────────────────────

STAGE_MAP = {
    "realized": CaptureStage.REALIZED,
    "invested": CaptureStage.INVESTED,
    "planned": CaptureStage.PLANNED,
}


def _make_passage(
    target: TargetDimension = TargetDimension.COST,
    stage: CaptureStage = CaptureStage.REALIZED,
    confidence: float = 1.0,
    source_type: str = "filing",
    metadata: dict | None = None,
) -> EvidencePassage:
    return EvidencePassage(
        source_type=source_type,
        source_document="test_doc.txt",
        passage_text="Test passage for estimation",
        target=target,
        stage=stage,
        confidence=confidence,
        metadata=metadata or {},
    )


# ── Product revenue bracket lookup ──────────────────────────────────────────


class TestProductRevenueBrackets:
    @pytest.mark.parametrize(
        "case",
        CASES["product_revenue_brackets"],
        ids=[c["description"] for c in CASES["product_revenue_brackets"]],
    )
    def test_bracket_lookup(self, case):
        result = _product_revenue_estimate(case["revenue"])
        assert result == case["expected"], (
            f"For revenue={case['revenue']}: expected {case['expected']}, got {result}"
        )

    def test_all_brackets_covered(self):
        """Verify the three defined brackets return expected values."""
        assert _product_revenue_estimate(2e9) == 5_000_000
        assert _product_revenue_estimate(5e8) == 500_000
        assert _product_revenue_estimate(1e7) == 50_000

    def test_negative_revenue(self):
        """Negative revenue should fall through to the lowest bracket."""
        result = _product_revenue_estimate(-100)
        assert result == 50_000


# ── Sector salary lookup ────────────────────────────────────────────────────


class TestGetSectorAvgSalary:
    @pytest.mark.parametrize(
        "case",
        CASES["sector_salaries"],
        ids=[f"sector={c['sector']}" for c in CASES["sector_salaries"]],
    )
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={},
    )
    def test_sector_salary_defaults(self, _mock_bls, case):
        result = get_sector_avg_salary(case["sector"])
        assert result == case["expected"]

    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={"15-0000": 120_000, "11-0000": 95_000},
    )
    def test_soc_groups_override_sector(self, _mock_bls):
        """When SOC groups are provided, their average overrides sector default."""
        result = get_sector_avg_salary(
            "Healthcare", soc_groups=["15-0000", "11-0000"]
        )
        assert result == pytest.approx(107_500.0)

    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={"15-0000": 120_000},
    )
    def test_soc_groups_missing_code_uses_fallback(self, _mock_bls):
        """Unknown SOC codes fall back to 50_000."""
        result = get_sector_avg_salary("Technology", soc_groups=["99-9999"])
        assert result == pytest.approx(50_000.0)

    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={},
    )
    def test_empty_soc_groups_uses_sector(self, _mock_bls):
        """Empty SOC groups list falls back to sector lookup."""
        result = get_sector_avg_salary("Energy", soc_groups=[])
        assert result == 60_000


# ── AI applicability ────────────────────────────────────────────────────────


class TestGetAiApplicability:
    def test_no_soc_groups_returns_default(self):
        result = get_ai_applicability(None)
        assert result == OPP_DEFAULT_SOC_SCORE

    def test_empty_soc_groups_returns_default(self):
        result = get_ai_applicability([])
        assert result == OPP_DEFAULT_SOC_SCORE


# ── Stage confidence multipliers ────────────────────────────────────────────


class TestStageMultipliers:
    @pytest.mark.parametrize(
        "case",
        CASES["stage_confidence_multipliers"],
        ids=[c["stage"] for c in CASES["stage_confidence_multipliers"]],
    )
    def test_stage_multiplier(self, case):
        stage = STAGE_MAP[case["stage"]]
        result = FormulaDollarEstimator._stage_multiplier(stage)
        assert result == pytest.approx(case["expected"])


# ── Horizon estimator ──────────────────────────────────────────────────────


class TestFormulaHorizonEstimator:
    @pytest.mark.parametrize(
        "case",
        CASES["horizon_multipliers"],
        ids=[c["description"] for c in CASES["horizon_multipliers"]],
    )
    def test_horizon_by_stage(self, case):
        estimator = FormulaHorizonEstimator()
        stage = STAGE_MAP[case["stage"]]
        passage = _make_passage(stage=stage)
        base = case["base_annual"]

        y1, y2, y3, shape = estimator.estimate_horizon(passage, base)

        assert y1 == pytest.approx(case["expected_y1"])
        assert y2 == pytest.approx(case["expected_y2"])
        assert y3 == pytest.approx(case["expected_y3"])
        assert shape == case["shape"]

    def test_realized_flat_shape(self):
        estimator = FormulaHorizonEstimator()
        passage = _make_passage(stage=CaptureStage.REALIZED)
        y1, y2, y3, shape = estimator.estimate_horizon(passage, 500_000)
        assert shape == "flat"
        assert y1 == y2 == y3 == pytest.approx(500_000)

    def test_invested_ramp_shape(self):
        estimator = FormulaHorizonEstimator()
        passage = _make_passage(stage=CaptureStage.INVESTED)
        y1, y2, y3, shape = estimator.estimate_horizon(passage, 1_000_000)
        assert shape == "linear_ramp"
        assert y1 < y2 < y3

    def test_planned_back_loaded_shape(self):
        estimator = FormulaHorizonEstimator()
        passage = _make_passage(stage=CaptureStage.PLANNED)
        y1, y2, y3, shape = estimator.estimate_horizon(passage, 1_000_000)
        assert shape == "back_loaded"
        assert y1 < y2 < y3

    def test_zero_base_annual(self):
        estimator = FormulaHorizonEstimator()
        passage = _make_passage(stage=CaptureStage.REALIZED)
        y1, y2, y3, shape = estimator.estimate_horizon(passage, 0)
        assert y1 == y2 == y3 == 0.0

    def test_horizon_config_values(self):
        """Verify the config constants match expected tuples."""
        assert DOLLAR_HORIZON_REALIZED == (1.0, 1.0, 1.0)
        assert DOLLAR_HORIZON_INVESTED == (0.33, 0.66, 1.0)
        assert DOLLAR_HORIZON_PLANNED == (0.10, 0.40, 1.0)


# ── Cost capture formula (end-to-end) ──────────────────────────────────────


class TestCostCaptureFormula:
    @pytest.mark.parametrize(
        "case",
        CASES["cost_capture"],
        ids=[c["description"] for c in CASES["cost_capture"]],
    )
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.get_ai_applicability",
        return_value=0.15,
    )
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={},
    )
    def test_cost_capture(self, _mock_bls, _mock_ai, case):
        estimator = FormulaDollarEstimator()
        stage = STAGE_MAP[case["stage"]]
        passage = _make_passage(
            target=TargetDimension.COST,
            stage=stage,
            confidence=case["confidence"],
        )
        financials = {
            "employees": case["employees"],
            "sector": case["sector"],
            "soc_groups": [],
        }

        result = estimator.estimate(passage, financials)

        # Verify base annual before evidence fraction
        expected_base = (
            case["employees"]
            * case["avg_salary"]
            * case["ai_applicability"]
            * case["productivity_gain_pct"]
        )
        assert expected_base == pytest.approx(case["expected_base_annual"])

        # Verify the annual value after evidence fraction
        evidence_fraction = case["confidence"] * case["stage_multiplier"]
        expected_annual = expected_base * evidence_fraction

        # Verify the 3-year total
        assert result.valuation_method == "formula"
        assert result.total_3yr == pytest.approx(
            result.dollar_year_1 + result.dollar_year_2 + result.dollar_year_3
        )

    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.get_ai_applicability",
        return_value=0.15,
    )
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={},
    )
    def test_cost_capture_realized_full_confidence(self, _mock_bls, _mock_ai):
        """Fully realized, confidence=1.0, Technology sector: verify exact values."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.COST,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
        )
        financials = {"employees": 1000, "sector": "Technology", "soc_groups": []}

        result = estimator.estimate(passage, financials)

        # base = 1000 * 100_000 * 0.15 * 0.15 = 2_250_000
        # evidence_fraction = 1.0 * 1.0 = 1.0
        # annual = 2_250_000
        # realized horizon = (1.0, 1.0, 1.0) => y1=y2=y3 = 2_250_000
        assert result.dollar_year_1 == pytest.approx(2_250_000)
        assert result.dollar_year_2 == pytest.approx(2_250_000)
        assert result.dollar_year_3 == pytest.approx(2_250_000)
        assert result.total_3yr == pytest.approx(6_750_000)
        assert result.horizon_shape == "flat"


# ── Revenue capture formula (end-to-end) ───────────────────────────────────


class TestRevenueCaptureFormula:
    def test_product_path_realized(self):
        """Revenue capture via product path uses bracket lookup."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.REVENUE,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
            source_type="news",
        )
        financials = {"revenue": 1_000_000_000}

        result = estimator.estimate(passage, financials)

        # product bracket for $1B = $5M, confidence=1.0, stage_mult=1.0
        # annual = 5_000_000, realized => y1=y2=y3 = 5_000_000
        assert result.dollar_year_1 == pytest.approx(5_000_000)
        assert result.total_3yr == pytest.approx(15_000_000)
        assert result.horizon_shape == "flat"

    def test_general_path_realized(self):
        """Revenue capture via general path uses penetration rate."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.REVENUE,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
            source_type="filing",  # not 'news', no is_product metadata
        )
        financials = {"revenue": 1_000_000_000}

        result = estimator.estimate(passage, financials)

        # general: revenue * 0.05 = 50_000_000, confidence=1.0, stage_mult=1.0
        assert result.dollar_year_1 == pytest.approx(50_000_000)
        assert result.total_3yr == pytest.approx(150_000_000)

    def test_product_metadata_triggers_product_path(self):
        """Metadata is_product=True triggers per-product bracket estimation."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.REVENUE,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
            source_type="filing",
            metadata={"is_product": True},
        )
        financials = {"revenue": 500_000_000}

        result = estimator.estimate(passage, financials)

        # product bracket for $500M = $500K
        assert result.dollar_year_1 == pytest.approx(500_000)

    def test_revenue_capture_invested_stage(self):
        """Invested stage applies 0.7 multiplier and linear_ramp horizon."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.REVENUE,
            stage=CaptureStage.INVESTED,
            confidence=1.0,
            source_type="news",
        )
        financials = {"revenue": 1_000_000_000}

        result = estimator.estimate(passage, financials)

        # product bracket $5M, evidence_fraction = 1.0 * 0.7 = 0.7
        # annual = 5_000_000 * 0.7 = 3_500_000
        # invested horizon: (0.33, 0.66, 1.0) * 3_500_000
        assert result.dollar_year_1 == pytest.approx(3_500_000 * 0.33)
        assert result.dollar_year_2 == pytest.approx(3_500_000 * 0.66)
        assert result.dollar_year_3 == pytest.approx(3_500_000 * 1.0)
        assert result.horizon_shape == "linear_ramp"

    @pytest.mark.parametrize(
        "case",
        CASES["revenue_capture"],
        ids=[c["description"] for c in CASES["revenue_capture"]],
    )
    def test_revenue_capture_parametrized(self, case):
        estimator = FormulaDollarEstimator()
        stage = STAGE_MAP[case["stage"]]

        if case["is_product"]:
            source_type = "news"
        else:
            source_type = "filing"

        passage = _make_passage(
            target=TargetDimension.REVENUE,
            stage=stage,
            confidence=case["confidence"],
            source_type=source_type,
        )
        financials = {"revenue": case["revenue"]}

        result = estimator.estimate(passage, financials)

        if case["is_product"]:
            expected_base = case["expected_base_annual_product"]
        else:
            expected_base = case["expected_base_annual_general"]

        evidence_fraction = case["confidence"] * case["stage_multiplier"]
        expected_annual = expected_base * evidence_fraction

        # Verify total is sum of years
        assert result.total_3yr == pytest.approx(
            result.dollar_year_1 + result.dollar_year_2 + result.dollar_year_3
        )
        assert result.valuation_method == "formula"


# ── General estimation (blended) ───────────────────────────────────────────


class TestGeneralEstimation:
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.get_ai_applicability",
        return_value=0.15,
    )
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={},
    )
    def test_general_blends_cost_and_revenue(self, _mock_bls, _mock_ai):
        """General target uses 50/50 blend of cost and revenue components."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.GENERAL,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
        )
        financials = {
            "employees": 1000,
            "sector": "Technology",
            "revenue": 1_000_000_000,
            "soc_groups": [],
        }

        result = estimator.estimate(passage, financials)

        # cost_component = 1000 * 100_000 * 0.15 * 0.15 * 0.5 = 1_125_000
        # revenue_component = 1_000_000_000 * 0.05 * 0.5 = 25_000_000
        # base_annual = 26_125_000
        # evidence_fraction = 1.0 * 1.0 = 1.0
        # annual = 26_125_000, realized => flat
        assert result.dollar_year_1 == pytest.approx(26_125_000)
        assert result.total_3yr == pytest.approx(78_375_000)
        assert result.horizon_shape == "flat"


# ── Edge cases ──────────────────────────────────────────────────────────────


class TestEdgeCases:
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.get_ai_applicability",
        return_value=0.15,
    )
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={},
    )
    def test_zero_employees_cost_capture(self, _mock_bls, _mock_ai):
        """Zero employees should produce $0 cost capture."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.COST,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
        )
        financials = {"employees": 0, "sector": "Technology", "soc_groups": []}

        result = estimator.estimate(passage, financials)
        assert result.total_3yr == pytest.approx(0.0)
        assert result.dollar_year_1 == pytest.approx(0.0)

    def test_zero_revenue_general_path(self):
        """Zero revenue with general path should produce $0 revenue capture."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.REVENUE,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
            source_type="filing",
        )
        financials = {"revenue": 0}

        result = estimator.estimate(passage, financials)
        assert result.total_3yr == pytest.approx(0.0)

    def test_zero_revenue_product_path_still_gets_minimum(self):
        """Zero revenue with product path gets $50K bracket minimum."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.REVENUE,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
            source_type="news",
        )
        financials = {"revenue": 0}

        result = estimator.estimate(passage, financials)
        # product bracket for $0 = $50K, realized, confidence=1.0
        assert result.dollar_year_1 == pytest.approx(50_000)
        assert result.total_3yr == pytest.approx(150_000)

    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.get_ai_applicability",
        return_value=0.15,
    )
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={},
    )
    def test_none_employees_treated_as_zero(self, _mock_bls, _mock_ai):
        """None employees should be treated as 0."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.COST,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
        )
        financials = {"employees": None, "sector": "Technology", "soc_groups": []}

        result = estimator.estimate(passage, financials)
        assert result.total_3yr == pytest.approx(0.0)

    def test_none_revenue_treated_as_zero(self):
        """None revenue should be treated as 0."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.REVENUE,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
            source_type="filing",
        )
        financials = {"revenue": None}

        result = estimator.estimate(passage, financials)
        assert result.total_3yr == pytest.approx(0.0)

    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.get_ai_applicability",
        return_value=0.15,
    )
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={},
    )
    def test_zero_confidence_produces_zero(self, _mock_bls, _mock_ai):
        """Zero confidence should produce $0 for any dimension."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.COST,
            stage=CaptureStage.REALIZED,
            confidence=0.0,
        )
        financials = {
            "employees": 1000,
            "sector": "Technology",
            "revenue": 1_000_000_000,
            "soc_groups": [],
        }

        result = estimator.estimate(passage, financials)
        assert result.total_3yr == pytest.approx(0.0)

    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.get_ai_applicability",
        return_value=0.15,
    )
    @patch(
        "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
        return_value={},
    )
    def test_missing_financials_keys(self, _mock_bls, _mock_ai):
        """Missing keys in financials dict should not crash."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.COST,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
        )
        # Empty financials
        result = estimator.estimate(passage, {})
        assert result.total_3yr == pytest.approx(0.0)

    def test_valued_evidence_has_passage_reference(self):
        """ValuedEvidence should preserve the original passage."""
        estimator = FormulaDollarEstimator()
        passage = _make_passage(
            target=TargetDimension.REVENUE,
            stage=CaptureStage.REALIZED,
            confidence=1.0,
            source_type="news",
        )
        financials = {"revenue": 1_000_000_000}

        result = estimator.estimate(passage, financials)
        assert result.passage is passage
        assert result.passage.target == TargetDimension.REVENUE
        assert result.passage.stage == CaptureStage.REALIZED

    def test_rationale_is_nonempty(self):
        """Valuation rationale should always be populated."""
        estimator = FormulaDollarEstimator()
        for target in [TargetDimension.COST, TargetDimension.REVENUE, TargetDimension.GENERAL]:
            passage = _make_passage(
                target=target,
                stage=CaptureStage.REALIZED,
                confidence=1.0,
                source_type="news" if target == TargetDimension.REVENUE else "filing",
            )
            financials = {
                "employees": 100,
                "sector": "Technology",
                "revenue": 1_000_000,
                "soc_groups": [],
            }
            with patch(
                "ai_opportunity_index.scoring.pipeline.estimators.get_ai_applicability",
                return_value=0.15,
            ), patch(
                "ai_opportunity_index.scoring.pipeline.estimators.load_bls_salaries",
                return_value={},
            ):
                result = estimator.estimate(passage, financials)
            assert result.valuation_rationale != ""
            assert result.valuation_method == "formula"
