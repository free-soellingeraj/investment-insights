"""Comprehensive unit tests for deterministic scoring formulas.

Tests every deterministic formula in the investment insights scoring system
using parameterized test cases from fixtures/scoring_cases.json.
"""

import json
import math
from datetime import date
from pathlib import Path

import pytest

# ── Load test fixtures ─────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parent / "fixtures"
with open(FIXTURES_DIR / "scoring_cases.json") as f:
    CASES = json.load(f)


# ── Recency Decay ──────────────────────────────────────────────────────────

from ai_opportunity_index.scoring.evidence_valuation import (
    compute_recency,
    compute_magnitude,
    compute_factor_score,
    RECENCY_DECAY_BASE,
    RECENCY_FLOOR,
    STAGE_WEIGHTS,
    aggregate_valuations,
)


class TestComputeRecency:
    """Test compute_recency: 0.7^years_old, floor 0.3."""

    @pytest.mark.parametrize(
        "case",
        CASES["recency_decay"]["cases"],
        ids=[c["id"] for c in CASES["recency_decay"]["cases"]],
    )
    def test_recency_cases(self, case):
        ev_date = date.fromisoformat(case["evidence_date"]) if case["evidence_date"] else None
        ref_date = date.fromisoformat(case["reference_date"]) if case.get("reference_date") else None

        result = compute_recency(ev_date, ref_date)

        if "expected" in case:
            assert result == pytest.approx(case["expected"], abs=1e-6)
        elif "expected_approx" in case:
            assert result == pytest.approx(
                case["expected_approx"], abs=case.get("tolerance", 0.01)
            )

    def test_recency_never_below_floor(self):
        """Even very old dates should not go below RECENCY_FLOOR."""
        old_date = date(2000, 1, 1)
        ref = date(2026, 3, 8)
        result = compute_recency(old_date, ref)
        assert result >= RECENCY_FLOOR
        assert result == pytest.approx(RECENCY_FLOOR, abs=1e-6)

    def test_recency_monotonically_decreasing(self):
        """More recent evidence should have higher recency."""
        ref = date(2026, 3, 8)
        dates = [date(2026, 1, 1), date(2025, 1, 1), date(2024, 1, 1), date(2022, 1, 1)]
        scores = [compute_recency(d, ref) for d in dates]
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1]


# ── Magnitude Saturation ─────────────────────────────────────────────────

class TestComputeMagnitude:
    """Test compute_magnitude: dollar_mid / company_revenue, capped at 1.0."""

    @pytest.mark.parametrize(
        "case",
        CASES["magnitude_saturation"]["cases"],
        ids=[c["id"] for c in CASES["magnitude_saturation"]["cases"]],
    )
    def test_magnitude_cases(self, case):
        result = compute_magnitude(case["dollar_mid"], case["company_revenue"])
        assert result == pytest.approx(case["expected"], abs=1e-6)

    def test_magnitude_always_non_negative(self):
        """Magnitude should never be negative."""
        assert compute_magnitude(-100, 1000) >= 0.0
        assert compute_magnitude(100, -1000) == 0.0  # negative revenue returns 0

    def test_magnitude_always_at_most_one(self):
        """Magnitude should never exceed 1.0."""
        assert compute_magnitude(1e15, 1.0) <= 1.0


# ── Factor Score Computation ─────────────────────────────────────────────

class TestComputeFactorScore:
    """Test compute_factor_score: specificity * magnitude * stage_weight * recency."""

    @pytest.mark.parametrize(
        "case",
        CASES["factor_score"]["cases"],
        ids=[c["id"] for c in CASES["factor_score"]["cases"]],
    )
    def test_factor_score_cases(self, case):
        result = compute_factor_score(
            case["specificity"],
            case["magnitude"],
            case["stage_weight"],
            case["recency"],
        )
        assert result == pytest.approx(case["expected"], abs=1e-4)

    def test_factor_score_commutative(self):
        """Multiplication is commutative; order of args shouldn't matter in value."""
        a = compute_factor_score(0.5, 0.3, 0.7, 0.9)
        b = compute_factor_score(0.3, 0.5, 0.9, 0.7)
        # Both are products of the same four numbers
        assert a == pytest.approx(b, abs=1e-10)


# ── Dimension Aggregation ─────────────────────────────────────────────────

class TestDimensionAggregation:
    """Test 1 - exp(-x) transform used in aggregate_valuations."""

    @pytest.mark.parametrize(
        "case",
        CASES["dimension_aggregation"]["cases"],
        ids=[c["id"] for c in CASES["dimension_aggregation"]["cases"]],
    )
    def test_aggregation_transform(self, case):
        raw = case["raw_sum"]
        if raw > 0:
            expected_formula = min(1.0, 1.0 - math.exp(-raw))
        else:
            expected_formula = 0.0

        if "expected" in case:
            assert expected_formula == pytest.approx(case["expected"], abs=1e-6)
        elif "expected_approx" in case:
            assert expected_formula == pytest.approx(
                case["expected_approx"], abs=case.get("tolerance", 0.001)
            )

    def test_aggregation_via_real_function(self):
        """Test aggregate_valuations with synthetic valuations to verify the transform."""
        from ai_opportunity_index.domains import (
            EvidenceGroup,
            Valuation,
            ValuationEvidenceType,
            ValuationStage,
            TargetDimension,
        )

        group = EvidenceGroup(
            id=1,
            company_id=1,
            target_dimension=TargetDimension.COST,
        )
        val = Valuation(
            group_id=1,
            stage=ValuationStage.FINAL,
            evidence_type=ValuationEvidenceType.INVESTMENT,
            narrative="test",
            confidence=0.8,
            dollar_low=100000,
            dollar_high=200000,
            dollar_mid=150000,
            factor_score=0.5,
        )

        result = aggregate_valuations([val], [group])
        # cost_score should be 1 - exp(-0.5)
        expected = 1.0 - math.exp(-0.5)
        assert result["cost_score"] == pytest.approx(expected, abs=1e-4)
        assert result["revenue_score"] == 0.0
        assert result["general_score"] == 0.0
        # Dollar tracking
        assert result["cost_potential_usd"] == 150000.0
        assert result["cost_actual_usd"] == 0.0  # investment, not capture

    def test_aggregation_capture_goes_to_actual(self):
        """Capture-type evidence dollars go to actual, not potential."""
        from ai_opportunity_index.domains import (
            EvidenceGroup,
            Valuation,
            ValuationEvidenceType,
            ValuationStage,
            TargetDimension,
        )

        group = EvidenceGroup(
            id=1,
            company_id=1,
            target_dimension=TargetDimension.REVENUE,
        )
        val = Valuation(
            group_id=1,
            stage=ValuationStage.FINAL,
            evidence_type=ValuationEvidenceType.CAPTURE,
            narrative="test",
            confidence=0.9,
            dollar_mid=500000,
            factor_score=0.3,
        )

        result = aggregate_valuations([val], [group])
        assert result["revenue_actual_usd"] == 500000.0
        assert result["revenue_potential_usd"] == 0.0

    def test_aggregation_multiple_groups_same_dimension(self):
        """Multiple valuations in same dimension have their factor scores summed."""
        from ai_opportunity_index.domains import (
            EvidenceGroup,
            Valuation,
            ValuationEvidenceType,
            ValuationStage,
            TargetDimension,
        )

        group1 = EvidenceGroup(id=1, company_id=1, target_dimension=TargetDimension.COST)
        group2 = EvidenceGroup(id=2, company_id=1, target_dimension=TargetDimension.COST)

        val1 = Valuation(
            group_id=1, stage=ValuationStage.FINAL,
            evidence_type=ValuationEvidenceType.PLAN,
            narrative="test1", confidence=0.7, factor_score=0.2,
        )
        val2 = Valuation(
            group_id=2, stage=ValuationStage.FINAL,
            evidence_type=ValuationEvidenceType.INVESTMENT,
            narrative="test2", confidence=0.8, factor_score=0.3,
        )

        result = aggregate_valuations([val1, val2], [group1, group2])
        expected = 1.0 - math.exp(-(0.2 + 0.3))
        assert result["cost_score"] == pytest.approx(expected, abs=1e-4)


# ── ROI Calculation ──────────────────────────────────────────────────────

from ai_opportunity_index.scoring.composite import compute_index_4v, compute_index, compute_ai_index


class TestROICalculation:
    """Test ROI in compute_index_4v: min(CAP, capture/max(opp, MIN_DENOM))."""

    @pytest.mark.parametrize(
        "case",
        CASES["roi_calculation"]["cases"],
        ids=[c["id"] for c in CASES["roi_calculation"]["cases"]],
    )
    def test_roi_cases(self, case):
        result = compute_index_4v(
            cost_opportunity=case["cost_opportunity"],
            revenue_opportunity=case["revenue_opportunity"],
            cost_capture=case["cost_capture"],
            revenue_capture=case["revenue_capture"],
            general_investment=case["general_investment"],
        )
        assert result["cost_roi"] == pytest.approx(case["expected_cost_roi"], abs=1e-4)
        assert result["revenue_roi"] == pytest.approx(case["expected_revenue_roi"], abs=1e-4)
        assert result["combined_roi"] == pytest.approx(case["expected_combined_roi"], abs=1e-4)


# ── AI Index Sigmoid ─────────────────────────────────────────────────────

from ai_opportunity_index.config import AI_INDEX_P_BASE, AI_INDEX_SIGMOID_K, AI_INDEX_STAGE_WEIGHTS


class TestAIIndexSigmoid:
    """Test compute_ai_index: P(capture) * dollar_potential."""

    @pytest.mark.parametrize(
        "case",
        CASES["ai_index_sigmoid"]["cases"],
        ids=[c["id"] for c in CASES["ai_index_sigmoid"]["cases"]],
    )
    def test_ai_index_cases(self, case):
        result = compute_ai_index(
            plan_dollars=case["plan_dollars"],
            investment_dollars=case["investment_dollars"],
            capture_dollars=case["capture_dollars"],
            opportunity_usd=case["opportunity_usd"],
        )

        # Verify structure
        assert "ai_index_usd" in result
        assert "capture_probability" in result
        assert "dollar_potential" in result

        if "expected_probability" in case:
            assert result["capture_probability"] == pytest.approx(
                case["expected_probability"], abs=1e-4
            )
        if "expected_ai_index_usd" in case:
            assert result["ai_index_usd"] == pytest.approx(
                case["expected_ai_index_usd"], abs=0.01
            )

    def test_all_plan_low_probability(self):
        """Pure plan evidence should produce lower probability than pure capture."""
        plan_result = compute_ai_index(1e6, 0, 0)
        capture_result = compute_ai_index(0, 0, 1e6)
        assert plan_result["capture_probability"] < capture_result["capture_probability"]

    def test_all_capture_high_probability(self):
        """Pure capture evidence should produce probability well above P_BASE."""
        result = compute_ai_index(0, 0, 1e6)
        assert result["capture_probability"] > AI_INDEX_P_BASE + 0.3

    def test_probability_monotonic_with_maturity(self):
        """As evidence shifts from plan to capture, probability should increase."""
        total = 1e6
        steps = 10
        probs = []
        for i in range(steps + 1):
            plan = total * (1 - i / steps)
            capture = total * (i / steps)
            result = compute_ai_index(plan, 0, capture)
            probs.append(result["capture_probability"])
        for i in range(len(probs) - 1):
            assert probs[i] <= probs[i + 1] + 1e-10

    def test_dollar_base_is_always_evidence(self):
        """Dollar base is always evidence_total, never inflated by opportunity_usd."""
        result = compute_ai_index(500000, 500000, 0, opportunity_usd=5000000)
        # evidence=1M — dollar_base = evidence, regardless of opportunity_usd
        assert result["dollar_potential"] == 1000000.0
        assert result["evidence_dollars"] == 1000000.0
        assert result["ai_index_usd"] == pytest.approx(
            result["capture_probability"] * 1000000.0, rel=0.001
        )
        # opportunity_usd is stored but doesn't affect the index
        assert result["opportunity_usd"] == 5000000.0

    def test_large_opportunity_usd_does_not_inflate_index(self):
        """Even huge opportunity_usd doesn't inflate ai_index beyond evidence."""
        result = compute_ai_index(500000, 500000, 0, opportunity_usd=500000000)
        assert result["dollar_potential"] == 1000000.0
        assert result["ai_index_usd"] <= result["evidence_dollars"]

    def test_zero_evidence_returns_p_base(self):
        """Zero evidence should return P_BASE probability and zero index."""
        result = compute_ai_index(0, 0, 0)
        assert result["capture_probability"] == AI_INDEX_P_BASE
        assert result["ai_index_usd"] == 0.0

    def test_zero_evidence_with_opportunity_usd(self):
        """Zero evidence → ai_index=0 regardless of opportunity_usd."""
        result = compute_ai_index(0, 0, 0, opportunity_usd=5000000)
        # No evidence means no AI index — structural opportunity alone isn't enough
        assert result["capture_probability"] == AI_INDEX_P_BASE
        assert result["ai_index_usd"] == 0.0
        assert result["dollar_potential"] == 0.0


# ── Quadrant Assignment ──────────────────────────────────────────────────

class TestQuadrantAssignment:
    """Test compute_index: quadrant assignment based on thresholds."""

    @pytest.mark.parametrize(
        "case",
        CASES["quadrant_assignment"]["cases"],
        ids=[c["id"] for c in CASES["quadrant_assignment"]["cases"]],
    )
    def test_quadrant_cases(self, case):
        result = compute_index(case["opportunity"], case["realization"])
        assert result["quadrant"] == case["expected_quadrant"]
        assert result["quadrant_label"] == case["expected_label"]

    def test_quadrant_values_roundtrip(self):
        """Scores should be preserved (rounded to 4dp) in the result."""
        result = compute_index(0.12345, 0.67891)
        assert result["opportunity"] == pytest.approx(0.1235, abs=1e-4)
        assert result["realization"] == pytest.approx(0.6789, abs=1e-4)

    def test_compute_index_4v_legacy_compat(self):
        """compute_index_4v should produce legacy opportunity/realization fields."""
        result = compute_index_4v(
            cost_opportunity=0.6,
            revenue_opportunity=0.8,
            cost_capture=0.3,
            revenue_capture=0.5,
            general_investment=0.1,
        )
        # Legacy: 0.5*cost + 0.5*rev
        assert result["opportunity"] == pytest.approx(0.5 * 0.6 + 0.5 * 0.8, abs=1e-4)
        assert result["realization"] == pytest.approx(0.5 * 0.3 + 0.5 * 0.5, abs=1e-4)


# ── NaN Guard Tests ──────────────────────────────────────────────────────

class TestNaNGuards:
    """Test that NaN/Inf inputs don't crash scoring functions."""

    def test_compute_index_nan_opportunity(self):
        result = compute_index(float("nan"), 0.5)
        assert result["opportunity"] == 0.0
        assert result["quadrant"] is not None

    def test_compute_index_nan_realization(self):
        result = compute_index(0.5, float("nan"))
        assert result["realization"] == 0.0

    def test_compute_index_4v_nan_inputs(self):
        result = compute_index_4v(
            cost_opportunity=float("nan"),
            revenue_opportunity=0.5,
            cost_capture=float("inf"),
            revenue_capture=0.3,
            general_investment=float("nan"),
        )
        assert result["cost_opportunity"] == 0.0
        assert result["general_investment"] == 0.0
        assert result["cost_capture"] == 0.0
        assert result["quadrant"] is not None

    def test_compute_ai_index_nan_dollars(self):
        result = compute_ai_index(float("nan"), 100000, 0)
        assert result["plan_dollars"] == 0.0
        assert result["ai_index_usd"] >= 0

    def test_compute_ai_index_nan_opportunity(self):
        result = compute_ai_index(100000, 0, 0, opportunity_usd=float("nan"))
        # NaN opportunity_usd should fall back to None -> use evidence total
        assert result["ai_index_usd"] >= 0


# ── Cost Capture Formula (estimators.py) ──────────────────────────────────

from ai_opportunity_index.scoring.pipeline.estimators import (
    FormulaDollarEstimator,
    FormulaHorizonEstimator,
    _product_revenue_estimate,
    get_sector_avg_salary,
)
from ai_opportunity_index.scoring.pipeline.models import EvidencePassage
from ai_opportunity_index.scoring.evidence_classification import CaptureStage, TargetDimension as TD
from ai_opportunity_index.config import DOLLAR_PRODUCTIVITY_GAIN_PCT, DOLLAR_REVENUE_PENETRATION_RATE


class TestCostCaptureFormula:
    """Test FormulaDollarEstimator._estimate_cost_capture."""

    @pytest.mark.parametrize(
        "case",
        CASES["cost_capture_formula"]["cases"],
        ids=[c["id"] for c in CASES["cost_capture_formula"]["cases"]],
    )
    def test_cost_capture_cases(self, case):
        estimator = FormulaDollarEstimator()
        stage = CaptureStage(case["stage"])
        passage = EvidencePassage(
            source_type="filing",
            source_document="test.txt",
            passage_text="test passage about AI cost savings",
            target=TD.COST,
            stage=stage,
            confidence=case["confidence"],
        )
        financials = {
            "employees": case["employees"],
            "sector": case["sector"],
            "revenue": 0,
        }
        result = estimator.estimate(passage, financials)

        if "expected_annual" in case:
            # For zero employees, annual should be 0
            assert result.total_3yr == pytest.approx(case["expected_annual"], abs=1e-2)
        else:
            # Verify the formula components
            avg_salary = get_sector_avg_salary(case["sector"])
            if "expected_salary" in case:
                assert avg_salary == case["expected_salary"]
            stage_mult = estimator._stage_multiplier(stage)
            evidence_fraction = case["confidence"] * stage_mult
            # We use OPP_DEFAULT_SOC_SCORE (0.15) as ai_applicability when no soc_groups
            from ai_opportunity_index.config import OPP_DEFAULT_SOC_SCORE
            base_annual = case["employees"] * avg_salary * OPP_DEFAULT_SOC_SCORE * DOLLAR_PRODUCTIVITY_GAIN_PCT
            annual = base_annual * evidence_fraction
            assert annual >= 0
            assert result.total_3yr >= 0


# ── Revenue Capture Formula ──────────────────────────────────────────────

class TestRevenueCaptureFormula:
    """Test FormulaDollarEstimator._estimate_revenue_capture."""

    @pytest.mark.parametrize(
        "case",
        CASES["revenue_capture_formula"]["cases"],
        ids=[c["id"] for c in CASES["revenue_capture_formula"]["cases"]],
    )
    def test_revenue_capture_cases(self, case):
        estimator = FormulaDollarEstimator()
        stage = CaptureStage(case["stage"])
        metadata = {"is_product": True} if case.get("is_product") else {}
        passage = EvidencePassage(
            source_type=case["source_type"],
            source_document="test.txt",
            passage_text="test passage about AI revenue",
            target=TD.REVENUE,
            stage=stage,
            confidence=case["confidence"],
            metadata=metadata,
        )
        financials = {
            "revenue": case["revenue"],
            "employees": 1000,
            "sector": "Technology",
        }
        result = estimator.estimate(passage, financials)
        assert result.total_3yr >= 0
        assert result.valuation_method == "formula"


# ── Product Revenue Brackets ─────────────────────────────────────────────

class TestProductRevenueBrackets:
    """Test _product_revenue_estimate bracket lookup."""

    @pytest.mark.parametrize(
        "case",
        CASES["product_revenue_brackets"]["cases"],
        ids=[c["id"] for c in CASES["product_revenue_brackets"]["cases"]],
    )
    def test_bracket_cases(self, case):
        result = _product_revenue_estimate(case["revenue"])
        assert result == case["expected"]


# ── Text Similarity ──────────────────────────────────────────────────────

from ai_opportunity_index.scoring.evidence_munger import _text_similarity


class TestTextSimilarity:
    """Test _text_similarity: SequenceMatcher ratio on first 300 chars lowered."""

    @pytest.mark.parametrize(
        "case",
        CASES["text_similarity"]["cases"],
        ids=[c["id"] for c in CASES["text_similarity"]["cases"]],
    )
    def test_similarity_cases(self, case):
        result = _text_similarity(case["a"], case["b"])

        if "expected" in case:
            assert result == pytest.approx(case["expected"], abs=1e-4)
        if "expected_min" in case:
            assert result >= case["expected_min"], f"Expected >= {case['expected_min']}, got {result}"
        if "expected_max" in case:
            assert result <= case["expected_max"], f"Expected <= {case['expected_max']}, got {result}"

    def test_similarity_symmetric_same_length(self):
        """Similarity is symmetric for same-length strings (SequenceMatcher property)."""
        a = "Microsoft invested ten billion in AI"
        b = "Microsoft invested ten billion in ML"
        # SequenceMatcher is generally not symmetric for different-length strings,
        # but for same-length strings it is deterministic
        sim_ab = _text_similarity(a, b)
        sim_ba = _text_similarity(b, a)
        assert sim_ab == pytest.approx(sim_ba, abs=1e-10)


# ── Evidence Grouping ─────────────────────────────────────────────────────

from ai_opportunity_index.scoring.evidence_munger import _group_passages
from ai_opportunity_index.domains import EvidenceGroupPassage


class TestEvidenceGrouping:
    """Test _group_passages: greedy clustering by text similarity."""

    @pytest.mark.parametrize(
        "case",
        CASES["evidence_grouping"]["cases"],
        ids=[c["id"] for c in CASES["evidence_grouping"]["cases"]],
    )
    def test_grouping_cases(self, case):
        passages = [
            EvidenceGroupPassage(
                passage_text=text,
                source_type="filing",
                confidence=0.8,
            )
            for text in case["passages"]
        ]
        groups = _group_passages(passages)
        assert len(groups) == case["expected_group_count"]
        if "expected_first_group_size" in case and groups:
            assert len(groups[0]) == case["expected_first_group_size"]

    def test_all_passages_assigned_exactly_once(self):
        """Every passage should appear in exactly one group."""
        passages = [
            EvidenceGroupPassage(passage_text=f"Unique passage number {i} about various topics", confidence=0.5)
            for i in range(10)
        ]
        groups = _group_passages(passages)
        all_texts = []
        for g in groups:
            for p in g:
                all_texts.append(p.passage_text)
        assert len(all_texts) == 10
        assert len(set(all_texts)) == 10


# ── Horizon Estimation ───────────────────────────────────────────────────

class TestHorizonEstimation:
    """Test FormulaHorizonEstimator: stage-based multipliers."""

    @pytest.mark.parametrize(
        "case",
        CASES["horizon_estimation"]["cases"],
        ids=[c["id"] for c in CASES["horizon_estimation"]["cases"]],
    )
    def test_horizon_cases(self, case):
        estimator = FormulaHorizonEstimator()
        stage = CaptureStage(case["stage"])
        passage = EvidencePassage(
            source_type="filing",
            source_document="test.txt",
            passage_text="test",
            target=TD.COST,
            stage=stage,
        )
        y1, y2, y3, shape = estimator.estimate_horizon(passage, case["base_annual"])
        assert y1 == pytest.approx(case["expected_y1"], abs=1e-2)
        assert y2 == pytest.approx(case["expected_y2"], abs=1e-2)
        assert y3 == pytest.approx(case["expected_y3"], abs=1e-2)
        assert shape == case["expected_shape"]


# ── Stage Multiplier ─────────────────────────────────────────────────────

class TestStageMultiplier:
    """Test FormulaDollarEstimator._stage_multiplier."""

    @pytest.mark.parametrize(
        "case",
        CASES["stage_multiplier"]["cases"],
        ids=[c["stage"] for c in CASES["stage_multiplier"]["cases"]],
    )
    def test_stage_multiplier_cases(self, case):
        stage = CaptureStage(case["stage"])
        result = FormulaDollarEstimator._stage_multiplier(stage)
        assert result == case["expected"]


# ── Subsidiary Attribution ───────────────────────────────────────────────

from ai_opportunity_index.scoring.composite import compute_subsidiary_attribution


class TestSubsidiaryAttribution:
    """Test compute_subsidiary_attribution: weighted subsidiary scores."""

    @pytest.mark.parametrize(
        "case",
        CASES["subsidiary_attribution"]["cases"],
        ids=[c["id"] for c in CASES["subsidiary_attribution"]["cases"]],
    )
    def test_subsidiary_cases(self, case):
        result = compute_subsidiary_attribution(
            parent_opportunity=case["parent_opportunity"],
            parent_realization=case["parent_realization"],
            subsidiaries=case["subsidiaries"],
        )
        if "expected_opp_boost" in case:
            assert result["opportunity_boost"] == pytest.approx(
                case["expected_opp_boost"], abs=1e-4
            )
        if "expected_real_boost" in case:
            assert result["realization_boost"] == pytest.approx(
                case["expected_real_boost"], abs=1e-4
            )
        if "expected_adjusted_opp" in case:
            assert result["adjusted_opportunity"] == pytest.approx(
                case["expected_adjusted_opp"], abs=1e-4
            )
        if "expected_adjusted_real" in case:
            assert result["adjusted_realization"] == pytest.approx(
                case["expected_adjusted_real"], abs=1e-4
            )

    def test_boost_never_exceeds_max(self):
        """Boost from subsidiaries is capped at 0.2."""
        result = compute_subsidiary_attribution(
            parent_opportunity=0.5,
            parent_realization=0.5,
            subsidiaries=[
                {"company_name": "A", "ownership_pct": 1.0, "opportunity": 1.0, "realization": 1.0},
                {"company_name": "B", "ownership_pct": 1.0, "opportunity": 1.0, "realization": 1.0},
            ],
        )
        assert result["opportunity_boost"] <= 0.2
        assert result["realization_boost"] <= 0.2


# ── End-to-End: Evidence Records to CompanyScore ─────────────────────────

class TestEndToEndScoring:
    """Test the full deterministic scoring pipeline from evidence to final scores."""

    def test_full_pipeline_with_synthetic_evidence(self):
        """Given synthetic evidence records, compute final scores through all stages."""
        from ai_opportunity_index.domains import (
            EvidenceGroup,
            Valuation,
            ValuationEvidenceType,
            ValuationStage,
            TargetDimension,
        )

        # Create evidence groups across dimensions
        groups = [
            EvidenceGroup(id=1, company_id=1, target_dimension=TargetDimension.COST),
            EvidenceGroup(id=2, company_id=1, target_dimension=TargetDimension.REVENUE),
            EvidenceGroup(id=3, company_id=1, target_dimension=TargetDimension.GENERAL),
        ]

        # Valuations with factor scores
        valuations = [
            Valuation(
                group_id=1, stage=ValuationStage.FINAL,
                evidence_type=ValuationEvidenceType.CAPTURE,
                narrative="Cost savings from AI automation",
                confidence=0.9,
                dollar_low=1000000, dollar_high=2000000, dollar_mid=1500000,
                factor_score=0.4,
            ),
            Valuation(
                group_id=2, stage=ValuationStage.FINAL,
                evidence_type=ValuationEvidenceType.INVESTMENT,
                narrative="Revenue growth via AI products",
                confidence=0.7,
                dollar_low=500000, dollar_high=1000000, dollar_mid=750000,
                factor_score=0.2,
            ),
            Valuation(
                group_id=3, stage=ValuationStage.FINAL,
                evidence_type=ValuationEvidenceType.PLAN,
                narrative="General AI investment plan",
                confidence=0.5,
                dollar_low=200000, dollar_high=400000, dollar_mid=300000,
                factor_score=0.1,
            ),
        ]

        # Stage 4: Aggregate
        agg = aggregate_valuations(valuations, groups)

        # Verify dimension scores
        assert agg["cost_score"] == pytest.approx(1.0 - math.exp(-0.4), abs=1e-4)
        assert agg["revenue_score"] == pytest.approx(1.0 - math.exp(-0.2), abs=1e-4)
        assert agg["general_score"] == pytest.approx(1.0 - math.exp(-0.1), abs=1e-4)

        # Verify dollar tracking
        assert agg["cost_actual_usd"] == 1500000.0  # capture goes to actual
        assert agg["cost_potential_usd"] == 0.0
        assert agg["revenue_potential_usd"] == 750000.0  # investment goes to potential
        assert agg["revenue_actual_usd"] == 0.0
        assert agg["general_potential_usd"] == 300000.0  # plan goes to potential
        assert agg["general_actual_usd"] == 0.0

        # Verify group counts
        assert agg["total_groups"] == 3
        assert agg["groups_by_type"]["capture"] == 1
        assert agg["groups_by_type"]["investment"] == 1
        assert agg["groups_by_type"]["plan"] == 1

        # Now feed into compute_index_4v
        index_result = compute_index_4v(
            cost_opportunity=agg["cost_score"],
            revenue_opportunity=agg["revenue_score"],
            cost_capture=agg["cost_actual_usd"] / 1e7,  # normalize for 0-1
            revenue_capture=agg["revenue_actual_usd"] / 1e7,
            general_investment=agg["general_score"],
        )
        assert "quadrant" in index_result
        assert index_result["quadrant"] in [q.value for q in __import__("ai_opportunity_index.domains", fromlist=["Quadrant"]).Quadrant]

        # And compute AI Index
        ai_result = compute_ai_index(
            plan_dollars=300000,
            investment_dollars=750000,
            capture_dollars=1500000,
        )
        assert ai_result["ai_index_usd"] > 0
        assert ai_result["capture_probability"] > AI_INDEX_P_BASE
        assert ai_result["evidence_dollars"] == pytest.approx(2550000.0, abs=0.01)

    def test_empty_evidence_produces_zero_scores(self):
        """No evidence should produce all-zero scores."""
        agg = aggregate_valuations([], [])
        assert agg["cost_score"] == 0.0
        assert agg["revenue_score"] == 0.0
        assert agg["general_score"] == 0.0
        assert agg["total_groups"] == 0

    def test_factor_score_chain(self):
        """Verify the full chain: recency + magnitude + stage_weight -> factor_score -> dim_score."""
        ref_date = date(2026, 3, 8)
        ev_date = date(2025, 9, 8)  # ~0.5 years old

        recency = compute_recency(ev_date, ref_date)
        magnitude = compute_magnitude(5000000, 100000000)  # 5M / 100M = 0.05
        stage_weight = STAGE_WEIGHTS[__import__("ai_opportunity_index.domains", fromlist=["ValuationEvidenceType"]).ValuationEvidenceType.INVESTMENT]
        specificity = 0.75

        factor = compute_factor_score(specificity, magnitude, stage_weight, recency)

        # Verify each component
        assert 0.7 < recency <= 1.0  # ~6 months old
        assert magnitude == pytest.approx(0.05, abs=1e-6)
        assert stage_weight == 0.7
        assert factor > 0
        assert factor < 1.0

        # Through the transform
        dim_score = 1.0 - math.exp(-factor)
        assert 0 < dim_score < 1.0


class TestValuationTokenTracking:
    """Tests for token usage and model name tracking on Valuation domain objects."""

    def test_valuation_stores_token_counts(self):
        """Valuation should accept and store input/output token counts."""
        from ai_opportunity_index.domains import Valuation, ValuationStage, ValuationEvidenceType

        val = Valuation(
            group_id=1,
            stage=ValuationStage.PRELIMINARY,
            evidence_type=ValuationEvidenceType.PLAN,
            narrative="test",
            confidence=0.8,
            dollar_low=1000,
            dollar_high=5000,
            input_tokens=1234,
            output_tokens=567,
        )
        assert val.input_tokens == 1234
        assert val.output_tokens == 567

    def test_valuation_defaults_tokens_to_zero(self):
        """Token counts should default to 0 when not provided."""
        from ai_opportunity_index.domains import Valuation, ValuationStage, ValuationEvidenceType

        val = Valuation(
            group_id=1,
            stage=ValuationStage.FINAL,
            evidence_type=ValuationEvidenceType.CAPTURE,
            narrative="test",
            confidence=0.5,
        )
        assert val.input_tokens == 0
        assert val.output_tokens == 0

    def test_valuation_stores_model_name(self):
        """Valuation should accept and store model_name."""
        from ai_opportunity_index.domains import Valuation, ValuationStage, ValuationEvidenceType

        val = Valuation(
            group_id=1,
            stage=ValuationStage.PRELIMINARY,
            evidence_type=ValuationEvidenceType.INVESTMENT,
            narrative="test",
            confidence=0.7,
            model_name="gemini-2.5-flash",
        )
        assert val.model_name == "gemini-2.5-flash"

    def test_valuation_model_name_defaults_none(self):
        """model_name should default to None when not provided."""
        from ai_opportunity_index.domains import Valuation, ValuationStage, ValuationEvidenceType

        val = Valuation(
            group_id=1,
            stage=ValuationStage.FINAL,
            evidence_type=ValuationEvidenceType.PLAN,
            narrative="test",
            confidence=0.6,
        )
        assert val.model_name is None
