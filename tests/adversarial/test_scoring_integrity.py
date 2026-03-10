"""Adversarial tests for scoring pipeline correctness.

These tests verify the deterministic math used for REAL MONEY investment
decisions. Every test must prove the formula is right.
"""

from __future__ import annotations

import math
from datetime import date

import pytest

# ---------------------------------------------------------------------------
# Markers
# ---------------------------------------------------------------------------
pytestmark = pytest.mark.adversarial


# ===================================================================
# 1. AI Index sigmoid model correctness
# ===================================================================


class TestAIIndexSigmoidModel:
    """P(capture) = P_BASE + (1 - P_BASE) * sigmoid(K * (weighted - 0.35))

    With P_BASE = 0.05, K = 4.0, midpoint = 0.35.
    """

    def _p_capture(self, weighted: float) -> float:
        """Recompute P(capture) from the formula in composite.py."""
        P_BASE = 0.05
        K = 4.0
        midpoint = 0.35
        raw_sigmoid = 1.0 / (1.0 + math.exp(-K * (weighted - midpoint)))
        return P_BASE + (1.0 - P_BASE) * raw_sigmoid

    def test_sigmoid_at_midpoint_equals_base_plus_half_range(self):
        """At weighted=0.35 (midpoint), sigmoid=0.5, so P = 0.05 + 0.95*0.5 = 0.525."""
        p = self._p_capture(0.35)
        assert abs(p - 0.525) < 1e-10, f"Expected 0.525, got {p}"

    def test_sigmoid_floor_at_zero_weighted(self):
        """At weighted=0, P > P_BASE (0.05) but not much higher."""
        p = self._p_capture(0.0)
        assert p > 0.05, f"Expected P > 0.05, got {p}"
        assert p < 0.30, f"At weighted=0, P should be modest; got {p}"

    def test_sigmoid_ceiling_at_one_weighted(self):
        """At weighted=1.0, P should be well above the midpoint.

        With K=4.0 and midpoint=0.35: sigmoid(4*(1-0.35)) = sigmoid(2.6) ~ 0.931.
        So P ~ 0.05 + 0.95*0.931 ~ 0.934. Not near 1.0 but substantially above 0.525.
        """
        p = self._p_capture(1.0)
        assert p > 0.90, f"Expected P > 0.90 at weighted=1.0, got {p}"
        assert p <= 1.0, f"P must not exceed 1.0; got {p}"
        # Verify exact value matches formula
        expected = 0.05 + 0.95 * (1.0 / (1.0 + math.exp(-4.0 * (1.0 - 0.35))))
        assert abs(p - expected) < 1e-10

    def test_output_range_always_0_05_to_1_0(self):
        """For any weighted in [0, 1], P(capture) must be in [0.05, 1.0]."""
        for w_int in range(0, 101):
            w = w_int / 100.0
            p = self._p_capture(w)
            assert 0.05 <= p <= 1.0, f"Out of range at w={w}: P={p}"

    def test_monotonicity_over_full_range(self):
        """P(capture) must be strictly monotonically increasing in weighted."""
        prev = self._p_capture(0.0)
        for w_int in range(1, 101):
            w = w_int / 100.0
            curr = self._p_capture(w)
            assert curr > prev, (
                f"Monotonicity violation at w={w}: P({w})={curr} <= P({w-0.01})={prev}"
            )
            prev = curr

    def test_compute_ai_index_uses_correct_formula(self):
        """Verify compute_ai_index returns values consistent with the formula."""
        from ai_opportunity_index.scoring.composite import compute_ai_index
        from ai_opportunity_index.config import AI_INDEX_STAGE_WEIGHTS

        plan = 100_000.0
        invest = 500_000.0
        capture = 200_000.0
        total = plan + invest + capture
        weighted = (
            AI_INDEX_STAGE_WEIGHTS["plan"] * plan
            + AI_INDEX_STAGE_WEIGHTS["investment"] * invest
            + AI_INDEX_STAGE_WEIGHTS["capture"] * capture
        ) / total

        result = compute_ai_index(plan, invest, capture)
        expected_p = self._p_capture(weighted)

        assert abs(result["capture_probability"] - round(expected_p, 4)) < 1e-4, (
            f"Mismatch: code returned {result['capture_probability']}, "
            f"formula gives {expected_p}"
        )

    def test_compute_ai_index_zero_evidence_returns_p_base(self):
        """Zero dollars should yield P_BASE = 0.05."""
        from ai_opportunity_index.scoring.composite import compute_ai_index

        result = compute_ai_index(0, 0, 0)
        assert result["capture_probability"] == 0.05
        assert result["ai_index_usd"] == 0.0

    def test_compute_ai_index_pure_capture_high_probability(self):
        """Pure capture evidence should yield the highest probability."""
        from ai_opportunity_index.scoring.composite import compute_ai_index
        from ai_opportunity_index.config import AI_INDEX_STAGE_WEIGHTS

        # Pure capture: weighted = stage_weights["capture"] = 0.70
        result = compute_ai_index(0, 0, 1_000_000)
        expected_weighted = AI_INDEX_STAGE_WEIGHTS["capture"]
        expected_p = self._p_capture(expected_weighted)
        assert abs(result["capture_probability"] - round(expected_p, 4)) < 1e-4

    def test_negative_sigmoid_inputs_dont_break(self):
        """Negative weighted values should still produce valid probabilities."""
        p = self._p_capture(-1.0)
        assert 0.05 <= p <= 1.0, f"Negative input broke range: P={p}"


# ===================================================================
# 2. Quadrant assignment
# ===================================================================


class TestQuadrantAssignment:
    """(opportunity >= 0.5, realization >= 0.5) -> correct quadrant labels."""

    def test_high_opp_high_real(self):
        from ai_opportunity_index.scoring.composite import compute_index
        from ai_opportunity_index.domains import Quadrant

        result = compute_index(0.8, 0.7)
        assert result["quadrant"] == Quadrant.HIGH_OPP_HIGH_REAL

    def test_high_opp_low_real(self):
        from ai_opportunity_index.scoring.composite import compute_index
        from ai_opportunity_index.domains import Quadrant

        result = compute_index(0.8, 0.3)
        assert result["quadrant"] == Quadrant.HIGH_OPP_LOW_REAL

    def test_low_opp_high_real(self):
        from ai_opportunity_index.scoring.composite import compute_index
        from ai_opportunity_index.domains import Quadrant

        result = compute_index(0.3, 0.7)
        assert result["quadrant"] == Quadrant.LOW_OPP_HIGH_REAL

    def test_low_opp_low_real(self):
        from ai_opportunity_index.scoring.composite import compute_index
        from ai_opportunity_index.domains import Quadrant

        result = compute_index(0.3, 0.3)
        assert result["quadrant"] == Quadrant.LOW_OPP_LOW_REAL

    def test_boundary_exact_threshold_is_high(self):
        """Exactly at 0.5 should be classified as high (>= comparison)."""
        from ai_opportunity_index.scoring.composite import compute_index
        from ai_opportunity_index.domains import Quadrant

        result = compute_index(0.5, 0.5)
        assert result["quadrant"] == Quadrant.HIGH_OPP_HIGH_REAL

    def test_boundary_just_below_threshold_is_low(self):
        """0.4999 should be classified as low."""
        from ai_opportunity_index.scoring.composite import compute_index
        from ai_opportunity_index.domains import Quadrant

        result = compute_index(0.4999, 0.4999)
        assert result["quadrant"] == Quadrant.LOW_OPP_LOW_REAL

    def test_boundary_opp_high_real_exact(self):
        """Opportunity exactly at threshold, realization exactly at threshold."""
        from ai_opportunity_index.scoring.composite import compute_index
        from ai_opportunity_index.domains import Quadrant

        result = compute_index(0.5, 0.4999)
        assert result["quadrant"] == Quadrant.HIGH_OPP_LOW_REAL

    def test_quadrant_labels_all_mapped(self):
        """Every Quadrant enum value must have a label in QUADRANT_LABELS."""
        from ai_opportunity_index.domains import Quadrant
        from ai_opportunity_index.config import QUADRANT_LABELS

        for q in Quadrant:
            assert q.value in QUADRANT_LABELS, f"Missing label for {q}"

    def test_extreme_values(self):
        """Scores of 0.0 and 1.0 should produce correct quadrants."""
        from ai_opportunity_index.scoring.composite import compute_index
        from ai_opportunity_index.domains import Quadrant

        assert compute_index(0.0, 0.0)["quadrant"] == Quadrant.LOW_OPP_LOW_REAL
        assert compute_index(1.0, 1.0)["quadrant"] == Quadrant.HIGH_OPP_HIGH_REAL
        assert compute_index(0.0, 1.0)["quadrant"] == Quadrant.LOW_OPP_HIGH_REAL
        assert compute_index(1.0, 0.0)["quadrant"] == Quadrant.HIGH_OPP_LOW_REAL


# ===================================================================
# 3. Factor score computation
# ===================================================================


class TestFactorScore:
    """factor_score = specificity * magnitude * stage_weight * recency * authority_weight"""

    def test_basic_product(self):
        from ai_opportunity_index.scoring.evidence_valuation import compute_factor_score

        result = compute_factor_score(0.8, 0.5, 0.7, 0.9, 1.0)
        expected = 0.8 * 0.5 * 0.7 * 0.9 * 1.0
        assert abs(result - expected) < 1e-10

    def test_commutativity(self):
        """Multiplication is commutative; reordering args shouldn't matter."""
        from ai_opportunity_index.scoring.evidence_valuation import compute_factor_score

        a, b, c, d, e = 0.7, 0.3, 1.0, 0.85, 0.6
        r1 = compute_factor_score(a, b, c, d, e)
        # Mathematically the same product regardless of order
        r2 = compute_factor_score(b, a, d, c, e)
        # Since multiplication is commutative, the raw product should match
        expected = a * b * c * d * e
        assert abs(r1 - expected) < 1e-10
        assert abs(r2 - expected) < 1e-10

    def test_zero_specificity_zeroes_score(self):
        from ai_opportunity_index.scoring.evidence_valuation import compute_factor_score

        result = compute_factor_score(0.0, 0.5, 0.7, 0.9, 1.0)
        assert result == 0.0

    def test_zero_magnitude_zeroes_score(self):
        from ai_opportunity_index.scoring.evidence_valuation import compute_factor_score

        result = compute_factor_score(0.8, 0.0, 0.7, 0.9, 1.0)
        assert result == 0.0

    def test_zero_authority_zeroes_score(self):
        from ai_opportunity_index.scoring.evidence_valuation import compute_factor_score

        result = compute_factor_score(0.8, 0.5, 0.7, 0.9, 0.0)
        assert result == 0.0

    def test_all_ones_produces_one(self):
        from ai_opportunity_index.scoring.evidence_valuation import compute_factor_score

        result = compute_factor_score(1.0, 1.0, 1.0, 1.0, 1.0)
        assert abs(result - 1.0) < 1e-10

    def test_default_authority_weight(self):
        """authority_weight defaults to 1.0 and should not affect the product."""
        from ai_opportunity_index.scoring.evidence_valuation import compute_factor_score

        r_with = compute_factor_score(0.5, 0.5, 0.5, 0.5, 1.0)
        r_default = compute_factor_score(0.5, 0.5, 0.5, 0.5)
        assert abs(r_with - r_default) < 1e-10

    def test_magnitude_computation(self):
        """compute_magnitude should be dollar_mid / revenue, capped at 1.0."""
        from ai_opportunity_index.scoring.evidence_valuation import compute_magnitude

        assert compute_magnitude(100_000, 1_000_000) == pytest.approx(0.1)
        assert compute_magnitude(2_000_000, 1_000_000) == 1.0  # capped
        assert compute_magnitude(None, 1_000_000) == 0.0
        assert compute_magnitude(100_000, 0) == 0.0
        assert compute_magnitude(100_000, None) == 0.0

    def test_recency_today_is_one(self):
        from ai_opportunity_index.scoring.evidence_valuation import compute_recency

        today = date.today()
        assert compute_recency(today, today) == 1.0

    def test_recency_one_year_old(self):
        """After 1 year, recency = 0.7^1 = 0.7."""
        from ai_opportunity_index.scoring.evidence_valuation import compute_recency

        ref = date(2025, 1, 1)
        old = date(2024, 1, 1)
        r = compute_recency(old, ref)
        # Approximately 0.7 (slight deviation due to 365.25 vs 365)
        assert abs(r - 0.7) < 0.01

    def test_recency_floor(self):
        """Very old evidence should hit the floor of 0.3."""
        from ai_opportunity_index.scoring.evidence_valuation import compute_recency

        ref = date(2025, 1, 1)
        ancient = date(2010, 1, 1)
        r = compute_recency(ancient, ref)
        assert r == pytest.approx(0.3, abs=0.01)

    def test_recency_none_date_returns_floor(self):
        from ai_opportunity_index.scoring.evidence_valuation import compute_recency

        assert compute_recency(None) == 0.3


# ===================================================================
# 4. Confidence calibration
# ===================================================================


class TestConfidenceCalibration:

    def test_platt_scaling_llm_source(self):
        """LLM uses platt scaling: sigmoid(2*x - 1)."""
        from ai_opportunity_index.scoring.calibration import calibrate_confidence, _sigmoid

        raw = 0.8
        # Expected: sigmoid(2 * 0.8 - 1) = sigmoid(0.6)
        expected_raw = _sigmoid(2.0 * 0.8 - 1.0)
        result = calibrate_confidence(raw, "llm")
        expected = max(0.05, min(0.95, expected_raw))
        assert abs(result - expected) < 1e-10

    def test_linear_scaling_sec_filing(self):
        """SEC filing uses linear scaling with factor 1.1."""
        from ai_opportunity_index.scoring.calibration import calibrate_confidence

        raw = 0.8
        # Expected: clamp(0.8 * 1.1) = clamp(0.88)
        result = calibrate_confidence(raw, "sec_filing")
        assert abs(result - 0.88) < 1e-10

    def test_linear_scaling_news(self):
        """News uses linear scaling with factor 0.8."""
        from ai_opportunity_index.scoring.calibration import calibrate_confidence

        raw = 0.7
        result = calibrate_confidence(raw, "news")
        assert abs(result - 0.56) < 1e-10

    def test_floor_enforced(self):
        """Calibrated confidence must not go below 0.05."""
        from ai_opportunity_index.scoring.calibration import calibrate_confidence

        result = calibrate_confidence(0.0, "llm")
        assert result >= 0.05

    def test_ceiling_enforced(self):
        """Calibrated confidence must not exceed 0.95."""
        from ai_opportunity_index.scoring.calibration import calibrate_confidence

        result = calibrate_confidence(1.0, "sec_filing")
        assert result <= 0.95

    def test_floor_ceiling_bounds_all_source_types(self):
        """For all source types, output must be in [0.05, 0.95]."""
        from ai_opportunity_index.scoring.calibration import calibrate_confidence

        for source in ["llm", "sec_filing", "news", "analyst", "github"]:
            for raw_int in range(0, 101):
                raw = raw_int / 100.0
                result = calibrate_confidence(raw, source)
                assert 0.05 <= result <= 0.95, (
                    f"Out of bounds: source={source}, raw={raw}, result={result}"
                )

    def test_unknown_source_falls_back_to_llm(self):
        """Unknown source types should use the LLM calibration curve."""
        from ai_opportunity_index.scoring.calibration import calibrate_confidence

        llm_result = calibrate_confidence(0.7, "llm")
        unknown_result = calibrate_confidence(0.7, "made_up_source")
        assert abs(llm_result - unknown_result) < 1e-10

    def test_platt_at_midpoint(self):
        """At x=0.5, LLM platt gives sigmoid(2*0.5 - 1) = sigmoid(0) = 0.5."""
        from ai_opportunity_index.scoring.calibration import calibrate_confidence

        result = calibrate_confidence(0.5, "llm")
        assert abs(result - 0.5) < 1e-10


# ===================================================================
# 5. Dollar sanity checking
# ===================================================================


class TestDollarSanityChecking:

    def test_cap_at_0_5x_revenue(self):
        from ai_opportunity_index.scoring.calibration import check_dollar_sanity

        adjusted, warnings = check_dollar_sanity(3_000_000_000, 1_000_000_000, None)
        assert adjusted == 500_000_000  # 0.5x revenue
        assert any("revenue" in w.lower() for w in warnings)

    def test_cap_at_0_5x_market_cap(self):
        from ai_opportunity_index.scoring.calibration import check_dollar_sanity

        adjusted, warnings = check_dollar_sanity(
            600_000_000, None, 1_000_000_000
        )
        assert adjusted == 500_000_000  # 0.5x market cap
        assert any("market cap" in w.lower() for w in warnings)

    def test_negative_floored_to_zero(self):
        from ai_opportunity_index.scoring.calibration import check_dollar_sanity

        adjusted, warnings = check_dollar_sanity(-500_000, None, None)
        assert adjusted == 0.0
        assert any("negative" in w.lower() for w in warnings)

    def test_round_number_detection(self):
        """Suspiciously round numbers (e.g. $1,000,000,000) should trigger a warning."""
        from ai_opportunity_index.scoring.calibration import check_dollar_sanity

        _, warnings = check_dollar_sanity(1_000_000_000, None, None)
        assert any("round" in w.lower() for w in warnings)

    def test_non_round_number_no_warning(self):
        """Non-round numbers like $1,234,567 should not trigger round-number warning."""
        from ai_opportunity_index.scoring.calibration import check_dollar_sanity

        _, warnings = check_dollar_sanity(1_234_567, None, None)
        assert not any("round" in w.lower() for w in warnings)

    def test_revenue_cap_applied_before_market_cap(self):
        """If both caps apply, the tighter one wins (due to sequential application)."""
        from ai_opportunity_index.scoring.calibration import check_dollar_sanity

        # Revenue cap = 0.5 * 100M = 50M. Market cap cap = 0.5 * 300M = 150M.
        # Revenue cap applied first (to 50M), market cap doesn't trigger.
        adjusted, _ = check_dollar_sanity(500_000_000, 100_000_000, 300_000_000)
        assert adjusted == 50_000_000

    def test_zero_estimate_no_warnings(self):
        from ai_opportunity_index.scoring.calibration import check_dollar_sanity

        adjusted, warnings = check_dollar_sanity(0, 1_000_000, 5_000_000)
        assert adjusted == 0.0
        assert len(warnings) == 0

    def test_estimate_within_bounds_unchanged(self):
        from ai_opportunity_index.scoring.calibration import check_dollar_sanity

        adjusted, warnings = check_dollar_sanity(
            100_000, 1_000_000, 1_000_000
        )
        assert adjusted == 100_000
        # Only round-number warnings possible, not cap warnings
        assert not any("revenue" in w.lower() or "market cap" in w.lower() for w in warnings)


# ===================================================================
# 6. Cross-source verification math
# ===================================================================


class TestCrossSourceVerification:

    def test_agreement_ratio_above_threshold_is_confirmation(self):
        """min(a,b)/max(a,b) >= 0.5 should be a confirmation."""
        from ai_opportunity_index.fact_graph.verification import _compare_pair, SourceAgreement

        result = _compare_pair("A", "B", 100, 150, "cost")
        assert isinstance(result, SourceAgreement)
        assert result.agreement_ratio == pytest.approx(100 / 150)

    def test_agreement_ratio_below_threshold_is_contradiction(self):
        """min(a,b)/max(a,b) < 0.5 should be a contradiction."""
        from ai_opportunity_index.fact_graph.verification import _compare_pair, SourceDisagreement

        result = _compare_pair("A", "B", 100, 300, "cost")
        assert isinstance(result, SourceDisagreement)
        assert result.disagreement_ratio == pytest.approx(300 / 100)

    def test_exact_boundary_0_5_is_confirmation(self):
        """Ratio of exactly 0.5 should be a confirmation (>= threshold)."""
        from ai_opportunity_index.fact_graph.verification import _compare_pair, SourceAgreement

        result = _compare_pair("A", "B", 50, 100, "cost")
        assert isinstance(result, SourceAgreement)

    def test_zero_values_return_none(self):
        from ai_opportunity_index.fact_graph.verification import _compare_pair

        assert _compare_pair("A", "B", 0, 100, "cost") is None
        assert _compare_pair("A", "B", 100, 0, "cost") is None

    def test_confidence_adjustment_max_boost(self):
        """Many confirmations should cap at +0.1."""
        from ai_opportunity_index.fact_graph.verification import (
            compute_confidence_adjustment_from_counts,
        )

        # 100 confirmations * 0.03 = 3.0, but capped at 0.1
        adj = compute_confidence_adjustment_from_counts(100, 0)
        assert adj == pytest.approx(0.1)

    def test_confidence_adjustment_min_penalty(self):
        """Many contradictions should floor at -0.2."""
        from ai_opportunity_index.fact_graph.verification import (
            compute_confidence_adjustment_from_counts,
        )

        adj = compute_confidence_adjustment_from_counts(0, 100)
        assert adj == pytest.approx(-0.2)

    def test_confidence_adjustment_neutral(self):
        """No data should yield 0.0."""
        from ai_opportunity_index.fact_graph.verification import (
            compute_confidence_adjustment_from_counts,
        )

        assert compute_confidence_adjustment_from_counts(0, 0) == 0.0

    def test_confidence_adjustment_mixed(self):
        """3 confirmations (0.09) - 1 contradiction (0.05) = 0.04."""
        from ai_opportunity_index.fact_graph.verification import (
            compute_confidence_adjustment_from_counts,
        )

        adj = compute_confidence_adjustment_from_counts(3, 1)
        assert adj == pytest.approx(0.04)

    def test_agreement_score_all_confirmations(self):
        """If all pairs agree, agreement_score = 1.0."""
        from ai_opportunity_index.fact_graph.verification import (
            _compute_agreement_score, SourceAgreement,
        )

        confs = [
            SourceAgreement(source_a="A", source_b="B", dimension="cost",
                           dollar_a=100, dollar_b=120, agreement_ratio=0.83),
        ]
        score = _compute_agreement_score(confs, [])
        assert score == pytest.approx(1.0)

    def test_agreement_score_no_data(self):
        from ai_opportunity_index.fact_graph.verification import _compute_agreement_score

        assert _compute_agreement_score([], []) == 0.0

    def test_severity_classification(self):
        """Verify _classify_severity thresholds."""
        from ai_opportunity_index.fact_graph.verification import _classify_severity

        assert _classify_severity(1.5) == "low"
        assert _classify_severity(2.0) == "medium"
        assert _classify_severity(3.0) == "medium"
        assert _classify_severity(3.1) == "high"


# ===================================================================
# 7. Constraint propagation (inference engine)
# ===================================================================


class TestConstraintPropagation:

    def _make_graph_with_nodes(self):
        """Create a simple FactGraph with two company nodes."""
        from ai_opportunity_index.fact_graph.graph import FactGraph
        from ai_opportunity_index.fact_graph.models import (
            FactNode, EntityType, InferenceMethod,
        )

        graph = FactGraph()
        n1 = FactNode(id="co1", entity_type=EntityType.COMPANY, label="Company A")
        n2 = FactNode(id="co2", entity_type=EntityType.COMPANY, label="Company B")
        graph.add_node(n1)
        graph.add_node(n2)
        return graph

    def test_equality_constraint_propagates(self):
        """If A = B and A is known, B should be derived."""
        from ai_opportunity_index.fact_graph.models import Constraint, InferenceMethod
        from ai_opportunity_index.fact_graph.inference import InferenceEngine

        graph = self._make_graph_with_nodes()
        graph.get_node("co1").set_attr("revenue", 1_000_000, p_true=0.9,
                                        method=InferenceMethod.OBSERVED)

        c = Constraint(
            name="rev_eq", description="Revenues must match",
            constraint_type="equality",
            participating_facts=["co1.revenue", "co2.revenue"],
            expression="co1.revenue == co2.revenue",
        )
        graph.add_constraint(c)

        engine = InferenceEngine(graph)
        result = engine.run_logical_pass()

        co2_attr = graph.get_node("co2").get_attr("revenue")
        assert co2_attr is not None
        assert float(co2_attr.value) == 1_000_000
        assert result.facts_updated >= 1

    def test_sum_constraint_derives_missing(self):
        """A + B = C: if A=300 and C=1000, B should be derived as 700."""
        from ai_opportunity_index.fact_graph.graph import FactGraph
        from ai_opportunity_index.fact_graph.models import (
            FactNode, EntityType, Constraint, InferenceMethod,
        )
        from ai_opportunity_index.fact_graph.inference import InferenceEngine

        graph = FactGraph()
        n = FactNode(id="co", entity_type=EntityType.COMPANY, label="Co")
        graph.add_node(n)
        n.set_attr("cost_a", 300, p_true=0.9, method=InferenceMethod.OBSERVED)
        n.set_attr("total", 1000, p_true=0.9, method=InferenceMethod.OBSERVED)

        c = Constraint(
            name="sum_costs", description="Parts sum to total",
            constraint_type="sum",
            participating_facts=["co.cost_a", "co.cost_b", "co.total"],
            expression="sum",
        )
        graph.add_constraint(c)

        engine = InferenceEngine(graph)
        engine.run_logical_pass()

        cost_b = graph.get_node("co").get_attr("cost_b")
        assert cost_b is not None
        assert abs(float(cost_b.value) - 700) < 1e-6

    def test_implication_constraint_antecedent_true(self):
        """A => B: if A is true (nonzero), B should be derived as 1.0."""
        from ai_opportunity_index.fact_graph.graph import FactGraph
        from ai_opportunity_index.fact_graph.models import (
            FactNode, EntityType, Constraint, InferenceMethod,
        )
        from ai_opportunity_index.fact_graph.inference import InferenceEngine

        graph = FactGraph()
        n = FactNode(id="co", entity_type=EntityType.COMPANY, label="Co")
        graph.add_node(n)
        n.set_attr("has_ai_team", 1.0, p_true=0.95, method=InferenceMethod.OBSERVED)

        c = Constraint(
            name="ai_impl", description="AI team implies AI investment",
            constraint_type="implication",
            participating_facts=["co.has_ai_team", "co.has_ai_investment"],
            expression="has_ai_team => has_ai_investment",
        )
        graph.add_constraint(c)

        engine = InferenceEngine(graph)
        engine.run_logical_pass()

        inv = graph.get_node("co").get_attr("has_ai_investment")
        assert inv is not None
        assert float(inv.value) == 1.0

    def test_implication_contradiction_detected(self):
        """A => B: if A=true and B=false, constraint should be violated."""
        from ai_opportunity_index.fact_graph.graph import FactGraph
        from ai_opportunity_index.fact_graph.models import (
            FactNode, EntityType, Constraint, InferenceMethod,
        )
        from ai_opportunity_index.fact_graph.inference import InferenceEngine

        graph = FactGraph()
        n = FactNode(id="co", entity_type=EntityType.COMPANY, label="Co")
        graph.add_node(n)
        n.set_attr("has_ai_team", 1.0, p_true=0.95, method=InferenceMethod.OBSERVED)
        n.set_attr("has_ai_investment", 0.0, p_true=0.95, method=InferenceMethod.OBSERVED)

        c = Constraint(
            name="ai_impl", description="AI team implies AI investment",
            constraint_type="implication",
            participating_facts=["co.has_ai_team", "co.has_ai_investment"],
            expression="has_ai_team => has_ai_investment",
        )
        graph.add_constraint(c)

        engine = InferenceEngine(graph)
        result = engine.run_logical_pass()

        assert result.constraints_violated >= 1

    def test_mutex_constraint_derives_last_option(self):
        """exactly_one(A, B, C): if A=0 and B=0, C must be 1."""
        from ai_opportunity_index.fact_graph.graph import FactGraph
        from ai_opportunity_index.fact_graph.models import (
            FactNode, EntityType, Constraint, InferenceMethod,
        )
        from ai_opportunity_index.fact_graph.inference import InferenceEngine

        graph = FactGraph()
        n = FactNode(id="co", entity_type=EntityType.COMPANY, label="Co")
        graph.add_node(n)
        n.set_attr("is_plan", 0.0, p_true=0.9, method=InferenceMethod.OBSERVED)
        n.set_attr("is_investment", 0.0, p_true=0.9, method=InferenceMethod.OBSERVED)

        c = Constraint(
            name="stage_mutex", description="Exactly one stage",
            constraint_type="mutex",
            participating_facts=["co.is_plan", "co.is_investment", "co.is_capture"],
            expression="exactly_one",
        )
        graph.add_constraint(c)

        engine = InferenceEngine(graph)
        engine.run_logical_pass()

        cap = graph.get_node("co").get_attr("is_capture")
        assert cap is not None
        assert float(cap.value) == 1.0

    def test_mutex_violation_multiple_true(self):
        """If two are true in a mutex, constraint should be violated."""
        from ai_opportunity_index.fact_graph.graph import FactGraph
        from ai_opportunity_index.fact_graph.models import (
            FactNode, EntityType, Constraint, InferenceMethod,
        )
        from ai_opportunity_index.fact_graph.inference import InferenceEngine

        graph = FactGraph()
        n = FactNode(id="co", entity_type=EntityType.COMPANY, label="Co")
        graph.add_node(n)
        n.set_attr("is_plan", 1.0, p_true=0.9, method=InferenceMethod.OBSERVED)
        n.set_attr("is_investment", 1.0, p_true=0.9, method=InferenceMethod.OBSERVED)
        n.set_attr("is_capture", 0.0, p_true=0.9, method=InferenceMethod.OBSERVED)

        c = Constraint(
            name="stage_mutex", description="Exactly one stage",
            constraint_type="mutex",
            participating_facts=["co.is_plan", "co.is_investment", "co.is_capture"],
            expression="exactly_one",
        )
        graph.add_constraint(c)

        engine = InferenceEngine(graph)
        result = engine.run_logical_pass()

        assert result.constraints_violated >= 1


# ===================================================================
# 8. Temporal weight decay
# ===================================================================


class TestTemporalWeightDecay:

    def test_day_zero_is_one(self):
        from ai_opportunity_index.scoring.calibration import temporal_weight

        assert temporal_weight(0, "news") == 1.0
        assert temporal_weight(0, "sec_filing") == 1.0

    def test_negative_days_is_one(self):
        from ai_opportunity_index.scoring.calibration import temporal_weight

        assert temporal_weight(-5, "news") == 1.0

    def test_half_life_produces_0_5(self):
        """At exactly the half-life, weight should be 0.5."""
        from ai_opportunity_index.scoring.calibration import temporal_weight, _HALF_LIVES

        for source_type, half_life in _HALF_LIVES.items():
            w = temporal_weight(int(half_life), source_type)
            assert abs(w - 0.5) < 0.01, (
                f"At half-life={half_life} for {source_type}, expected ~0.5, got {w}"
            )

    def test_double_half_life_produces_0_25(self):
        """At 2x the half-life, weight should be ~0.25."""
        from ai_opportunity_index.scoring.calibration import temporal_weight, _HALF_LIVES

        for source_type, half_life in _HALF_LIVES.items():
            w = temporal_weight(int(2 * half_life), source_type)
            assert abs(w - 0.25) < 0.02, (
                f"At 2x half-life for {source_type}, expected ~0.25, got {w}"
            )

    def test_monotonic_decay(self):
        """Weight must decrease as days_old increases."""
        from ai_opportunity_index.scoring.calibration import temporal_weight

        prev = temporal_weight(0, "news")
        for d in range(1, 365):
            curr = temporal_weight(d, "news")
            assert curr < prev, f"Non-monotonic at day {d}: {curr} >= {prev}"
            prev = curr

    def test_weight_always_positive(self):
        """Weight must always be > 0 (exponential decay never reaches 0)."""
        from ai_opportunity_index.scoring.calibration import temporal_weight

        for d in [1, 10, 100, 1000, 10000]:
            w = temporal_weight(d, "sec_filing")
            assert w > 0, f"Weight hit zero at day {d}"

    def test_sec_filing_decays_slowly(self):
        """SEC filings (365-day half-life) should retain > 0.9 after 30 days."""
        from ai_opportunity_index.scoring.calibration import temporal_weight

        w = temporal_weight(30, "sec_filing")
        assert w > 0.9, f"SEC filing decayed too fast: {w} after 30 days"

    def test_news_decays_fast(self):
        """News (30-day half-life) should be ~0.5 after 30 days."""
        from ai_opportunity_index.scoring.calibration import temporal_weight

        w = temporal_weight(30, "news")
        assert abs(w - 0.5) < 0.01

    def test_default_half_life_for_unknown_source(self):
        """Unknown source types should use _DEFAULT_HALF_LIFE (90 days)."""
        from ai_opportunity_index.scoring.calibration import temporal_weight

        w = temporal_weight(90, "unknown_source_xyz")
        assert abs(w - 0.5) < 0.01


# ===================================================================
# 9. Score bounds auditing
# ===================================================================


class TestScoreBoundsAuditing:

    def test_out_of_bounds_score_flagged_critical(self):
        from ai_opportunity_index.fact_graph.auditor import audit_score_bounds

        findings = audit_score_bounds(1, "TEST", 1.5, 0.5)
        critical = [f for f in findings if f.severity == "critical"]
        assert len(critical) >= 1
        assert any("opportunity" in f.description for f in critical)

    def test_negative_score_flagged_critical(self):
        from ai_opportunity_index.fact_graph.auditor import audit_score_bounds

        findings = audit_score_bounds(1, "TEST", -0.1, 0.5)
        critical = [f for f in findings if f.severity == "critical"]
        assert len(critical) >= 1

    def test_nan_score_flagged_critical(self):
        from ai_opportunity_index.fact_graph.auditor import audit_score_bounds

        findings = audit_score_bounds(1, "TEST", float("nan"), 0.5)
        critical = [f for f in findings if f.severity == "critical"]
        assert len(critical) >= 1
        assert any("nan" in f.description.lower() for f in critical)

    def test_inf_score_flagged_critical(self):
        from ai_opportunity_index.fact_graph.auditor import audit_score_bounds

        findings = audit_score_bounds(1, "TEST", float("inf"), 0.5)
        critical = [f for f in findings if f.severity == "critical"]
        assert len(critical) >= 1

    def test_negative_inf_score_flagged_critical(self):
        from ai_opportunity_index.fact_graph.auditor import audit_score_bounds

        findings = audit_score_bounds(1, "TEST", float("-inf"), 0.5)
        critical = [f for f in findings if f.severity == "critical"]
        # Should get both out-of-bounds AND inf findings
        assert len(critical) >= 1

    def test_valid_scores_no_findings(self):
        from ai_opportunity_index.fact_graph.auditor import audit_score_bounds

        findings = audit_score_bounds(1, "TEST", 0.5, 0.7, 0.3, 0.6, 0.4, 0.8)
        assert len(findings) == 0

    def test_boundary_scores_zero_and_one_valid(self):
        from ai_opportunity_index.fact_graph.auditor import audit_score_bounds

        findings = audit_score_bounds(1, "TEST", 0.0, 1.0)
        assert len(findings) == 0

    def test_all_subscores_checked(self):
        """Every sub-score field must be individually bounds-checked."""
        from ai_opportunity_index.fact_graph.auditor import audit_score_bounds

        # Each of the 4 sub-scores set to out-of-bounds
        findings = audit_score_bounds(
            1, "TEST", 0.5, 0.5,
            cost_opp=1.5, revenue_opp=-0.1, cost_capture=2.0, revenue_capture=-0.5,
        )
        critical = [f for f in findings if f.severity == "critical"]
        assert len(critical) == 4

    def test_nan_in_subscores(self):
        from ai_opportunity_index.fact_graph.auditor import audit_score_bounds

        findings = audit_score_bounds(
            1, "TEST", 0.5, 0.5,
            cost_opp=float("nan"),
        )
        critical = [f for f in findings if f.severity == "critical"]
        assert len(critical) >= 1

    def test_quadrant_mismatch_flagged_critical(self):
        """Quadrant audit should catch score-label mismatches."""
        from ai_opportunity_index.fact_graph.auditor import audit_quadrant_assignment

        # Scores say high-high, but label says low-low
        finding = audit_quadrant_assignment(
            0.8, 0.7, "low_opp_low_real", 1, "TEST"
        )
        assert finding is not None
        assert finding.severity == "critical"

    def test_quadrant_correct_no_finding(self):
        from ai_opportunity_index.fact_graph.auditor import audit_quadrant_assignment

        finding = audit_quadrant_assignment(
            0.8, 0.7, "high_opp_high_real", 1, "TEST"
        )
        assert finding is None
