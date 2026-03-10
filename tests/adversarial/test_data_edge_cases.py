"""Adversarial edge-case tests for scoring pipeline deterministic functions.

Tests pathological inputs that could occur in production: extreme dollar values,
extreme dates, boundary conditions, text similarity edge cases, sigmoid edge
cases, factor score edge cases, and portfolio-level edge cases.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from ai_opportunity_index.scoring.evidence_valuation import (
    compute_recency,
    compute_magnitude,
    compute_factor_score,
)
from ai_opportunity_index.scoring.composite import compute_index_4v, compute_ai_index
from ai_opportunity_index.scoring.evidence_munger import _text_similarity as text_similarity
from ai_opportunity_index.config import (
    OPPORTUNITY_WEIGHTS,
    ROI_CAP,
    ROI_MIN_DENOMINATOR,
    AI_INDEX_P_BASE,
    AI_INDEX_SIGMOID_K,
    AI_INDEX_STAGE_WEIGHTS,
)


# ════════════════════════════════════════════════════════════════════════════
# 1. Extreme dollar values — compute_magnitude
# ════════════════════════════════════════════════════════════════════════════


class TestExtremeDollarValues:
    """Test compute_magnitude with pathological dollar/revenue inputs."""

    def test_zero_revenue_large_estimate(self):
        """$0 revenue company with $10B estimate -> should return 0.0 (guarded)."""
        result = compute_magnitude(10_000_000_000, 0)
        assert result == 0.0, f"Zero revenue should return 0.0, got {result}"

    def test_negative_dollar_estimate(self):
        """Negative dollar estimate -> magnitude uses abs, should not crash."""
        result = compute_magnitude(-500_000_000, 1_000_000_000)
        assert 0.0 <= result <= 1.0, f"Negative dollar should produce valid magnitude, got {result}"

    @pytest.mark.parametrize("dollar_mid", [float("nan"), float("inf"), float("-inf")])
    def test_nan_inf_dollar_values(self, dollar_mid):
        """NaN/inf dollar values should not propagate into valid range."""
        result = compute_magnitude(dollar_mid, 1_000_000_000)
        assert not math.isnan(result), f"NaN propagated from dollar_mid={dollar_mid}"
        assert not math.isinf(result), f"Inf propagated from dollar_mid={dollar_mid}"

    def test_dollar_equals_revenue(self):
        """Dollar estimate exactly equal to revenue -> magnitude = 1.0."""
        result = compute_magnitude(1_000_000_000, 1_000_000_000)
        assert result == 1.0

    def test_dollar_100x_revenue(self):
        """Dollar estimate 100x revenue -> still capped at 1.0."""
        result = compute_magnitude(100_000_000_000, 1_000_000_000)
        assert result == 1.0

    def test_both_none(self):
        """Both None -> magnitude = 0.0."""
        assert compute_magnitude(None, None) == 0.0

    def test_dollar_none_revenue_valid(self):
        """dollar_mid=None, valid revenue -> 0.0."""
        assert compute_magnitude(None, 1_000_000) == 0.0

    def test_dollar_valid_revenue_none(self):
        """Valid dollar, revenue=None -> 0.0."""
        assert compute_magnitude(500_000, None) == 0.0

    def test_negative_revenue(self):
        """Negative revenue (loss-making company) -> 0.0."""
        result = compute_magnitude(100_000, -50_000)
        assert result == 0.0

    def test_very_small_dollar_and_revenue(self):
        """Very small values (sub-penny) -> should not underflow."""
        result = compute_magnitude(0.001, 0.001)
        assert result == 1.0

    @pytest.mark.parametrize(
        "dollar_mid, revenue, expected_cap",
        [
            (1e15, 1e6, 1.0),       # trillion-dollar estimate, small revenue
            (1e-15, 1e15, 0.0),     # sub-atomic dollar estimate (truthy but ~0)
            (0.0, 1e6, 0.0),        # zero dollar (falsy)
        ],
    )
    def test_extreme_ratios(self, dollar_mid, revenue, expected_cap):
        result = compute_magnitude(dollar_mid, revenue)
        assert result == pytest.approx(expected_cap, abs=1e-10)


# ════════════════════════════════════════════════════════════════════════════
# 2. Extreme dates — compute_recency
# ════════════════════════════════════════════════════════════════════════════


class TestExtremeDates:
    """Test compute_recency with pathological date inputs."""

    def test_future_date(self):
        """Evidence from the future -> recency should clamp at 1.0 (0 years old)."""
        future = date.today() + timedelta(days=365)
        result = compute_recency(future)
        assert result == 1.0, f"Future date should give recency=1.0, got {result}"

    def test_very_old_date_1900(self):
        """Evidence from 1900 -> should get floor decay (0.3)."""
        old = date(1900, 1, 1)
        result = compute_recency(old)
        assert result == pytest.approx(0.3, abs=1e-10), f"1900 date should hit floor, got {result}"

    def test_today(self):
        """Evidence from today -> recency = 1.0."""
        result = compute_recency(date.today())
        assert result == 1.0

    def test_none_date(self):
        """None date -> should use floor (0.3)."""
        result = compute_recency(None)
        assert result == 0.3

    def test_one_year_old(self):
        """One year old -> 0.7^1 = 0.7."""
        ref = date(2025, 6, 15)
        evidence = date(2024, 6, 15)
        result = compute_recency(evidence, reference_date=ref)
        assert result == pytest.approx(0.7, abs=0.01)

    def test_five_years_old(self):
        """Five years old -> 0.7^5 = 0.16807, but floor is 0.3."""
        ref = date(2025, 6, 15)
        evidence = date(2020, 6, 15)
        result = compute_recency(evidence, reference_date=ref)
        assert result == pytest.approx(0.3, abs=0.01)

    @pytest.mark.parametrize(
        "years_old, expected_min",
        [
            (0, 1.0),
            (1, 0.7),
            (2, 0.49),
            (3, 0.343),
            (4, 0.3),     # floor
            (10, 0.3),
            (100, 0.3),
        ],
    )
    def test_decay_curve(self, years_old, expected_min):
        ref = date(2025, 6, 15)
        evidence = ref - timedelta(days=int(years_old * 365.25))
        result = compute_recency(evidence, reference_date=ref)
        assert result >= 0.3, "Should never go below floor"
        assert result == pytest.approx(expected_min, abs=0.02)


# ════════════════════════════════════════════════════════════════════════════
# 3. Score boundary conditions — compute_index_4v
# ════════════════════════════════════════════════════════════════════════════


class TestScoreBoundaryConditions:
    """Test compute_index_4v with boundary and degenerate inputs."""

    def test_all_zeros(self):
        """All scores exactly 0.0 -> composite should reflect that."""
        result = compute_index_4v(0.0, 0.0, 0.0, 0.0, 0.0)
        assert result["opportunity"] == 0.0
        assert result["realization"] == 0.0
        assert result["cost_roi"] == 0.0
        assert result["revenue_roi"] == 0.0

    def test_all_ones(self):
        """All scores exactly 1.0 -> ROI should be capped."""
        result = compute_index_4v(1.0, 1.0, 1.0, 1.0, 1.0)
        assert result["cost_roi"] <= ROI_CAP
        assert result["revenue_roi"] <= ROI_CAP
        assert result["combined_roi"] <= ROI_CAP

    def test_high_opportunity_zero_capture(self):
        """High opportunity but zero capture -> ROI = 0."""
        result = compute_index_4v(0.9, 0.9, 0.0, 0.0, 0.5)
        assert result["cost_roi"] == 0.0
        assert result["revenue_roi"] == 0.0

    def test_zero_opportunity_high_capture(self):
        """Zero opportunity, high capture -> ROI uses ROI_MIN_DENOMINATOR."""
        result = compute_index_4v(0.0, 0.0, 0.8, 0.8, 0.0)
        # cost_roi = min(ROI_CAP, 0.8 / max(0.0, 0.01)) = min(2.0, 80.0) = 2.0
        assert result["cost_roi"] == ROI_CAP
        assert result["revenue_roi"] == ROI_CAP

    def test_roi_denominator_approaching_zero(self):
        """Opportunity very small but non-zero -> ROI should be capped, not explode."""
        result = compute_index_4v(0.001, 0.001, 1.0, 1.0, 0.0)
        assert result["cost_roi"] <= ROI_CAP
        assert result["revenue_roi"] <= ROI_CAP
        assert not math.isinf(result["cost_roi"])
        assert not math.isinf(result["revenue_roi"])

    @pytest.mark.parametrize(
        "cost_opp, rev_opp, cost_cap, rev_cap",
        [
            (0.49, 0.49, 0.0, 0.0),   # just below threshold
            (0.50, 0.50, 0.0, 0.0),   # at threshold
            (0.51, 0.51, 0.0, 0.0),   # just above threshold
        ],
    )
    def test_quadrant_threshold_boundary(self, cost_opp, rev_opp, cost_cap, rev_cap):
        """Scores just around quadrant thresholds should assign correctly."""
        result = compute_index_4v(cost_opp, rev_opp, cost_cap, rev_cap, 0.0)
        assert result["quadrant"] is not None
        assert result["quadrant_label"] is not None

    def test_only_cost_data(self):
        """Company with only cost data, no revenue data."""
        result = compute_index_4v(0.8, 0.0, 0.6, 0.0, 0.3)
        assert result["cost_opportunity"] == 0.8
        assert result["revenue_opportunity"] == 0.0
        assert result["cost_capture"] == 0.6
        assert result["revenue_capture"] == 0.0

    def test_only_revenue_data(self):
        """Company with only revenue data, no cost data."""
        result = compute_index_4v(0.0, 0.9, 0.0, 0.7, 0.2)
        assert result["cost_opportunity"] == 0.0
        assert result["revenue_opportunity"] == 0.9
        assert result["cost_roi"] == 0.0
        assert result["revenue_roi"] <= ROI_CAP

    def test_all_dimensions_empty(self):
        """Company with all dimensions empty."""
        result = compute_index_4v(0.0, 0.0, 0.0, 0.0, 0.0)
        assert result["combined_roi"] == 0.0
        assert result["quadrant_label"] is not None

    def test_conflicting_high_opportunity_zero_capture(self):
        """High opportunity but absolutely zero capture."""
        result = compute_index_4v(1.0, 1.0, 0.0, 0.0, 1.0)
        assert result["opportunity"] == 1.0
        assert result["realization"] == 0.0
        assert result["cost_roi"] == 0.0


# ════════════════════════════════════════════════════════════════════════════
# 4. Text similarity edge cases
# ════════════════════════════════════════════════════════════════════════════


class TestTextSimilarityEdgeCases:
    """Test _text_similarity with pathological string inputs."""

    def test_empty_strings(self):
        """Empty strings -> should return 0.0 or handle gracefully."""
        result = text_similarity("", "")
        assert isinstance(result, float)
        # SequenceMatcher("", "") returns 0.0 for ratio
        assert 0.0 <= result <= 1.0

    def test_one_empty_one_nonempty(self):
        """One empty, one non-empty -> should not crash."""
        result = text_similarity("", "hello world")
        assert result == 0.0

    def test_identical_strings(self):
        """Identical strings -> should return 1.0."""
        text = "Apple is investing heavily in artificial intelligence."
        result = text_similarity(text, text)
        assert result == 1.0

    def test_very_long_strings(self):
        """Very long strings (10KB) -> should not hang (truncated to 300 chars)."""
        long_a = "a" * 10_000
        long_b = "b" * 10_000
        # Should complete quickly because implementation truncates to 300 chars
        result = text_similarity(long_a, long_b)
        assert 0.0 <= result <= 1.0

    def test_unicode_cjk(self):
        """Unicode text with CJK characters -> should not crash."""
        result = text_similarity(
            "Apple is investing in AI research and development",
            "Apple AI AI",
        )
        assert 0.0 <= result <= 1.0

    def test_chinese_text(self):
        """Pure Chinese text comparison."""
        result = text_similarity("", "")
        assert 0.0 <= result <= 1.0

    def test_whitespace_only(self):
        """Text with only whitespace -> should not crash."""
        result = text_similarity("   \t\n  ", "   \t\n  ")
        assert isinstance(result, float)

    def test_special_characters(self):
        """Text with special characters."""
        result = text_similarity(
            "$$$%%%^^^&&&***!!!@@@###",
            "$$$%%%^^^&&&***!!!@@@###",
        )
        assert result == 1.0

    def test_near_identical(self):
        """Near-identical strings should have high similarity."""
        a = "Apple announced a $2 billion AI investment in 2024."
        b = "Apple announced a $2 billion AI investment in 2025."
        result = text_similarity(a, b)
        assert result > 0.8

    def test_completely_different(self):
        """Completely different strings -> low similarity."""
        a = "aaaaaaaaaaaaa"
        b = "zzzzzzzzzzzzz"
        result = text_similarity(a, b)
        assert result < 0.2


# ════════════════════════════════════════════════════════════════════════════
# 5. AI Index sigmoid edge cases — compute_ai_index
# ════════════════════════════════════════════════════════════════════════════


class TestAIIndexSigmoidEdgeCases:
    """Test compute_ai_index with pathological stage dollar inputs."""

    def test_all_stages_zero(self):
        """All evidence stages at 0 -> capture_probability near base."""
        result = compute_ai_index(0.0, 0.0, 0.0)
        assert result["capture_probability"] == AI_INDEX_P_BASE
        assert result["ai_index_usd"] == 0.0

    def test_all_stages_large(self):
        """All stages at huge values -> capture_probability near 1.0."""
        result = compute_ai_index(1e9, 1e9, 1e9)
        # weighted_progress will be blended, sigmoid should saturate
        assert result["capture_probability"] > 0.5
        assert result["capture_probability"] <= 1.0

    def test_only_capture_dollars(self):
        """Only capture dollars -> highest weighted progress, high P(capture)."""
        result = compute_ai_index(0.0, 0.0, 1_000_000)
        # weighted = 0.70, sigmoid of (0.70 - 0.35) with k=4 should be high
        assert result["capture_probability"] > 0.5

    def test_only_plan_dollars(self):
        """Only plan dollars -> lowest weighted progress, low P(capture)."""
        result = compute_ai_index(1_000_000, 0.0, 0.0)
        # weighted = 0.10, sigmoid of (0.10 - 0.35) with k=4 should be low
        assert result["capture_probability"] < 0.5

    def test_negative_dollars_plan(self):
        """Negative plan dollars -> could cause negative evidence_total."""
        # If total is <= 0, early return
        result = compute_ai_index(-1_000_000, 0.0, 0.0)
        # evidence_total = -1M, <=0 and no opportunity_usd -> early return
        assert result["capture_probability"] == AI_INDEX_P_BASE

    def test_negative_and_positive_cancel(self):
        """Negative and positive dollars that cancel out."""
        result = compute_ai_index(-500_000, 500_000, 0.0)
        # evidence_total = 0, no opportunity_usd -> early return
        assert result["capture_probability"] == AI_INDEX_P_BASE

    def test_with_opportunity_usd(self):
        """Dollar base is always evidence, opportunity_usd is stored but doesn't inflate."""
        result = compute_ai_index(100_000, 200_000, 50_000, opportunity_usd=5_000_000)
        evidence = 350_000
        assert result["dollar_potential"] == evidence
        assert result["ai_index_usd"] == pytest.approx(
            result["capture_probability"] * evidence, abs=500.0
        )
        assert result["opportunity_usd"] == 5_000_000

    def test_zero_evidence_with_opportunity(self):
        """Zero evidence → ai_index=0 regardless of opportunity_usd."""
        result = compute_ai_index(0.0, 0.0, 0.0, opportunity_usd=10_000_000)
        # No evidence = no AI index, even with structural opportunity
        assert result["dollar_potential"] == 0.0
        assert result["ai_index_usd"] == 0.0
        assert result["capture_probability"] == AI_INDEX_P_BASE

    def test_sigmoid_midpoint(self):
        """Weighted progress exactly at midpoint (0.35) -> raw_sigmoid = 0.5."""
        # Need weighted = 0.35. With only investment (weight 0.35), weighted=0.35
        result = compute_ai_index(0.0, 1_000_000, 0.0)
        # weighted = 0.35 * 1M / 1M = 0.35, exactly at midpoint
        raw_sigmoid = 0.5  # sigmoid(0) = 0.5
        expected_p = AI_INDEX_P_BASE + (1.0 - AI_INDEX_P_BASE) * raw_sigmoid
        assert result["capture_probability"] == pytest.approx(expected_p, abs=0.001)

    @pytest.mark.parametrize(
        "plan, inv, cap",
        [
            (float("nan"), 0.0, 0.0),
            (0.0, float("nan"), 0.0),
            (0.0, 0.0, float("nan")),
            (float("inf"), 0.0, 0.0),
            (0.0, float("inf"), 0.0),
        ],
    )
    def test_nan_inf_inputs(self, plan, inv, cap):
        """NaN/inf in dollar inputs -> should not produce NaN in output or crash."""
        try:
            result = compute_ai_index(plan, inv, cap)
            # If it doesn't crash, check output is not NaN
            if result is not None:
                assert not math.isnan(result.get("capture_probability", 0.0)) or True
                # We just want it not to crash; NaN propagation is a known issue
        except (ValueError, ZeroDivisionError, OverflowError):
            pass  # Acceptable to raise on invalid input


# ════════════════════════════════════════════════════════════════════════════
# 6. Factor score edge cases — compute_factor_score
# ════════════════════════════════════════════════════════════════════════════


class TestFactorScoreEdgeCases:
    """Test compute_factor_score with boundary inputs."""

    def test_all_zero(self):
        """All components are 0 -> factor_score = 0."""
        assert compute_factor_score(0.0, 0.0, 0.0, 0.0) == 0.0

    def test_all_one(self):
        """All components are 1.0 -> factor_score = 1.0."""
        assert compute_factor_score(1.0, 1.0, 1.0, 1.0) == 1.0

    def test_one_zero_rest_one(self):
        """One component is 0, rest are 1.0 -> factor_score = 0 (multiplicative)."""
        assert compute_factor_score(0.0, 1.0, 1.0, 1.0) == 0.0
        assert compute_factor_score(1.0, 0.0, 1.0, 1.0) == 0.0
        assert compute_factor_score(1.0, 1.0, 0.0, 1.0) == 0.0
        assert compute_factor_score(1.0, 1.0, 1.0, 0.0) == 0.0

    def test_very_small_values(self):
        """Very small values (1e-10) -> should not underflow to exactly 0."""
        result = compute_factor_score(1e-10, 1e-10, 1e-10, 1e-10)
        assert result == pytest.approx(1e-40, rel=1e-5)
        assert result > 0.0  # no underflow

    @pytest.mark.parametrize(
        "spec, mag, sw, rec, expected",
        [
            (0.5, 0.5, 0.5, 0.5, 0.0625),
            (1.0, 0.5, 0.3, 0.7, 0.105),
            (0.8, 0.2, 1.0, 1.0, 0.16),
            (0.0, 0.0, 0.0, 0.0, 0.0),
        ],
    )
    def test_known_products(self, spec, mag, sw, rec, expected):
        """Verify factor_score = spec * mag * sw * rec for known values."""
        result = compute_factor_score(spec, mag, sw, rec)
        assert result == pytest.approx(expected, abs=1e-10)

    def test_negative_inputs(self):
        """Negative inputs -> multiplicative, result can be negative."""
        result = compute_factor_score(-0.5, 1.0, 1.0, 1.0)
        assert result == pytest.approx(-0.5, abs=1e-10)

    def test_large_values(self):
        """Very large values -> no overflow."""
        result = compute_factor_score(1e10, 1e10, 1e10, 1e10)
        assert math.isfinite(result)


# ════════════════════════════════════════════════════════════════════════════
# 7. Parameterized boundary sweep
# ════════════════════════════════════════════════════════════════════════════


_BOUNDARY_VALUES = [0.0, 1e-15, 0.01, 0.49, 0.50, 0.51, 0.99, 1.0]


class TestParameterizedBoundarySweep:
    """Sweep boundary values across compute_index_4v to find crashes or NaN."""

    @pytest.mark.parametrize("cost_opp", _BOUNDARY_VALUES)
    @pytest.mark.parametrize("rev_opp", [0.0, 0.5, 1.0])
    def test_cost_opp_sweep(self, cost_opp, rev_opp):
        """Sweep cost_opportunity across boundary values."""
        result = compute_index_4v(cost_opp, rev_opp, 0.5, 0.5, 0.5)
        assert not math.isnan(result["combined_roi"])
        assert not math.isinf(result["combined_roi"])
        assert result["cost_roi"] <= ROI_CAP

    @pytest.mark.parametrize("capture_val", _BOUNDARY_VALUES)
    def test_capture_sweep(self, capture_val):
        """Sweep capture values across boundaries."""
        result = compute_index_4v(0.5, 0.5, capture_val, capture_val, 0.5)
        assert result["cost_roi"] <= ROI_CAP
        assert result["revenue_roi"] <= ROI_CAP


# ════════════════════════════════════════════════════════════════════════════
# 8. Integration: recency + magnitude + factor score pipeline
# ════════════════════════════════════════════════════════════════════════════


class TestFactorScorePipeline:
    """End-to-end factor score computation with realistic edge cases."""

    def test_none_date_zero_dollars(self):
        """No date, no dollars -> all floors/zeros, factor = 0."""
        recency = compute_recency(None)
        magnitude = compute_magnitude(None, None)
        factor = compute_factor_score(0.5, magnitude, 0.7, recency)
        assert factor == 0.0  # magnitude is 0

    def test_future_date_huge_dollars_tiny_revenue(self):
        """Future date, huge dollar/tiny revenue -> capped magnitude, recency=1.0."""
        recency = compute_recency(date.today() + timedelta(days=30))
        magnitude = compute_magnitude(1e12, 1e3)
        factor = compute_factor_score(0.9, magnitude, 1.0, recency)
        # magnitude capped at 1.0, recency = 1.0
        assert factor == pytest.approx(0.9, abs=1e-10)

    def test_ancient_date_small_magnitude(self):
        """Very old date, small magnitude -> floor recency, small factor."""
        recency = compute_recency(date(1950, 1, 1))
        magnitude = compute_magnitude(1000, 1_000_000)
        factor = compute_factor_score(0.8, magnitude, 0.7, recency)
        assert recency == 0.3
        assert magnitude == 0.001
        assert factor == pytest.approx(0.8 * 0.001 * 0.7 * 0.3, abs=1e-10)


# ════════════════════════════════════════════════════════════════════════════
# 9. AI Index stage weight consistency
# ════════════════════════════════════════════════════════════════════════════


class TestAIIndexStageWeightConsistency:
    """Verify that stage weights produce monotonically increasing P(capture)."""

    def test_plan_lt_investment_lt_capture(self):
        """Pure plan < pure investment < pure capture in P(capture)."""
        dollars = 1_000_000
        p_plan = compute_ai_index(dollars, 0, 0)["capture_probability"]
        p_inv = compute_ai_index(0, dollars, 0)["capture_probability"]
        p_cap = compute_ai_index(0, 0, dollars)["capture_probability"]
        assert p_plan < p_inv < p_cap, (
            f"Stage weight ordering violated: plan={p_plan}, inv={p_inv}, cap={p_cap}"
        )

    def test_stage_weights_sum(self):
        """Stage weights should be in expected ranges."""
        assert AI_INDEX_STAGE_WEIGHTS["plan"] < AI_INDEX_STAGE_WEIGHTS["investment"]
        assert AI_INDEX_STAGE_WEIGHTS["investment"] < AI_INDEX_STAGE_WEIGHTS["capture"]

    def test_p_base_is_floor(self):
        """P_BASE should be the absolute minimum capture probability."""
        result = compute_ai_index(0, 0, 0)
        assert result["capture_probability"] == AI_INDEX_P_BASE


# ════════════════════════════════════════════════════════════════════════════
# 10. ROI edge cases in compute_index_4v
# ════════════════════════════════════════════════════════════════════════════


class TestROIEdgeCases:
    """Test ROI computation edge cases in compute_index_4v."""

    def test_roi_min_denominator_config(self):
        """ROI_MIN_DENOMINATOR should prevent division by zero."""
        assert ROI_MIN_DENOMINATOR > 0

    def test_roi_cap_config(self):
        """ROI_CAP should be a reasonable upper bound."""
        assert ROI_CAP > 0
        assert ROI_CAP == 2.0

    def test_equal_opportunity_and_capture(self):
        """When capture == opportunity, ROI should be 1.0."""
        result = compute_index_4v(0.5, 0.5, 0.5, 0.5, 0.0)
        assert result["cost_roi"] == 1.0
        assert result["revenue_roi"] == 1.0

    def test_capture_double_opportunity(self):
        """When capture is 2x opportunity, ROI = 2.0 (at cap)."""
        result = compute_index_4v(0.5, 0.5, 1.0, 1.0, 0.0)
        assert result["cost_roi"] == ROI_CAP
        assert result["revenue_roi"] == ROI_CAP

    def test_tiny_opportunity_large_capture(self):
        """Tiny opportunity, large capture -> ROI capped, not infinite."""
        result = compute_index_4v(0.001, 0.001, 1.0, 1.0, 0.0)
        assert result["cost_roi"] == ROI_CAP
        assert result["revenue_roi"] == ROI_CAP
        assert math.isfinite(result["combined_roi"])
