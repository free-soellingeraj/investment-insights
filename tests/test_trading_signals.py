"""Unit tests for TradeSignalGenerator pure computation logic.

Tests _determine_action, _assess_risks, and _generate_summary without
any database access.
"""

from __future__ import annotations

import pytest

from ai_opportunity_index.trading.models import TradeAction, SignalStrength
from ai_opportunity_index.trading.signal_generator import (
    TradeSignalGenerator,
    BASE_WEIGHT,
    MAX_WEIGHT,
    SCORE_SCALING,
)


@pytest.fixture
def gen() -> TradeSignalGenerator:
    """Signal generator with no DB session."""
    return TradeSignalGenerator(session=None)


# ---------------------------------------------------------------------------
# _determine_action — parametrised
# ---------------------------------------------------------------------------

# Helper to compute expected target weight from combined score
def _target(combined: float) -> float:
    return min(MAX_WEIGHT, BASE_WEIGHT + SCORE_SCALING * combined)


@pytest.mark.parametrize(
    "opp, real, current, flags, expected_action, expected_strength, weight_check",
    [
        # 1. Strong BUY — new position, high scores
        pytest.param(
            0.9, 0.8, 0.0, [],
            TradeAction.BUY, SignalStrength.STRONG,
            lambda w: abs(w - _target(0.85)) < 1e-9,
            id="strong-buy-new-position",
        ),
        # 2. Moderate BUY — new position, above thresholds but combined < 0.75
        pytest.param(
            0.65, 0.45, 0.0, [],
            TradeAction.BUY, SignalStrength.MODERATE,
            lambda w: abs(w - _target(0.55)) < 1e-9,
            id="moderate-buy-new-position",
        ),
        # 3. SELL — holding, scores well below sell thresholds
        pytest.param(
            0.1, 0.1, 0.03, [],
            TradeAction.SELL, SignalStrength.MODERATE,
            lambda w: w == 0.0,
            id="sell-low-scores",
        ),
        # 4. HOLD — no position, mediocre scores below weak-buy trigger
        pytest.param(
            0.4, 0.3, 0.0, [],
            TradeAction.HOLD, SignalStrength.WEAK,
            lambda w: w == 0.0,
            id="hold-no-position-low-scores",
        ),
        # 5. INCREASE — already holding small position, high scores drive higher target
        pytest.param(
            0.8, 0.6, 0.01, [],
            TradeAction.INCREASE, SignalStrength.MODERATE,
            lambda w: abs(w - _target(0.70)) < 1e-9,
            id="increase-existing-position",
        ),
        # 6. DECREASE — holding large position, middling scores produce lower target
        #    opp=0.4, real=0.3 → falls through to "hold or gradual adjustment" branch
        #    combined=0.35, target=0.02+0.03*0.35=0.0305, current=0.05
        #    target(0.0305) < 0.05*0.8(=0.04) → DECREASE
        pytest.param(
            0.4, 0.3, 0.05, [],
            TradeAction.DECREASE, SignalStrength.WEAK,
            lambda w: abs(w - _target(0.35)) < 1e-9,
            id="decrease-large-position",
        ),
        # 7. Weak BUY — no position, one score above 0.5
        pytest.param(
            0.5, 0.1, 0.0, [],
            TradeAction.BUY, SignalStrength.WEAK,
            lambda w: w == BASE_WEIGHT,
            id="weak-buy-opportunity-above-half",
        ),
        # 7b. Weak BUY triggered by realization >= 0.5
        pytest.param(
            0.35, 0.5, 0.0, [],
            TradeAction.BUY, SignalStrength.WEAK,
            lambda w: w == BASE_WEIGHT,
            id="weak-buy-realization-above-half",
        ),
        # 8. Edge — exactly at BUY thresholds, new position
        pytest.param(
            0.6, 0.4, 0.0, [],
            TradeAction.BUY, SignalStrength.MODERATE,
            lambda w: abs(w - _target(0.50)) < 1e-9,
            id="edge-exact-buy-threshold",
        ),
        # 9. Edge — just below SELL thresholds, no position → HOLD
        pytest.param(
            0.29, 0.19, 0.0, [],
            TradeAction.HOLD, SignalStrength.WEAK,
            lambda w: w == 0.0,
            id="edge-just-below-sell-threshold-no-position",
        ),
        # 10. Edge — at SELL thresholds (not below) with position → not a sell
        #     opp=0.3, real=0.2: NOT < 0.3 and NOT < 0.2, so falls to adjustment branch
        #     combined=0.25, target=0.02+0.03*0.25=0.0275, current=0.03
        #     0.0275 < 0.03*0.8(=0.024)? No → HOLD at current_weight
        pytest.param(
            0.3, 0.2, 0.03, [],
            TradeAction.HOLD, SignalStrength.WEAK,
            lambda w: w == 0.03,
            id="edge-at-sell-threshold-with-position",
        ),
    ],
)
def test_determine_action(
    gen: TradeSignalGenerator,
    opp: float,
    real: float,
    current: float,
    flags: list,
    expected_action: TradeAction,
    expected_strength: SignalStrength,
    weight_check,
):
    action, weight, strength = gen._determine_action(opp, real, current, flags)
    assert action == expected_action, f"Expected {expected_action}, got {action}"
    assert strength == expected_strength, f"Expected {expected_strength}, got {strength}"
    assert weight_check(weight), f"Weight {weight} failed check"


# ---------------------------------------------------------------------------
# Additional _determine_action edge cases (non-parametrised for clarity)
# ---------------------------------------------------------------------------

class TestDetermineActionExtra:
    """Extra scenarios that benefit from explicit assertions."""

    def test_hold_when_buy_target_close_to_current(self, gen: TradeSignalGenerator):
        """If already holding and target is within 10% of current → HOLD."""
        # opp=0.7, real=0.5 → combined=0.6, target=0.02+0.03*0.6=0.038
        # current=0.035 → target(0.038) > 0.035*1.1(=0.0385)? No → HOLD
        action, weight, strength = gen._determine_action(0.7, 0.5, 0.035, [])
        assert action == TradeAction.HOLD
        assert weight == 0.035  # keeps current weight
        assert strength == SignalStrength.WEAK

    def test_increase_when_target_exceeds_current_by_ten_percent(self, gen: TradeSignalGenerator):
        """If target > current * 1.1 → INCREASE."""
        # opp=0.9, real=0.9 → combined=0.9, target=0.02+0.03*0.9=0.047
        # current=0.02 → 0.047 > 0.022 → INCREASE
        action, weight, strength = gen._determine_action(0.9, 0.9, 0.02, [])
        assert action == TradeAction.INCREASE
        assert strength == SignalStrength.STRONG
        assert abs(weight - _target(0.9)) < 1e-9

    def test_strong_signal_threshold_boundary(self, gen: TradeSignalGenerator):
        """Combined exactly at 0.75 → STRONG."""
        # Need combined = (opp+real)/2 = 0.75 → opp+real = 1.5
        # opp=0.9, real=0.6 → combined=0.75
        action, weight, strength = gen._determine_action(0.9, 0.6, 0.0, [])
        assert action == TradeAction.BUY
        assert strength == SignalStrength.STRONG

    def test_just_below_strong_threshold(self, gen: TradeSignalGenerator):
        """Combined just below 0.75 → MODERATE."""
        # opp=0.8, real=0.69 → combined=0.745
        action, weight, strength = gen._determine_action(0.8, 0.69, 0.0, [])
        assert action == TradeAction.BUY
        assert strength == SignalStrength.MODERATE

    def test_target_capped_at_max_weight(self, gen: TradeSignalGenerator):
        """Target weight never exceeds MAX_WEIGHT."""
        action, weight, strength = gen._determine_action(1.0, 1.0, 0.0, [])
        assert weight <= MAX_WEIGHT

    def test_sell_no_position_returns_hold(self, gen: TradeSignalGenerator):
        """Low scores with no current position → HOLD at 0."""
        action, weight, strength = gen._determine_action(0.1, 0.05, 0.0, [])
        assert action == TradeAction.HOLD
        assert weight == 0.0


# ---------------------------------------------------------------------------
# _assess_risks
# ---------------------------------------------------------------------------

class TestAssessRisks:

    def test_high_opportunity_low_realization(self, gen: TradeSignalGenerator):
        risks = gen._assess_risks(0.85, 0.2, [], "TEST")
        assert any("may not materialize" in r for r in risks)

    def test_high_realization_low_opportunity(self, gen: TradeSignalGenerator):
        risks = gen._assess_risks(0.2, 0.8, [], "TEST")
        assert any("fully priced in" in r for r in risks)

    def test_discrepancy_flag(self, gen: TradeSignalGenerator):
        flags = ["score discrepancy between sources"]
        risks = gen._assess_risks(0.5, 0.5, flags, "TEST")
        assert any("discrepancy" in r.lower() for r in risks)

    def test_thin_evidence_flag(self, gen: TradeSignalGenerator):
        flags = ["thin evidence base"]
        risks = gen._assess_risks(0.5, 0.5, flags, "TEST")
        assert any("Limited evidence" in r for r in risks)

    def test_sparse_evidence_flag(self, gen: TradeSignalGenerator):
        flags = ["sparse data coverage"]
        risks = gen._assess_risks(0.5, 0.5, flags, "TEST")
        assert any("Limited evidence" in r for r in risks)

    def test_no_risks_normal_scores(self, gen: TradeSignalGenerator):
        risks = gen._assess_risks(0.5, 0.5, [], "TEST")
        assert risks == []

    def test_multiple_risks(self, gen: TradeSignalGenerator):
        flags = ["data discrepancy noted", "thin evidence"]
        risks = gen._assess_risks(0.9, 0.1, flags, "TEST")
        # Should have: thesis risk + discrepancy + thin evidence = 3
        assert len(risks) >= 3

    def test_boundary_no_risk_opp_exactly_0_8(self, gen: TradeSignalGenerator):
        """opp=0.8 is NOT > 0.8, so no thesis risk."""
        risks = gen._assess_risks(0.8, 0.2, [], "TEST")
        assert not any("may not materialize" in r for r in risks)

    def test_boundary_no_risk_real_exactly_0_7(self, gen: TradeSignalGenerator):
        """real=0.7 is NOT > 0.7, so no pricing risk."""
        risks = gen._assess_risks(0.2, 0.7, [], "TEST")
        assert not any("fully priced in" in r for r in risks)


# ---------------------------------------------------------------------------
# _generate_summary
# ---------------------------------------------------------------------------

class TestGenerateSummary:

    def test_buy_summary(self, gen: TradeSignalGenerator):
        s = gen._generate_summary(
            "AAPL", "Apple Inc", TradeAction.BUY, SignalStrength.STRONG,
            0.9, 0.8, "High Opportunity",
        )
        assert "STRONG BUY" in s
        assert "AAPL" in s
        assert "Apple Inc" in s
        assert "opportunity" in s.lower()

    def test_sell_summary(self, gen: TradeSignalGenerator):
        s = gen._generate_summary(
            "XYZ", "XYZ Corp", TradeAction.SELL, SignalStrength.MODERATE,
            0.1, 0.1, "Low Scores",
        )
        assert "SELL" in s
        assert "XYZ" in s
        assert "low opportunity" in s.lower()

    def test_increase_summary(self, gen: TradeSignalGenerator):
        s = gen._generate_summary(
            "MSFT", "Microsoft", TradeAction.INCREASE, SignalStrength.MODERATE,
            0.8, 0.6, "Growing",
        )
        assert "INCREASE" in s
        assert "MSFT" in s

    def test_decrease_summary(self, gen: TradeSignalGenerator):
        s = gen._generate_summary(
            "IBM", "IBM Corp", TradeAction.DECREASE, SignalStrength.WEAK,
            0.4, 0.3, "Declining",
        )
        assert "DECREASE" in s
        assert "declining" in s.lower()

    def test_hold_summary(self, gen: TradeSignalGenerator):
        s = gen._generate_summary(
            "GOOG", "Alphabet", TradeAction.HOLD, SignalStrength.WEAK,
            0.5, 0.5, "Stable",
        )
        assert "HOLD" in s
        assert "GOOG" in s

    def test_summary_uses_ticker_when_no_name(self, gen: TradeSignalGenerator):
        s = gen._generate_summary(
            "AAPL", None, TradeAction.BUY, SignalStrength.MODERATE,
            0.7, 0.5, "Q1",
        )
        # The name portion should fall back to ticker
        assert s.startswith("MODERATE BUY AAPL")
