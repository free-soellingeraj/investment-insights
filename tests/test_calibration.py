"""Tests for ai_opportunity_index.scoring.calibration.

All functions under test are pure and deterministic — no mocking needed.
"""

from __future__ import annotations

import math

import pytest

from ai_opportunity_index.scoring.calibration import (
    CONFIDENCE_CEILING,
    CONFIDENCE_FLOOR,
    calibrate_confidence,
    check_dollar_sanity,
    source_authority_weight,
    temporal_weight,
)


# ── calibrate_confidence ──────────────────────────────────────────────────


class TestCalibrateConfidence:
    """Tests for the confidence calibration function."""

    # -- Floor and ceiling enforcement --

    def test_floor_enforced(self):
        """Even 0.0 raw confidence maps to the floor."""
        assert calibrate_confidence(0.0, "llm") >= CONFIDENCE_FLOOR

    def test_ceiling_enforced(self):
        """Even 1.0 raw confidence maps at or below the ceiling."""
        assert calibrate_confidence(1.0, "llm") <= CONFIDENCE_CEILING

    def test_floor_enforced_all_sources(self):
        for src in ("llm", "sec_filing", "news", "analyst", "github"):
            result = calibrate_confidence(0.0, src)
            assert result >= CONFIDENCE_FLOOR, f"Floor violated for {src}"

    def test_ceiling_enforced_all_sources(self):
        for src in ("llm", "sec_filing", "news", "analyst", "github"):
            result = calibrate_confidence(1.0, src)
            assert result <= CONFIDENCE_CEILING, f"Ceiling violated for {src}"

    # -- LLM Platt scaling --

    def test_llm_midpoint(self):
        """At 0.5 raw, sigmoid(2*0.5 - 1) = sigmoid(0) = 0.5."""
        result = calibrate_confidence(0.5, "llm")
        assert abs(result - 0.5) < 0.01

    def test_llm_overconfidence_pulled_down(self):
        """Raw 0.9 should be pulled below 0.9 for LLM source."""
        result = calibrate_confidence(0.9, "llm")
        assert result < 0.9

    def test_llm_underconfidence_pulled_up(self):
        """Raw 0.1 should be pulled above 0.1 for LLM source."""
        result = calibrate_confidence(0.1, "llm")
        assert result > 0.1

    def test_llm_monotonic(self):
        """Higher raw confidence should produce higher calibrated confidence."""
        values = [calibrate_confidence(x / 10.0, "llm") for x in range(11)]
        for i in range(len(values) - 1):
            assert values[i] <= values[i + 1]

    # -- Linear source types --

    def test_sec_filing_boost(self):
        """SEC filings get a 1.1x boost (capped at ceiling)."""
        result = calibrate_confidence(0.5, "sec_filing")
        assert abs(result - 0.55) < 0.01

    def test_news_discount(self):
        """News gets a 0.8x discount."""
        result = calibrate_confidence(0.5, "news")
        assert abs(result - 0.40) < 0.01

    def test_analyst_discount(self):
        """Analyst gets a 0.9x discount."""
        result = calibrate_confidence(0.5, "analyst")
        assert abs(result - 0.45) < 0.01

    def test_github_discount(self):
        """GitHub gets a 0.7x discount."""
        result = calibrate_confidence(0.5, "github")
        assert abs(result - 0.35) < 0.01

    def test_sec_high_capped(self):
        """SEC 1.0 * 1.1 = 1.1 should be capped to ceiling."""
        result = calibrate_confidence(1.0, "sec_filing")
        assert result == CONFIDENCE_CEILING

    # -- Unknown source type --

    def test_unknown_source_falls_back_to_llm(self):
        """Unknown source type uses the LLM calibration curve."""
        llm_result = calibrate_confidence(0.7, "llm")
        unknown_result = calibrate_confidence(0.7, "unknown_thing")
        assert llm_result == unknown_result

    # -- Determinism --

    def test_deterministic(self):
        """Same inputs always produce same outputs."""
        a = calibrate_confidence(0.73, "news")
        b = calibrate_confidence(0.73, "news")
        assert a == b


# ── check_dollar_sanity ───────────────────────────────────────────────────


class TestCheckDollarSanity:
    """Tests for the dollar estimate sanity checker."""

    # -- Basic pass-through --

    def test_reasonable_estimate_unchanged(self):
        """A reasonable estimate passes through without adjustment."""
        adjusted, warnings = check_dollar_sanity(1_234_567, 1e9, 10e9)
        assert adjusted == 1_234_567
        assert warnings == []

    # -- Negative floor --

    def test_negative_floored_to_zero(self):
        adjusted, warnings = check_dollar_sanity(-500_000, None, None)
        assert adjusted == 0.0
        assert any("Negative" in w for w in warnings)

    # -- Revenue cap --

    def test_revenue_cap(self):
        """Estimate > 0.5x revenue is capped."""
        adjusted, warnings = check_dollar_sanity(
            3_000_000_000, company_revenue=1_000_000_000, company_market_cap=None
        )
        assert adjusted == 500_000_000
        assert any("revenue" in w.lower() for w in warnings)

    def test_revenue_cap_no_revenue(self):
        """No revenue info means no revenue cap."""
        adjusted, warnings = check_dollar_sanity(5e9, None, None)
        # Only the round-number warning, no cap applied (under $10B global)
        assert adjusted == 5e9

    # -- Market cap cap --

    def test_market_cap_cap(self):
        """Estimate > 0.5x market cap is capped."""
        adjusted, warnings = check_dollar_sanity(
            6_000_000_000, company_revenue=None, company_market_cap=10_000_000_000
        )
        assert adjusted == 5_000_000_000
        assert any("market cap" in w.lower() for w in warnings)

    # -- Both caps (revenue is more restrictive) --

    def test_both_caps_revenue_wins(self):
        """When both caps apply, the tighter one wins."""
        # Revenue cap: 0.5 * 1B = 0.5B;  Market cap: 0.5 * 20B = 10B
        # Revenue is more restrictive, applied first.
        adjusted, warnings = check_dollar_sanity(5e9, 1e9, 20e9)
        assert adjusted == 0.5e9

    def test_both_caps_market_cap_wins(self):
        """Market cap cap can be tighter than revenue cap."""
        # Revenue cap: 0.5 * 100B = 50B;  Market cap: 0.5 * 2B = 1B
        adjusted, warnings = check_dollar_sanity(5e9, 100e9, 2e9)
        assert adjusted == 1e9

    # -- Global $10B cap --

    def test_global_cap_flag(self):
        """Estimates over $10B are capped to $10B."""
        adjusted, warnings = check_dollar_sanity(15e9, 100e9, 100e9)
        assert adjusted == 10e9
        assert any("global cap" in w.lower() for w in warnings)

    # -- Round number detection --

    def test_round_number_flagged(self):
        """$1,000,000,000 is suspiciously round."""
        _, warnings = check_dollar_sanity(1_000_000_000, 10e9, 100e9)
        assert any("round" in w.lower() for w in warnings)

    def test_not_round_not_flagged(self):
        """$1,234,567 is not suspiciously round."""
        _, warnings = check_dollar_sanity(1_234_567, 10e9, 100e9)
        assert not any("round" in w.lower() for w in warnings)

    def test_small_round_not_flagged(self):
        """$1,000 is round but below the $1M threshold for the flag."""
        _, warnings = check_dollar_sanity(1_000, 10e9, 100e9)
        assert not any("round" in w.lower() for w in warnings)

    # -- Zero estimate --

    def test_zero_passes_through(self):
        adjusted, warnings = check_dollar_sanity(0, 1e9, 10e9)
        assert adjusted == 0
        assert warnings == []

    # -- Determinism --

    def test_deterministic(self):
        a = check_dollar_sanity(123_456_789, 1e9, 10e9)
        b = check_dollar_sanity(123_456_789, 1e9, 10e9)
        assert a == b


# ── source_authority_weight ───────────────────────────────────────────────


class TestSourceAuthorityWeight:
    """Tests for source authority weighting."""

    def test_sec_filing_with_cik(self):
        assert source_authority_weight("sec_filing", "CIK0001234567") == 1.0

    def test_analyst_known_firm(self):
        assert source_authority_weight("analyst", "Goldman Sachs") == 0.9

    def test_earnings_call(self):
        assert source_authority_weight("news", "earnings_call") == 0.85

    def test_news_major_outlet_reuters(self):
        assert source_authority_weight("news", "Reuters") == 0.7

    def test_news_major_outlet_bloomberg(self):
        assert source_authority_weight("news", "Bloomberg") == 0.7

    def test_news_major_outlet_wsj(self):
        assert source_authority_weight("news", "WSJ") == 0.7

    def test_blog_authority(self):
        assert source_authority_weight("news", "blog") == 0.4

    def test_medium_as_blog(self):
        assert source_authority_weight("news", "Medium") == 0.4

    def test_unknown_source_default(self):
        assert source_authority_weight("unknown", None) == 0.5

    def test_sec_no_authority_fallback(self):
        """SEC filing without specific authority uses type default."""
        result = source_authority_weight("sec_filing", None)
        assert result == 0.9

    def test_github_no_authority(self):
        result = source_authority_weight("github", None)
        assert result == 0.5

    def test_authority_range(self):
        """All values should be in (0, 1]."""
        cases = [
            ("sec_filing", "CIK123"),
            ("analyst", "Morgan Stanley"),
            ("news", "Reuters"),
            ("news", "blog"),
            ("unknown", None),
            ("github", None),
        ]
        for src, auth in cases:
            w = source_authority_weight(src, auth)
            assert 0 < w <= 1.0, f"Out of range for ({src}, {auth}): {w}"


# ── temporal_weight ───────────────────────────────────────────────────────


class TestTemporalWeight:
    """Tests for temporal decay weighting."""

    def test_zero_days_is_one(self):
        assert temporal_weight(0, "news") == 1.0

    def test_negative_days_is_one(self):
        assert temporal_weight(-5, "sec_filing") == 1.0

    def test_news_half_life(self):
        """After 30 days, news weight should be ~0.5."""
        result = temporal_weight(30, "news")
        assert abs(result - 0.5) < 0.01

    def test_sec_filing_half_life(self):
        """After 365 days, SEC filing weight should be ~0.5."""
        result = temporal_weight(365, "sec_filing")
        assert abs(result - 0.5) < 0.01

    def test_analyst_half_life(self):
        """After 180 days, analyst weight should be ~0.5."""
        result = temporal_weight(180, "analyst")
        assert abs(result - 0.5) < 0.01

    def test_github_half_life(self):
        """After 60 days, GitHub weight should be ~0.5."""
        result = temporal_weight(60, "github")
        assert abs(result - 0.5) < 0.01

    def test_earnings_call_half_life(self):
        """After 90 days, earnings call weight should be ~0.5."""
        result = temporal_weight(90, "earnings_call")
        assert abs(result - 0.5) < 0.01

    def test_two_half_lives_is_quarter(self):
        """After 2 half-lives, weight should be ~0.25."""
        result = temporal_weight(60, "news")  # 2 * 30-day half-life
        assert abs(result - 0.25) < 0.01

    def test_monotonically_decreasing(self):
        """Older evidence should always have lower weight."""
        values = [temporal_weight(d, "news") for d in range(0, 365, 30)]
        for i in range(len(values) - 1):
            assert values[i] >= values[i + 1]

    def test_very_old_approaches_zero(self):
        """Very old evidence should have near-zero weight."""
        result = temporal_weight(3650, "news")  # 10 years of news
        assert result < 0.001

    def test_unknown_source_uses_default(self):
        """Unknown source type uses 90-day default half-life."""
        result = temporal_weight(90, "unknown_source")
        assert abs(result - 0.5) < 0.01

    def test_deterministic(self):
        a = temporal_weight(42, "analyst")
        b = temporal_weight(42, "analyst")
        assert a == b


# ── Integration: all functions are pure ───────────────────────────────────


class TestPurity:
    """Verify functions have no side effects and are deterministic."""

    @pytest.mark.parametrize("raw", [0.0, 0.25, 0.5, 0.75, 1.0])
    @pytest.mark.parametrize("src", ["llm", "sec_filing", "news", "analyst", "github"])
    def test_calibrate_confidence_pure(self, raw, src):
        a = calibrate_confidence(raw, src)
        b = calibrate_confidence(raw, src)
        assert a == b
        assert CONFIDENCE_FLOOR <= a <= CONFIDENCE_CEILING

    @pytest.mark.parametrize(
        "estimate,rev,mcap",
        [
            (0, None, None),
            (1e6, 1e9, 10e9),
            (-1e6, None, None),
            (1e12, 1e9, 5e9),
            (1_000_000_000, None, None),
        ],
    )
    def test_check_dollar_sanity_pure(self, estimate, rev, mcap):
        a = check_dollar_sanity(estimate, rev, mcap)
        b = check_dollar_sanity(estimate, rev, mcap)
        assert a == b
        assert a[0] >= 0  # never negative
