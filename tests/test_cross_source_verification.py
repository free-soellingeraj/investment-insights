"""Tests for cross-source verification system.

Covers: confirmations, contradictions, mixed scenarios, edge cases,
confidence adjustment, agreement score calculation, and parametrized
comparison logic.

All tests are pure unit tests -- no database, no mocks of complex objects.
"""

from __future__ import annotations

import pytest

from ai_opportunity_index.domains import (
    EvidenceGroup,
    TargetDimension,
    Valuation,
    ValuationEvidenceType,
    ValuationStage,
)
from ai_opportunity_index.fact_graph.verification import (
    AGREEMENT_RATIO_THRESHOLD,
    CONFIRMATION_BOOST,
    CONTRADICTION_PENALTY,
    MAX_BOOST,
    MIN_PENALTY,
    CrossSourceVerifier,
    SourceAgreement,
    SourceDisagreement,
    VerificationResult,
    _compare_pair,
    _compute_agreement_score,
    compute_confidence_adjustment,
    compute_confidence_adjustment_from_counts,
)


# ── Helpers ───────────────────────────────────────────────────────────────


def _make_group(
    group_id: int,
    company_id: int = 1,
    dimension: TargetDimension = TargetDimension.COST,
    source_types: list[str] | None = None,
) -> EvidenceGroup:
    return EvidenceGroup(
        id=group_id,
        company_id=company_id,
        target_dimension=dimension,
        source_types=source_types or ["filing"],
    )


def _make_valuation(
    group_id: int,
    dollar_low: float | None = None,
    dollar_high: float | None = None,
    dollar_mid: float | None = None,
    evidence_type: ValuationEvidenceType = ValuationEvidenceType.INVESTMENT,
    stage: ValuationStage = ValuationStage.FINAL,
) -> Valuation:
    return Valuation(
        group_id=group_id,
        stage=stage,
        evidence_type=evidence_type,
        narrative="test",
        confidence=0.8,
        dollar_low=dollar_low,
        dollar_high=dollar_high,
        dollar_mid=dollar_mid,
    )


def _compare_dollar_estimates(dollar_a: float, dollar_b: float):
    """Compare two dollar estimates, return (agreement_ratio, is_confirmation, is_contradiction, severity).

    Thin wrapper around _compare_pair for ergonomic testing of the pure math.
    agreement_ratio is the min/max ratio for agreements, or the max/min ratio
    for contradictions (disagreement_ratio).
    """
    result = _compare_pair("src_a", "src_b", dollar_a, dollar_b, "test_dim")
    if result is None:
        return (None, False, False, None)
    if isinstance(result, SourceAgreement):
        return (result.agreement_ratio, True, False, None)
    # SourceDisagreement
    return (result.disagreement_ratio, False, True, result.severity)


# ── _compare_pair unit tests ─────────────────────────────────────────────


class TestComparePair:
    """Unit tests for _compare_pair: the core comparison function."""

    def test_perfect_agreement(self):
        """Perfect agreement: $1M vs $1M -> ratio=1.0, confirmation=True."""
        result = _compare_pair("A", "B", 1_000_000, 1_000_000, "cost")
        assert isinstance(result, SourceAgreement)
        assert result.agreement_ratio == 1.0

    def test_close_agreement(self):
        """Close agreement: $1M vs $800K -> ratio=0.8, confirmation=True."""
        result = _compare_pair("A", "B", 1_000_000, 800_000, "cost")
        assert isinstance(result, SourceAgreement)
        assert result.agreement_ratio == pytest.approx(0.8)

    def test_borderline_agreement(self):
        """Borderline: $1M vs $500K -> ratio=0.5, confirmation=True (barely)."""
        result = _compare_pair("A", "B", 1_000_000, 500_000, "cost")
        assert isinstance(result, SourceAgreement)
        assert result.agreement_ratio == pytest.approx(0.5)

    def test_mild_disagreement(self):
        """$1M vs $400K -> ratio=0.4 < 0.5: contradiction, disagreement_ratio=2.5, severity='medium'."""
        result = _compare_pair("A", "B", 1_000_000, 400_000, "cost")
        assert isinstance(result, SourceDisagreement)
        assert result.disagreement_ratio == pytest.approx(2.5)
        assert result.severity == "medium"

    def test_strong_disagreement_high_severity(self):
        """$1M vs $200K -> disagreement_ratio=5.0, severity='high' (>3x)."""
        result = _compare_pair("A", "B", 1_000_000, 200_000, "cost")
        assert isinstance(result, SourceDisagreement)
        assert result.disagreement_ratio == pytest.approx(5.0)
        assert result.severity == "high"

    def test_medium_disagreement(self):
        """$1M vs $400K -> disagreement_ratio=2.5, severity='medium' (2-3x)."""
        result = _compare_pair("A", "B", 1_000_000, 400_000, "cost")
        assert isinstance(result, SourceDisagreement)
        assert result.disagreement_ratio == pytest.approx(2.5)
        assert result.severity == "medium"

    def test_severity_low_under_2x(self):
        """Disagreement ratio < 2.0 -> severity='low'.

        Note: with AGREEMENT_RATIO_THRESHOLD=0.5, a ratio < 0.5 means
        disagreement_ratio > 2.0, so 'low' severity requires the threshold
        to produce ratio in (0, 0.5) with disagreement_ratio < 2.0.
        In practice, ratio < 0.5 implies disagreement >= 2x, so 'low' severity
        only occurs if the threshold were different. We still test the classifier.
        """
        # 499k / 1M = 0.499 < 0.5 threshold -> contradiction
        # disagreement_ratio = 1M / 499k = ~2.004 -> "medium" not "low"
        # To get "low" severity, we'd need ratio very close to 0.5 from below,
        # but math says min/max < 0.5 implies max/min > 2.0.
        # So "low" is unreachable with current thresholds. Verify this:
        result = _compare_pair("A", "B", 1_000_000, 499_000, "cost")
        assert isinstance(result, SourceDisagreement)
        assert result.disagreement_ratio > 2.0
        assert result.severity == "medium"

    def test_severity_boundary_at_3x(self):
        """Exactly 3x disagreement -> severity='medium' (<=3.0)."""
        # Use values that produce exactly 3.0: 900_000 / 300_000 = 3.0
        result = _compare_pair("A", "B", 900_000, 300_000, "cost")
        assert isinstance(result, SourceDisagreement)
        assert result.disagreement_ratio == pytest.approx(3.0)
        assert result.severity == "medium"

    def test_severity_just_over_3x(self):
        """Just over 3x disagreement -> severity='high'."""
        result = _compare_pair("A", "B", 1_000_000, 330_000, "cost")
        assert isinstance(result, SourceDisagreement)
        assert result.disagreement_ratio > 3.0
        assert result.severity == "high"

    def test_zero_dollar_a_returns_none(self):
        """$0 vs $1M -> handle gracefully (None)."""
        result = _compare_pair("A", "B", 0, 1_000_000, "cost")
        assert result is None

    def test_zero_dollar_b_returns_none(self):
        """$1M vs $0 -> handle gracefully (None)."""
        result = _compare_pair("A", "B", 1_000_000, 0, "cost")
        assert result is None

    def test_negative_dollar_a_returns_none(self):
        """-$500K vs $1M -> handle gracefully (None)."""
        result = _compare_pair("A", "B", -500_000, 1_000_000, "cost")
        assert result is None

    def test_negative_dollar_b_returns_none(self):
        """$1M vs -$500K -> handle gracefully (None)."""
        result = _compare_pair("A", "B", 1_000_000, -500_000, "cost")
        assert result is None

    def test_both_negative_returns_none(self):
        """-$500K vs -$300K -> handle gracefully (None)."""
        result = _compare_pair("A", "B", -500_000, -300_000, "cost")
        assert result is None

    def test_both_zero_returns_none(self):
        """$0 vs $0 -> handle gracefully (None)."""
        result = _compare_pair("A", "B", 0, 0, "cost")
        assert result is None

    def test_preserves_source_labels(self):
        """Source labels and dimension are correctly stored in the result."""
        result = _compare_pair("filing", "news", 1_000_000, 900_000, "revenue")
        assert isinstance(result, SourceAgreement)
        assert result.source_a == "filing"
        assert result.source_b == "news"
        assert result.dimension == "revenue"
        assert result.dollar_a == 1_000_000
        assert result.dollar_b == 900_000

    def test_preserves_source_labels_disagreement(self):
        """Source labels are preserved on disagreement results too."""
        result = _compare_pair("analyst", "sec", 1_000_000, 100_000, "cost")
        assert isinstance(result, SourceDisagreement)
        assert result.source_a == "analyst"
        assert result.source_b == "sec"
        assert result.dimension == "cost"

    def test_symmetry_agreement(self):
        """Swapping dollar_a and dollar_b produces the same agreement ratio."""
        r1 = _compare_pair("A", "B", 1_000_000, 700_000, "cost")
        r2 = _compare_pair("A", "B", 700_000, 1_000_000, "cost")
        assert isinstance(r1, SourceAgreement)
        assert isinstance(r2, SourceAgreement)
        assert r1.agreement_ratio == pytest.approx(r2.agreement_ratio)

    def test_symmetry_disagreement(self):
        """Swapping dollar_a and dollar_b produces the same disagreement ratio."""
        r1 = _compare_pair("A", "B", 1_000_000, 100_000, "cost")
        r2 = _compare_pair("A", "B", 100_000, 1_000_000, "cost")
        assert isinstance(r1, SourceDisagreement)
        assert isinstance(r2, SourceDisagreement)
        assert r1.disagreement_ratio == pytest.approx(r2.disagreement_ratio)

    def test_very_small_positive_values(self):
        """Very small positive values should still produce a valid comparison."""
        result = _compare_pair("A", "B", 0.01, 0.01, "cost")
        assert isinstance(result, SourceAgreement)
        assert result.agreement_ratio == 1.0

    def test_very_large_values(self):
        """Very large values should produce valid comparisons."""
        result = _compare_pair("A", "B", 1e12, 9e11, "cost")
        assert isinstance(result, SourceAgreement)
        assert result.agreement_ratio == pytest.approx(0.9)


# ── _compare_dollar_estimates helper tests (parametrized) ────────────────


class TestCompareDollarEstimates:
    """Parametrized tests using the _compare_dollar_estimates helper."""

    @pytest.mark.parametrize(
        "dollar_a, dollar_b, expected_ratio, is_conf, is_contra, severity",
        [
            # Perfect agreement
            (1_000_000, 1_000_000, 1.0, True, False, None),
            # Close agreement
            (1_000_000, 800_000, 0.8, True, False, None),
            # Borderline agreement (ratio=0.5, exactly at threshold)
            (1_000_000, 500_000, 0.5, True, False, None),
            # Below threshold -> contradiction, medium severity (2.5x)
            (1_000_000, 400_000, 2.5, False, True, "medium"),
            # High severity contradiction (>3x)
            (1_000_000, 100_000, 10.0, False, True, "high"),
            # Exactly at 3x boundary -> medium severity (<=3.0)
            (900_000, 300_000, 3.0, False, True, "medium"),
            # Zero dollar_a -> None
            (0, 1_000_000, None, False, False, None),
            # Both zero -> None
            (0, 0, None, False, False, None),
            # Negative -> None
            (-500_000, 1_000_000, None, False, False, None),
        ],
        ids=[
            "perfect_agreement",
            "close_agreement_0.8",
            "borderline_agreement_0.5",
            "medium_contradiction_2.5x",
            "high_contradiction_10x",
            "medium_at_3x_boundary",
            "zero_dollar_a",
            "both_zero",
            "negative_dollar",
        ],
    )
    def test_comparison_cases(self, dollar_a, dollar_b, expected_ratio,
                              is_conf, is_contra, severity):
        ratio, conf, contra, sev = _compare_dollar_estimates(dollar_a, dollar_b)
        assert conf == is_conf
        assert contra == is_contra
        assert sev == severity
        if expected_ratio is not None:
            assert ratio == pytest.approx(expected_ratio, abs=0.01)
        else:
            assert ratio is None


# ── CrossSourceVerifier tests ─────────────────────────────────────────────


class TestCrossSourceVerifier:
    def setup_method(self):
        self.verifier = CrossSourceVerifier()

    def test_two_sources_agreeing(self):
        """Two independent sources with similar dollar estimates -> confirmation."""
        groups = [
            _make_group(1, source_types=["filing"]),
            _make_group(2, source_types=["news"]),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),
            _make_valuation(2, dollar_mid=900_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 1
        assert len(result.contradictions) == 0
        assert result.agreement_score == 1.0
        conf = result.confirmations[0]
        assert conf.dollar_a == 1_000_000
        assert conf.dollar_b == 900_000
        assert conf.agreement_ratio == pytest.approx(0.9)

    def test_two_sources_disagreeing(self):
        """Two independent sources with very different estimates -> contradiction."""
        groups = [
            _make_group(1, source_types=["filing"]),
            _make_group(2, source_types=["news"]),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),
            _make_valuation(2, dollar_mid=100_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 0
        assert len(result.contradictions) == 1
        assert result.agreement_score == 0.0
        contra = result.contradictions[0]
        assert contra.disagreement_ratio == pytest.approx(10.0)
        assert contra.severity == "high"

    def test_mixed_scenario(self):
        """Three sources: two agree, one contradicts both."""
        groups = [
            _make_group(1, source_types=["filing"], dimension=TargetDimension.COST),
            _make_group(2, source_types=["news"], dimension=TargetDimension.COST),
            _make_group(3, source_types=["analyst"], dimension=TargetDimension.COST),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),  # filing
            _make_valuation(2, dollar_mid=900_000),     # news (agrees with filing)
            _make_valuation(3, dollar_mid=100_000),     # analyst (contradicts both)
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        # filing vs news: agree (0.9 ratio)
        # filing vs analyst: contradict (0.1 ratio, high severity)
        # news vs analyst: contradict (~0.111 ratio, high severity)
        assert len(result.confirmations) == 1
        assert len(result.contradictions) == 2
        assert result.agreement_score == pytest.approx(1 / 3)

    def test_mixed_scenario_confidence_adjustment(self):
        """Mixed scenario: 1 confirmation + 2 contradictions -> negative adjustment."""
        groups = [
            _make_group(1, source_types=["filing"], dimension=TargetDimension.COST),
            _make_group(2, source_types=["news"], dimension=TargetDimension.COST),
            _make_group(3, source_types=["analyst"], dimension=TargetDimension.COST),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),
            _make_valuation(2, dollar_mid=900_000),
            _make_valuation(3, dollar_mid=100_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        # 1 confirmation * 0.03 - 2 contradictions * 0.05 = 0.03 - 0.10 = -0.07
        expected = 1 * CONFIRMATION_BOOST - 2 * CONTRADICTION_PENALTY
        assert result.confidence_adjustment == pytest.approx(expected)
        assert result.confidence_adjustment < 0.0

    def test_single_source_no_comparison(self):
        """Only one source -> no comparisons possible, neutral adjustment."""
        groups = [_make_group(1, source_types=["filing"])]
        valuations = [_make_valuation(1, dollar_mid=1_000_000)]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 0
        assert len(result.contradictions) == 0
        assert result.agreement_score == 0.0
        assert result.confidence_adjustment == 0.0

    def test_same_source_types_skipped(self):
        """Two groups from the same source type are not compared."""
        groups = [
            _make_group(1, source_types=["filing"]),
            _make_group(2, source_types=["filing"]),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),
            _make_valuation(2, dollar_mid=900_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 0
        assert len(result.contradictions) == 0

    def test_zero_dollar_estimates_ignored(self):
        """Valuations with zero dollar estimates are skipped."""
        groups = [
            _make_group(1, source_types=["filing"]),
            _make_group(2, source_types=["news"]),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=0),
            _make_valuation(2, dollar_mid=1_000_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 0
        assert len(result.contradictions) == 0

    def test_negative_dollar_estimates_ignored(self):
        """Valuations with negative dollar estimates are skipped."""
        groups = [
            _make_group(1, source_types=["filing"]),
            _make_group(2, source_types=["news"]),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=-500_000),
            _make_valuation(2, dollar_mid=1_000_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 0
        assert len(result.contradictions) == 0

    def test_dollar_mid_computed_from_low_high(self):
        """dollar_mid is computed from (dollar_low + dollar_high) / 2 when not set."""
        groups = [
            _make_group(1, source_types=["filing"]),
            _make_group(2, source_types=["news"]),
        ]
        valuations = [
            _make_valuation(1, dollar_low=800_000, dollar_high=1_200_000),  # mid = 1M
            _make_valuation(2, dollar_low=850_000, dollar_high=1_150_000),  # mid = 1M
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 1
        assert result.confirmations[0].agreement_ratio == 1.0

    def test_no_groups_provided_uses_valuation_labels(self):
        """When groups are not provided, source labels come from valuation metadata."""
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000, evidence_type=ValuationEvidenceType.PLAN),
            _make_valuation(2, dollar_mid=900_000, evidence_type=ValuationEvidenceType.CAPTURE),
        ]
        result = self.verifier.verify_valuations(valuations, groups=None)

        # Different source labels since group_ids differ
        assert len(result.confirmations) == 1

    def test_different_dimensions_not_compared(self):
        """Valuations in different dimensions are not compared."""
        groups = [
            _make_group(1, source_types=["filing"], dimension=TargetDimension.COST),
            _make_group(2, source_types=["news"], dimension=TargetDimension.REVENUE),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),
            _make_valuation(2, dollar_mid=1_000_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 0
        assert len(result.contradictions) == 0

    def test_verify_groups_returns_empty(self):
        """verify_groups returns empty result (groups lack dollar data)."""
        groups = [
            _make_group(1, source_types=["filing"]),
            _make_group(2, source_types=["news"]),
        ]
        result = self.verifier.verify_groups(groups)

        assert len(result.confirmations) == 0
        assert len(result.contradictions) == 0
        assert result.agreement_score == 0.0
        assert result.confidence_adjustment == 0.0

    def test_empty_inputs(self):
        """Empty valuations list returns neutral result."""
        result = self.verifier.verify_valuations([], groups=[])
        assert result.agreement_score == 0.0
        assert result.confidence_adjustment == 0.0

    def test_all_confirmations_boost_confidence(self):
        """All confirmations -> agreement_score=1.0, confidence_adjustment > 0."""
        groups = [
            _make_group(1, source_types=["filing"], dimension=TargetDimension.COST),
            _make_group(2, source_types=["news"], dimension=TargetDimension.COST),
            _make_group(3, source_types=["analyst"], dimension=TargetDimension.COST),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),
            _make_valuation(2, dollar_mid=950_000),
            _make_valuation(3, dollar_mid=1_050_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.contradictions) == 0
        assert len(result.confirmations) == 3  # all 3 pairs agree
        assert result.agreement_score == 1.0
        assert result.confidence_adjustment > 0.0

    def test_all_contradictions_reduce_confidence(self):
        """All contradictions -> agreement_score=0.0, confidence_adjustment < 0."""
        groups = [
            _make_group(1, source_types=["filing"], dimension=TargetDimension.COST),
            _make_group(2, source_types=["news"], dimension=TargetDimension.COST),
            _make_group(3, source_types=["analyst"], dimension=TargetDimension.COST),
        ]
        # Each pair disagrees: 1M vs 50K vs 10M (all ratios < 0.5)
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),
            _make_valuation(2, dollar_mid=50_000),
            _make_valuation(3, dollar_mid=10_000_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 0
        assert len(result.contradictions) == 3
        assert result.agreement_score == 0.0
        assert result.confidence_adjustment < 0.0

    def test_four_sources_pairwise_combinations(self):
        """Four sources in the same dimension produce C(4,2)=6 pairwise comparisons."""
        groups = [
            _make_group(1, source_types=["filing"], dimension=TargetDimension.COST),
            _make_group(2, source_types=["news"], dimension=TargetDimension.COST),
            _make_group(3, source_types=["analyst"], dimension=TargetDimension.COST),
            _make_group(4, source_types=["web"], dimension=TargetDimension.COST),
        ]
        # All agree closely
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),
            _make_valuation(2, dollar_mid=950_000),
            _make_valuation(3, dollar_mid=1_050_000),
            _make_valuation(4, dollar_mid=980_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        total = len(result.confirmations) + len(result.contradictions)
        assert total == 6  # C(4,2) = 6

    def test_valuation_without_dollar_data_skipped(self):
        """Valuations with no dollar_mid and no dollar_low/high are skipped."""
        groups = [
            _make_group(1, source_types=["filing"]),
            _make_group(2, source_types=["news"]),
        ]
        valuations = [
            _make_valuation(1),  # no dollar data
            _make_valuation(2, dollar_mid=1_000_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 0
        assert len(result.contradictions) == 0

    def test_multi_source_type_group_label(self):
        """A group with multiple source types produces a sorted, joined label."""
        groups = [
            _make_group(1, source_types=["news", "filing"]),
            _make_group(2, source_types=["analyst"]),
        ]
        valuations = [
            _make_valuation(1, dollar_mid=1_000_000),
            _make_valuation(2, dollar_mid=900_000),
        ]
        result = self.verifier.verify_valuations(valuations, groups=groups)

        assert len(result.confirmations) == 1
        # Source label for group 1 should be "filing+news" (sorted)
        conf = result.confirmations[0]
        assert conf.source_a == "filing+news"
        assert conf.source_b == "analyst"


# ── Agreement score tests ─────────────────────────────────────────────────


class TestAgreementScore:
    def test_all_confirmations(self):
        confs = [SourceAgreement(source_a="A", source_b="B", dimension="cost",
                                  dollar_a=1e6, dollar_b=9e5, agreement_ratio=0.9)]
        score = _compute_agreement_score(confs, [])
        assert score == 1.0

    def test_all_contradictions(self):
        contras = [SourceDisagreement(source_a="A", source_b="B", dimension="cost",
                                       dollar_a=1e6, dollar_b=1e5,
                                       disagreement_ratio=10.0, severity="high")]
        score = _compute_agreement_score([], contras)
        assert score == 0.0

    def test_mixed(self):
        confs = [SourceAgreement(source_a="A", source_b="B", dimension="cost",
                                  dollar_a=1e6, dollar_b=9e5, agreement_ratio=0.9)]
        contras = [SourceDisagreement(source_a="A", source_b="C", dimension="cost",
                                       dollar_a=1e6, dollar_b=1e5,
                                       disagreement_ratio=10.0, severity="high")]
        score = _compute_agreement_score(confs, contras)
        assert score == pytest.approx(0.5)

    def test_no_comparisons(self):
        score = _compute_agreement_score([], [])
        assert score == 0.0

    def test_many_confirmations_one_contradiction(self):
        """5 confirmations + 1 contradiction -> score = 5/6."""
        confs = [
            SourceAgreement(source_a="A", source_b=f"B{i}", dimension="cost",
                            dollar_a=1e6, dollar_b=9e5, agreement_ratio=0.9)
            for i in range(5)
        ]
        contras = [SourceDisagreement(source_a="A", source_b="C", dimension="cost",
                                       dollar_a=1e6, dollar_b=1e5,
                                       disagreement_ratio=10.0, severity="high")]
        score = _compute_agreement_score(confs, contras)
        assert score == pytest.approx(5 / 6)

    def test_one_confirmation_many_contradictions(self):
        """1 confirmation + 5 contradictions -> score = 1/6."""
        confs = [SourceAgreement(source_a="A", source_b="B", dimension="cost",
                                  dollar_a=1e6, dollar_b=9e5, agreement_ratio=0.9)]
        contras = [
            SourceDisagreement(source_a="A", source_b=f"C{i}", dimension="cost",
                                dollar_a=1e6, dollar_b=1e5,
                                disagreement_ratio=10.0, severity="high")
            for i in range(5)
        ]
        score = _compute_agreement_score(confs, contras)
        assert score == pytest.approx(1 / 6)


# ── Confidence adjustment tests ───────────────────────────────────────────


class TestConfidenceAdjustment:
    """Tests for the additive confidence adjustment formula.

    The formula is:
        adjustment = num_confirmations * CONFIRMATION_BOOST - num_contradictions * CONTRADICTION_PENALTY
        clamped to [MIN_PENALTY, MAX_BOOST]
    With no data, returns 0.0 (neutral).
    """

    def test_no_data_neutral(self):
        assert compute_confidence_adjustment_from_counts(0, 0) == 0.0

    def test_one_confirmation_boost(self):
        adj = compute_confidence_adjustment_from_counts(1, 0)
        assert adj == pytest.approx(CONFIRMATION_BOOST)

    def test_two_confirmations_boost(self):
        adj = compute_confidence_adjustment_from_counts(2, 0)
        assert adj == pytest.approx(2 * CONFIRMATION_BOOST)

    def test_three_confirmations_boost(self):
        adj = compute_confidence_adjustment_from_counts(3, 0)
        assert adj == pytest.approx(3 * CONFIRMATION_BOOST)

    def test_max_boost_capped(self):
        """Many confirmations should cap at MAX_BOOST."""
        adj = compute_confidence_adjustment_from_counts(20, 0)
        assert adj == MAX_BOOST

    def test_one_contradiction_penalty(self):
        adj = compute_confidence_adjustment_from_counts(0, 1)
        assert adj == pytest.approx(-CONTRADICTION_PENALTY)

    def test_two_contradictions_penalty(self):
        adj = compute_confidence_adjustment_from_counts(0, 2)
        assert adj == pytest.approx(-2 * CONTRADICTION_PENALTY)

    def test_three_contradictions_penalty(self):
        adj = compute_confidence_adjustment_from_counts(0, 3)
        assert adj == pytest.approx(-3 * CONTRADICTION_PENALTY)

    def test_min_penalty_floor(self):
        """Many contradictions should floor at MIN_PENALTY."""
        adj = compute_confidence_adjustment_from_counts(0, 20)
        assert adj == MIN_PENALTY

    def test_mixed_confirmations_and_contradictions(self):
        # 1 confirmation (+0.03) + 1 contradiction (-0.05) = -0.02
        adj = compute_confidence_adjustment_from_counts(1, 1)
        assert adj == pytest.approx(CONFIRMATION_BOOST - CONTRADICTION_PENALTY)

    def test_compute_from_result(self):
        result = VerificationResult(
            confirmations=[
                SourceAgreement(source_a="A", source_b="B", dimension="cost",
                                dollar_a=1e6, dollar_b=9e5, agreement_ratio=0.9),
            ],
            contradictions=[],
            agreement_score=1.0,
            confidence_adjustment=CONFIRMATION_BOOST,
        )
        adj = compute_confidence_adjustment(result)
        assert adj == pytest.approx(CONFIRMATION_BOOST)

    def test_confirmations_boost_contradictions_reduce(self):
        """Verify that confirmations increase and contradictions decrease the adjustment."""
        boost_only = compute_confidence_adjustment_from_counts(3, 0)
        penalty_only = compute_confidence_adjustment_from_counts(0, 3)
        assert boost_only > 0.0
        assert penalty_only < 0.0

    def test_adjustment_always_within_bounds(self):
        """Adjustment is always between MIN_PENALTY and MAX_BOOST."""
        for n_conf in range(0, 15):
            for n_contra in range(0, 15):
                adj = compute_confidence_adjustment_from_counts(n_conf, n_contra)
                assert MIN_PENALTY <= adj <= MAX_BOOST

    def test_more_confirmations_means_higher_adjustment(self):
        """Increasing confirmations (with fixed contradictions) monotonically increases adjustment."""
        adjs = [compute_confidence_adjustment_from_counts(n, 0) for n in range(10)]
        for i in range(len(adjs) - 1):
            assert adjs[i] <= adjs[i + 1]

    def test_more_contradictions_means_lower_adjustment(self):
        """Increasing contradictions (with fixed confirmations) monotonically decreases adjustment."""
        adjs = [compute_confidence_adjustment_from_counts(0, n) for n in range(10)]
        for i in range(len(adjs) - 1):
            assert adjs[i] >= adjs[i + 1]

    def test_confirmations_can_offset_contradictions(self):
        """Enough confirmations can bring adjustment positive after a contradiction."""
        # 1 contradiction = -0.05, 2 confirmations = +0.06 -> net +0.01
        adj = compute_confidence_adjustment_from_counts(2, 1)
        assert adj > 0.0

    def test_contradictions_can_offset_confirmations(self):
        """Enough contradictions can bring adjustment negative after confirmations."""
        # 1 confirmation = +0.03, 1 contradiction = -0.05 -> net -0.02
        adj = compute_confidence_adjustment_from_counts(1, 1)
        assert adj < 0.0

    def test_exact_boundary_values(self):
        """Verify exact values at specific counts to lock down the formula."""
        # 0 conf, 0 contra -> 0.0
        assert compute_confidence_adjustment_from_counts(0, 0) == 0.0
        # 1 conf -> +0.03
        assert compute_confidence_adjustment_from_counts(1, 0) == pytest.approx(0.03)
        # 2 conf -> +0.06
        assert compute_confidence_adjustment_from_counts(2, 0) == pytest.approx(0.06)
        # 1 contra -> -0.05
        assert compute_confidence_adjustment_from_counts(0, 1) == pytest.approx(-0.05)
        # 2 contra -> -0.10
        assert compute_confidence_adjustment_from_counts(0, 2) == pytest.approx(-0.10)
        # 4 conf -> 0.12 but capped at MAX_BOOST (0.1)
        assert compute_confidence_adjustment_from_counts(4, 0) == MAX_BOOST
        # 5 contra -> -0.25 but floored at MIN_PENALTY (-0.2)
        assert compute_confidence_adjustment_from_counts(0, 5) == MIN_PENALTY


# ── VerificationResult model tests ────────────────────────────────────────


class TestVerificationResultModel:
    def test_default_values(self):
        result = VerificationResult()
        assert result.confirmations == []
        assert result.contradictions == []
        assert result.agreement_score == 0.0
        assert result.confidence_adjustment == 0.0
        assert result.company_id == 0
        assert result.ticker == ""

    def test_backward_compat_fields(self):
        """company_id and ticker are settable for GraphQL resolver compat."""
        result = VerificationResult(company_id=42, ticker="AAPL")
        assert result.company_id == 42
        assert result.ticker == "AAPL"
        # Can also be set after construction
        result.ticker = "MSFT"
        assert result.ticker == "MSFT"

    def test_result_with_confirmations_only(self):
        """VerificationResult with all confirmations has agreement_score > 0.5."""
        confs = [
            SourceAgreement(source_a="A", source_b="B", dimension="cost",
                            dollar_a=1e6, dollar_b=9e5, agreement_ratio=0.9),
            SourceAgreement(source_a="A", source_b="C", dimension="cost",
                            dollar_a=1e6, dollar_b=8e5, agreement_ratio=0.8),
        ]
        adj = 2 * CONFIRMATION_BOOST
        result = VerificationResult(
            confirmations=confs,
            contradictions=[],
            agreement_score=1.0,
            confidence_adjustment=adj,
        )
        assert result.agreement_score > 0.5
        assert result.confidence_adjustment > 0.0

    def test_result_with_contradictions_only(self):
        """VerificationResult with all contradictions has confidence_adjustment < 0."""
        contras = [
            SourceDisagreement(source_a="A", source_b="B", dimension="cost",
                                dollar_a=1e6, dollar_b=1e5,
                                disagreement_ratio=10.0, severity="high"),
        ]
        result = VerificationResult(
            confirmations=[],
            contradictions=contras,
            agreement_score=0.0,
            confidence_adjustment=-CONTRADICTION_PENALTY,
        )
        assert result.agreement_score == 0.0
        assert result.confidence_adjustment < 0.0

    def test_result_with_mixed(self):
        """VerificationResult with mixed confirmations/contradictions."""
        confs = [
            SourceAgreement(source_a="A", source_b="B", dimension="cost",
                            dollar_a=1e6, dollar_b=9e5, agreement_ratio=0.9),
        ]
        contras = [
            SourceDisagreement(source_a="A", source_b="C", dimension="cost",
                                dollar_a=1e6, dollar_b=1e5,
                                disagreement_ratio=10.0, severity="high"),
        ]
        adj = CONFIRMATION_BOOST - CONTRADICTION_PENALTY
        result = VerificationResult(
            confirmations=confs,
            contradictions=contras,
            agreement_score=0.5,
            confidence_adjustment=adj,
        )
        assert result.agreement_score == 0.5
        assert result.confidence_adjustment == pytest.approx(adj)

    def test_no_comparisons_result(self):
        """VerificationResult with no comparisons -> agreement_score=0.0, adjustment=0.0."""
        result = VerificationResult()
        assert result.agreement_score == 0.0
        assert result.confidence_adjustment == 0.0


# ── _build_result integration tests ──────────────────────────────────────


class TestBuildResult:
    """Test that CrossSourceVerifier._build_result correctly computes scores."""

    def setup_method(self):
        self.verifier = CrossSourceVerifier()

    def test_build_result_no_data(self):
        result = self.verifier._build_result([], [])
        assert result.agreement_score == 0.0
        assert result.confidence_adjustment == 0.0

    def test_build_result_with_confirmations(self):
        confs = [
            SourceAgreement(source_a="A", source_b="B", dimension="cost",
                            dollar_a=1e6, dollar_b=9e5, agreement_ratio=0.9),
            SourceAgreement(source_a="A", source_b="C", dimension="cost",
                            dollar_a=1e6, dollar_b=8e5, agreement_ratio=0.8),
        ]
        result = self.verifier._build_result(confs, [])
        assert result.agreement_score == 1.0
        assert result.confidence_adjustment == pytest.approx(2 * CONFIRMATION_BOOST)

    def test_build_result_with_contradictions(self):
        contras = [
            SourceDisagreement(source_a="A", source_b="B", dimension="cost",
                                dollar_a=1e6, dollar_b=1e5,
                                disagreement_ratio=10.0, severity="high"),
        ]
        result = self.verifier._build_result([], contras)
        assert result.agreement_score == 0.0
        assert result.confidence_adjustment == pytest.approx(-CONTRADICTION_PENALTY)

    def test_build_result_mixed(self):
        confs = [
            SourceAgreement(source_a="A", source_b="B", dimension="cost",
                            dollar_a=1e6, dollar_b=9e5, agreement_ratio=0.9),
        ]
        contras = [
            SourceDisagreement(source_a="A", source_b="C", dimension="cost",
                                dollar_a=1e6, dollar_b=1e5,
                                disagreement_ratio=10.0, severity="high"),
        ]
        result = self.verifier._build_result(confs, contras)
        assert result.agreement_score == pytest.approx(0.5)
        assert result.confidence_adjustment == pytest.approx(
            CONFIRMATION_BOOST - CONTRADICTION_PENALTY
        )
