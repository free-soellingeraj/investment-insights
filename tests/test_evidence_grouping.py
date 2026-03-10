"""Tests for the evidence munger grouping/similarity system.

Covers:
- _text_similarity: pairwise string similarity via SequenceMatcher
- _group_passages: greedy clustering of passages by similarity
- Dimension separation in munge_evidence (cost vs revenue never mixed)
- MAX_GROUP_SIZE enforcement
- Full munge_evidence flow with mocked file I/O
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from ai_opportunity_index.domains import (
    EvidenceGroup,
    EvidenceGroupPassage,
    TargetDimension,
)
from ai_opportunity_index.scoring.evidence_munger import (
    MAX_GROUP_SIZE,
    SIMILARITY_THRESHOLD,
    _group_passages,
    _text_similarity,
    munge_evidence,
)

# ── Load fixture data ────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parent / "fixtures"

with open(FIXTURES_DIR / "grouping_cases.json") as f:
    CASES = json.load(f)


# ── Helpers ──────────────────────────────────────────────────────────────


def _make_passage(
    text: str,
    dimension: str = "cost",
    confidence: float = 0.5,
    source_type: str = "filing",
    source_date: date | None = None,
) -> EvidenceGroupPassage:
    """Build a minimal EvidenceGroupPassage for testing."""
    return EvidenceGroupPassage(
        passage_text=text,
        target_dimension=dimension,
        confidence=confidence,
        source_type=source_type,
        source_date=source_date,
    )


# ══════════════════════════════════════════════════════════════════════════
# _text_similarity tests
# ══════════════════════════════════════════════════════════════════════════


class TestTextSimilarity:
    """Parameterized tests for _text_similarity using fixture data."""

    @pytest.mark.parametrize(
        "case",
        CASES["text_similarity"],
        ids=[c["id"] for c in CASES["text_similarity"]],
    )
    def test_similarity_within_tolerance(self, case: dict) -> None:
        result = _text_similarity(case["a"], case["b"])
        assert result == pytest.approx(
            case["expected"], abs=case["tolerance"]
        ), f"similarity({case['a'][:40]!r}, {case['b'][:40]!r}) = {result}"

    def test_symmetry(self) -> None:
        """Similarity should be symmetric: sim(a,b) == sim(b,a)."""
        a = "The company invested in AI cost reduction."
        b = "AI cost reduction investments by the company."
        assert _text_similarity(a, b) == pytest.approx(
            _text_similarity(b, a), abs=1e-9
        )

    def test_returns_float_between_0_and_1(self) -> None:
        result = _text_similarity("hello world", "goodbye moon")
        assert 0.0 <= result <= 1.0


# ══════════════════════════════════════════════════════════════════════════
# _group_passages tests
# ══════════════════════════════════════════════════════════════════════════


class TestGroupPassages:
    """Tests for the greedy clustering in _group_passages."""

    @pytest.mark.parametrize(
        "case",
        CASES["passage_grouping"],
        ids=[c["id"] for c in CASES["passage_grouping"]],
    )
    def test_grouping_from_fixtures(self, case: dict) -> None:
        passages = [
            _make_passage(
                text=p["passage_text"],
                dimension=p.get("target_dimension", "cost"),
                confidence=p.get("confidence", 0.5),
            )
            for p in case["passages"]
        ]
        groups = _group_passages(passages)
        assert len(groups) == case["expected_num_groups"]

        if "expected_group_sizes" in case:
            actual_sizes = sorted([len(g) for g in groups])
            expected_sizes = sorted(case["expected_group_sizes"])
            assert actual_sizes == expected_sizes

        if "first_group_max_size" in case:
            assert len(groups[0]) <= case["first_group_max_size"]

    def test_empty_returns_empty(self) -> None:
        assert _group_passages([]) == []

    def test_single_passage_returns_one_group(self) -> None:
        p = _make_passage("A single passage about AI.")
        groups = _group_passages([p])
        assert len(groups) == 1
        assert groups[0] == [p]

    def test_max_group_size_enforced(self) -> None:
        """Groups must not exceed MAX_GROUP_SIZE (8)."""
        # 12 identical passages should produce at least 2 groups
        passages = [
            _make_passage("Identical AI cost text for grouping test.")
            for _ in range(12)
        ]
        groups = _group_passages(passages)
        for g in groups:
            assert len(g) <= MAX_GROUP_SIZE
        # All passages should be accounted for
        assert sum(len(g) for g in groups) == 12

    def test_dissimilar_passages_stay_separate(self) -> None:
        passages = [
            _make_passage("Apple reported record iPhone revenue in Q4 2025."),
            _make_passage("Quantum computing research accelerates at CERN labs."),
            _make_passage("New regulations on autonomous vehicles passed in EU."),
        ]
        groups = _group_passages(passages)
        assert len(groups) == 3

    def test_all_passages_assigned_exactly_once(self) -> None:
        """Every input passage should appear in exactly one group."""
        passages = [
            _make_passage("AI cost reduction through automation of workflows."),
            _make_passage("AI cost reduction through automation of processes."),
            _make_passage("Revenue growth from new AI product launch."),
            _make_passage("Completely unrelated text about marine biology."),
        ]
        groups = _group_passages(passages)
        all_grouped = [p for g in groups for p in g]
        assert len(all_grouped) == len(passages)
        # Check no duplicates by identity
        ids_original = {id(p) for p in passages}
        ids_grouped = {id(p) for p in all_grouped}
        assert ids_original == ids_grouped


# ══════════════════════════════════════════════════════════════════════════
# Dimension separation tests
# ══════════════════════════════════════════════════════════════════════════


class TestDimensionSeparation:
    """Cost and revenue passages must never be grouped together in munge_evidence."""

    def test_cost_and_revenue_separated(self) -> None:
        """munge_evidence separates by target_dimension before clustering."""
        cost_passages = [
            _make_passage("AI reduces operational costs via automation of backend systems.", dimension="cost", confidence=0.8),
            _make_passage("AI reduces operational costs via automation of backend workflows.", dimension="cost", confidence=0.7),
        ]
        revenue_passages = [
            _make_passage("New AI-powered products are driving significant revenue growth.", dimension="revenue", confidence=0.6),
            _make_passage("New AI-powered products are driving substantial revenue increases.", dimension="revenue", confidence=0.5),
        ]
        all_passages = cost_passages + revenue_passages

        # Mock _load_passages to return our synthetic data, and bypass DB/file I/O
        with (
            patch("ai_opportunity_index.scoring.evidence_munger._load_passages", return_value=all_passages),
            patch("ai_opportunity_index.scoring.evidence_munger._load_filing_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._load_news_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._get_child_tickers", return_value=[]),
        ):
            groups = munge_evidence("TEST", company_id=1)

        # Should have separate groups for cost and revenue
        cost_groups = [g for g in groups if g.target_dimension == "cost"]
        revenue_groups = [g for g in groups if g.target_dimension == "revenue"]
        assert len(cost_groups) >= 1
        assert len(revenue_groups) >= 1

        # No group should mix dimensions
        for g in groups:
            dims = {p.target_dimension for p in g.passages}
            assert len(dims) == 1, f"Group mixes dimensions: {dims}"

    def test_general_dimension_separate_from_cost_and_revenue(self) -> None:
        passages = [
            _make_passage("AI automation reduces costs significantly.", dimension="cost", confidence=0.8),
            _make_passage("General AI strategy announcement by CEO.", dimension="general", confidence=0.7),
            _make_passage("New AI product drives revenue growth.", dimension="revenue", confidence=0.6),
        ]

        with (
            patch("ai_opportunity_index.scoring.evidence_munger._load_passages", return_value=passages),
            patch("ai_opportunity_index.scoring.evidence_munger._load_filing_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._load_news_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._get_child_tickers", return_value=[]),
        ):
            groups = munge_evidence("TEST", company_id=1)

        dimensions = {g.target_dimension for g in groups}
        assert "cost" in dimensions
        assert "revenue" in dimensions
        assert "general" in dimensions


# ══════════════════════════════════════════════════════════════════════════
# Full munge_evidence flow tests
# ══════════════════════════════════════════════════════════════════════════


class TestMungeEvidence:
    """Integration-level tests for the full munge_evidence function."""

    def test_empty_passages_returns_empty(self) -> None:
        with (
            patch("ai_opportunity_index.scoring.evidence_munger._load_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._load_filing_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._load_news_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._get_child_tickers", return_value=[]),
        ):
            groups = munge_evidence("EMPTY", company_id=99)
        assert groups == []

    def test_returns_evidence_groups(self) -> None:
        passages = [
            _make_passage(
                "Company invests $200M in AI infrastructure for cost automation.",
                dimension="cost",
                confidence=0.9,
                source_type="filing",
                source_date=date(2025, 6, 1),
            ),
            _make_passage(
                "Company invests $200M in AI infrastructure for cost reduction.",
                dimension="cost",
                confidence=0.85,
                source_type="news",
                source_date=date(2025, 7, 15),
            ),
        ]

        with (
            patch("ai_opportunity_index.scoring.evidence_munger._load_passages", return_value=passages),
            patch("ai_opportunity_index.scoring.evidence_munger._load_filing_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._load_news_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._get_child_tickers", return_value=[]),
        ):
            groups = munge_evidence("TEST", company_id=42, pipeline_run_id=7)

        assert len(groups) >= 1
        for g in groups:
            assert isinstance(g, EvidenceGroup)
            assert g.company_id == 42
            assert g.pipeline_run_id == 7
            assert g.passage_count == len(g.passages)
            assert g.representative_text is not None

    def test_dates_and_confidence_aggregated(self) -> None:
        passages = [
            _make_passage("AI cost savings of $10M reported.", dimension="cost", confidence=0.9, source_date=date(2025, 1, 1)),
            _make_passage("AI cost savings of $10M announced.", dimension="cost", confidence=0.7, source_date=date(2025, 6, 1)),
        ]

        with (
            patch("ai_opportunity_index.scoring.evidence_munger._load_passages", return_value=passages),
            patch("ai_opportunity_index.scoring.evidence_munger._load_filing_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._load_news_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._get_child_tickers", return_value=[]),
        ):
            groups = munge_evidence("TEST", company_id=1)

        # Find the group containing both passages
        big_group = [g for g in groups if g.passage_count == 2]
        assert len(big_group) == 1
        g = big_group[0]
        assert g.date_earliest == date(2025, 1, 1)
        assert g.date_latest == date(2025, 6, 1)
        assert g.mean_confidence == pytest.approx(0.8, abs=0.01)
        assert g.max_confidence == pytest.approx(0.9, abs=0.01)

    def test_deduplication_by_text(self) -> None:
        """Passages with identical first 200 chars should be deduplicated."""
        passage = _make_passage(
            "Exact duplicate passage text that appears in both unified and legacy caches for dedup testing.",
            dimension="cost",
            confidence=0.8,
        )

        with (
            patch("ai_opportunity_index.scoring.evidence_munger._load_passages", return_value=[passage]),
            patch("ai_opportunity_index.scoring.evidence_munger._load_filing_passages", return_value=[passage]),
            patch("ai_opportunity_index.scoring.evidence_munger._load_news_passages", return_value=[passage]),
            patch("ai_opportunity_index.scoring.evidence_munger._get_child_tickers", return_value=[]),
        ):
            groups = munge_evidence("TEST", company_id=1)

        total_passages = sum(g.passage_count for g in groups)
        assert total_passages == 1, f"Expected 1 after dedup, got {total_passages}"

    def test_source_types_collected(self) -> None:
        passages = [
            _make_passage("AI cost reduction via automation.", dimension="cost", confidence=0.8, source_type="filing"),
            _make_passage("AI cost reduction via automation.", dimension="cost", confidence=0.7, source_type="news"),
        ]
        # Make them just different enough to not dedup but similar enough to group
        passages[1] = _make_passage(
            "AI cost reduction through automation tools.",
            dimension="cost",
            confidence=0.7,
            source_type="news",
        )

        with (
            patch("ai_opportunity_index.scoring.evidence_munger._load_passages", return_value=passages),
            patch("ai_opportunity_index.scoring.evidence_munger._load_filing_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._load_news_passages", return_value=[]),
            patch("ai_opportunity_index.scoring.evidence_munger._get_child_tickers", return_value=[]),
        ):
            groups = munge_evidence("TEST", company_id=1)

        # Find the multi-passage group
        multi = [g for g in groups if g.passage_count >= 2]
        if multi:
            assert set(multi[0].source_types) == {"filing", "news"}


# ══════════════════════════════════════════════════════════════════════════
# Threshold and constant tests
# ══════════════════════════════════════════════════════════════════════════


class TestConstants:
    def test_similarity_threshold_value(self) -> None:
        assert SIMILARITY_THRESHOLD == 0.55

    def test_max_group_size_value(self) -> None:
        assert MAX_GROUP_SIZE == 8
