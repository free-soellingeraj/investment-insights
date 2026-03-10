"""Adversarial tests: verify evidence passage provenance coverage meets minimum bars."""

import pytest
from sqlalchemy import text
from ai_opportunity_index.storage.db import get_session


pytestmark = pytest.mark.adversarial


class TestPassageProvenance:
    """Verify provenance fields are populated on evidence_group_passages."""

    def test_filing_passages_have_publisher(self):
        """All filing passages must have source_publisher set."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(*) FROM evidence_group_passages "
                "WHERE source_type = 'filing' AND source_publisher IS NULL"
            )).scalar()
            assert r == 0, f"{r} filing passages missing source_publisher"

    def test_filing_passages_have_authority(self):
        """All filing passages must have source_authority set."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(*) FROM evidence_group_passages "
                "WHERE source_type = 'filing' AND source_authority IS NULL"
            )).scalar()
            assert r == 0, f"{r} filing passages missing source_authority"

    def test_news_passages_have_url(self):
        """All news passages must have source_url."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(*) FROM evidence_group_passages "
                "WHERE source_type = 'news' AND source_url IS NULL"
            )).scalar()
            assert r == 0, f"{r} news passages missing source_url"

    def test_overall_url_coverage_above_90_pct(self):
        """At least 90% of all passages should have source_url."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(source_url)::float / NULLIF(COUNT(*), 0) "
                "FROM evidence_group_passages"
            )).scalar()
            assert r is not None and r >= 0.90, (
                f"URL coverage is {r:.1%}, expected >= 90%"
            )

    def test_overall_publisher_coverage_above_80_pct(self):
        """At least 80% of all passages should have source_publisher."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(source_publisher)::float / NULLIF(COUNT(*), 0) "
                "FROM evidence_group_passages"
            )).scalar()
            assert r is not None and r >= 0.80, (
                f"Publisher coverage is {r:.1%}, expected >= 80%"
            )

    def test_overall_authority_coverage_above_90_pct(self):
        """At least 90% of passages should have source_authority."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(source_authority)::float / NULLIF(COUNT(*), 0) "
                "FROM evidence_group_passages"
            )).scalar()
            assert r is not None and r >= 0.90, (
                f"Authority coverage is {r:.1%}, expected >= 90%"
            )

    def test_no_empty_passage_text(self):
        """No passages should have empty or very short passage_text."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(*) FROM evidence_group_passages "
                "WHERE passage_text IS NULL OR length(passage_text) < 20"
            )).scalar()
            assert r == 0, f"{r} passages have empty or trivially short text"


class TestValuationDollarCoverage:
    """Verify valuation dollar estimates exist and are reasonable."""

    def test_all_valuations_have_dollar_mid(self):
        """Every valuation should have a dollar_mid estimate."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(*) FROM valuations WHERE dollar_mid IS NULL"
            )).scalar()
            assert r == 0, f"{r} valuations missing dollar_mid"

    def test_dollar_mid_positive(self):
        """Dollar estimates should generally be non-negative."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(*) FROM valuations WHERE dollar_mid < 0"
            )).scalar()
            # Some negative values may be legitimate (cost savings shown as negative)
            total = s.execute(text("SELECT COUNT(*) FROM valuations")).scalar()
            pct = r / total if total else 0
            assert pct < 0.10, (
                f"{r}/{total} ({pct:.1%}) valuations have negative dollar_mid"
            )

    def test_dollar_range_consistency(self):
        """dollar_low <= dollar_mid <= dollar_high."""
        with get_session() as s:
            r = s.execute(text(
                "SELECT COUNT(*) FROM valuations "
                "WHERE dollar_low IS NOT NULL AND dollar_high IS NOT NULL "
                "AND (dollar_low > dollar_mid OR dollar_mid > dollar_high)"
            )).scalar()
            assert r == 0, f"{r} valuations have inconsistent dollar ranges"
