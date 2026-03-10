"""Tests for the journalism subsystem (Editor, Researcher, Reporter)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from ai_opportunity_index.domains import (
    CollectedItem,
    SourceAuthority,
    TargetDimension,
    ValuationEvidenceType,
    ValuationStage,
)
from ai_opportunity_index.journalism.editor import Editor, STALENESS_THRESHOLD_DAYS
from ai_opportunity_index.journalism.models import (
    Citation,
    CompanyNarrative,
    CoverageReport,
    DimensionCoverage,
    NarrativeSection,
    ResearchPriority,
    ResearchResult,
    ResearchTask,
)
from ai_opportunity_index.journalism.reporter import Reporter, _format_dollars, _confidence_label
from ai_opportunity_index.journalism.researcher import Researcher


# -- Fixtures -----------------------------------------------------------------


def _make_company_model(
    id: int = 1,
    ticker: str = "AAPL",
    company_name: str = "Apple Inc.",
    sector: str = "Technology",
    industry: str = "Consumer Electronics",
    is_active: bool = True,
):
    """Create a mock CompanyModel."""
    m = MagicMock()
    m.id = id
    m.ticker = ticker
    m.company_name = company_name
    m.sector = sector
    m.industry = industry
    m.is_active = is_active
    m.careers_url = None
    m.ir_url = None
    m.blog_url = None
    return m


def _make_evidence_group(
    id: int = 1,
    company_id: int = 1,
    target_dimension: str = "cost",
    evidence_type: str | None = None,
    passage_count: int = 2,
):
    """Create a mock EvidenceGroupModel."""
    m = MagicMock()
    m.id = id
    m.company_id = company_id
    m.target_dimension = target_dimension
    m.evidence_type = evidence_type
    m.passage_count = passage_count
    return m


def _make_passage(
    id: int = 1,
    group_id: int = 1,
    passage_text: str = "The company plans to invest in AI automation.",
    source_type: str = "news",
    source_date: date | None = None,
    source_url: str = "https://example.com/article",
    source_author: str = "John Doe",
    source_publisher: str = "TechNews",
    source_authority: str = "third_party_journalism",
):
    """Create a mock EvidenceGroupPassageModel."""
    m = MagicMock()
    m.id = id
    m.group_id = group_id
    m.passage_text = passage_text
    m.source_type = source_type
    m.source_date = source_date or date.today()
    m.source_url = source_url
    m.source_author = source_author
    m.source_publisher = source_publisher
    m.source_authority = source_authority
    return m


def _make_valuation(
    id: int = 1,
    group_id: int = 1,
    stage: str = "final",
    evidence_type: str = "plan",
    narrative: str = "Apple plans to automate supply chain with AI.",
    confidence: float = 0.75,
    dollar_low: float = 1_000_000,
    dollar_mid: float = 5_000_000,
    dollar_high: float = 10_000_000,
):
    """Create a mock ValuationModel."""
    m = MagicMock()
    m.id = id
    m.group_id = group_id
    m.stage = stage
    m.evidence_type = evidence_type
    m.narrative = narrative
    m.confidence = confidence
    m.dollar_low = dollar_low
    m.dollar_mid = dollar_mid
    m.dollar_high = dollar_high
    return m


def _make_score(
    opportunity: float = 0.75,
    realization: float = 0.60,
    quadrant: str = "high_opp_high_real",
    quadrant_label: str = "AI Leaders",
    ai_index_usd: float = 50_000_000,
    combined_rank: int = 5,
    scored_at: datetime | None = None,
):
    """Create a mock CompanyScoreModel."""
    m = MagicMock()
    m.opportunity = opportunity
    m.realization = realization
    m.quadrant = quadrant
    m.quadrant_label = quadrant_label
    m.ai_index_usd = ai_index_usd
    m.combined_rank = combined_rank
    m.scored_at = scored_at or datetime.utcnow()
    return m


def _make_discrepancy(
    description: str = "Conflicting cost estimates",
    resolution: str = "Used more recent source",
):
    """Create a mock ValuationDiscrepancyModel."""
    m = MagicMock()
    m.description = description
    m.resolution = resolution
    return m


def _mock_session_with_data(
    company=None,
    groups=None,
    passages=None,
    valuations=None,
    score=None,
    discrepancies=None,
    companies=None,
):
    """Build a mock session that returns predictable query results."""
    session = MagicMock()

    def execute_side_effect(stmt):
        result = MagicMock()
        # Introspect the statement to determine what's being queried.
        stmt_str = str(stmt)

        if "companies" in stmt_str.lower() and companies is not None:
            scalars = MagicMock()
            scalars.all.return_value = companies
            result.scalars.return_value = scalars
            result.scalar_one_or_none.return_value = companies[0] if companies else None
        elif "companies" in stmt_str.lower():
            result.scalar_one_or_none.return_value = company
            scalars = MagicMock()
            scalars.all.return_value = [company] if company else []
            result.scalars.return_value = scalars
        elif "evidence_groups" in stmt_str.lower():
            scalars = MagicMock()
            scalars.all.return_value = groups or []
            result.scalars.return_value = scalars
        elif "evidence_group_passages" in stmt_str.lower():
            scalars = MagicMock()
            scalars.all.return_value = passages or []
            result.scalars.return_value = scalars
        elif "valuations" in stmt_str.lower() and "discrepancies" not in stmt_str.lower():
            scalars = MagicMock()
            scalars.all.return_value = valuations or []
            result.scalars.return_value = scalars
        elif "company_scores" in stmt_str.lower():
            result.scalar_one_or_none.return_value = score
        elif "discrepancies" in stmt_str.lower():
            scalars = MagicMock()
            scalars.all.return_value = discrepancies or []
            result.scalars.return_value = scalars
        else:
            result.scalar_one_or_none.return_value = None
            scalars = MagicMock()
            scalars.all.return_value = []
            result.scalars.return_value = scalars

        return result

    session.execute.side_effect = execute_side_effect
    return session


# -- Editor Tests -------------------------------------------------------------


class TestEditorCoverage:
    """Test Editor.assess_coverage."""

    def test_empty_company_returns_empty_report(self):
        """Company with no evidence returns empty coverage report."""
        session = _mock_session_with_data(company=None)
        editor = Editor()
        report = editor.assess_coverage(company_id=999, session=session)

        assert report.company_id == 999
        assert report.total_evidence_groups == 0
        assert report.total_passages == 0

    def test_full_coverage_detected(self):
        """Company with evidence across all dimensions shows good coverage."""
        company = _make_company_model()
        groups = []
        passages = []

        # Create groups for cost, revenue, general — 2 source types each
        gid = 0
        for dim in ["cost", "revenue", "general"]:
            gid += 1
            groups.append(_make_evidence_group(
                id=gid, target_dimension=dim,
            ))
            # Two distinct source types per dimension
            passages.append(_make_passage(
                id=gid * 10, group_id=gid, source_type="news",
            ))
            passages.append(_make_passage(
                id=gid * 10 + 1, group_id=gid, source_type="filing",
            ))

        session = _mock_session_with_data(
            company=company, groups=groups, passages=passages,
        )

        editor = Editor()
        report = editor.assess_coverage(1, session)

        assert report.ticker == "AAPL"
        assert report.total_evidence_groups == 3
        assert len(report.gaps) == 0
        assert report.overall_coverage_score == 1.0

    def test_gap_detection(self):
        """Dimensions with fewer than 2 sources are flagged as gaps."""
        company = _make_company_model()
        # Only one group for cost with one source type
        groups = [_make_evidence_group(
            id=1, target_dimension="cost",
        )]
        passages = [_make_passage(id=1, group_id=1, source_type="news")]

        session = _mock_session_with_data(
            company=company, groups=groups, passages=passages,
        )

        editor = Editor()
        report = editor.assess_coverage(1, session)

        # cost has only 1 source, revenue and general have 0
        assert len(report.gaps) > 0
        assert any("cost" in g for g in report.gaps)
        assert report.overall_coverage_score < 1.0

    def test_staleness_detection(self):
        """Evidence older than threshold is flagged as stale."""
        company = _make_company_model()
        old_date = date.today() - timedelta(days=90)
        groups = [_make_evidence_group(
            id=1, target_dimension="cost",
        )]
        passages = [
            _make_passage(id=1, group_id=1, source_type="news", source_date=old_date),
            _make_passage(id=2, group_id=1, source_type="filing", source_date=old_date),
        ]

        session = _mock_session_with_data(
            company=company, groups=groups, passages=passages,
        )

        editor = Editor()
        report = editor.assess_coverage(1, session)

        assert len(report.stale_dimensions) > 0
        assert any("cost" in s for s in report.stale_dimensions)


class TestEditorPrioritize:
    """Test Editor.prioritize_research."""

    def test_no_evidence_is_critical(self):
        """Companies with zero evidence get CRITICAL priority."""
        company = _make_company_model()
        session = _mock_session_with_data(company=company, groups=[], passages=[])

        editor = Editor()
        tasks = editor.prioritize_research(session, limit=10)

        assert len(tasks) == 1
        assert tasks[0].priority == ResearchPriority.CRITICAL
        assert tasks[0].company_id == 1

    def test_single_source_is_high(self):
        """Companies with single-source dimensions get HIGH priority."""
        company = _make_company_model()
        # Coverage for all dimensions but only 1 source type each
        groups = []
        passages = []
        gid = 0
        for dim in ["cost", "revenue", "general"]:
            gid += 1
            groups.append(_make_evidence_group(
                id=gid, target_dimension=dim,
            ))
            passages.append(_make_passage(
                id=gid * 10, group_id=gid, source_type="news",
            ))

        session = _mock_session_with_data(
            company=company, groups=groups, passages=passages,
        )

        editor = Editor()
        tasks = editor.prioritize_research(session, limit=10)

        assert len(tasks) == 1
        assert tasks[0].priority == ResearchPriority.HIGH

    def test_limit_respected(self):
        """Output is limited to the requested number of tasks."""
        companies = [
            _make_company_model(id=i, ticker=f"T{i}") for i in range(1, 6)
        ]
        session = _mock_session_with_data(
            companies=companies, groups=[], passages=[],
        )

        editor = Editor()
        tasks = editor.prioritize_research(session, limit=3)

        assert len(tasks) <= 3


class TestEditorQualityReview:
    """Test Editor.review_quality."""

    def test_accept_good_result(self):
        """Result with proper content and provenance is accepted."""
        task = ResearchTask(company_id=1, ticker="AAPL", company_name="Apple")
        items = [
            CollectedItem(
                item_id="test-1",
                title="AI article",
                content="A" * 100,
                url="https://example.com/article",
                publisher="TechNews",
                authority=SourceAuthority.THIRD_PARTY_JOURNALISM,
            )
        ]
        result = ResearchResult(task=task, collected_items=items)

        editor = Editor()
        accepted, issues = editor.review_quality(result)

        assert accepted is True

    def test_reject_empty_result(self):
        """Result with no items is rejected."""
        task = ResearchTask(company_id=1, ticker="AAPL")
        result = ResearchResult(task=task, collected_items=[])

        editor = Editor()
        accepted, issues = editor.review_quality(result)

        assert accepted is False
        assert "No items collected" in issues

    def test_flag_missing_provenance(self):
        """Items without URL or publisher are flagged."""
        task = ResearchTask(company_id=1, ticker="AAPL")
        items = [
            CollectedItem(
                item_id="test-1",
                content="A" * 100,
                # No url, no publisher
            )
        ]
        result = ResearchResult(task=task, collected_items=items)

        editor = Editor()
        accepted, issues = editor.review_quality(result)

        # Still accepted (has content) but with issues
        assert accepted is True
        assert any("provenance" in i.lower() for i in issues)

    def test_flag_short_content(self):
        """Items with very short content are flagged."""
        task = ResearchTask(company_id=1, ticker="AAPL")
        items = [
            CollectedItem(
                item_id="test-1",
                content="Short",
                url="https://example.com",
                authority=SourceAuthority.THIRD_PARTY_JOURNALISM,
            )
        ]
        result = ResearchResult(task=task, collected_items=items)

        editor = Editor()
        accepted, issues = editor.review_quality(result)

        assert accepted is False  # only item has short content
        assert any("too short" in i for i in issues)

    def test_flag_missing_authority(self):
        """Items without source authority are flagged."""
        task = ResearchTask(company_id=1, ticker="AAPL")
        items = [
            CollectedItem(
                item_id="test-1",
                content="A" * 100,
                url="https://example.com",
                # No authority
            )
        ]
        result = ResearchResult(task=task, collected_items=items)

        editor = Editor()
        accepted, issues = editor.review_quality(result)

        assert accepted is True
        assert any("authority" in i.lower() for i in issues)


# -- Reporter Tests -----------------------------------------------------------


class TestReporterNarrative:
    """Test Reporter.generate_narrative."""

    def test_narrative_for_missing_company(self):
        """Missing company returns empty narrative."""
        session = _mock_session_with_data(company=None)
        reporter = Reporter()
        narrative = reporter.generate_narrative(999, session)

        assert narrative.company_id == 999
        assert narrative.summary == ""
        assert len(narrative.sections) == 0

    def test_narrative_with_score_and_evidence(self):
        """Full narrative includes summary, opportunity, and realization."""
        company = _make_company_model()
        score = _make_score()

        # Opportunity group (plan)
        opp_group = _make_evidence_group(
            id=1, target_dimension="cost", evidence_type="plan",
        )
        # Realization group (capture)
        real_group = _make_evidence_group(
            id=2, target_dimension="cost", evidence_type="capture",
        )
        groups = [opp_group, real_group]

        passages = [
            _make_passage(id=1, group_id=1),
            _make_passage(id=2, group_id=2),
        ]

        valuations = [
            _make_valuation(id=1, group_id=1, evidence_type="plan"),
            _make_valuation(id=2, group_id=2, evidence_type="capture"),
        ]

        session = _mock_session_with_data(
            company=company,
            groups=groups,
            passages=passages,
            valuations=valuations,
            score=score,
        )

        reporter = Reporter()
        narrative = reporter.generate_narrative(1, session)

        assert narrative.company_id == 1
        assert narrative.ticker == "AAPL"
        assert narrative.company_name == "Apple Inc."
        assert len(narrative.sections) >= 2  # at least summary + opportunity or realization
        assert narrative.total_citations > 0
        assert narrative.overall_confidence > 0

    def test_summary_section_content(self):
        """Summary section includes company name, quadrant, and scores."""
        company = _make_company_model()
        score = _make_score()

        session = _mock_session_with_data(company=company, score=score)
        reporter = Reporter()
        narrative = reporter.generate_narrative(1, session)

        summary = narrative.sections[0]
        assert summary.title == "Executive Summary"
        assert "Apple Inc." in summary.body
        assert "0.75" in summary.body  # opportunity score
        assert "AI Leaders" in summary.body  # quadrant label

    def test_verification_section_with_discrepancies(self):
        """Verification section lists discrepancies when present."""
        company = _make_company_model()
        discrepancies = [
            _make_discrepancy("Cost estimate conflict", "Used newer source"),
            _make_discrepancy("Revenue projection mismatch", "Averaged values"),
        ]

        session = _mock_session_with_data(
            company=company,
            discrepancies=discrepancies,
        )

        reporter = Reporter()
        narrative = reporter.generate_narrative(1, session)

        verif_sections = [s for s in narrative.sections if s.title == "Cross-Source Verification"]
        assert len(verif_sections) == 1
        assert "2 cross-source discrepancies" in verif_sections[0].body


class TestCitationFormatting:
    """Test Reporter._format_citation."""

    def test_basic_citation(self):
        """Citation captures all provenance fields from a passage."""
        passage = _make_passage(
            source_url="https://example.com/article",
            source_author="Jane Smith",
            source_publisher="AI Weekly",
            source_date=date(2024, 6, 15),
            source_authority="professional_analysis",
            passage_text="The company is investing heavily in AI.",
        )

        reporter = Reporter()
        citation = reporter._format_citation(passage)

        assert citation.source_url == "https://example.com/article"
        assert citation.author == "Jane Smith"
        assert citation.publisher == "AI Weekly"
        assert citation.source_date == date(2024, 6, 15)
        assert citation.authority == "professional_analysis"
        assert "investing heavily in AI" in citation.excerpt

    def test_long_excerpt_truncated(self):
        """Passages longer than 200 chars are truncated in citations."""
        passage = _make_passage(passage_text="A" * 300)

        reporter = Reporter()
        citation = reporter._format_citation(passage)

        assert len(citation.excerpt) == 203  # 200 + "..."
        assert citation.excerpt.endswith("...")


class TestFormatHelpers:
    """Test utility functions in reporter module."""

    def test_format_dollars_billions(self):
        assert _format_dollars(5_000_000_000) == "$5.0B"

    def test_format_dollars_millions(self):
        assert _format_dollars(42_000_000) == "$42.0M"

    def test_format_dollars_thousands(self):
        assert _format_dollars(7_500) == "$8K"

    def test_format_dollars_small(self):
        assert _format_dollars(500) == "$500"

    def test_format_dollars_none(self):
        assert _format_dollars(None) == "N/A"

    def test_confidence_label_high(self):
        assert _confidence_label(0.9) == "high"

    def test_confidence_label_moderate(self):
        assert _confidence_label(0.6) == "moderate"

    def test_confidence_label_low(self):
        assert _confidence_label(0.35) == "low"

    def test_confidence_label_very_low(self):
        assert _confidence_label(0.1) == "very low"


# -- Researcher Tests ---------------------------------------------------------


class TestResearcher:
    """Test Researcher.execute_task."""

    @patch("ai_opportunity_index.journalism.researcher.Researcher._collect_from_sources")
    def test_execute_task_success(self, mock_collect):
        """Successful task execution returns collected items."""
        mock_collect.return_value = [
            CollectedItem(
                item_id="test-1",
                title="AI article",
                content="Content about AI " * 10,
                url="https://example.com/ai",
                publisher="TechNews",
                authority=SourceAuthority.THIRD_PARTY_JOURNALISM,
            )
        ]

        task = ResearchTask(
            company_id=1,
            ticker="AAPL",
            company_name="Apple Inc.",
            dimensions=["cost"],
        )

        researcher = Researcher()
        result = researcher.execute_task(task)

        assert len(result.collected_items) == 1
        assert result.sources_succeeded == 1
        assert len(result.errors) == 0

    @patch("ai_opportunity_index.journalism.researcher.Researcher._collect_from_sources")
    def test_execute_task_handles_errors(self, mock_collect):
        """Task execution catches and records errors."""
        mock_collect.side_effect = RuntimeError("API unavailable")

        task = ResearchTask(
            company_id=1,
            ticker="AAPL",
            company_name="Apple Inc.",
        )

        researcher = Researcher()
        result = researcher.execute_task(task)

        assert len(result.collected_items) == 0
        assert len(result.errors) == 1
        assert "API unavailable" in result.errors[0]

    def test_collect_without_ticker_returns_empty(self):
        """Collection without ticker returns empty list."""
        researcher = Researcher()
        items = researcher._collect_from_sources(
            ticker=None, company_name=None, dimensions=[],
        )
        assert items == []


# -- Model Tests --------------------------------------------------------------


class TestModels:
    """Test journalism domain models."""

    def test_research_task_defaults(self):
        task = ResearchTask(company_id=1)
        assert task.priority == ResearchPriority.MEDIUM
        assert task.dimensions == []
        assert task.coverage_gaps == []

    def test_coverage_report_defaults(self):
        report = CoverageReport(company_id=1)
        assert report.total_evidence_groups == 0
        assert report.overall_coverage_score == 0.0
        assert report.assessed_at is not None

    def test_narrative_section_defaults(self):
        section = NarrativeSection(title="Test", body="Test body")
        assert section.confidence == 0.0
        assert section.citations == []
        assert section.evidence_count == 0

    def test_company_narrative_defaults(self):
        narrative = CompanyNarrative(company_id=1)
        assert narrative.summary == ""
        assert narrative.sections == []
        assert narrative.total_citations == 0

    def test_citation_all_optional(self):
        citation = Citation()
        assert citation.source_url is None
        assert citation.author is None
