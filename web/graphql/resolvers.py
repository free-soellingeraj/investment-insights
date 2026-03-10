"""GraphQL resolvers -- thin layer between schema and async repositories."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import strawberry

from ai_opportunity_index.config import (
    SCORE_STALENESS_WARNING_DAYS,
    SCORE_STALENESS_CRITICAL_DAYS,
)

from .types import (
    CompanyType,
    CompanyScoreType,
    CompanyDetailType,
    EvidenceType,
    FinancialObservationType,
    PipelineRunType,
    TradeSignalType,
    RationaleNodeType,
    RebalanceResultType,
    FactGraphStatsType,
    PipelineStatusType,
    SystemInternalsType,
    ReleaseType,
    ChangelogEntryType,
    EvidenceGroupType,
    ValuationType,
    VerificationResultType,
    SourceAgreementType,
    SourceDisagreementType,
    AuditReportType,
    AuditFindingType,
    InferenceResultType,
    CitationType,
    NarrativeSectionType,
    CompanyNarrativeType,
    DimensionCoverageType,
    CoverageReportType,
    ResearchTaskType,
    InvestmentProjectType,
)

logger = logging.getLogger(__name__)


def _compute_staleness(scored_at: datetime | None) -> tuple[int | None, str | None]:
    """Return (age_days, staleness_level) for a scored_at timestamp."""
    if scored_at is None:
        return None, None
    now = datetime.now(timezone.utc)
    # Make scored_at offset-aware if it isn't already
    if scored_at.tzinfo is None:
        scored_at = scored_at.replace(tzinfo=timezone.utc)
    age_days = (now - scored_at).days
    if age_days >= SCORE_STALENESS_CRITICAL_DAYS:
        level = "critical"
    elif age_days >= SCORE_STALENESS_WARNING_DAYS:
        level = "warning"
    else:
        level = "fresh"
    return age_days, level


@strawberry.type
class Query:

    @strawberry.field
    async def company(self, ticker: str) -> Optional[CompanyType]:
        """Get a single company by ticker."""
        from ai_opportunity_index.storage.repositories import CompanyRepository, get_async_session

        async with get_async_session() as session:
            repo = CompanyRepository(session)
            company = await repo.get_company_by_ticker(ticker)
            if not company:
                return None
            return _company_to_gql(company)

    @strawberry.field
    async def companies(
        self,
        limit: int = 50,
        offset: int = 0,
        sector: Optional[str] = None,
        search: Optional[str] = None,
    ) -> list[CompanyType]:
        """List companies with optional filters."""
        from ai_opportunity_index.storage.repositories import get_async_session
        from ai_opportunity_index.storage.models import CompanyModel
        from sqlalchemy import select

        async with get_async_session() as session:
            stmt = select(CompanyModel).where(CompanyModel.is_active == True)  # noqa: E712
            if sector:
                stmt = stmt.where(CompanyModel.sector == sector)
            if search:
                stmt = stmt.where(
                    CompanyModel.company_name.ilike(f"%{search}%")
                    | CompanyModel.ticker.ilike(f"%{search}%")
                )
            stmt = stmt.order_by(CompanyModel.ticker).offset(offset).limit(limit)
            result = await session.execute(stmt)
            return [_company_model_to_gql(m) for m in result.scalars().all()]

    @strawberry.field
    async def company_detail(self, ticker: str) -> Optional[CompanyDetailType]:
        """Get full company detail with scores, evidence, financials, peers."""
        from ai_opportunity_index.storage.repositories import (
            CompanyRepository,
            EvidenceRepository,
            ScoreRepository,
            FinancialObservationRepository,
            ValuationRepository,
            get_async_session,
        )

        async with get_async_session() as session:
            company_repo = CompanyRepository(session)
            company = await company_repo.get_company_by_ticker(ticker)
            if not company:
                return None

            evidence_repo = EvidenceRepository(session)
            score_repo = ScoreRepository(session)
            financial_repo = FinancialObservationRepository(session)
            valuation_repo = ValuationRepository(session)

            evidence = await evidence_repo.get_evidence_for_company(company.id)
            latest_score = await score_repo.get_latest_score(company.id)
            financials_map = await financial_repo.get_latest_financials(company.id)
            peers_dicts = await company_repo.get_industry_peers(ticker)
            groups = await valuation_repo.get_evidence_groups_for_company(company.id)
            valuations = await valuation_repo.get_final_valuations_for_company(company.id)
            projects = await valuation_repo.get_investment_projects_for_company(company.id)

            return CompanyDetailType(
                company=_company_to_gql(company),
                latest_score=_score_to_gql(latest_score) if latest_score else None,
                evidence=[_evidence_to_gql(e) for e in evidence],
                financials=[_financial_to_gql(f) for f in financials_map.values()],
                peers=[_peer_dict_to_gql(p) for p in peers_dicts],
                evidence_groups=[_group_to_gql(g) for g in groups],
                valuations=[_valuation_to_gql(v) for v in valuations],
                investment_projects=[_project_to_gql(p) for p in projects],
            )

    @strawberry.field
    async def latest_scores(
        self,
        limit: int = 50,
        offset: int = 0,
        quadrant: Optional[str] = None,
        sector: Optional[str] = None,
    ) -> list[CompanyScoreType]:
        """Get latest scores for ranked companies."""
        from ai_opportunity_index.storage.repositories import ScoreRepository, get_async_session

        async with get_async_session() as session:
            repo = ScoreRepository(session)
            scores = await repo.get_latest_scores(
                limit=limit,
                offset=offset,
                quadrant=quadrant,
                sector=sector,
            )
            return [_score_dict_to_gql(s) for s in scores]

    @strawberry.field
    async def pipeline_status(self) -> PipelineStatusType:
        """Get current pipeline status."""
        from ai_opportunity_index.storage.repositories import (
            CompanyRepository,
            EvidenceRepository,
            ScoreRepository,
            get_async_session,
        )
        from ai_opportunity_index.storage.models import PipelineRunModel
        from sqlalchemy import select

        async with get_async_session() as session:
            company_count = await CompanyRepository(session).count()
            evidence_count = await EvidenceRepository(session).count()
            score_count = await ScoreRepository(session).count()

            result = await session.execute(
                select(PipelineRunModel)
                .order_by(PipelineRunModel.started_at.desc())
                .limit(20)
            )
            models = result.scalars().all()

            runs = [_run_model_to_gql(m) for m in models]
            last_run = runs[0] if runs else None

            return PipelineStatusType(
                total_companies=company_count,
                companies_scored=score_count,
                total_evidence=evidence_count,
                last_run=last_run,
                recent_runs=runs,
            )

    @strawberry.field
    async def trade_signals(self) -> RebalanceResultType:
        """Generate dry-run trade signals."""
        from ai_opportunity_index.trading.signal_generator import TradeSignalGenerator
        from ai_opportunity_index.trading.models import Portfolio
        from ai_opportunity_index.storage.db import get_session

        session = get_session()  # Signal generator uses sync session
        try:
            generator = TradeSignalGenerator(session=session)
            portfolio = Portfolio(name="graphql-query")
            result = generator.generate_signals(portfolio)

            return RebalanceResultType(
                portfolio_id=result.portfolio_id,
                total_buys=result.total_buys,
                total_sells=result.total_sells,
                total_holds=result.total_holds,
                turnover=result.turnover,
                signals=[_signal_to_gql(s) for s in result.signals],
            )
        finally:
            session.close()

    @strawberry.field
    async def fact_graph_stats(self) -> FactGraphStatsType:
        """Get fact graph statistics."""
        try:
            from ai_opportunity_index.fact_graph.bridge import build_graph_from_db
            from ai_opportunity_index.storage.db import get_session

            session = get_session()
            try:
                graph = build_graph_from_db(session)
                stats = graph.stats()
                total = stats["total_attributes"]
                missing = stats["missing_values"]

                return FactGraphStatsType(
                    total_nodes=stats["total_nodes"],
                    total_edges=stats["total_edges"],
                    total_attributes=total,
                    missing_values=missing,
                    low_confidence_values=stats["low_confidence_values"],
                    counterfactual_branches=stats["counterfactual_branches"],
                    completeness_pct=round(
                        ((total - missing) / total * 100) if total > 0 else 0, 1
                    ),
                    nodes_by_type=stats["nodes_by_type"],
                )
            finally:
                session.close()
        except Exception as e:
            logger.exception("Failed to build fact graph")
            return FactGraphStatsType(
                total_nodes=0,
                total_edges=0,
                total_attributes=0,
                missing_values=0,
                low_confidence_values=0,
                counterfactual_branches=0,
                completeness_pct=0.0,
                nodes_by_type={},
            )

    @strawberry.field
    async def company_verification(self, ticker: str) -> Optional[VerificationResultType]:
        """Get cross-source verification results for a company."""
        from ai_opportunity_index.storage.repositories import CompanyRepository, get_async_session
        from ai_opportunity_index.fact_graph.verification import verify_company_evidence
        from ai_opportunity_index.storage.db import get_session

        async with get_async_session() as async_session:
            company = await CompanyRepository(async_session).get_company_by_ticker(ticker)
            if not company:
                return None

        # verification uses sync session (like fact_graph_stats)
        session = get_session()
        try:
            result = verify_company_evidence(company.id, session)
            result.ticker = ticker
            return VerificationResultType(
                company_id=result.company_id,
                ticker=result.ticker,
                confirmations=[
                    SourceAgreementType(
                        source_a=c.source_a,
                        source_b=c.source_b,
                        dimension=c.dimension,
                        dollar_a=c.dollar_a,
                        dollar_b=c.dollar_b,
                        agreement_ratio=c.agreement_ratio,
                    )
                    for c in result.confirmations
                ],
                contradictions=[
                    SourceDisagreementType(
                        source_a=d.source_a,
                        source_b=d.source_b,
                        dimension=d.dimension,
                        dollar_a=d.dollar_a,
                        dollar_b=d.dollar_b,
                        disagreement_ratio=d.disagreement_ratio,
                        severity=d.severity,
                    )
                    for d in result.contradictions
                ],
                agreement_score=result.agreement_score,
                confidence_adjustment=result.confidence_adjustment,
            )
        finally:
            session.close()

    @strawberry.field
    async def changelog(self) -> list[ReleaseType]:
        """Get all changelog releases."""
        from ai_opportunity_index.changelog import load_changelog

        releases = load_changelog()
        return [
            ReleaseType(
                version=r.version,
                title=r.title,
                date=r.date,
                summary=r.summary,
                status=r.status,
                changes=[
                    ChangelogEntryType(
                        description=c.description,
                        change_type=c.change_type.value
                        if hasattr(c.change_type, "value")
                        else c.change_type,
                        component=c.component,
                    )
                    for c in r.changes
                ],
            )
            for r in releases
        ]

    @strawberry.field
    async def evidence_for_company(
        self, ticker: str, limit: int = 50
    ) -> list[EvidenceType]:
        """Get evidence records for a company."""
        from ai_opportunity_index.storage.repositories import (
            CompanyRepository,
            EvidenceRepository,
            get_async_session,
        )

        async with get_async_session() as session:
            company = await CompanyRepository(session).get_company_by_ticker(ticker)
            if not company:
                return []
            evidence = await EvidenceRepository(session).get_evidence_for_company(
                company.id, evidence_type=None
            )
            # Apply limit in Python since the repo method doesn't accept a limit param
            return [_evidence_to_gql(e) for e in evidence[:limit]]

    @strawberry.field
    async def stale_scores(self, limit: int = 100) -> list[CompanyScoreType]:
        """Get scores older than the staleness warning threshold (helps traders know which need refresh)."""
        from ai_opportunity_index.storage.repositories import ScoreRepository, get_async_session
        from ai_opportunity_index.storage.models import CompanyScoreModel
        from sqlalchemy import select, func as sa_func, text as sa_text
        from sqlalchemy.orm import selectinload
        from datetime import timedelta

        # Use naive datetime to match DB column type (scored_at is TIMESTAMP WITHOUT TIME ZONE)
        cutoff = datetime.utcnow() - timedelta(days=SCORE_STALENESS_WARNING_DAYS)
        async with get_async_session() as session:
            # Get the latest score per company, then filter to stale ones
            latest_subq = (
                select(
                    CompanyScoreModel.company_id,
                    sa_func.max(CompanyScoreModel.scored_at).label("max_scored_at"),
                )
                .group_by(CompanyScoreModel.company_id)
                .subquery()
            )
            stmt = (
                select(CompanyScoreModel)
                .options(selectinload(CompanyScoreModel.company))
                .join(
                    latest_subq,
                    (CompanyScoreModel.company_id == latest_subq.c.company_id)
                    & (CompanyScoreModel.scored_at == latest_subq.c.max_scored_at),
                )
                .where(CompanyScoreModel.scored_at < cutoff)
                .order_by(CompanyScoreModel.scored_at.asc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            models = result.scalars().all()
            return [_score_model_to_gql(m) for m in models]

    @strawberry.field
    async def pipeline_runs(self, limit: int = 20) -> list[PipelineRunType]:
        """Get recent pipeline runs."""
        from ai_opportunity_index.storage.repositories import get_async_session
        from ai_opportunity_index.storage.models import PipelineRunModel
        from sqlalchemy import select

        async with get_async_session() as session:
            result = await session.execute(
                select(PipelineRunModel)
                .order_by(PipelineRunModel.started_at.desc())
                .limit(limit)
            )
            return [_run_model_to_gql(m) for m in result.scalars().all()]

    @strawberry.field
    async def audit_report(self, limit: int = 100) -> AuditReportType:
        """Run correctness audit across scored companies."""
        from ai_opportunity_index.fact_graph.auditor import audit_company, AuditReport
        from ai_opportunity_index.storage.repositories import get_async_session
        from ai_opportunity_index.storage.models import CompanyScoreModel, CompanyModel, EvidenceModel
        from sqlalchemy import select, func as sa_func
        from sqlalchemy.orm import selectinload

        report = AuditReport()

        async with get_async_session() as session:
            # Get latest score per company
            latest_subq = (
                select(
                    CompanyScoreModel.company_id,
                    sa_func.max(CompanyScoreModel.scored_at).label("max_scored_at"),
                )
                .group_by(CompanyScoreModel.company_id)
                .subquery()
            )
            stmt = (
                select(CompanyScoreModel)
                .join(
                    latest_subq,
                    (CompanyScoreModel.company_id == latest_subq.c.company_id)
                    & (CompanyScoreModel.scored_at == latest_subq.c.max_scored_at),
                )
                .options(selectinload(CompanyScoreModel.company))
                .limit(limit)
            )
            result = await session.execute(stmt)
            scores = result.scalars().all()

            # Get evidence counts per company
            ev_counts_result = await session.execute(
                select(
                    EvidenceModel.company_id,
                    sa_func.count(EvidenceModel.id).label("cnt"),
                )
                .group_by(EvidenceModel.company_id)
            )
            ev_counts = {row[0]: row[1] for row in ev_counts_result}

            for score in scores:
                report.companies_audited += 1
                ticker = score.company.ticker if score.company else "?"
                findings = audit_company(
                    company_id=score.company_id,
                    ticker=ticker,
                    opportunity=score.opportunity,
                    realization=score.realization,
                    quadrant=score.quadrant,
                    cost_opp=score.cost_opp_score,
                    revenue_opp=score.revenue_opp_score,
                    cost_capture=score.cost_capture_score,
                    revenue_capture=score.revenue_capture_score,
                    ai_index_usd=score.ai_index_usd,
                    opportunity_usd=score.opportunity_usd,
                    evidence_dollars=score.evidence_dollars,
                    evidence_group_ids=score.evidence_group_ids,
                    valuation_ids=score.valuation_ids,
                    evidence_count=ev_counts.get(score.company_id, 0),
                    scored_at=score.scored_at,
                )
                if not any(f.severity in ("critical", "warning") for f in findings):
                    report.clean_companies += 1
                for f in findings:
                    report.add(f)

        summary = report.summary()
        return AuditReportType(
            companies_audited=summary["companies_audited"],
            clean_companies=summary["clean_companies"],
            total_findings=summary["total_findings"],
            critical=summary["critical"],
            warning=summary["warning"],
            info=summary["info"],
            pass_rate=summary["pass_rate"],
            findings=[
                AuditFindingType(
                    severity=f.severity,
                    category=f.category,
                    company_id=f.company_id,
                    ticker=f.ticker,
                    description=f.description,
                    expected=f.expected,
                    actual=f.actual,
                )
                for f in report.findings
            ],
        )


    @strawberry.field
    async def run_inference(self) -> InferenceResultType:
        """Build fact graph from DB, add scoring constraints, run logical inference."""
        from ai_opportunity_index.fact_graph.bridge import build_graph_from_db
        from ai_opportunity_index.fact_graph.inference import InferenceEngine
        from ai_opportunity_index.fact_graph.models import Constraint
        from ai_opportunity_index.storage.db import get_session

        session = get_session()
        try:
            graph = build_graph_from_db(session)

            # Add scoring constraints for each company node
            for node in graph.find_nodes():
                opp = node.get_attr("opportunity")
                real = node.get_attr("realization")
                cost_opp = node.get_attr("cost_opp_score")
                rev_opp = node.get_attr("revenue_opp_score")

                if opp and cost_opp and rev_opp:
                    graph.add_constraint(Constraint(
                        name=f"{node.label}_opp_range",
                        description=f"{node.label} opportunity score in [0,1]",
                        constraint_type="range",
                        participating_facts=[f"{node.id}.opportunity"],
                        expression="range=0.0,1.0",
                    ))
                if real:
                    graph.add_constraint(Constraint(
                        name=f"{node.label}_real_range",
                        description=f"{node.label} realization score in [0,1]",
                        constraint_type="range",
                        participating_facts=[f"{node.id}.realization"],
                        expression="range=0.0,1.0",
                    ))

            engine = InferenceEngine(graph)
            result = engine.run_logical_pass()

            return InferenceResultType(
                method=result.method.value,
                facts_updated=result.facts_updated,
                facts_created=result.facts_created,
                constraints_satisfied=result.constraints_satisfied,
                constraints_violated=result.constraints_violated,
                duration_ms=result.duration_ms,
                reasoning_log=result.reasoning_log,
            )
        except Exception as e:
            logger.exception("Inference failed")
            return InferenceResultType(
                method="logical",
                facts_updated=0,
                facts_created=0,
                constraints_satisfied=0,
                constraints_violated=0,
                duration_ms=0,
                reasoning_log=[f"Error: {str(e)}"],
            )
        finally:
            session.close()

    @strawberry.field
    async def coverage_report(self, ticker: str) -> Optional[CoverageReportType]:
        """Get evidence coverage report for a company."""
        from ai_opportunity_index.storage.repositories import CompanyRepository, get_async_session
        from ai_opportunity_index.journalism.editor import Editor
        from ai_opportunity_index.storage.db import get_session

        async with get_async_session() as async_session:
            company = await CompanyRepository(async_session).get_company_by_ticker(ticker)
            if not company:
                return None

        session = get_session()
        try:
            editor = Editor()
            report = editor.assess_coverage(company.id, session)
            return CoverageReportType(
                company_id=report.company_id,
                ticker=report.ticker,
                company_name=report.company_name,
                total_evidence_groups=report.total_evidence_groups,
                total_passages=report.total_passages,
                dimension_coverage=[
                    DimensionCoverageType(
                        dimension=dc.dimension.value if hasattr(dc.dimension, "value") else dc.dimension,
                        stage=dc.stage.value if dc.stage and hasattr(dc.stage, "value") else dc.stage,
                        source_count=dc.source_count,
                        distinct_source_types=dc.distinct_source_types,
                        newest_date=dc.newest_date,
                        oldest_date=dc.oldest_date,
                        is_stale=dc.is_stale,
                        has_gap=dc.has_gap,
                    )
                    for dc in report.dimension_coverage
                ],
                gaps=report.gaps,
                stale_dimensions=report.stale_dimensions,
                overall_coverage_score=report.overall_coverage_score,
                assessed_at=report.assessed_at,
            )
        finally:
            session.close()

    @strawberry.field
    async def research_priorities(self, limit: int = 20) -> list[ResearchTaskType]:
        """Get prioritized research tasks across all companies."""
        from ai_opportunity_index.journalism.editor import Editor
        from ai_opportunity_index.storage.db import get_session

        session = get_session()
        try:
            editor = Editor()
            tasks = editor.prioritize_research(session, limit=limit)
            return [
                ResearchTaskType(
                    company_id=t.company_id,
                    ticker=t.ticker,
                    company_name=t.company_name,
                    dimensions=t.dimensions,
                    priority=t.priority.value if hasattr(t.priority, "value") else t.priority,
                    coverage_gaps=t.coverage_gaps,
                )
                for t in tasks
            ]
        finally:
            session.close()

    @strawberry.field
    async def company_narrative(self, ticker: str) -> Optional[CompanyNarrativeType]:
        """Generate a structured narrative for a company."""
        from ai_opportunity_index.storage.repositories import CompanyRepository, get_async_session
        from ai_opportunity_index.journalism.reporter import Reporter
        from ai_opportunity_index.storage.db import get_session

        async with get_async_session() as async_session:
            company = await CompanyRepository(async_session).get_company_by_ticker(ticker)
            if not company:
                return None

        session = get_session()
        try:
            reporter = Reporter()
            narrative = reporter.generate_narrative(company.id, session)
            return CompanyNarrativeType(
                company_id=narrative.company_id,
                ticker=narrative.ticker,
                company_name=narrative.company_name,
                summary=narrative.summary,
                sections=[
                    NarrativeSectionType(
                        title=s.title,
                        body=s.body,
                        citations=[
                            CitationType(
                                source_url=c.source_url,
                                author=c.author,
                                publisher=c.publisher,
                                date=c.source_date,
                                authority=c.authority,
                                excerpt=c.excerpt,
                            )
                            for c in s.citations
                        ],
                        confidence=s.confidence,
                        evidence_count=s.evidence_count,
                    )
                    for s in narrative.sections
                ],
                generated_at=narrative.generated_at,
                total_citations=narrative.total_citations,
                overall_confidence=narrative.overall_confidence,
            )
        finally:
            session.close()


# -- Converters ---------------------------------------------------------------


def _company_to_gql(c) -> CompanyType:
    """Convert domain Company to GraphQL type."""
    return CompanyType(
        id=c.id,
        ticker=c.ticker,
        slug=c.slug,
        company_name=c.company_name,
        exchange=c.exchange,
        sector=c.sector,
        industry=c.industry,
        sic=c.sic,
        naics=c.naics,
        is_active=c.is_active,
    )


def _company_model_to_gql(m) -> CompanyType:
    """Convert SQLAlchemy CompanyModel directly to GraphQL type."""
    return CompanyType(
        id=m.id,
        ticker=m.ticker,
        slug=m.slug,
        company_name=m.company_name,
        exchange=m.exchange,
        sector=m.sector,
        industry=m.industry,
        sic=m.sic,
        naics=m.naics,
        is_active=m.is_active,
        github_url=m.github_url,
        careers_url=m.careers_url,
        ir_url=m.ir_url,
        blog_url=m.blog_url,
    )


def _peer_dict_to_gql(p: dict) -> CompanyType:
    """Convert a peer dict (from get_industry_peers) to GraphQL type."""
    return CompanyType(
        id=p.get("company_id") or p.get("id", 0),
        ticker=p.get("ticker"),
        slug=p.get("slug"),
        company_name=p.get("company_name"),
        exchange=p.get("exchange"),
        sector=p.get("sector"),
        industry=p.get("industry"),
        sic=p.get("sic"),
        naics=p.get("naics"),
        is_active=p.get("is_active", True),
    )


def _score_to_gql(s) -> CompanyScoreType:
    """Convert domain CompanyScore to GraphQL type."""
    age_days, staleness_level = _compute_staleness(s.scored_at)
    return CompanyScoreType(
        id=s.id,
        company_id=s.company_id,
        opportunity=s.opportunity,
        realization=s.realization,
        quadrant=s.quadrant.value if hasattr(s.quadrant, "value") else s.quadrant,
        quadrant_label=s.quadrant_label,
        cost_opp_score=s.cost_opp_score,
        revenue_opp_score=s.revenue_opp_score,
        composite_opp_score=s.composite_opp_score,
        cost_capture_score=s.cost_capture_score,
        revenue_capture_score=s.revenue_capture_score,
        filing_nlp_score=s.filing_nlp_score,
        product_score=s.product_score,
        github_score=s.github_score,
        analyst_score=s.analyst_score,
        cost_roi=s.cost_roi,
        revenue_roi=s.revenue_roi,
        combined_rank=s.combined_rank,
        ai_index_usd=s.ai_index_usd,
        capture_probability=s.capture_probability,
        opportunity_usd=s.opportunity_usd,
        evidence_dollars=s.evidence_dollars,
        flags=s.flags or [],
        scored_at=s.scored_at,
        score_age_days=age_days,
        staleness_level=staleness_level,
        agreement_score=getattr(s, "agreement_score", None),
        num_confirmations=getattr(s, "num_confirmations", None),
        num_contradictions=getattr(s, "num_contradictions", None),
    )


def _score_dict_to_gql(s: dict) -> CompanyScoreType:
    """Convert a score dict (from get_latest_scores materialized view) to GraphQL type."""
    age_days, staleness_level = _compute_staleness(s.get("scored_at"))
    return CompanyScoreType(
        id=s.get("score_id") or s.get("id", 0),
        company_id=s.get("company_id", 0),
        ticker=s.get("ticker"),
        company_name=s.get("company_name"),
        opportunity=s.get("opportunity", 0),
        realization=s.get("realization", 0),
        quadrant=s.get("quadrant"),
        quadrant_label=s.get("quadrant_label"),
        cost_opp_score=s.get("cost_opp_score"),
        revenue_opp_score=s.get("revenue_opp_score"),
        composite_opp_score=s.get("composite_opp_score"),
        cost_capture_score=s.get("cost_capture_score"),
        revenue_capture_score=s.get("revenue_capture_score"),
        filing_nlp_score=s.get("filing_nlp_score"),
        product_score=s.get("product_score"),
        github_score=s.get("github_score"),
        analyst_score=s.get("analyst_score"),
        cost_roi=s.get("cost_roi"),
        revenue_roi=s.get("revenue_roi"),
        combined_rank=s.get("combined_rank"),
        ai_index_usd=s.get("ai_index_usd"),
        capture_probability=s.get("capture_probability"),
        opportunity_usd=s.get("opportunity_usd"),
        evidence_dollars=s.get("evidence_dollars"),
        flags=s.get("flags") or [],
        scored_at=s.get("scored_at"),
        score_age_days=age_days,
        staleness_level=staleness_level,
        agreement_score=s.get("agreement_score"),
        num_confirmations=s.get("num_confirmations"),
        num_contradictions=s.get("num_contradictions"),
    )


def _score_model_to_gql(m) -> CompanyScoreType:
    """Convert SQLAlchemy CompanyScoreModel directly to GraphQL type."""
    age_days, staleness_level = _compute_staleness(m.scored_at)
    company = getattr(m, "company", None)
    return CompanyScoreType(
        id=m.id,
        company_id=m.company_id,
        ticker=company.ticker if company else None,
        company_name=company.company_name if company else None,
        opportunity=m.opportunity,
        realization=m.realization,
        quadrant=m.quadrant,
        quadrant_label=m.quadrant_label,
        cost_opp_score=m.cost_opp_score,
        revenue_opp_score=m.revenue_opp_score,
        composite_opp_score=m.composite_opp_score,
        cost_capture_score=m.cost_capture_score,
        revenue_capture_score=m.revenue_capture_score,
        filing_nlp_score=m.filing_nlp_score,
        product_score=m.product_score,
        github_score=m.github_score,
        analyst_score=m.analyst_score,
        cost_roi=m.cost_roi,
        revenue_roi=m.revenue_roi,
        combined_rank=m.combined_rank,
        ai_index_usd=m.ai_index_usd,
        capture_probability=m.capture_probability,
        opportunity_usd=m.opportunity_usd,
        evidence_dollars=m.evidence_dollars,
        flags=m.flags or [],
        scored_at=m.scored_at,
        score_age_days=age_days,
        staleness_level=staleness_level,
        agreement_score=getattr(m, "agreement_score", None),
        num_confirmations=getattr(m, "num_confirmations", None),
        num_contradictions=getattr(m, "num_contradictions", None),
    )


def _evidence_to_gql(e) -> EvidenceType:
    """Convert domain AIOpportunityEvidence to GraphQL type."""
    return EvidenceType(
        id=e.id,
        company_id=e.company_id,
        evidence_type=e.evidence_type.value
        if hasattr(e.evidence_type, "value")
        else e.evidence_type,
        evidence_subtype=e.evidence_subtype,
        source_name=e.source_name,
        source_url=e.source_url,
        source_date=e.source_date,
        source_excerpt=e.source_excerpt,
        target_dimension=e.target_dimension.value
        if hasattr(e.target_dimension, "value")
        else e.target_dimension,
        capture_stage=e.capture_stage.value
        if hasattr(e.capture_stage, "value")
        else e.capture_stage,
        signal_strength=e.signal_strength.value
        if hasattr(e.signal_strength, "value")
        else e.signal_strength,
        dollar_estimate_usd=e.dollar_estimate_usd,
        observed_at=e.observed_at,
    )


def _financial_to_gql(f) -> FinancialObservationType:
    """Convert domain FinancialObservation to GraphQL type."""
    return FinancialObservationType(
        id=f.id,
        company_id=f.company_id,
        metric=f.metric.value if hasattr(f.metric, "value") else f.metric,
        value=f.value,
        value_units=f.value_units.value
        if hasattr(f.value_units, "value")
        else f.value_units,
        source_name=f.source_name,
        fiscal_period=f.fiscal_period,
        source_datetime=f.source_datetime,
    )


def _run_to_gql(r) -> PipelineRunType:
    """Convert domain PipelineRun to GraphQL type."""
    return PipelineRunType(
        id=r.id,
        run_id=r.run_id,
        task=r.task.value if hasattr(r.task, "value") else r.task,
        subtask=r.subtask.value if hasattr(r.subtask, "value") else r.subtask,
        run_type=r.run_type.value if hasattr(r.run_type, "value") else r.run_type,
        status=r.status.value if hasattr(r.status, "value") else r.status,
        tickers_succeeded=r.tickers_succeeded,
        tickers_failed=r.tickers_failed,
        started_at=r.started_at,
        completed_at=r.completed_at,
        error_message=r.error_message,
    )


def _run_model_to_gql(m) -> PipelineRunType:
    """Convert PipelineRunModel directly to GraphQL type (bypasses domain enum validation)."""
    return PipelineRunType(
        id=m.id,
        run_id=m.run_id,
        task=str(m.task) if m.task else None,
        subtask=str(m.subtask) if m.subtask else None,
        run_type=str(m.run_type) if m.run_type else None,
        status=str(m.status) if m.status else None,
        tickers_succeeded=m.tickers_succeeded,
        tickers_failed=m.tickers_failed,
        started_at=m.started_at,
        completed_at=m.completed_at,
        error_message=m.error_message,
    )


def _group_to_gql(g) -> EvidenceGroupType:
    """Convert domain EvidenceGroup to GraphQL type."""
    return EvidenceGroupType(
        id=g.id,
        company_id=g.company_id,
        target_dimension=g.target_dimension.value
        if hasattr(g.target_dimension, "value")
        else g.target_dimension,
        evidence_type=g.evidence_type.value
        if hasattr(g.evidence_type, "value")
        else g.evidence_type,
        passage_count=g.passage_count,
        representative_text=g.representative_text,
        mean_confidence=g.mean_confidence,
        date_earliest=g.date_earliest,
        date_latest=g.date_latest,
    )


def _valuation_to_gql(v) -> ValuationType:
    """Convert domain Valuation to GraphQL type."""
    return ValuationType(
        id=v.id,
        group_id=v.group_id,
        stage=v.stage.value if hasattr(v.stage, "value") else v.stage,
        evidence_type=v.evidence_type.value
        if hasattr(v.evidence_type, "value")
        else v.evidence_type,
        narrative=v.narrative,
        confidence=v.confidence,
        dollar_low=v.dollar_low,
        dollar_mid=v.dollar_mid,
        dollar_high=v.dollar_high,
        specificity=v.specificity,
        magnitude=v.magnitude,
    )


def _project_to_gql(p) -> InvestmentProjectType:
    """Convert domain SynthesizedProject to GraphQL type."""
    return InvestmentProjectType(
        id=p.id,
        company_id=p.company_id,
        short_title=p.short_title,
        description=p.description,
        target_dimension=p.target_dimension,
        target_subcategory=p.target_subcategory,
        target_detail=p.target_detail or None,
        status=p.status,
        dollar_total=p.dollar_total,
        dollar_low=p.dollar_low,
        dollar_high=p.dollar_high,
        confidence=p.confidence,
        evidence_count=p.evidence_count,
        date_start=p.date_start,
        date_end=p.date_end,
        technology_area=p.technology_area or None,
        deployment_scope=p.deployment_scope or None,
        evidence_group_ids=p.evidence_group_ids or [],
    )


def _rationale_node_to_gql(node) -> RationaleNodeType:
    """Recursively convert a domain RationaleNode tree to GraphQL type."""
    return RationaleNodeType(
        level=node.level,
        description=node.description,
        data=node.data,
        source_url=node.source_url,
        source_date=node.source_date,
        confidence=node.confidence,
        children=[_rationale_node_to_gql(child) for child in (node.children or [])],
    )


def _signal_to_gql(s) -> TradeSignalType:
    """Convert domain TradeSignal to GraphQL type."""
    return TradeSignalType(
        id=s.id,
        ticker=s.ticker,
        company_name=s.company_name,
        action=s.action.value,
        strength=s.strength.value,
        target_weight=s.target_weight,
        current_weight=s.current_weight,
        weight_change=s.weight_change,
        opportunity_score=s.opportunity_score,
        realization_score=s.realization_score,
        quadrant=s.quadrant,
        rationale_summary=s.rationale_summary,
        risk_factors=s.risk_factors,
        flags=s.flags,
        status=s.status.value,
        rationale=_rationale_node_to_gql(s.rationale) if s.rationale else None,
    )
