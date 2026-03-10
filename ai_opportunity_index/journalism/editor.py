"""Editor agent: oversees evidence collection strategy.

Analyzes current evidence coverage, identifies gaps, assigns research tasks,
and reviews quality of incoming research results.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ai_opportunity_index.domains import (
    TargetDimension,
    ValuationEvidenceType,
)
from ai_opportunity_index.storage.models import (
    CompanyModel,
    CompanyScoreModel,
    EvidenceGroupModel,
    EvidenceGroupPassageModel,
)

from .models import (
    CoverageReport,
    DimensionCoverage,
    ResearchPriority,
    ResearchResult,
    ResearchTask,
)

logger = logging.getLogger(__name__)

# Evidence older than this many days is considered stale
STALENESS_THRESHOLD_DAYS = 60

# Minimum independent sources per dimension for adequate coverage
MIN_SOURCES_PER_DIMENSION = 2

# Dimensions we expect evidence coverage for.
# Note: evidence_type (plan/investment/capture) is often NULL on evidence groups,
# so we assess coverage primarily by target_dimension. Valuation stages are tracked
# separately via the valuation pipeline.
_ALL_DIMENSIONS = [
    (TargetDimension.COST, None),
    (TargetDimension.REVENUE, None),
    (TargetDimension.GENERAL, None),
]


class Editor:
    """Oversees evidence collection strategy for the journalism subsystem."""

    def __init__(self, staleness_days: int = STALENESS_THRESHOLD_DAYS):
        self.staleness_days = staleness_days

    def assess_coverage(self, company_id: int, session: Session) -> CoverageReport:
        """Analyze current evidence coverage for a company and identify gaps.

        Looks at existing evidence groups and passages across all dimensions
        (cost/revenue x plan/invest/capture). Counts distinct sources per
        dimension, checks staleness, and identifies which dimensions have
        fewer than MIN_SOURCES_PER_DIMENSION independent sources.
        """
        # Get company info
        company = session.execute(
            select(CompanyModel).where(CompanyModel.id == company_id)
        ).scalar_one_or_none()

        if not company:
            return CoverageReport(company_id=company_id)

        # Get all evidence groups for this company
        groups = session.execute(
            select(EvidenceGroupModel)
            .where(EvidenceGroupModel.company_id == company_id)
        ).scalars().all()

        # Get all passages for these groups
        group_ids = [g.id for g in groups]
        passages = []
        if group_ids:
            passages = session.execute(
                select(EvidenceGroupPassageModel)
                .where(EvidenceGroupPassageModel.group_id.in_(group_ids))
            ).scalars().all()

        # Build passage lookup by group
        passages_by_group: dict[int, list] = {}
        for p in passages:
            passages_by_group.setdefault(p.group_id, []).append(p)

        # Assess each dimension x stage combination
        today = date.today()
        staleness_cutoff = today - timedelta(days=self.staleness_days)
        dimension_coverages = []
        gaps = []
        stale_dims = []

        for dim, stage in _ALL_DIMENSIONS:
            dim_key = dim.value if stage is None else f"{dim.value}:{stage.value}"
            # Find groups matching this dimension (and optionally evidence type)
            matching_groups = [
                g for g in groups
                if g.target_dimension == dim.value
                and (stage is None or g.evidence_type == stage.value)
            ]

            # Collect passages for matching groups
            dim_passages = []
            for g in matching_groups:
                dim_passages.extend(passages_by_group.get(g.id, []))

            # Count distinct source types
            source_types = list(set(
                p.source_type for p in dim_passages if p.source_type
            ))

            # Find date range
            passage_dates = [p.source_date for p in dim_passages if p.source_date]
            newest = max(passage_dates) if passage_dates else None
            oldest = min(passage_dates) if passage_dates else None

            is_stale = newest is not None and newest < staleness_cutoff
            has_gap = len(source_types) < MIN_SOURCES_PER_DIMENSION

            if has_gap:
                if len(source_types) == 0:
                    gaps.append(f"No evidence for {dim_key}")
                else:
                    gaps.append(
                        f"Only {len(source_types)} source(s) for {dim_key} "
                        f"(need {MIN_SOURCES_PER_DIMENSION})"
                    )

            if is_stale:
                stale_dims.append(
                    f"{dim_key} newest evidence from {newest} "
                    f"(>{self.staleness_days} days old)"
                )

            dimension_coverages.append(DimensionCoverage(
                dimension=dim,
                stage=stage,
                source_count=len(source_types),
                distinct_source_types=source_types,
                newest_date=newest,
                oldest_date=oldest,
                is_stale=is_stale,
                has_gap=has_gap,
            ))

        # Compute overall coverage score (0-1)
        total_slots = len(_ALL_DIMENSIONS)
        covered = sum(
            1 for dc in dimension_coverages
            if not dc.has_gap and not dc.is_stale
        )
        coverage_score = covered / total_slots if total_slots > 0 else 0.0

        return CoverageReport(
            company_id=company_id,
            ticker=company.ticker,
            company_name=company.company_name,
            total_evidence_groups=len(groups),
            total_passages=len(passages),
            dimension_coverage=dimension_coverages,
            gaps=gaps,
            stale_dimensions=stale_dims,
            overall_coverage_score=round(coverage_score, 3),
        )

    def prioritize_research(
        self, session: Session, limit: int = 20
    ) -> list[ResearchTask]:
        """Scan all active companies and return prioritized research tasks.

        Priority factors (highest to lowest):
        (a) No evidence at all -> CRITICAL
        (b) Single-source dimensions -> HIGH
        (c) Stale evidence (>60 days) -> MEDIUM
        (d) High-value companies with thin coverage -> LOW
        """
        # Get all active companies with tickers (skip unidentified entries)
        companies = session.execute(
            select(CompanyModel).where(
                CompanyModel.is_active == True,  # noqa: E712
                CompanyModel.ticker.is_not(None),
            )
        ).scalars().all()

        tasks: list[ResearchTask] = []

        for company in companies:
            report = self.assess_coverage(company.id, session)

            # Determine priority and coverage gaps
            no_evidence_dims = [
                dc for dc in report.dimension_coverage
                if dc.source_count == 0
            ]
            single_source_dims = [
                dc for dc in report.dimension_coverage
                if dc.source_count == 1
            ]
            stale_dims = [
                dc for dc in report.dimension_coverage
                if dc.is_stale and dc.source_count > 0
            ]

            def _dim_label(dc):
                label = dc.dimension.value if hasattr(dc.dimension, "value") else dc.dimension
                if dc.stage:
                    label += f":{dc.stage.value if hasattr(dc.stage, 'value') else dc.stage}"
                return label

            if no_evidence_dims:
                priority = ResearchPriority.CRITICAL
                dimensions = [_dim_label(dc) for dc in no_evidence_dims]
                coverage_gaps = [
                    f"No evidence for {d}" for d in dimensions
                ]
            elif single_source_dims:
                priority = ResearchPriority.HIGH
                dimensions = [_dim_label(dc) for dc in single_source_dims]
                coverage_gaps = [
                    f"Only 1 source for {d}" for d in dimensions
                ]
            elif stale_dims:
                priority = ResearchPriority.MEDIUM
                dimensions = [_dim_label(dc) for dc in stale_dims]
                coverage_gaps = [
                    f"Stale evidence for {d}" for d in dimensions
                ]
            else:
                # Skip companies with adequate coverage
                continue

            tasks.append(ResearchTask(
                company_id=company.id,
                ticker=company.ticker,
                company_name=company.company_name,
                dimensions=dimensions,
                priority=priority,
                coverage_gaps=coverage_gaps,
            ))

        # Sort by priority (critical first), then by number of gaps (more gaps first)
        priority_order = {
            ResearchPriority.CRITICAL: 0,
            ResearchPriority.HIGH: 1,
            ResearchPriority.MEDIUM: 2,
            ResearchPriority.LOW: 3,
        }
        tasks.sort(key=lambda t: (priority_order[t.priority], -len(t.coverage_gaps)))

        return tasks[:limit]

    def review_quality(
        self, research_result: ResearchResult
    ) -> tuple[bool, list[str]]:
        """Check if a research result meets minimum quality bar.

        Criteria:
        - At least one collected item
        - Each item must have non-trivial content (>50 chars)
        - Each item must have provenance (URL or publisher)
        - Items should have a valid source authority

        Returns:
            (accepted, issues): whether the result is accepted and any issues found
        """
        issues: list[str] = []

        if not research_result.collected_items:
            issues.append("No items collected")
            return False, issues

        for i, item in enumerate(research_result.collected_items):
            prefix = f"Item {i} ({item.item_id})"

            # Check content
            if not item.content or len(item.content.strip()) < 50:
                issues.append(
                    f"{prefix}: content too short or missing "
                    f"({len(item.content.strip()) if item.content else 0} chars)"
                )

            # Check provenance
            if not item.url and not item.publisher:
                issues.append(f"{prefix}: no URL or publisher (missing provenance)")

            # Check authority
            if item.authority is None:
                issues.append(f"{prefix}: no source authority set")

        # Accept if we have items and at most minor issues (each item has content)
        items_with_content = sum(
            1 for item in research_result.collected_items
            if item.content and len(item.content.strip()) >= 50
        )
        accepted = items_with_content > 0

        return accepted, issues
