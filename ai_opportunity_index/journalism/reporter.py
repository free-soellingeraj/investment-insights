"""Reporter agent: synthesizes evidence into structured narratives.

Generates human-readable output from the fact graph using deterministic
template-based synthesis. Does NOT call LLMs.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from ai_opportunity_index.domains import (
    TargetDimension,
    ValuationEvidenceType,
    ValuationStage,
)
from ai_opportunity_index.storage.models import (
    CompanyModel,
    CompanyScoreModel,
    EvidenceGroupModel,
    EvidenceGroupPassageModel,
    ValuationDiscrepancyModel,
    ValuationModel,
)

from .models import Citation, CompanyNarrative, NarrativeSection

logger = logging.getLogger(__name__)


def _format_dollars(value: float | None) -> str:
    """Format a dollar value for narrative display."""
    if value is None:
        return "N/A"
    if abs(value) >= 1_000_000_000:
        return f"${value / 1_000_000_000:.1f}B"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:.0f}"


def _confidence_label(confidence: float) -> str:
    """Convert a confidence score to a human-readable label."""
    if confidence >= 0.8:
        return "high"
    if confidence >= 0.5:
        return "moderate"
    if confidence >= 0.3:
        return "low"
    return "very low"


class Reporter:
    """Synthesizes evidence into structured narratives (no LLM)."""

    def generate_narrative(
        self, company_id: int, session: Session
    ) -> CompanyNarrative:
        """Build a structured narrative from evidence, valuations, and scores.

        Uses template-based synthesis from existing evidence groups,
        valuations, and scores. Does NOT use LLM.
        """
        # Get company info
        company = session.execute(
            select(CompanyModel).where(CompanyModel.id == company_id)
        ).scalar_one_or_none()

        if not company:
            return CompanyNarrative(company_id=company_id)

        # Get latest score
        latest_score = session.execute(
            select(CompanyScoreModel)
            .where(CompanyScoreModel.company_id == company_id)
            .order_by(CompanyScoreModel.scored_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        # Get evidence groups with passages
        groups = session.execute(
            select(EvidenceGroupModel)
            .where(EvidenceGroupModel.company_id == company_id)
        ).scalars().all()

        group_ids = [g.id for g in groups]
        passages_by_group: dict[int, list] = {}
        if group_ids:
            passages = session.execute(
                select(EvidenceGroupPassageModel)
                .where(EvidenceGroupPassageModel.group_id.in_(group_ids))
            ).scalars().all()
            for p in passages:
                passages_by_group.setdefault(p.group_id, []).append(p)

        # Get valuations (final stage only)
        valuations: list = []
        if group_ids:
            valuations = session.execute(
                select(ValuationModel)
                .where(
                    ValuationModel.group_id.in_(group_ids),
                    ValuationModel.stage == ValuationStage.FINAL.value,
                )
            ).scalars().all()

        valuations_by_group: dict[int, list] = {}
        for v in valuations:
            valuations_by_group.setdefault(v.group_id, []).append(v)

        # Get discrepancies for verification section
        discrepancies = session.execute(
            select(ValuationDiscrepancyModel)
            .where(ValuationDiscrepancyModel.company_id == company_id)
        ).scalars().all()

        # Separate groups by evidence type or target dimension.
        # evidence_type is often NULL, so fall back to target_dimension for grouping.
        opportunity_groups = [
            g for g in groups
            if g.evidence_type in (
                ValuationEvidenceType.PLAN.value,
                ValuationEvidenceType.INVESTMENT.value,
            ) or (
                g.evidence_type is None
                and g.target_dimension in (
                    TargetDimension.COST.value,
                    TargetDimension.REVENUE.value,
                    TargetDimension.GENERAL.value,
                )
            )
        ]
        realization_groups = [
            g for g in groups
            if g.evidence_type == ValuationEvidenceType.CAPTURE.value
        ]

        # Build sections
        sections = []

        # Summary section
        summary_section = self._build_summary_section(company, latest_score)
        sections.append(summary_section)

        # Opportunity section
        opp_section = self._build_opportunity_section(
            opportunity_groups, valuations_by_group, passages_by_group
        )
        if opp_section.evidence_count > 0:
            sections.append(opp_section)

        # Realization section
        real_section = self._build_realization_section(
            realization_groups, valuations_by_group, passages_by_group
        )
        if real_section.evidence_count > 0:
            sections.append(real_section)

        # Verification section
        verif_section = self._build_verification_section(discrepancies)
        if verif_section.evidence_count > 0:
            sections.append(verif_section)

        # Compute totals
        all_citations = []
        for s in sections:
            all_citations.extend(s.citations)
        total_citations = len(all_citations)

        confidence_values = [s.confidence for s in sections if s.confidence > 0]
        overall_confidence = (
            sum(confidence_values) / len(confidence_values)
            if confidence_values
            else 0.0
        )

        # Build summary text
        summary = self._build_summary_text(company, latest_score, sections)

        return CompanyNarrative(
            company_id=company_id,
            ticker=company.ticker,
            company_name=company.company_name,
            summary=summary,
            sections=sections,
            total_citations=total_citations,
            overall_confidence=round(overall_confidence, 3),
        )

    def _build_summary_section(
        self, company, latest_score
    ) -> NarrativeSection:
        """Build the executive summary section."""
        parts = []
        parts.append(
            f"{company.company_name or company.ticker} "
            f"({company.ticker})"
        )

        if company.sector:
            parts[0] += f" operates in the {company.sector} sector"
            if company.industry:
                parts[0] += f" ({company.industry})"
            parts[0] += "."

        if latest_score:
            quadrant_label = latest_score.quadrant_label or latest_score.quadrant or "unclassified"
            parts.append(
                f"The company is classified as '{quadrant_label}' with an "
                f"opportunity score of {latest_score.opportunity:.2f} and "
                f"a realization score of {latest_score.realization:.2f}."
            )
            if latest_score.ai_index_usd is not None:
                parts.append(
                    f"Estimated AI index value: {_format_dollars(latest_score.ai_index_usd)}."
                )
            if latest_score.combined_rank is not None:
                parts.append(f"Overall rank: #{latest_score.combined_rank}.")

        return NarrativeSection(
            title="Executive Summary",
            body=" ".join(parts),
            confidence=0.9 if latest_score else 0.3,
            evidence_count=1 if latest_score else 0,
        )

    def _build_opportunity_section(
        self,
        groups: list,
        valuations_by_group: dict[int, list],
        passages_by_group: dict[int, list],
    ) -> NarrativeSection:
        """Summarize AI opportunity evidence (plan + investment)."""
        parts = []
        citations = []
        confidence_values = []
        evidence_count = 0

        # Group by dimension
        cost_groups = [g for g in groups if g.target_dimension == TargetDimension.COST.value]
        revenue_groups = [g for g in groups if g.target_dimension == TargetDimension.REVENUE.value]

        for label, dim_groups in [("Cost", cost_groups), ("Revenue", revenue_groups)]:
            if not dim_groups:
                continue

            dim_parts = []
            dim_passage_count = 0
            for group in dim_groups:
                group_vals = valuations_by_group.get(group.id, [])
                group_passages = passages_by_group.get(group.id, [])
                evidence_count += len(group_passages)
                dim_passage_count += len(group_passages)

                # Extract dollar estimates from valuations
                for val in group_vals:
                    confidence_values.append(val.confidence)
                    narrative_text = val.narrative or ""
                    dollar_text = ""
                    if val.dollar_mid is not None:
                        dollar_text = (
                            f" (estimated {_format_dollars(val.dollar_mid)}, "
                            f"range {_format_dollars(val.dollar_low)}"
                            f"-{_format_dollars(val.dollar_high)})"
                        )
                    evidence_type_label = (val.evidence_type or "").replace("_", " ").title()
                    dim_parts.append(
                        f"{evidence_type_label}: {narrative_text}{dollar_text} "
                        f"[{_confidence_label(val.confidence)} confidence]."
                    )

                # Build citations from passages
                for passage in group_passages:
                    citations.append(self._format_citation(passage))

            if dim_parts:
                parts.append(f"{label} Opportunity: " + " ".join(dim_parts))
            elif dim_passage_count > 0:
                # No valuations but passages exist — synthesize from passage data
                source_types = set()
                for g in dim_groups:
                    for p in passages_by_group.get(g.id, []):
                        if p.source_type:
                            source_types.add(p.source_type)
                sources_text = ", ".join(sorted(source_types)) if source_types else "various sources"
                mean_conf = sum(
                    p.confidence for g in dim_groups
                    for p in passages_by_group.get(g.id, [])
                    if p.confidence
                ) / max(dim_passage_count, 1)
                confidence_values.append(mean_conf)
                parts.append(
                    f"{label} Opportunity: {dim_passage_count} evidence passages "
                    f"from {len(dim_groups)} groups ({sources_text}) "
                    f"[{_confidence_label(mean_conf)} confidence]. "
                    f"Dollar estimates pending valuation."
                )

        body = " ".join(parts) if parts else "No opportunity evidence available."
        avg_confidence = (
            sum(confidence_values) / len(confidence_values)
            if confidence_values
            else 0.0
        )

        return NarrativeSection(
            title="AI Opportunity",
            body=body,
            citations=citations,
            confidence=round(avg_confidence, 3),
            evidence_count=evidence_count,
        )

    def _build_realization_section(
        self,
        groups: list,
        valuations_by_group: dict[int, list],
        passages_by_group: dict[int, list],
    ) -> NarrativeSection:
        """Summarize AI realization/capture evidence."""
        parts = []
        citations = []
        confidence_values = []
        evidence_count = 0

        cost_groups = [g for g in groups if g.target_dimension == TargetDimension.COST.value]
        revenue_groups = [g for g in groups if g.target_dimension == TargetDimension.REVENUE.value]

        for label, dim_groups in [("Cost", cost_groups), ("Revenue", revenue_groups)]:
            if not dim_groups:
                continue

            dim_parts = []
            dim_passage_count = 0
            for group in dim_groups:
                group_vals = valuations_by_group.get(group.id, [])
                group_passages = passages_by_group.get(group.id, [])
                evidence_count += len(group_passages)
                dim_passage_count += len(group_passages)

                for val in group_vals:
                    confidence_values.append(val.confidence)
                    narrative_text = val.narrative or ""
                    dollar_text = ""
                    if val.dollar_mid is not None:
                        dollar_text = (
                            f" (measured impact: {_format_dollars(val.dollar_mid)})"
                        )
                    dim_parts.append(
                        f"{narrative_text}{dollar_text} "
                        f"[{_confidence_label(val.confidence)} confidence]."
                    )

                for passage in group_passages:
                    citations.append(self._format_citation(passage))

            if dim_parts:
                parts.append(f"{label} Capture: " + " ".join(dim_parts))
            elif dim_passage_count > 0:
                source_types = set()
                for g in dim_groups:
                    for p in passages_by_group.get(g.id, []):
                        if p.source_type:
                            source_types.add(p.source_type)
                sources_text = ", ".join(sorted(source_types)) if source_types else "various sources"
                mean_conf = sum(
                    p.confidence for g in dim_groups
                    for p in passages_by_group.get(g.id, [])
                    if p.confidence
                ) / max(dim_passage_count, 1)
                confidence_values.append(mean_conf)
                parts.append(
                    f"{label} Capture: {dim_passage_count} evidence passages "
                    f"from {len(dim_groups)} groups ({sources_text}) "
                    f"[{_confidence_label(mean_conf)} confidence]. "
                    f"Impact measurement pending valuation."
                )

        body = " ".join(parts) if parts else "No realization evidence available."
        avg_confidence = (
            sum(confidence_values) / len(confidence_values)
            if confidence_values
            else 0.0
        )

        return NarrativeSection(
            title="AI Realization",
            body=body,
            citations=citations,
            confidence=round(avg_confidence, 3),
            evidence_count=evidence_count,
        )

    def _build_verification_section(
        self, discrepancies: list
    ) -> NarrativeSection:
        """Summarize cross-source verification results."""
        if not discrepancies:
            return NarrativeSection(
                title="Cross-Source Verification",
                body="No cross-source discrepancies have been identified.",
                confidence=1.0,
                evidence_count=0,
            )

        parts = []
        for disc in discrepancies:
            resolution_text = disc.resolution or "unresolved"
            parts.append(
                f"Discrepancy: {disc.description} "
                f"Resolution: {resolution_text}."
            )

        body = (
            f"{len(discrepancies)} cross-source discrepancies identified. "
            + " ".join(parts)
        )

        return NarrativeSection(
            title="Cross-Source Verification",
            body=body,
            confidence=0.7,
            evidence_count=len(discrepancies),
        )

    def _format_citation(self, passage) -> Citation:
        """Convert an EvidenceGroupPassageModel to a Citation."""
        return Citation(
            source_url=passage.source_url,
            author=passage.source_author,
            publisher=passage.source_publisher,
            source_date=passage.source_date,
            authority=passage.source_authority,
            excerpt=(
                passage.passage_text[:200] + "..."
                if passage.passage_text and len(passage.passage_text) > 200
                else passage.passage_text
            ),
        )

    def _build_summary_text(
        self, company, latest_score, sections: list[NarrativeSection]
    ) -> str:
        """Build a one-paragraph executive summary from sections."""
        parts = []
        name = company.company_name or company.ticker

        if latest_score:
            parts.append(
                f"{name} has an AI opportunity score of "
                f"{latest_score.opportunity:.2f} and realization score of "
                f"{latest_score.realization:.2f}."
            )
        else:
            parts.append(f"{name} has not yet been scored.")

        section_count = sum(1 for s in sections if s.evidence_count > 0)
        citation_count = sum(len(s.citations) for s in sections)
        parts.append(
            f"This narrative covers {section_count} evidence area(s) "
            f"with {citation_count} citation(s)."
        )

        return " ".join(parts)
