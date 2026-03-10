"""Async repository for evidence operations."""

from __future__ import annotations

import logging
from typing import Sequence

from sqlalchemy import delete, select

from ai_opportunity_index.domains import AIOpportunityEvidence
from ai_opportunity_index.storage.models import EvidenceModel
from ai_opportunity_index.storage.repositories.base import BaseRepository

logger = logging.getLogger(__name__)


class EvidenceRepository(BaseRepository[EvidenceModel]):
    """Async repository for evidence CRUD."""

    model_class = EvidenceModel

    @staticmethod
    def _to_domain(m: EvidenceModel) -> AIOpportunityEvidence:
        return AIOpportunityEvidence(
            id=m.id,
            company_id=m.company_id,
            pipeline_run_id=m.pipeline_run_id,
            evidence_type=m.evidence_type,
            evidence_subtype=m.evidence_subtype,
            source_name=m.source_name,
            source_url=m.source_url,
            source_date=m.source_date,
            score_contribution=m.score_contribution,
            weight=m.weight,
            signal_strength=m.signal_strength,
            target_dimension=m.target_dimension,
            capture_stage=m.capture_stage,
            source_excerpt=m.source_excerpt,
            payload=m.payload or {},
            observed_at=m.observed_at,
            valid_from=m.valid_from,
            valid_to=m.valid_to,
        )

    @staticmethod
    def _to_model(ev: AIOpportunityEvidence) -> EvidenceModel:
        return EvidenceModel(
            company_id=ev.company_id,
            pipeline_run_id=ev.pipeline_run_id,
            evidence_type=ev.evidence_type,
            evidence_subtype=ev.evidence_subtype,
            source_name=ev.source_name,
            source_url=ev.source_url,
            source_date=ev.source_date,
            score_contribution=ev.score_contribution,
            weight=ev.weight,
            signal_strength=ev.signal_strength,
            target_dimension=ev.target_dimension,
            capture_stage=ev.capture_stage,
            source_excerpt=ev.source_excerpt,
            payload=ev.payload,
            valid_from=ev.valid_from,
            valid_to=ev.valid_to,
        )

    async def save_evidence(self, evidence: AIOpportunityEvidence) -> AIOpportunityEvidence:
        """Save a single evidence row."""
        model = self._to_model(evidence)
        self.session.add(model)
        await self.session.flush()
        return self._to_domain(model)

    async def save_evidence_batch(self, items: list[AIOpportunityEvidence]) -> None:
        """Bulk insert evidence rows."""
        for ev in items:
            model = self._to_model(ev)
            self.session.add(model)
        await self.session.flush()

    async def get_evidence_for_company(
        self, company_id: int, evidence_type: str | None = None
    ) -> list[AIOpportunityEvidence]:
        """Get evidence for a company, optionally filtered by type."""
        stmt = select(EvidenceModel).where(EvidenceModel.company_id == company_id)
        if evidence_type:
            stmt = stmt.where(EvidenceModel.evidence_type == evidence_type)
        stmt = stmt.order_by(EvidenceModel.observed_at.desc())
        result = await self.session.execute(stmt)
        models = result.scalars().all()
        return [self._to_domain(m) for m in models]

    async def delete_evidence_for_company(self, company_id: int) -> int:
        """Remove all evidence for a company (called before re-scoring).

        Returns the number of rows deleted.
        """
        result = await self.session.execute(
            delete(EvidenceModel).where(EvidenceModel.company_id == company_id)
        )
        await self.session.flush()
        logger.info("Deleted %d evidence rows for company_id=%d", result.rowcount, company_id)
        return result.rowcount

    async def delete_evidence_for_run(
        self, company_id: int, pipeline_run_id: int, evidence_type: str
    ) -> None:
        """Delete evidence for a specific company/run/type before re-scoring."""
        await self.session.execute(
            delete(EvidenceModel).where(
                EvidenceModel.company_id == company_id,
                EvidenceModel.pipeline_run_id == pipeline_run_id,
                EvidenceModel.evidence_type == evidence_type,
            )
        )
        await self.session.flush()
