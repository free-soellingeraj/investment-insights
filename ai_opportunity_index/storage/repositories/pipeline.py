"""Async repository for pipeline run operations."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select, text, update

from ai_opportunity_index.domains import PipelineRun, RunStatus
from ai_opportunity_index.storage.models import PipelineRunModel
from ai_opportunity_index.storage.repositories.base import BaseRepository


class PipelineRunRepository(BaseRepository[PipelineRunModel]):
    """Async repository for pipeline run CRUD."""

    model_class = PipelineRunModel

    @staticmethod
    def _to_domain(m: PipelineRunModel) -> PipelineRun:
        return PipelineRun(
            id=m.id,
            run_id=m.run_id,
            task=m.task,
            subtask=m.subtask,
            run_type=m.run_type,
            status=m.status,
            parameters=m.parameters or {},
            tickers_requested=m.tickers_requested or [],
            tickers_succeeded=m.tickers_succeeded,
            tickers_failed=m.tickers_failed,
            parent_run_id=m.parent_run_id,
            started_at=m.started_at,
            completed_at=m.completed_at,
            error_message=m.error_message,
        )

    async def create_pipeline_run(self, run: PipelineRun) -> PipelineRun:
        """Create a new pipeline run record."""
        model = PipelineRunModel(
            run_id=run.run_id,
            task=run.task,
            subtask=run.subtask,
            run_type=run.run_type,
            status=run.status,
            parameters=run.parameters,
            tickers_requested=run.tickers_requested,
            tickers_succeeded=run.tickers_succeeded,
            tickers_failed=run.tickers_failed,
            parent_run_id=run.parent_run_id,
        )
        self.session.add(model)
        await self.session.flush()
        return self._to_domain(model)

    async def complete_pipeline_run(
        self,
        run_id: str,
        status: RunStatus | str = RunStatus.COMPLETED,
        tickers_succeeded: int = 0,
        tickers_failed: int = 0,
        error_message: str | None = None,
    ) -> None:
        """Mark a pipeline run as completed or failed."""
        values: dict = {
            "status": status,
            "tickers_succeeded": tickers_succeeded,
            "tickers_failed": tickers_failed,
            "completed_at": datetime.utcnow(),
        }
        if error_message is not None:
            values["error_message"] = error_message
        await self.session.execute(
            update(PipelineRunModel)
            .where(PipelineRunModel.run_id == run_id)
            .values(**values)
        )
        await self.session.flush()

    async def get_last_daily_refresh_time(self) -> datetime | None:
        """Get the most recent completed daily refresh run time.

        Queries pipeline_runs for the most recent completed run where
        parameters->>'source' = 'daily_refresh'.
        """
        result = await self.session.execute(
            select(PipelineRunModel)
            .where(PipelineRunModel.status == "completed")
            .where(PipelineRunModel.parameters["source"].astext == "daily_refresh")
            .order_by(PipelineRunModel.completed_at.desc())
            .limit(1)
        )
        model = result.scalar_one_or_none()
        if model and model.completed_at:
            return model.completed_at
        return None

    async def get_last_completed_run_time(
        self, stages_filter: set[str] | None = None
    ) -> datetime | None:
        """Source-agnostic watermark: find the most recent completed pipeline run.

        Optionally filtered to runs whose ``parameters.stages_requested`` overlaps
        the given *stages_filter*.
        """
        stmt = (
            select(PipelineRunModel)
            .where(PipelineRunModel.status == "completed")
            .order_by(PipelineRunModel.completed_at.desc())
            .limit(1)
        )
        if stages_filter:
            overlap_values = ",".join(f"'{s}'" for s in stages_filter)
            stmt = stmt.where(
                text(f"parameters->'stages_requested' ?| array[{overlap_values}]")
            )
        result = await self.session.execute(stmt)
        model = result.scalar_one_or_none()
        if model and model.completed_at:
            return model.completed_at
        return None

    async def get_pipeline_run(self, run_id: str) -> PipelineRun | None:
        """Look up a pipeline run by its UUID run_id."""
        result = await self.session.execute(
            select(PipelineRunModel).where(PipelineRunModel.run_id == run_id)
        )
        model = result.scalar_one_or_none()
        return self._to_domain(model) if model else None

    async def get_pipeline_runs(
        self,
        task: str | None = None,
        subtask: str | None = None,
        limit: int = 10,
    ) -> list[PipelineRun]:
        """List pipeline runs, optionally filtered by task and/or subtask."""
        stmt = select(PipelineRunModel).order_by(PipelineRunModel.started_at.desc())
        if task:
            stmt = stmt.where(PipelineRunModel.task == task)
        if subtask:
            stmt = stmt.where(PipelineRunModel.subtask == subtask)
        stmt = stmt.limit(limit)
        result = await self.session.execute(stmt)
        models = result.scalars().all()
        return [self._to_domain(m) for m in models]
