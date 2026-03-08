"""Unified pipeline controller.

All entry points (CLI, daily refresh, web UI, refresh requests) delegate
to PipelineController.run() so there is one DAG, one watermark, and one
orchestration path.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from ai_opportunity_index.pipeline.dag import (
    DAG,
    StageResult,
    resolve_stages,
    topological_layers,
)

logger = logging.getLogger(__name__)


class TriggerSource(str, Enum):
    """Who triggered the pipeline run."""
    CLI = "cli"
    DAILY_REFRESH = "daily_refresh"
    WEB_UI = "web_ui"
    REFRESH_REQUEST = "refresh_request"


@dataclass
class PipelineRequest:
    """All parameters needed to run the pipeline, from any entry point."""
    tickers: list[str] | None = None        # None = all active companies
    stages: set[str] | None = None          # None = all stages
    force_stages: set[str] | None = None
    since_date: datetime | None = None      # incremental mode
    source: TriggerSource = TriggerSource.CLI
    max_concurrency: int = 20
    llm_concurrency: int = 50
    include_inactive: bool = False
    pipeline_run_id: int | None = None      # reuse existing run record
    limit: int | None = None


class PipelineController:
    """Unified entry point for all pipeline triggers."""

    @staticmethod
    def get_stage_names() -> list[str]:
        """Topologically ordered stage names. Web UI calls this."""
        layers = topological_layers(DAG)
        return [s for layer in layers for s in layer]

    @staticmethod
    def get_last_completed_run(stages_filter: set[str] | None = None) -> datetime | None:
        """Source-agnostic watermark.

        Finds the last completed pipeline run, optionally filtering to runs
        that included specific stages. Any source counts. Replaces
        get_last_daily_refresh_time().
        """
        from ai_opportunity_index.storage.db import get_last_completed_run_time
        return get_last_completed_run_time(stages_filter=stages_filter)

    @staticmethod
    def _resolve_companies(request: PipelineRequest):
        """Load companies from DB based on request parameters."""
        from ai_opportunity_index.domains import FinancialMetric
        from ai_opportunity_index.storage.db import get_session, init_db
        from ai_opportunity_index.storage.models import CompanyModel, FinancialObservationModel
        from sqlalchemy import desc, func as sa_func, or_

        init_db()
        session = get_session()

        # Order by market cap (largest first)
        mktcap_sub = (
            session.query(
                FinancialObservationModel.company_id,
                sa_func.max(FinancialObservationModel.value).label("market_cap"),
            )
            .filter(FinancialObservationModel.metric == FinancialMetric.MARKET_CAP)
            .group_by(FinancialObservationModel.company_id)
            .subquery()
        )

        query = (
            session.query(CompanyModel)
            .outerjoin(mktcap_sub, CompanyModel.id == mktcap_sub.c.company_id)
            .order_by(desc(mktcap_sub.c.market_cap).nulls_last())
        )
        if not request.include_inactive:
            query = query.filter(CompanyModel.is_active.is_(True))
        if request.tickers:
            upper_tickers = [t.upper() for t in request.tickers]
            query = query.filter(
                or_(
                    CompanyModel.ticker.in_(upper_tickers),
                    CompanyModel.slug.in_(upper_tickers),
                )
            )
        if request.limit:
            query = query.limit(request.limit)
        companies = query.all()
        session.close()
        return companies

    @staticmethod
    async def run(request: PipelineRequest) -> list[StageResult]:
        """THE unified entry point. All triggers call this."""
        from ai_opportunity_index.pipeline.runner import run_pipeline
        from ai_opportunity_index.storage.db import init_db

        init_db()

        # Resolve stages
        stages = request.stages if request.stages else set(DAG.keys())
        force_stages = request.force_stages or set()

        # Resolve companies
        companies = PipelineController._resolve_companies(request)
        if not companies:
            logger.warning("No companies found matching the criteria")
            return []

        logger.info(
            "PipelineController.run: source=%s, %d companies, stages=%s",
            request.source.value, len(companies), sorted(stages),
        )

        # Run the pipeline
        results = await run_pipeline(
            companies,
            stages=stages,
            force_stages=force_stages,
            max_concurrency=request.max_concurrency,
            pipeline_run_id=request.pipeline_run_id,
            llm_concurrency=request.llm_concurrency,
            source=request.source.value,
        )

        return results

    @staticmethod
    def run_sync(request: PipelineRequest) -> list[StageResult]:
        """Sync wrapper for CLI scripts."""
        return asyncio.run(PipelineController.run(request))

    @staticmethod
    def build_subprocess_cmd(request: PipelineRequest) -> list[str]:
        """Build argv for subprocess execution (web trigger mode)."""
        cmd = [
            sys.executable, "scripts/run_pipeline.py",
        ]
        if request.tickers:
            cmd.extend(["--tickers"] + [t.upper() for t in request.tickers])
        if request.stages:
            cmd.extend(["--stages", ",".join(sorted(request.stages))])
        cmd.append("--with-deps")
        if request.force_stages:
            cmd.extend(["--force", ",".join(sorted(request.force_stages))])
        cmd.extend(["--concurrency", str(request.max_concurrency)])
        cmd.extend(["--llm-concurrency", str(request.llm_concurrency)])
        if request.include_inactive:
            cmd.append("--include-inactive")
        return cmd
