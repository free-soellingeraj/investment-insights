"""Pipeline execution engine: run_pipeline() and run_company_dag().

Moved from scripts/run_pipeline.py. These async functions orchestrate
concurrent per-company DAG execution with semaphore-gated concurrency.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import uuid

from ai_opportunity_index.domains import PipelineRun, PipelineTask, PipelineSubtask, RunStatus, RunType
from ai_opportunity_index.pipeline.dag import DAG, StageResult, topological_layers
from ai_opportunity_index.pipeline.executors import STAGE_EXECUTORS, exec_score
from ai_opportunity_index.storage.db import complete_pipeline_run, create_pipeline_run, get_session
from ai_opportunity_index.storage.models import CompanyModel

logger = logging.getLogger(__name__)


async def run_company_dag(
    company: CompanyModel,
    stages: set[str],
    force_stages: set[str],
    pipeline_run_id: int,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> list[StageResult]:
    """Execute the DAG for a single company.

    Each stage launches immediately as a task but waits on its
    dependency events before executing. Failed dependencies cause
    dependent stages to be skipped. The company completes all stages
    before its concurrency slot is released.
    """
    ticker = company.ticker or company.slug
    if not ticker:
        logger.warning("[id=%s] Company has no ticker or slug, skipping all stages", company.id)
        return [StageResult(s, "unknown", success=False, error="No ticker or slug") for s in stages]
    events: dict[str, asyncio.Event] = {s: asyncio.Event() for s in DAG}
    results: dict[str, StageResult] = {}

    # Mark stages not in our subset as already done (no-op)
    for stage in DAG:
        if stage not in stages:
            events[stage].set()
            results[stage] = StageResult(stage, ticker, success=True, skipped=True)

    async def _run_stage(stage: str) -> StageResult:
        # Wait for all dependencies
        for dep in DAG[stage]:
            await events[dep].wait()
            # If a dependency failed, skip this stage
            if dep in results and not results[dep].success and not results[dep].skipped:
                result = StageResult(
                    stage, ticker, success=False,
                    error=f"Skipped: dependency '{dep}' failed",
                    skipped=True,
                )
                results[stage] = result
                events[stage].set()
                return result

        # Execute the stage -- per-stage force
        stage_force = stage in force_stages
        if stage == "score":
            result = await exec_score(company, pipeline_run_id, llm_semaphore=llm_semaphore, force=stage_force)
        elif stage in ("extract_filings", "extract_news", "extract_unified", "value_evidence"):
            executor = STAGE_EXECUTORS[stage]
            result = await executor(company, stage_force, llm_semaphore=llm_semaphore)
        else:
            executor = STAGE_EXECUTORS[stage]
            result = await executor(company, stage_force)

        results[stage] = result
        events[stage].set()
        return result

    # Launch all active stages concurrently
    active_stages = [s for s in stages if s in DAG]
    stage_results = await asyncio.gather(
        *[_run_stage(s) for s in active_stages],
        return_exceptions=True,
    )

    # Handle any unexpected exceptions from gather
    final_results = []
    for i, r in enumerate(stage_results):
        if isinstance(r, Exception):
            stage_name = active_stages[i]
            logger.error("[%s] Unexpected error in %s: %s", ticker, stage_name, r)
            final_results.append(
                StageResult(stage_name, ticker, success=False, error=str(r))
            )
        else:
            final_results.append(r)

    return final_results


async def run_pipeline(
    companies: list[CompanyModel],
    stages: set[str],
    force_stages: set[str] | None = None,
    max_concurrency: int = 5,
    pipeline_run_id: int | None = None,
    llm_concurrency: int = 50,
    source: str = "run_pipeline",
) -> list[StageResult]:
    """Run the DAG pipeline for multiple companies concurrently.

    Concurrency is gated at the company level: at most max_concurrency
    companies are in-flight at once. Within each company, stages flow
    through the DAG freely (parallel where independent, sequential
    where dependent). This ensures each company completes all stages
    before its slot is released.
    """
    if force_stages is None:
        force_stages = set()

    # Expand the default thread pool so asyncio.to_thread can run
    # enough blocking stages in parallel across all in-flight companies.
    loop = asyncio.get_running_loop()
    loop.set_default_executor(
        concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency * 4)
    )

    company_sem = asyncio.Semaphore(max_concurrency)
    llm_semaphore = asyncio.Semaphore(llm_concurrency)

    # Create pipeline run if not provided
    run_id = str(uuid.uuid4())
    if pipeline_run_id is None:
        ticker_list = [c.ticker for c in companies]
        run_type = RunType.PARTIAL if len(companies) < 100 else RunType.FULL
        pr = create_pipeline_run(PipelineRun(
            run_id=run_id,
            task=PipelineTask.COLLECT,
            subtask=PipelineSubtask.ALL,
            run_type=run_type,
            status=RunStatus.RUNNING,
            tickers_requested=ticker_list,
            parameters={
                "stages_requested": sorted(stages),
                "force_stages": sorted(force_stages),
                "max_concurrency": max_concurrency,
                "source": source,
            },
        ))
        pipeline_run_id = pr.id

    logger.info(
        "Starting pipeline: %d companies, stages=%s, concurrency=%d",
        len(companies), sorted(stages), max_concurrency,
    )

    # Display DAG layers for active stages
    active_dag = {s: DAG[s] & stages for s in stages if s in DAG}
    layers = topological_layers(active_dag)
    for i, layer in enumerate(layers):
        logger.info("  Layer %d: %s", i, ", ".join(layer))

    # Fan out per-company DAGs, gated by company-level semaphore
    all_results: list[StageResult] = []

    async def _run_company(company: CompanyModel) -> list[StageResult]:
        async with company_sem:
            return await run_company_dag(
                company, stages, force_stages, pipeline_run_id,
                llm_semaphore=llm_semaphore,
            )

    company_tasks = [_run_company(company) for company in companies]
    company_results = await asyncio.gather(*company_tasks)

    succeeded = 0
    failed_count = 0
    for company, results in zip(companies, company_results):
        all_results.extend(results)
        # A company succeeded if its final requested stages succeeded
        company_failures = [r for r in results if not r.success and not r.skipped]
        if company_failures:
            failed_count += 1
        else:
            succeeded += 1

    # Complete the pipeline run
    complete_pipeline_run(
        run_id=run_id,
        status=RunStatus.COMPLETED,
        tickers_succeeded=succeeded,
        tickers_failed=failed_count,
    )

    # Summary
    stage_summary: dict[str, dict[str, int]] = {}
    for r in all_results:
        if r.skipped and r.success:
            continue  # not-in-subset stages
        counts = stage_summary.setdefault(r.stage, {"ok": 0, "fail": 0, "skip": 0})
        if r.skipped:
            counts["skip"] += 1
        elif r.success:
            counts["ok"] += 1
        else:
            counts["fail"] += 1

    logger.info("=== Pipeline complete: %d succeeded, %d failed ===", succeeded, failed_count)
    for stage, counts in sorted(stage_summary.items()):
        logger.info(
            "  %-25s ok=%d  fail=%d  skip=%d",
            stage, counts["ok"], counts["fail"], counts["skip"],
        )

    return all_results
