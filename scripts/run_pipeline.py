#!/usr/bin/env python3
"""Per-company DAG pipeline for the AI Opportunity Index.

Defines a dependency graph of stages per company and executes them
concurrently using asyncio. Independent stages run in parallel;
dependent stages wait only on their specific prerequisites.

DAG:
                    company in DB
                         |
          +--------------+--------------+
          |              |              |
          v              v              v
    [discover_links] [collect_news] [collect_github]
          |              |              |
          |              |         [collect_analysts]
          v              |
  [collect_web_enrichment]
          |              |
          +------+-------+
                 v
             [score]

Usage:
    python scripts/run_pipeline.py --tickers AAPL MSFT
    python scripts/run_pipeline.py --stages collect --tickers AAPL
    python scripts/run_pipeline.py --stages score --tickers AAPL
    python scripts/run_pipeline.py --concurrency 10 --limit 100
"""

import argparse
import asyncio
import logging
import sys
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_opportunity_index.domains import PipelineRun, PipelineSubtask, PipelineTask
from ai_opportunity_index.storage.db import (
    complete_pipeline_run,
    create_pipeline_run,
    get_session,
    init_db,
)
from ai_opportunity_index.storage.models import CompanyModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ── DAG Definition ────────────────────────────────────────────────────────

DAG: dict[str, set[str]] = {
    "discover_links": set(),
    "collect_news": set(),
    "collect_github": set(),
    "collect_analysts": set(),
    "collect_web_enrichment": {"discover_links"},
    "extract_filings": set(),
    "extract_news": {"collect_news"},
    "value_evidence": {"extract_news", "extract_filings"},
    "score": {"value_evidence", "collect_web_enrichment", "collect_github", "collect_analysts"},
}

# Stage aliases for CLI convenience
STAGE_ALIASES: dict[str, set[str]] = {
    "all": set(DAG.keys()),
    "collect": {"discover_links", "collect_news", "collect_github",
                "collect_analysts", "collect_web_enrichment"},
    "extract": {"extract_filings", "extract_news"},
    "value": {"value_evidence"},
}


def topological_layers(dag: dict[str, set[str]]) -> list[list[str]]:
    """Compute topological layers using Kahn's algorithm.

    Returns layers where all stages in a layer can run concurrently.
    Raises ValueError if the DAG contains a cycle.
    """
    in_degree = {node: len(deps) for node, deps in dag.items()}
    dependents: dict[str, list[str]] = {node: [] for node in dag}
    for node, deps in dag.items():
        for dep in deps:
            dependents[dep].append(node)

    queue = deque(node for node, deg in in_degree.items() if deg == 0)
    layers: list[list[str]] = []
    visited = 0

    while queue:
        layer = []
        for _ in range(len(queue)):
            node = queue.popleft()
            layer.append(node)
            visited += 1
            for dependent in dependents[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        layers.append(sorted(layer))

    if visited != len(dag):
        raise ValueError("DAG contains a cycle")

    return layers


def resolve_stages(requested: list[str], *, with_deps: bool = False) -> set[str]:
    """Expand aliases and optionally add transitive dependencies.

    By default only alias expansion is performed. Pass ``with_deps=True``
    to pull in all transitive DAG dependencies (old behaviour).
    """
    stages: set[str] = set()
    for s in requested:
        if s in STAGE_ALIASES:
            stages |= STAGE_ALIASES[s]
        elif s in DAG:
            stages.add(s)
        else:
            raise ValueError(f"Unknown stage: {s}")

    if with_deps:
        added = True
        while added:
            added = False
            for stage in list(stages):
                for dep in DAG.get(stage, set()):
                    if dep not in stages:
                        stages.add(dep)
                        added = True

    return stages


def parse_force_stages(force_arg: list[str] | None) -> set[str]:
    """Parse the ``--force`` CLI value into a set of stage names.

    * ``None``  → ``--force`` was not passed at all → empty set
    * ``[]``    → bare ``--force`` with no arguments → ALL stages
    * ``["extract", "value"]`` → expand aliases, return matching stages
    """
    if force_arg is None:
        return set()
    if len(force_arg) == 0:
        return set(DAG.keys())
    # Expand aliases
    stages: set[str] = set()
    for s in force_arg:
        if s in STAGE_ALIASES:
            stages |= STAGE_ALIASES[s]
        elif s in DAG:
            stages.add(s)
        else:
            raise ValueError(f"Unknown force stage: {s}")
    return stages


# ── Stage Result ──────────────────────────────────────────────────────────


@dataclass
class StageResult:
    stage: str
    ticker: str
    success: bool
    error: str | None = None
    skipped: bool = False


# ── Stage Executors ───────────────────────────────────────────────────────


async def exec_discover_links(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker
    name = company.company_name or ticker
    logger.info("[%s] discover_links starting", ticker)
    try:
        import json
        from scripts.discover_links import discover_company_links, DISCOVERED_LINKS_DIR

        result = await discover_company_links(ticker, name, force=force)

        # If cached (result is None), read from cache file for DB update
        if result is None:
            cache_path = DISCOVERED_LINKS_DIR / f"{ticker.upper()}.json"
            if cache_path.exists():
                try:
                    result = json.loads(cache_path.read_text())
                except Exception:
                    pass

        # Update company URL columns in DB from discovered links
        if result:
            session = get_session()
            try:
                co = session.query(CompanyModel).filter_by(ticker=ticker).first()
                if co:
                    changed = False
                    for attr in ("github_url", "careers_url", "ir_url", "blog_url"):
                        url = result.get(attr)
                        if url and (not getattr(co, attr, None) or force):
                            setattr(co, attr, url)
                            changed = True
                    if changed:
                        session.commit()
                    else:
                        session.close()
                else:
                    session.close()
            except Exception:
                session.rollback()
                session.close()

        logger.info("[%s] discover_links done", ticker)
        return StageResult("discover_links", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] discover_links failed: %s", ticker, e)
        return StageResult("discover_links", ticker, success=False, error=str(e))


async def exec_collect_news(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker
    logger.info("[%s] collect_news starting", ticker)
    try:
        from scripts.collect_evidence import collect_news

        await asyncio.to_thread(collect_news, [company], force=force)
        logger.info("[%s] collect_news done", ticker)
        return StageResult("collect_news", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] collect_news failed: %s", ticker, e)
        return StageResult("collect_news", ticker, success=False, error=str(e))


async def exec_collect_github(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker
    logger.info("[%s] collect_github starting", ticker)
    try:
        from scripts.collect_evidence import collect_github

        await asyncio.to_thread(collect_github, [company], force=force)
        logger.info("[%s] collect_github done", ticker)
        return StageResult("collect_github", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] collect_github failed: %s", ticker, e)
        return StageResult("collect_github", ticker, success=False, error=str(e))


async def exec_collect_analysts(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker
    logger.info("[%s] collect_analysts starting", ticker)
    try:
        from scripts.collect_evidence import collect_analysts

        await asyncio.to_thread(collect_analysts, [company], force=force)
        logger.info("[%s] collect_analysts done", ticker)
        return StageResult("collect_analysts", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] collect_analysts failed: %s", ticker, e)
        return StageResult("collect_analysts", ticker, success=False, error=str(e))


async def exec_collect_web_enrichment(
    company: CompanyModel, force: bool,
) -> StageResult:
    ticker = company.ticker
    logger.info("[%s] collect_web_enrichment starting", ticker)
    try:
        from scripts.collect_evidence import collect_web_enrichment

        await asyncio.to_thread(collect_web_enrichment, [company], force=force)
        logger.info("[%s] collect_web_enrichment done", ticker)
        return StageResult("collect_web_enrichment", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] collect_web_enrichment failed: %s", ticker, e)
        return StageResult("collect_web_enrichment", ticker, success=False, error=str(e))


async def exec_extract_filings(
    company: CompanyModel,
    force: bool,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> StageResult:
    ticker = company.ticker
    logger.info("[%s] extract_filings starting", ticker)
    try:
        from ai_opportunity_index.data.filing_extraction import extract_and_cache_filings

        name = company.company_name or ticker
        await extract_and_cache_filings(
            ticker, company_name=name,
            sector=company.sector or "",
            force=force,
            semaphore=llm_semaphore,
        )
        logger.info("[%s] extract_filings done", ticker)
        return StageResult("extract_filings", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] extract_filings failed: %s", ticker, e)
        return StageResult("extract_filings", ticker, success=False, error=str(e))


async def exec_extract_news(
    company: CompanyModel,
    force: bool,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> StageResult:
    ticker = company.ticker
    logger.info("[%s] extract_news starting", ticker)
    try:
        from ai_opportunity_index.data.news_extraction import extract_and_cache_news

        name = company.company_name or ticker
        await extract_and_cache_news(
            ticker, company_name=name,
            force=force,
            semaphore=llm_semaphore,
        )
        logger.info("[%s] extract_news done", ticker)
        return StageResult("extract_news", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] extract_news failed: %s", ticker, e)
        return StageResult("extract_news", ticker, success=False, error=str(e))


async def exec_value_evidence(
    company: CompanyModel,
    force: bool,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> StageResult:
    ticker = company.ticker
    logger.info("[%s] value_evidence starting", ticker)
    try:
        from ai_opportunity_index.scoring.evidence_valuation import value_evidence_for_company
        from ai_opportunity_index.storage.db import get_latest_financials

        financials = get_latest_financials(company.id)
        revenue_obs = financials.get("revenue")
        revenue = revenue_obs.value if revenue_obs else 0.0

        result = await value_evidence_for_company(
            ticker=ticker,
            company_id=company.id,
            company_name=company.company_name or ticker,
            sector=company.sector or "",
            revenue=revenue,
            llm_semaphore=llm_semaphore,
        )
        if result:
            logger.info(
                "[%s] value_evidence done: %d groups, cost=%.4f rev=%.4f gen=%.4f",
                ticker, result["total_groups"],
                result["cost_score"], result["revenue_score"], result["general_score"],
            )
        else:
            logger.info("[%s] value_evidence done: no evidence to value", ticker)
        return StageResult("value_evidence", ticker, success=True)
    except Exception as e:
        logger.warning("[%s] value_evidence failed: %s", ticker, e)
        return StageResult("value_evidence", ticker, success=False, error=str(e))


async def exec_score(
    company: CompanyModel,
    pipeline_run_id: int,
    llm_semaphore: asyncio.Semaphore | None = None,
) -> StageResult:
    ticker = company.ticker
    logger.info("[%s] score starting", ticker)
    try:
        from scripts.score_companies import score_and_save_company_async

        session = get_session()
        try:
            now = datetime.utcnow()
            success = await score_and_save_company_async(
                company, session, pipeline_run_id, now,
                llm_semaphore=llm_semaphore,
            )
            session.commit()
            logger.info("[%s] score done", ticker)
            return StageResult("score", ticker, success=success)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    except Exception as e:
        logger.warning("[%s] score failed: %s", ticker, e)
        return StageResult("score", ticker, success=False, error=str(e))


STAGE_EXECUTORS = {
    "discover_links": exec_discover_links,
    "collect_news": exec_collect_news,
    "collect_github": exec_collect_github,
    "collect_analysts": exec_collect_analysts,
    "collect_web_enrichment": exec_collect_web_enrichment,
    "extract_filings": exec_extract_filings,
    "extract_news": exec_extract_news,
    "value_evidence": exec_value_evidence,
    "score": exec_score,
}


# ── Per-Company DAG Execution ─────────────────────────────────────────────


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
    ticker = company.ticker
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

        # Execute the stage — per-stage force
        stage_force = stage in force_stages
        if stage == "score":
            result = await exec_score(company, pipeline_run_id, llm_semaphore=llm_semaphore)
        elif stage in ("extract_filings", "extract_news", "value_evidence"):
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


# ── Multi-Company Pipeline ────────────────────────────────────────────────


async def run_pipeline(
    companies: list[CompanyModel],
    stages: set[str],
    force_stages: set[str] | None = None,
    max_concurrency: int = 5,
    pipeline_run_id: int | None = None,
    llm_concurrency: int = 50,
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
    import concurrent.futures
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
        run_type = "partial" if len(companies) < 100 else "full"
        pr = create_pipeline_run(PipelineRun(
            run_id=run_id,
            task=PipelineTask.COLLECT,
            subtask=PipelineSubtask.ALL,
            run_type=run_type,
            status="running",
            tickers_requested=ticker_list,
            parameters={
                "stages": sorted(stages),
                "force_stages": sorted(force_stages),
                "max_concurrency": max_concurrency,
                "source": "run_pipeline",
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
        status="completed" if failed_count == 0 else "completed",
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


# ── CLI ───────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Run per-company DAG pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Stage aliases:\n"
            "  all      All stages (default)\n"
            "  collect  All collection stages (discover_links + collect_*)\n"
            "  extract  All extraction stages (extract_filings + extract_news)\n"
            "  value    Evidence valuation (value_evidence)\n"
            "  score    Just scoring\n"
            "\n"
            "Individual stages:\n"
            "  discover_links, collect_news, collect_github,\n"
            "  collect_analysts, collect_web_enrichment,\n"
            "  extract_filings, extract_news, value_evidence, score\n"
            "\n"
            "Force examples:\n"
            "  --force                  Force ALL stages\n"
            "  --force extract_news     Force only news extraction\n"
            "  --force extract          Force extract_filings + extract_news\n"
            "  --force extract value    Force multiple stage groups\n"
            "\n"
            "By default --stages only runs the listed stages (no deps).\n"
            "Use --with-deps to auto-include transitive dependencies."
        ),
    )
    parser.add_argument("--tickers", nargs="*", help="Specific tickers to process")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of companies")
    parser.add_argument(
        "--stages", nargs="*", default=["all"],
        help="Stages to run (default: all). Aliases: all, collect, score",
    )
    parser.add_argument(
        "--concurrency", type=int, default=20,
        help="Max concurrent companies in-flight (default: 20)",
    )
    parser.add_argument(
        "--llm-concurrency", type=int, default=50,
        help="Max concurrent LLM API calls across all companies (default: 50)",
    )
    parser.add_argument(
        "--force", nargs="*", default=None,
        help="Force-refresh stages. Bare --force = all stages. "
             "--force extract_news = only news extraction. "
             "Accepts stage names and aliases (collect, extract, value).",
    )
    parser.add_argument(
        "--with-deps", action="store_true",
        help="Also pull in transitive DAG dependencies for --stages",
    )
    args = parser.parse_args()

    # Resolve stages
    try:
        stages = resolve_stages(args.stages, with_deps=args.with_deps)
    except ValueError as e:
        parser.error(str(e))

    # Resolve force stages
    try:
        force_stages = parse_force_stages(args.force)
    except ValueError as e:
        parser.error(str(e))

    init_db()
    session = get_session()

    # Get companies
    query = session.query(CompanyModel).filter(CompanyModel.is_active.is_(True))
    if args.tickers:
        query = query.filter(CompanyModel.ticker.in_([t.upper() for t in args.tickers]))
    if args.limit:
        query = query.limit(args.limit)
    companies = query.all()
    session.close()

    if not companies:
        logger.warning("No companies found matching the criteria")
        return

    logger.info("Found %d companies to process", len(companies))

    # Run the pipeline
    results = asyncio.run(
        run_pipeline(
            companies,
            stages=stages,
            force_stages=force_stages,
            max_concurrency=args.concurrency,
            llm_concurrency=args.llm_concurrency,
        )
    )

    # Exit with error code if any stages failed
    failures = [r for r in results if not r.success and not r.skipped]
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
