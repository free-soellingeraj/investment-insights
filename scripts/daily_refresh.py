#!/usr/bin/env python3
"""Daily incremental refresh for the AI Opportunity Index.

Two-phase orchestration:
  Phase 1: Incremental collection (cheap API calls, all companies)
    1. Determine since_date from last completed run (source-agnostic watermark)
    2. Get all active companies
    3. Run all collectors with since_date -- new items -> new files
    4. Track which companies got new files per source

  Phase 2: Process affected companies (expensive LLM, targeted)
    5. Filter to companies that got new collected items
    6. Run unified extraction (skips already-extracted items)
    7. Run value_evidence + score for companies with new extracted items

Usage:
    python scripts/daily_refresh.py
    python scripts/daily_refresh.py --dry-run
    python scripts/daily_refresh.py --fallback-days 3
    python scripts/daily_refresh.py --concurrency 10 --llm-concurrency 30
"""

import argparse
import asyncio
import logging
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_opportunity_index.config import SOURCES_DIR
from ai_opportunity_index.domains import (
    PipelineRun,
    PipelineSubtask,
    PipelineTask,
    RunStatus,
    RunType,
    SourceType,
)
from ai_opportunity_index.pipeline import PipelineController, PipelineRequest, TriggerSource
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


def _count_new_items(ticker: str, since: datetime) -> dict[str, int]:
    """Count new collected items for a ticker since a given time.

    Checks file modification times in the sources directory.
    """
    counts: dict[str, int] = {}
    ticker_dir = SOURCES_DIR / ticker.upper()
    if not ticker_dir.exists():
        return counts

    since_ts = since.timestamp()
    for st in SourceType:
        type_dir = ticker_dir / st.value
        if not type_dir.is_dir():
            continue
        new_count = 0
        for p in type_dir.rglob("*.json"):
            if p.name.startswith("_"):
                continue
            if p.stat().st_mtime >= since_ts:
                new_count += 1
        if new_count:
            counts[st.value] = new_count
    return counts


async def daily_refresh(
    fallback_days: int = 2,
    dry_run: bool = False,
    max_concurrency: int = 20,
    llm_concurrency: int = 50,
):
    """Run the two-phase daily refresh."""
    init_db()

    # ── Phase 0: Determine since_date (source-agnostic watermark) ─────
    last_refresh = PipelineController.get_last_completed_run()
    if last_refresh:
        since_date = last_refresh
        logger.info("Last completed pipeline run: %s", last_refresh.isoformat())
    else:
        since_date = datetime.utcnow() - timedelta(days=fallback_days)
        logger.info("No previous completed run found; using fallback: %d days ago", fallback_days)

    logger.info("Collecting data since: %s", since_date.isoformat())

    # ── Phase 0b: Get companies ────────────────────────────────────────
    session = get_session()
    companies = session.query(CompanyModel).filter(CompanyModel.is_active.is_(True)).all()
    session.close()
    logger.info("Found %d active companies", len(companies))

    if dry_run:
        logger.info("[DRY RUN] Would collect for %d companies since %s", len(companies), since_date)
        return

    # ── Create pipeline run ────────────────────────────────────────────
    run_id = str(uuid.uuid4())
    ticker_list = [c.ticker or c.slug for c in companies]
    pr = create_pipeline_run(PipelineRun(
        run_id=run_id,
        task=PipelineTask.COLLECT,
        subtask=PipelineSubtask.ALL,
        run_type=RunType.FULL,
        status=RunStatus.RUNNING,
        tickers_requested=ticker_list,
        parameters={
            "source": "daily_refresh",
            "since_date": since_date.isoformat(),
            "fallback_days": fallback_days,
            "stages_requested": ["collect_news", "collect_github", "collect_analysts",
                                 "collect_web_enrichment", "extract_unified",
                                 "value_evidence", "score"],
        },
    ))
    pipeline_run_id = pr.id

    try:
        # ── Phase 1: Incremental collection ────────────────────────────
        logger.info("=== Phase 1: Incremental Collection ===")
        collection_start = datetime.utcnow()

        from scripts.collect_evidence import (
            collect_analysts,
            collect_github,
            collect_news,
            collect_web_enrichment,
        )

        # Run collectors with since_date (news, github, analysts benefit)
        collect_news(companies, force=False, since_date=since_date)
        collect_github(companies, force=False, since_date=since_date)
        collect_analysts(companies, force=False, since_date=since_date)
        collect_web_enrichment(companies, force=False)

        # ── Phase 1b: Identify affected companies ─────────────────────
        affected: dict[str, dict[str, int]] = {}
        for company in companies:
            ticker = company.ticker or company.slug
            counts = _count_new_items(ticker, collection_start)
            if counts:
                affected[ticker] = counts

        logger.info(
            "Phase 1 complete: %d/%d companies have new data",
            len(affected), len(companies),
        )
        if not affected:
            logger.info("No new data collected; skipping Phase 2")
            complete_pipeline_run(
                run_id=run_id,
                status=RunStatus.COMPLETED,
                tickers_succeeded=len(companies),
            )
            return

        for ticker, counts in sorted(affected.items()):
            logger.info("  %s: %s", ticker, counts)

        # ── Phase 2: Process affected companies ────────────────────────
        logger.info("=== Phase 2: Extract + Value + Score ===")

        affected_tickers = set(affected.keys())
        request = PipelineRequest(
            tickers=list(affected_tickers),
            stages={"extract_unified", "value_evidence", "score"},
            force_stages={"extract_unified"},  # re-extract new items
            source=TriggerSource.DAILY_REFRESH,
            max_concurrency=max_concurrency,
            llm_concurrency=llm_concurrency,
            pipeline_run_id=pipeline_run_id,
        )

        results = await PipelineController.run(request)

        succeeded = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success and not r.skipped)

        complete_pipeline_run(
            run_id=run_id,
            status=RunStatus.COMPLETED,
            tickers_succeeded=len(affected_tickers),
            tickers_failed=failed,
        )

        logger.info(
            "Daily refresh complete: %d companies processed, %d stage results (%d ok, %d fail)",
            len(affected_tickers), len(results), succeeded, failed,
        )

    except Exception as e:
        complete_pipeline_run(
            run_id=run_id,
            status=RunStatus.FAILED,
            error_message=str(e),
        )
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Daily incremental refresh for the AI Opportunity Index",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Only show what would be done, don't actually collect or process",
    )
    parser.add_argument(
        "--fallback-days", type=int, default=2,
        help="Days to look back if no previous daily refresh exists (default: 2)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=20,
        help="Max concurrent companies in Phase 2 (default: 20)",
    )
    parser.add_argument(
        "--llm-concurrency", type=int, default=50,
        help="Max concurrent LLM API calls (default: 50)",
    )
    args = parser.parse_args()

    asyncio.run(
        daily_refresh(
            fallback_days=args.fallback_days,
            dry_run=args.dry_run,
            max_concurrency=args.concurrency,
            llm_concurrency=args.llm_concurrency,
        )
    )


if __name__ == "__main__":
    main()
