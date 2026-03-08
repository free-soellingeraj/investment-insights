#!/usr/bin/env python3
"""Per-company DAG pipeline for the AI Opportunity Index.

Thin CLI wrapper that delegates to the unified PipelineController.

Usage:
    python scripts/run_pipeline.py --tickers AAPL MSFT
    python scripts/run_pipeline.py --stages collect --tickers AAPL
    python scripts/run_pipeline.py --stages score --tickers AAPL
    python scripts/run_pipeline.py --concurrency 10 --limit 100
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_opportunity_index.pipeline import (
    PipelineController,
    PipelineRequest,
    TriggerSource,
    resolve_stages,
    parse_force_stages,
)
from ai_opportunity_index.storage.db import get_session, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


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
            "  extract_filings, extract_news, extract_unified,\n"
            "  value_evidence, score\n"
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
    parser.add_argument(
        "--run-id",
        help="Reuse an existing pipeline_run record (created by web UI) instead of creating a new one",
    )
    parser.add_argument(
        "--include-inactive", action="store_true",
        help="Include inactive companies (used by web UI for explicit single-company runs)",
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

    # Look up existing pipeline_run if --run-id provided
    existing_run_id = None
    if args.run_id:
        init_db()
        from ai_opportunity_index.storage.models import PipelineRunModel
        s = get_session()
        pr_row = s.query(PipelineRunModel).filter(PipelineRunModel.run_id == args.run_id).first()
        if pr_row:
            existing_run_id = pr_row.id
        s.close()

    request = PipelineRequest(
        tickers=args.tickers,
        stages=stages,
        force_stages=force_stages,
        source=TriggerSource.CLI,
        max_concurrency=args.concurrency,
        llm_concurrency=args.llm_concurrency,
        include_inactive=args.include_inactive,
        pipeline_run_id=existing_run_id,
        limit=args.limit,
    )

    results = PipelineController.run_sync(request)

    # Exit with error code if any stages failed
    failures = [r for r in results if not r.success and not r.skipped]
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
