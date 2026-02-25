#!/usr/bin/env python3
"""Data collection script with cost tracking.

Phase 1: Basic info (Yahoo Finance fundamentals) for all companies
Phase 2: Full 3-year filing data (10-K, 10-Q) for top N companies
"""

import argparse
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from ai_opportunity_index.config import RAW_DIR
from ai_opportunity_index.cost_tracker import CostTracker
from ai_opportunity_index.data.company_universe import load_universe
from ai_opportunity_index.data.financial_data import fetch_company_financials
from ai_opportunity_index.data.sec_edgar import fetch_and_cache_filings
from ai_opportunity_index.domains import FinancialObservation, PipelineRun, PipelineSubtask, PipelineTask
from ai_opportunity_index.storage.db import (
    complete_pipeline_run,
    create_pipeline_run,
    get_company_by_ticker,
    init_db,
    save_financial_observations_batch,
    upsert_company,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_basic_info(tracker: CostTracker, limit: int | None = None):
    """Fetch Yahoo Finance fundamentals for all companies and save as FinancialObservations."""
    universe = load_universe()
    tickers = universe["ticker"].tolist()

    if limit:
        tickers = tickers[:limit]
        logger.info("Limited to %d companies", len(tickers))

    # Load cache
    cache_path = RAW_DIR / "financials_cache.csv"
    cached = set()
    cached_records = []
    if cache_path.exists():
        cached_df = pd.read_csv(cache_path)
        cached_records = cached_df.to_dict("records")
        cached = set(cached_df["ticker"].values)
        logger.info("Loaded %d cached financial records", len(cached))

    remaining = [t for t in tickers if t not in cached]
    logger.info("Need to fetch %d tickers (%d already cached)", len(remaining), len(cached))

    # First, save cached records to DB if they haven't been saved yet
    tracker.start_timer("save_cached_to_db")
    _save_financials_to_db(cached_records, tracker, "yahoo_finance")
    tracker.stop_timer("save_cached_to_db")

    # Fetch remaining
    tracker.start_timer("yahoo_finance_fetch")
    new_records = []
    db_flush_batch = []
    for i, ticker in enumerate(remaining):
        data = fetch_company_financials(ticker)
        tracker.record_call("yahoo_finance", error=(data is None))

        if data:
            new_records.append(data)
            cached_records.append(data)
            db_flush_batch.append(data)

        # Save cache every 50 tickers
        if (i + 1) % 50 == 0:
            df = pd.DataFrame(cached_records)
            df.to_csv(cache_path, index=False)
            logger.info(
                "Progress: %d/%d tickers fetched (%d new successes)",
                i + 1, len(remaining), len(new_records),
            )
            tracker.log_event("yahoo_finance_progress", {
                "fetched": i + 1,
                "total": len(remaining),
                "successes": len(new_records),
            })

        # Flush to DB every 250 tickers so status page updates live
        if (i + 1) % 250 == 0 and db_flush_batch:
            _save_financials_to_db(db_flush_batch, tracker, "yahoo_finance")
            db_flush_batch = []

    # Final cache save
    if new_records:
        df = pd.DataFrame(cached_records)
        df.to_csv(cache_path, index=False)

    tracker.stop_timer("yahoo_finance_fetch")

    # Save any remaining records to DB
    if db_flush_batch:
        tracker.start_timer("save_new_to_db")
        _save_financials_to_db(db_flush_batch, tracker, "yahoo_finance")
        tracker.stop_timer("save_new_to_db")

    logger.info(
        "Basic info complete: %d total cached, %d newly fetched",
        len(cached_records), len(new_records),
    )


def _save_financials_to_db(records: list[dict], tracker: CostTracker, source: str):
    """Save financial records as FinancialObservations + upsert company metadata."""
    now = datetime.utcnow()
    saved = 0
    for data in records:
        ticker = data.get("ticker")
        if not ticker:
            continue

        # Upsert non-financial company fields
        non_financial = {
            k: v for k, v in data.items()
            if v is not None and k in (
                "ticker", "sector", "industry", "country",
                "exchange", "company_name", "cik", "sic", "naics",
            )
        }
        if non_financial.get("ticker"):
            try:
                upsert_company(non_financial)
            except Exception as e:
                logger.debug("Upsert failed for %s: %s", ticker, e)

        # Save financial observations
        company = get_company_by_ticker(ticker)
        if not company or not company.id:
            continue

        metrics = {
            "market_cap": ("usd", data.get("market_cap")),
            "revenue": ("usd", data.get("revenue")),
            "net_income": ("usd", data.get("net_income")),
            "employees": ("count", data.get("employees")),
        }
        obs_items = []
        for metric, (units, value) in metrics.items():
            if value is not None:
                obs_items.append(FinancialObservation(
                    company_id=company.id,
                    metric=metric,
                    value=float(value),
                    value_units=units,
                    source_datetime=now,
                    source_name=source,
                ))
        if obs_items:
            try:
                save_financial_observations_batch(obs_items)
                saved += 1
            except Exception as e:
                logger.debug("Failed to save observations for %s: %s", ticker, e)

    logger.info("Saved financial observations for %d companies", saved)


def fetch_full_data(tracker: CostTracker, n_companies: int = 100, years: int = 3):
    """Fetch 3 years of 10-K and 10-Q filings for top N companies."""
    universe = load_universe()

    # Pick top companies by market cap from cache
    cache_path = RAW_DIR / "financials_cache.csv"
    if cache_path.exists():
        fin_df = pd.read_csv(cache_path)
        # Sort by market cap descending, take top N with valid CIK
        fin_with_cik = fin_df.merge(
            universe[["ticker", "cik"]].dropna(subset=["cik"]),
            on="ticker",
            how="inner",
        )
        fin_with_cik = fin_with_cik.dropna(subset=["market_cap"])
        fin_with_cik = fin_with_cik.sort_values("market_cap", ascending=False)
        selected = fin_with_cik.head(n_companies)
    else:
        # Fallback: take first N from universe with CIK
        selected = universe.dropna(subset=["cik"]).head(n_companies)

    logger.info(
        "Selected %d companies for full data collection (top by market cap)",
        len(selected),
    )
    if len(selected) > 0:
        logger.info(
            "Range: %s (largest) to %s (smallest in selection)",
            selected.iloc[0]["ticker"],
            selected.iloc[-1]["ticker"],
        )

    # Fetch filings
    filing_types = ["10-K", "10-Q"]
    # 10-K: 1 per year × 3 years = 3
    # 10-Q: 3 per year × 3 years = 9
    counts = {"10-K": years, "10-Q": years * 3}

    tracker.start_timer("sec_edgar_fetch")
    total_filings = 0
    total_bytes = 0

    for i, (_, row) in enumerate(selected.iterrows()):
        ticker = row["ticker"]
        cik = int(row["cik"])

        for filing_type in filing_types:
            try:
                paths = fetch_and_cache_filings(
                    cik=cik,
                    ticker=ticker,
                    filing_type=filing_type,
                    count=counts[filing_type],
                )
                for p in paths:
                    size = p.stat().st_size if p.exists() else 0
                    total_bytes += size
                    tracker.record_call("sec_edgar", bytes_size=size)
                total_filings += len(paths)
            except Exception as e:
                logger.warning("Failed to fetch %s for %s: %s", filing_type, ticker, e)
                tracker.record_call("sec_edgar", error=True)

        if (i + 1) % 10 == 0:
            logger.info(
                "Filing fetch progress: %d/%d companies, %d filings, %.1f MB",
                i + 1, len(selected), total_filings, total_bytes / (1024 * 1024),
            )
            tracker.log_event("sec_edgar_progress", {
                "companies": i + 1,
                "total_companies": len(selected),
                "filings": total_filings,
                "bytes": total_bytes,
            })

    tracker.stop_timer("sec_edgar_fetch")
    logger.info(
        "Full data collection complete: %d companies, %d filings, %.1f MB",
        len(selected), total_filings, total_bytes / (1024 * 1024),
    )


def main():
    parser = argparse.ArgumentParser(description="Collect data with cost tracking")
    parser.add_argument("--phase", choices=["basic", "full", "both"], default="both",
                        help="Which phase to run")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit number of companies for basic info")
    parser.add_argument("--n-companies", type=int, default=100,
                        help="Number of companies for full data collection")
    parser.add_argument("--years", type=int, default=3,
                        help="Years of filing history to fetch")
    args = parser.parse_args()

    init_db()
    tracker = CostTracker(
        run_name=f"data_collection_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    )
    tracker.log_event("started", {
        "phase": args.phase,
        "limit": args.limit,
        "n_companies": args.n_companies,
        "years": args.years,
    })

    try:
        if args.phase in ("basic", "both"):
            run_id = str(uuid.uuid4())
            create_pipeline_run(PipelineRun(
                run_id=run_id,
                task=PipelineTask.COLLECT,
                subtask=PipelineSubtask.YAHOO_FUNDAMENTALS,
                run_type="full",
                status="running",
                parameters={"limit": args.limit},
            ))
            try:
                logger.info("=== Phase 1: Fetching basic info for all companies ===")
                fetch_basic_info(tracker, limit=args.limit)
                complete_pipeline_run(run_id=run_id, status="completed")
            except Exception as exc:
                complete_pipeline_run(run_id=run_id, status="failed", error_message=str(exc))
                raise

        if args.phase in ("full", "both"):
            run_id = str(uuid.uuid4())
            create_pipeline_run(PipelineRun(
                run_id=run_id,
                task=PipelineTask.COLLECT,
                subtask=PipelineSubtask.SEC_FILINGS,
                run_type="full",
                status="running",
                parameters={"n_companies": args.n_companies, "years": args.years},
            ))
            try:
                logger.info("=== Phase 2: Fetching full 3-year data for %d companies ===",
                            args.n_companies)
                fetch_full_data(tracker, n_companies=args.n_companies, years=args.years)
                complete_pipeline_run(run_id=run_id, status="completed")
            except Exception as exc:
                complete_pipeline_run(run_id=run_id, status="failed", error_message=str(exc))
                raise

    finally:
        tracker.log_event("completed")
        tracker.save_summary()
        tracker.print_summary()


if __name__ == "__main__":
    main()
