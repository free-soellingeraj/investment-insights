#!/usr/bin/env python3
"""Step 2: Fetch financial fundamentals and SEC filings.

Pulls Yahoo Finance data and SEC EDGAR filings for companies in the universe.
"""

import argparse
import logging
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime

from ai_opportunity_index.data.company_universe import load_universe
from ai_opportunity_index.data.financial_data import fetch_bulk_financials
from ai_opportunity_index.data.sec_edgar import fetch_and_cache_filings
from ai_opportunity_index.domains import FinancialObservation
from ai_opportunity_index.storage.db import (
    get_company_by_ticker,
    get_session,
    init_db,
    save_financial_observations_batch,
    upsert_company,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Fetch financial data and filings")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of companies to fetch")
    parser.add_argument("--skip-financials", action="store_true", help="Skip Yahoo Finance data")
    parser.add_argument("--skip-filings", action="store_true", help="Skip SEC filings")
    parser.add_argument("--filing-type", default="10-K", help="Filing type to fetch (default: 10-K)")
    args = parser.parse_args()

    init_db()
    universe = load_universe()

    if args.limit:
        universe = universe.head(args.limit)
        logger.info("Limited to %d companies", len(universe))

    # Fetch Yahoo Finance fundamentals
    if not args.skip_financials:
        logger.info("=== Fetching Yahoo Finance fundamentals ===")
        tickers = universe["ticker"].tolist()
        financials_df = fetch_bulk_financials(tickers)
        logger.info("Fetched financials for %d companies", len(financials_df))

        # Update database with financial data
        now = datetime.utcnow()
        for _, row in financials_df.iterrows():
            data = row.to_dict()
            ticker = data.get("ticker")
            if not ticker:
                continue

            # Upsert non-financial company fields (filter out NaN from pandas)
            non_financial = {
                k: v for k, v in data.items()
                if v is not None and k in ("ticker", "sector", "industry", "country", "exchange", "company_name", "cik", "sic", "naics")
                and not (isinstance(v, float) and math.isnan(v))
            }
            if non_financial.get("ticker"):
                upsert_company(non_financial)

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
                if value is not None and not (isinstance(value, float) and math.isnan(value)):
                    obs_items.append(FinancialObservation(
                        company_id=company.id,
                        metric=metric,
                        value=float(value),
                        value_units=units,
                        source_datetime=now,
                        source_name="yahoo_finance",
                    ))
            if obs_items:
                save_financial_observations_batch(obs_items)

    # Fetch SEC filings
    if not args.skip_filings:
        logger.info("=== Fetching SEC filings ===")
        cik_tickers = universe[["cik", "ticker"]].dropna(subset=["cik"])

        for i, (_, row) in enumerate(cik_tickers.iterrows()):
            try:
                paths = fetch_and_cache_filings(
                    cik=int(row["cik"]),
                    ticker=row["ticker"],
                    filing_type=args.filing_type,
                    count=1,
                )
                if paths:
                    logger.debug("Fetched %d filings for %s", len(paths), row["ticker"])
            except Exception as e:
                logger.warning("Failed to fetch filings for %s: %s", row["ticker"], e)

            if (i + 1) % 100 == 0:
                logger.info("Filing fetch progress: %d/%d", i + 1, len(cik_tickers))

    logger.info("=== Data fetch complete ===")


if __name__ == "__main__":
    main()
