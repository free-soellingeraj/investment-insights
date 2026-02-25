#!/usr/bin/env python3
"""Step 1: Build the company universe from SEC EDGAR.

Downloads the full list of publicly traded US companies with tickers,
SIC codes, and exchange information, then stores them in the database.
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_opportunity_index.data.company_universe import build_universe
from ai_opportunity_index.storage.db import init_db, upsert_companies_bulk

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("=== Building Company Universe ===")

    # Initialize database
    init_db()

    # Build universe from SEC EDGAR
    df = build_universe(save=True)
    logger.info("Universe contains %d companies", len(df))

    # Preview
    logger.info("Sample tickers: %s", df["ticker"].head(10).tolist())

    if "sic" in df.columns:
        sic_coverage = df["sic"].notna().sum()
        logger.info("Companies with SIC codes: %d (%.1f%%)", sic_coverage, 100 * sic_coverage / len(df))

    if "exchange" in df.columns:
        logger.info("Exchange distribution:\n%s", df["exchange"].value_counts().head(10))

    # Store in database
    upsert_companies_bulk(df)

    logger.info("=== Universe build complete: %d companies ===", len(df))
    return df


if __name__ == "__main__":
    main()
