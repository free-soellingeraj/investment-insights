#!/usr/bin/env python3
"""Compute index history for all 3 portfolio variants and save results."""

import json
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from ai_opportunity_index.config import PROCESSED_DIR
from ai_opportunity_index.index_computation.portfolio import (
    build_index_variants,
    build_investable_universe,
    compute_index_history,
    compute_performance_metrics,
)
from ai_opportunity_index.storage.db import init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    init_db()

    # Step 1: Build investable universe
    logger.info("Building investable universe...")
    universe = build_investable_universe()
    if universe.empty:
        logger.error("No companies in investable universe. Run scoring pipeline first.")
        sys.exit(1)

    logger.info("Investable universe: %d companies", len(universe))

    # Step 2: Build index variants
    logger.info("Building index variants...")
    variants = build_index_variants(universe)

    # Step 3: Compute history + metrics for each variant
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    all_metrics = {}
    top_holdings = {}

    for variant_name, variant_df in variants.items():
        logger.info("Computing history for variant: %s", variant_name)

        tickers = variant_df["ticker"].tolist()
        weights = variant_df["weight"].tolist()

        history = compute_index_history(tickers, weights)

        if history.empty:
            logger.warning("No history data for variant %s, skipping", variant_name)
            continue

        # Save history CSV
        csv_path = PROCESSED_DIR / f"index_history_{variant_name}.csv"
        history.to_csv(csv_path, index=False)
        logger.info("Saved %s (%d rows)", csv_path, len(history))

        # Compute metrics
        metrics = compute_performance_metrics(history)
        all_metrics[variant_name] = metrics
        logger.info("Metrics for %s: return=%.2f%%, sharpe=%.2f", variant_name, metrics.get("annualized_return", 0) * 100, metrics.get("sharpe_ratio", 0))

        # Top holdings
        holdings = variant_df.head(10)[["ticker", "company_name", "sector", "composite", "weight"]].to_dict("records")
        top_holdings[variant_name] = holdings

    # Save metrics JSON
    metrics_path = PROCESSED_DIR / "index_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(all_metrics, f, indent=2)
    logger.info("Saved metrics to %s", metrics_path)

    # Save top holdings JSON
    holdings_path = PROCESSED_DIR / "top_holdings.json"
    with open(holdings_path, "w") as f:
        json.dump(top_holdings, f, indent=2)
    logger.info("Saved top holdings to %s", holdings_path)

    logger.info("Done! All variants computed successfully.")


if __name__ == "__main__":
    main()
