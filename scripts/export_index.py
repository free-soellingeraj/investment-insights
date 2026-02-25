#!/usr/bin/env python3
"""Step 4: Export the AI Opportunity Index to CSV or Parquet."""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_opportunity_index.export.csv_export import export_index
from ai_opportunity_index.storage.db import init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Export the AI Opportunity Index")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv")
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--sector", type=str, default=None)
    parser.add_argument("--exchange", type=str, default=None)
    parser.add_argument("--quadrant", type=str, default=None)
    parser.add_argument("--min-market-cap", type=float, default=None)
    parser.add_argument("--max-market-cap", type=float, default=None)
    parser.add_argument("--min-opportunity", type=float, default=None)
    parser.add_argument("--min-realization", type=float, default=None)
    args = parser.parse_args()

    init_db()

    filters = {}
    if args.sector:
        filters["sector"] = args.sector
    if args.exchange:
        filters["exchange"] = args.exchange
    if args.quadrant:
        filters["quadrant"] = args.quadrant
    if args.min_market_cap is not None:
        filters["min_market_cap"] = args.min_market_cap
    if args.max_market_cap is not None:
        filters["max_market_cap"] = args.max_market_cap
    if args.min_opportunity is not None:
        filters["min_opportunity"] = args.min_opportunity
    if args.min_realization is not None:
        filters["min_realization"] = args.min_realization

    output_dir = Path(args.output_dir) if args.output_dir else None
    filepath = export_index(output_dir=output_dir, format=args.format, filters=filters or None)

    logger.info("Export complete: %s", filepath)


if __name__ == "__main__":
    main()
