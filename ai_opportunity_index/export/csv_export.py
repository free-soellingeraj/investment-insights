"""CSV and Parquet export of the AI Opportunity Index."""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from ai_opportunity_index.config import PROCESSED_DIR
from ai_opportunity_index.storage.db import get_full_index

logger = logging.getLogger(__name__)


def export_index(
    output_dir: Path | None = None,
    format: str = "csv",
    filters: dict | None = None,
) -> Path:
    """Export the full index to CSV or Parquet.

    Args:
        output_dir: Output directory. Defaults to data/processed/.
        format: "csv" or "parquet".
        filters: Optional dict of filters:
            - country: str
            - exchange: str
            - sector: str
            - quadrant: str
            - min_opportunity: float
            - min_realization: float

    Returns:
        Path to the exported file.
    """
    if output_dir is None:
        output_dir = PROCESSED_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = get_full_index()

    if df.empty:
        logger.warning("No index data to export")
        return output_dir / "empty_export.csv"

    # Apply filters
    if filters:
        df = _apply_filters(df, filters)

    # Add export metadata
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df["export_timestamp"] = timestamp

    if format == "parquet":
        filename = f"ai_opportunity_index_{timestamp}.parquet"
        filepath = output_dir / filename
        df.to_parquet(filepath, index=False)
    else:
        filename = f"ai_opportunity_index_{timestamp}.csv"
        filepath = output_dir / filename
        df.to_csv(filepath, index=False)

    logger.info("Exported %d companies to %s", len(df), filepath)
    return filepath


def _apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """Apply filters to the index DataFrame."""
    if "country" in filters and filters["country"]:
        # Country filtering would need the column in the join
        pass

    if "exchange" in filters and filters["exchange"]:
        df = df[df["exchange"] == filters["exchange"]]

    if "sector" in filters and filters["sector"]:
        df = df[df["sector"] == filters["sector"]]

    if "quadrant" in filters and filters["quadrant"]:
        df = df[df["quadrant"] == filters["quadrant"]]

    if "min_opportunity" in filters and filters["min_opportunity"] is not None:
        df = df[df["opportunity"] >= filters["min_opportunity"]]

    if "min_realization" in filters and filters["min_realization"] is not None:
        df = df[df["realization"] >= filters["min_realization"]]

    return df


def export_company_details(ticker: str, output_dir: Path | None = None) -> Path:
    """Export detailed data for a single company."""
    from ai_opportunity_index.storage.db import get_company_detail

    if output_dir is None:
        output_dir = PROCESSED_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    detail = get_company_detail(ticker)
    if not detail:
        raise ValueError(f"Company {ticker} not found in index")

    filepath = output_dir / f"{ticker}_detail.json"
    import json
    filepath.write_text(json.dumps(detail, indent=2, default=str))

    return filepath
