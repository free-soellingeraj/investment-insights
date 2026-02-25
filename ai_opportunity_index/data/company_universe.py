"""Build the master list of publicly traded companies from SEC EDGAR."""

import json
import logging
import time

import pandas as pd
import requests

from ai_opportunity_index.config import (
    RAW_DIR,
    SEC_COMPANY_TICKERS_URL,
    SEC_COMPANY_TICKERS_EXCHANGE_URL,
    SEC_RATE_LIMIT_SECONDS,
    SEC_USER_AGENT,
)

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": SEC_USER_AGENT}


def fetch_sec_company_tickers() -> pd.DataFrame:
    """Download the SEC EDGAR company tickers JSON and return as DataFrame.

    Returns DataFrame with columns: cik, ticker, company_name.
    """
    logger.info("Fetching SEC EDGAR company tickers from %s", SEC_COMPANY_TICKERS_URL)
    resp = requests.get(SEC_COMPANY_TICKERS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    records = []
    for _idx, entry in data.items():
        records.append(
            {
                "cik": int(entry["cik_str"]),
                "ticker": str(entry["ticker"]).upper().strip(),
                "company_name": str(entry["title"]).strip(),
            }
        )

    df = pd.DataFrame(records)
    logger.info("Retrieved %d companies from SEC EDGAR", len(df))
    return df


def fetch_sec_company_tickers_with_exchange() -> pd.DataFrame:
    """Download the extended SEC tickers file that includes exchange and SIC.

    Returns DataFrame with columns: cik, ticker, company_name, exchange, sic.
    """
    logger.info("Fetching SEC EDGAR company tickers (with exchange) from %s",
                SEC_COMPANY_TICKERS_EXCHANGE_URL)
    resp = requests.get(SEC_COMPANY_TICKERS_EXCHANGE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # This endpoint returns {"fields": [...], "data": [[...], ...]}
    fields = data.get("fields", [])
    rows = data.get("data", [])

    df = pd.DataFrame(rows, columns=fields)
    df.columns = [c.lower().strip() for c in df.columns]

    # Normalize column names to our standard
    rename_map = {}
    for col in df.columns:
        if "cik" in col:
            rename_map[col] = "cik"
        elif "ticker" in col or "symbol" in col:
            rename_map[col] = "ticker"
        elif "name" in col or "title" in col:
            rename_map[col] = "company_name"
        elif "exchange" in col:
            rename_map[col] = "exchange"
        elif "sic" in col:
            rename_map[col] = "sic"

    df = df.rename(columns=rename_map)
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    if "cik" in df.columns:
        df["cik"] = df["cik"].astype(int)

    logger.info("Retrieved %d companies (with exchange info) from SEC EDGAR", len(df))
    return df


def build_universe(save: bool = True) -> pd.DataFrame:
    """Build the company universe from SEC EDGAR data.

    Tries the extended endpoint first (includes exchange/SIC), falls back
    to the basic endpoint.
    """
    try:
        df = fetch_sec_company_tickers_with_exchange()
    except Exception:
        logger.warning("Extended tickers endpoint failed, falling back to basic")
        df = fetch_sec_company_tickers()

    # Deduplicate by ticker, keeping the first occurrence
    df = df.drop_duplicates(subset="ticker", keep="first").reset_index(drop=True)

    # Filter to non-empty tickers
    df = df[df["ticker"].str.len() > 0].reset_index(drop=True)

    if save:
        out_path = RAW_DIR / "company_universe.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        logger.info("Saved company universe to %s (%d companies)", out_path, len(df))

    return df


def load_universe() -> pd.DataFrame:
    """Load the previously-built company universe from disk."""
    path = RAW_DIR / "company_universe.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Company universe not found at {path}. Run build_universe() first."
        )
    return pd.read_csv(path)
