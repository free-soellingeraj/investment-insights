"""Fetch financial fundamentals via Yahoo Finance."""

import logging
import time

import pandas as pd

from ai_opportunity_index.config import YF_RATE_LIMIT_SECONDS, RAW_DIR

logger = logging.getLogger(__name__)


def fetch_company_financials(ticker: str) -> dict | None:
    """Fetch key financial metrics for a single ticker.

    Returns dict with: market_cap, revenue, net_income, employees,
    sector, industry, or None on failure.
    """
    import yfinance as yf

    time.sleep(YF_RATE_LIMIT_SECONDS)

    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        return {
            "ticker": ticker,
            "market_cap": info.get("marketCap"),
            "revenue": info.get("totalRevenue"),
            "net_income": info.get("netIncomeToCommon"),
            "employees": info.get("fullTimeEmployees"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "country": info.get("country"),
            "trailing_pe": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
        }
    except Exception as e:
        logger.warning("Failed to fetch financials for %s: %s", ticker, e)
        return None


def fetch_bulk_financials(tickers: list[str], batch_size: int = 50) -> pd.DataFrame:
    """Fetch financials for a list of tickers.

    Args:
        tickers: List of ticker symbols.
        batch_size: Number of tickers to process before saving progress.

    Returns:
        DataFrame with financial data for all successfully fetched tickers.
    """
    results = []
    cache_path = RAW_DIR / "financials_cache.csv"

    # Load cache if exists
    cached = set()
    if cache_path.exists():
        cached_df = pd.read_csv(cache_path)
        results = cached_df.to_dict("records")
        cached = set(cached_df["ticker"].values)
        logger.info("Loaded %d cached financial records", len(cached))

    remaining = [t for t in tickers if t not in cached]
    logger.info("Fetching financials for %d tickers (%d cached)", len(remaining), len(cached))

    for i, ticker in enumerate(remaining):
        data = fetch_company_financials(ticker)
        if data:
            results.append(data)

        if (i + 1) % batch_size == 0:
            df = pd.DataFrame(results)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(cache_path, index=False)
            logger.info("Progress: %d/%d tickers fetched", i + 1, len(remaining))

    df = pd.DataFrame(results)
    if not df.empty:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(cache_path, index=False)

    return df
