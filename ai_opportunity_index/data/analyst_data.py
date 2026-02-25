"""Fetch analyst consensus data from Yahoo Finance.

Uses yfinance (already a project dependency) to extract analyst
recommendations, price targets, and earnings estimates.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

from ai_opportunity_index.config import RAW_DIR, YF_RATE_LIMIT_SECONDS

logger = logging.getLogger(__name__)


def fetch_analyst_data(ticker: str) -> dict:
    """Fetch analyst consensus data for a single ticker.

    Returns dict with:
    - recommendation_mean (1=strong buy, 5=strong sell)
    - recommendation_key (e.g. "buy", "hold")
    - target_mean_price, target_low_price, target_high_price
    - number_of_analysts
    - earnings_estimate_next_quarter
    - revenue_estimate_next_quarter
    """
    import yfinance as yf

    time.sleep(YF_RATE_LIMIT_SECONDS)

    result = {
        "ticker": ticker,
        "collected_at": datetime.utcnow().isoformat(),
        "recommendation_mean": None,
        "recommendation_key": None,
        "target_mean_price": None,
        "target_low_price": None,
        "target_high_price": None,
        "number_of_analysts": None,
        "current_price": None,
        "earnings_estimate_next_quarter": None,
        "revenue_estimate_next_quarter": None,
    }

    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        result["recommendation_mean"] = info.get("recommendationMean")
        result["recommendation_key"] = info.get("recommendationKey")
        result["target_mean_price"] = info.get("targetMeanPrice")
        result["target_low_price"] = info.get("targetLowPrice")
        result["target_high_price"] = info.get("targetHighPrice")
        result["number_of_analysts"] = info.get("numberOfAnalystOpinions")
        result["current_price"] = info.get("currentPrice")

        # Earnings and revenue estimates from the earnings calendar if available
        try:
            earnings_est = info.get("earningsQuarterlyGrowth")
            if earnings_est is not None:
                result["earnings_estimate_next_quarter"] = earnings_est
        except Exception:
            pass

        try:
            revenue_est = info.get("revenueGrowth")
            if revenue_est is not None:
                result["revenue_estimate_next_quarter"] = revenue_est
        except Exception:
            pass

    except Exception as e:
        logger.warning("Analyst data fetch failed for %s: %s", ticker, e)

    return result
