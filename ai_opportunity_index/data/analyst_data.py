"""Fetch analyst consensus data from Yahoo Finance.

Uses yfinance (already a project dependency) to extract analyst
recommendations, price targets, and earnings estimates.
"""

import json
import logging
import time
from datetime import date, datetime
from pathlib import Path

from ai_opportunity_index.config import RAW_DIR, YF_RATE_LIMIT_SECONDS
from ai_opportunity_index.domains import CollectedItem, SourceAuthority

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


def analyst_dict_to_collected_item(data: dict) -> CollectedItem:
    """Convert an analyst data dict to a CollectedItem with narrative summary."""
    ticker = data.get("ticker", "")
    rec_mean = data.get("recommendation_mean")
    rec_key = data.get("recommendation_key", "N/A")
    target_mean = data.get("target_mean_price")
    num_analysts = data.get("number_of_analysts")
    earnings_est = data.get("earnings_estimate_next_quarter")
    revenue_est = data.get("revenue_estimate_next_quarter")

    parts = [f"Analyst consensus for {ticker}: {rec_key}"]
    if rec_mean is not None:
        parts.append(f"({rec_mean:.1f}/5.0)")
    if target_mean is not None:
        parts.append(f", mean target price ${target_mean:,.0f}")
    if num_analysts is not None:
        parts.append(f", {num_analysts} analysts covering")
    if earnings_est is not None:
        parts.append(f". Earnings growth estimate: {earnings_est:.1%}")
    if revenue_est is not None:
        parts.append(f". Revenue growth estimate: {revenue_est:.1%}")
    narrative = "".join(parts) + "."

    today = date.today()
    return CollectedItem(
        item_id=f"{ticker}_{today.isoformat()}",
        title=f"Analyst Consensus for {ticker}",
        content=narrative,
        author=None,  # aggregated — no single analyst identifiable
        author_role="sell-side analyst consensus",
        author_affiliation=None,
        publisher="Yahoo Finance (aggregated)",
        url=None,
        source_date=today,  # point-in-time snapshot
        access_date=today,
        authority=SourceAuthority.AGGREGATED_CONSENSUS,
        metadata={
            k: v for k, v in data.items()
            if k not in ("ticker", "collected_at") and v is not None
        },
    )
