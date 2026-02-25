"""Portfolio engine for the AI Opportunity Index.

Builds investable universe, constructs 3 index variants, computes
backtested price history vs S&P 500, and calculates performance metrics.
"""

import logging
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
from sqlalchemy import func

from ai_opportunity_index.storage.db import get_session
from ai_opportunity_index.storage.models import (
    AIRealizationScore,
    Company,
    IndexValue,
)

logger = logging.getLogger(__name__)


def build_investable_universe() -> pd.DataFrame:
    """Query DB for companies with real scores (have sector AND filing NLP != None).

    Returns a DataFrame ranked by composite score (opportunity + realization)
    with columns: ticker, company_name, sector, industry,
    opportunity, realization, composite, filing_nlp_score.
    """
    session = get_session()
    try:
        # Get latest index values per company
        latest_idx = (
            session.query(
                IndexValue.company_id,
                func.max(IndexValue.scored_at).label("max_scored"),
            )
            .group_by(IndexValue.company_id)
            .subquery()
        )

        # Get latest realization scores per company
        latest_real = (
            session.query(
                AIRealizationScore.company_id,
                func.max(AIRealizationScore.scored_at).label("max_scored"),
            )
            .group_by(AIRealizationScore.company_id)
            .subquery()
        )

        query = (
            session.query(
                Company.ticker,
                Company.company_name,
                Company.sector,
                Company.industry,
                IndexValue.opportunity,
                IndexValue.realization,
                AIRealizationScore.filing_nlp_score,
            )
            .join(IndexValue, Company.id == IndexValue.company_id)
            .join(
                latest_idx,
                (IndexValue.company_id == latest_idx.c.company_id)
                & (IndexValue.scored_at == latest_idx.c.max_scored),
            )
            .join(AIRealizationScore, Company.id == AIRealizationScore.company_id)
            .join(
                latest_real,
                (AIRealizationScore.company_id == latest_real.c.company_id)
                & (AIRealizationScore.scored_at == latest_real.c.max_scored),
            )
            .filter(
                Company.sector.isnot(None),
                Company.sector != "",
                AIRealizationScore.filing_nlp_score.isnot(None),
            )
        )

        df = pd.read_sql(query.statement, session.bind)

        if df.empty:
            logger.warning("No companies found in investable universe")
            return df

        # Composite = average of opportunity and realization
        df["composite"] = (df["opportunity"] + df["realization"]) / 2.0
        df = df.sort_values("composite", ascending=False).reset_index(drop=True)

        logger.info("Investable universe: %d companies", len(df))
        return df
    finally:
        session.close()


def build_index_variants(universe_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build 3 index variants from the investable universe.

    Returns dict mapping variant name to DataFrame with columns:
    ticker, company_name, sector, composite, weight.
    """
    variants = {}

    # 1. Top 30 score-weighted
    top30 = universe_df.head(30).copy()
    score_sum = top30["composite"].sum()
    top30["weight"] = top30["composite"] / score_sum if score_sum > 0 else 1.0 / len(top30)
    variants["top_30_score_weighted"] = top30[["ticker", "company_name", "sector", "composite", "weight"]].copy()

    # 2. Top 50 equal-weighted
    top50_eq = universe_df.head(50).copy()
    top50_eq["weight"] = 1.0 / len(top50_eq)
    variants["top_50_equal_weighted"] = top50_eq[["ticker", "company_name", "sector", "composite", "weight"]].copy()

    # 3. Top 50 score-weighted
    top50_sw = universe_df.head(50).copy()
    score_sum_50 = top50_sw["composite"].sum()
    top50_sw["weight"] = top50_sw["composite"] / score_sum_50 if score_sum_50 > 0 else 1.0 / len(top50_sw)
    variants["top_50_score_weighted"] = top50_sw[["ticker", "company_name", "sector", "composite", "weight"]].copy()

    for name, v_df in variants.items():
        logger.info("Variant %s: %d holdings, weight sum=%.4f", name, len(v_df), v_df["weight"].sum())

    return variants


def compute_index_history(
    tickers: list[str],
    weights: list[float],
    start_date: str = "2020-01-01",
) -> pd.DataFrame:
    """Compute backtested daily portfolio vs SPY performance.

    Uses yfinance to download daily close prices. Handles missing tickers
    by redistributing their weight proportionally.

    Returns DataFrame with columns: date, portfolio_value, spy_value,
    portfolio_return, spy_return.
    """
    end_date = datetime.now().strftime("%Y-%m-%d")

    # Download all tickers + SPY
    all_tickers = list(tickers) + ["SPY"]
    logger.info("Downloading price data for %d tickers from %s to %s", len(all_tickers), start_date, end_date)

    data = yf.download(all_tickers, start=start_date, end=end_date, auto_adjust=True, progress=False)

    if data.empty:
        logger.error("No price data downloaded")
        return pd.DataFrame()

    # Extract close prices
    if isinstance(data.columns, pd.MultiIndex):
        closes = data["Close"]
    else:
        closes = data[["Close"]].rename(columns={"Close": all_tickers[0]})

    # Determine which tickers we actually have data for
    available = [t for t in tickers if t in closes.columns and closes[t].notna().sum() > 0]
    missing = set(tickers) - set(available)
    if missing:
        logger.warning("Missing price data for %d tickers: %s", len(missing), sorted(missing))

    if not available:
        logger.error("No portfolio tickers have price data")
        return pd.DataFrame()

    # Redistribute weights to available tickers
    ticker_weight = dict(zip(tickers, weights))
    available_weights = {t: ticker_weight[t] for t in available}
    weight_sum = sum(available_weights.values())
    if weight_sum > 0:
        available_weights = {t: w / weight_sum for t, w in available_weights.items()}

    # Forward-fill missing dates, drop rows where SPY is missing
    portfolio_closes = closes[available].ffill()
    spy_close = closes["SPY"].ffill()

    # Align on common dates
    common_idx = portfolio_closes.dropna(how="all").index.intersection(spy_close.dropna().index)
    portfolio_closes = portfolio_closes.loc[common_idx]
    spy_close = spy_close.loc[common_idx]

    # Compute daily returns
    portfolio_returns = portfolio_closes.pct_change()
    spy_returns = spy_close.pct_change()

    # Weighted portfolio daily return
    weight_series = pd.Series(available_weights)
    portfolio_daily_return = portfolio_returns[available].fillna(0).dot(weight_series)

    # Build cumulative value (growth of $10K)
    portfolio_value = (1 + portfolio_daily_return).cumprod() * 10000
    spy_value = (1 + spy_returns).cumprod() * 10000

    # Set initial values
    portfolio_value.iloc[0] = 10000
    spy_value.iloc[0] = 10000

    result = pd.DataFrame({
        "date": common_idx,
        "portfolio_value": portfolio_value.values,
        "spy_value": spy_value.values,
        "portfolio_return": portfolio_daily_return.values,
        "spy_return": spy_returns.values,
    })

    result["date"] = pd.to_datetime(result["date"])
    logger.info(
        "Index history: %d trading days, %d available tickers (of %d)",
        len(result), len(available), len(tickers),
    )
    return result


def compute_performance_metrics(history_df: pd.DataFrame) -> dict:
    """Compute performance metrics from index history.

    Returns dict with: annualized_return, spy_annualized_return, sharpe_ratio,
    max_drawdown, max_drawdown_peak, max_drawdown_trough, alpha_vs_spy, beta,
    tracking_error, information_ratio, total_return, spy_total_return, period_years.
    """
    if history_df.empty:
        return {}

    # Filter out first row (no return)
    returns = history_df["portfolio_return"].iloc[1:].values
    spy_returns = history_df["spy_return"].iloc[1:].values

    trading_days = len(returns)
    period_years = trading_days / 252.0
    rf_daily = (1 + 0.05) ** (1 / 252) - 1  # 5% annualized risk-free rate

    # Total return
    total_return = history_df["portfolio_value"].iloc[-1] / 10000 - 1
    spy_total_return = history_df["spy_value"].iloc[-1] / 10000 - 1

    # Annualized return
    ann_return = (1 + total_return) ** (1 / period_years) - 1 if period_years > 0 else 0
    spy_ann_return = (1 + spy_total_return) ** (1 / period_years) - 1 if period_years > 0 else 0

    # Sharpe ratio
    excess_returns = returns - rf_daily
    sharpe = np.sqrt(252) * np.mean(excess_returns) / np.std(excess_returns) if np.std(excess_returns) > 0 else 0

    # Max drawdown
    portfolio_values = history_df["portfolio_value"].values
    running_max = np.maximum.accumulate(portfolio_values)
    drawdowns = (portfolio_values - running_max) / running_max
    max_dd = drawdowns.min()
    max_dd_trough_idx = drawdowns.argmin()
    max_dd_peak_idx = portfolio_values[:max_dd_trough_idx + 1].argmax()

    peak_date = str(history_df["date"].iloc[max_dd_peak_idx].date())
    trough_date = str(history_df["date"].iloc[max_dd_trough_idx].date())

    # Beta and Alpha (CAPM)
    cov_matrix = np.cov(returns, spy_returns)
    beta = cov_matrix[0, 1] / cov_matrix[1, 1] if cov_matrix[1, 1] > 0 else 1.0
    alpha = ann_return - (0.05 + beta * (spy_ann_return - 0.05))

    # Tracking error and information ratio
    active_returns = returns - spy_returns
    tracking_error = np.std(active_returns) * np.sqrt(252)
    information_ratio = (ann_return - spy_ann_return) / tracking_error if tracking_error > 0 else 0

    return {
        "annualized_return": round(ann_return, 4),
        "spy_annualized_return": round(spy_ann_return, 4),
        "sharpe_ratio": round(sharpe, 2),
        "max_drawdown": round(max_dd, 4),
        "max_drawdown_peak": peak_date,
        "max_drawdown_trough": trough_date,
        "alpha_vs_spy": round(alpha, 4),
        "beta": round(beta, 2),
        "tracking_error": round(tracking_error, 4),
        "information_ratio": round(information_ratio, 2),
        "total_return": round(total_return, 4),
        "spy_total_return": round(spy_total_return, 4),
        "period_years": round(period_years, 1),
    }
