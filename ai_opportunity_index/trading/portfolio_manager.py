"""Portfolio manager -- tracks positions and applies trade signals."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from .models import Portfolio, TradeSignal, TradeAction, RebalanceResult

logger = logging.getLogger(__name__)

PORTFOLIOS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "portfolios"


def save_portfolio(portfolio: Portfolio):
    """Save portfolio state to disk."""
    PORTFOLIOS_DIR.mkdir(parents=True, exist_ok=True)
    path = PORTFOLIOS_DIR / f"{portfolio.id}.json"
    path.write_text(portfolio.model_dump_json(indent=2))
    logger.info("Saved portfolio %s to %s", portfolio.name, path)


def load_portfolio(portfolio_id: str) -> Portfolio | None:
    """Load portfolio from disk."""
    path = PORTFOLIOS_DIR / f"{portfolio_id}.json"
    if not path.exists():
        return None
    return Portfolio.model_validate_json(path.read_text())


def list_portfolios() -> list[Portfolio]:
    """List all saved portfolios."""
    if not PORTFOLIOS_DIR.exists():
        return []
    portfolios = []
    for path in PORTFOLIOS_DIR.glob("*.json"):
        try:
            portfolios.append(Portfolio.model_validate_json(path.read_text()))
        except Exception as e:
            logger.warning("Failed to load portfolio %s: %s", path, e)
    return portfolios


def apply_signals(portfolio: Portfolio, signals: list[TradeSignal]) -> Portfolio:
    """Apply approved trade signals to a portfolio (dry-run -- just updates weights)."""
    for signal in signals:
        if signal.status != "approved" and signal.status != "signal":
            continue

        if signal.action == TradeAction.BUY or signal.action == TradeAction.INCREASE:
            portfolio.positions[signal.ticker] = signal.target_weight
            portfolio.cash_weight -= signal.weight_change
        elif signal.action == TradeAction.SELL:
            freed = portfolio.positions.pop(signal.ticker, 0.0)
            portfolio.cash_weight += freed
        elif signal.action == TradeAction.DECREASE:
            old_weight = portfolio.positions.get(signal.ticker, 0.0)
            portfolio.positions[signal.ticker] = signal.target_weight
            portfolio.cash_weight += (old_weight - signal.target_weight)

    # Normalize weights
    total = sum(portfolio.positions.values()) + portfolio.cash_weight
    if total > 0 and abs(total - 1.0) > 0.001:
        factor = 1.0 / total
        portfolio.positions = {k: v * factor for k, v in portfolio.positions.items()}
        portfolio.cash_weight *= factor

    portfolio.rebalance_count += 1
    portfolio.last_rebalanced = datetime.utcnow()

    return portfolio
