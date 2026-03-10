"""Trading package -- dry-run trade signal generation with full rationale chains.

Re-exports the key types and functions for convenient access.
"""

from ai_opportunity_index.trading.models import (
    Portfolio,
    RationaleNode,
    RebalanceResult,
    SignalStrength,
    TradeAction,
    TradeSignal,
    TradeStatus,
)
from ai_opportunity_index.trading.signal_generator import TradeSignalGenerator
from ai_opportunity_index.trading.portfolio_manager import (
    apply_signals,
    list_portfolios,
    load_portfolio,
    save_portfolio,
)

__all__ = [
    "Portfolio",
    "RationaleNode",
    "RebalanceResult",
    "SignalStrength",
    "TradeAction",
    "TradeSignal",
    "TradeSignalGenerator",
    "TradeStatus",
    "apply_signals",
    "list_portfolios",
    "load_portfolio",
    "save_portfolio",
]
