"""Generate dry-run trade signals from current company scores.

Usage:
    python -m scripts.generate_signals [--portfolio NAME] [--save]
"""

import argparse
import logging
import sys
from datetime import datetime

from ai_opportunity_index.storage.db import get_session, init_db
from ai_opportunity_index.trading.signal_generator import TradeSignalGenerator
from ai_opportunity_index.trading.models import Portfolio
from ai_opportunity_index.trading.portfolio_manager import save_portfolio, list_portfolios

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Generate dry-run trade signals")
    parser.add_argument("--portfolio", default="default", help="Portfolio name")
    parser.add_argument("--save", action="store_true", help="Save portfolio state")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    init_db()

    portfolio = Portfolio(
        name=args.portfolio,
        description=f"Generated {datetime.utcnow().isoformat()}",
    )

    generator = TradeSignalGenerator()
    result = generator.generate_signals(portfolio)

    if args.json:
        print(result.model_dump_json(indent=2))
        return

    # Human-readable output
    print(f"\n{'=' * 80}")
    print(f"TRADE SIGNALS -- {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'=' * 80}")
    print(f"Portfolio: {portfolio.name}")
    print(
        f"Signals: {len(result.signals)} "
        f"({result.total_buys} buys, {result.total_sells} sells, "
        f"{result.total_holds} holds)"
    )
    print(f"Turnover: {result.turnover:.1%}")
    print()

    # Group by action
    for action_label, action_filter in [
        ("STRONG BUY", lambda s: s.action.value == "buy" and s.strength.value == "strong"),
        ("BUY", lambda s: s.action.value == "buy" and s.strength.value != "strong"),
        ("INCREASE", lambda s: s.action.value == "increase"),
        ("DECREASE", lambda s: s.action.value == "decrease"),
        ("SELL", lambda s: s.action.value == "sell"),
    ]:
        filtered = [s for s in result.signals if action_filter(s)]
        if not filtered:
            continue

        print(f"\n--- {action_label} ({len(filtered)}) ---")
        for signal in sorted(
            filtered,
            key=lambda s: s.opportunity_score + s.realization_score,
            reverse=True,
        )[:20]:
            print(f"  {signal.ticker:<8} {signal.rationale_summary}")
            if signal.risk_factors:
                for risk in signal.risk_factors:
                    print(f"           ! {risk}")
            if signal.rationale and signal.rationale.children:
                for child in signal.rationale.children[:3]:
                    print(f"           > {child.description}")

    if args.save:
        # Apply signals to portfolio
        from ai_opportunity_index.trading.portfolio_manager import apply_signals

        portfolio = apply_signals(portfolio, result.signals)
        save_portfolio(portfolio)
        print(
            f"\nPortfolio saved: {len(portfolio.positions)} positions, "
            f"{portfolio.cash_weight:.1%} cash"
        )


if __name__ == "__main__":
    main()
