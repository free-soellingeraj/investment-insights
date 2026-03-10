"""Trade signal generator -- converts company scores into actionable signals.

Each signal includes a full rationale chain from trade action back through
scoring insights to the original evidence provenance.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from typing import Any

from .models import (
    TradeSignal,
    TradeAction,
    TradeStatus,
    SignalStrength,
    RationaleNode,
    Portfolio,
    RebalanceResult,
)

logger = logging.getLogger(__name__)


# -- Signal Generation Config --------------------------------------------------

# Score thresholds for BUY signals
BUY_OPPORTUNITY_THRESHOLD = 0.6    # High opportunity
BUY_REALIZATION_THRESHOLD = 0.4    # Some realization (not zero)
STRONG_SIGNAL_THRESHOLD = 0.75     # Both scores above this = strong

# SELL thresholds
SELL_OPPORTUNITY_THRESHOLD = 0.3   # Low opportunity
SELL_REALIZATION_THRESHOLD = 0.2   # Low realization

# Position sizing
BASE_WEIGHT = 0.02                 # 2% base position
MAX_WEIGHT = 0.05                  # 5% max position
SCORE_SCALING = 0.03               # Extra weight per combined score unit

# Signal expiry
SIGNAL_EXPIRY_DAYS = 7


class TradeSignalGenerator:
    """Generates trade signals from company scores with full rationale chains."""

    def __init__(self, session=None):
        self.session = session

    def generate_signals(self, portfolio: Portfolio | None = None) -> RebalanceResult:
        """Generate trade signals for all scored companies.

        Returns a RebalanceResult with all signals, including full rationale chains.
        """
        from ai_opportunity_index.storage.db import get_session, get_latest_scores

        session = self.session or get_session()
        portfolio = portfolio or Portfolio(
            name="AI Opportunity Index",
            description="Score-weighted AI opportunity portfolio",
        )

        try:
            scores = get_latest_scores(limit=500)
            signals = []

            for score_row in scores:
                signal = self._score_to_signal(score_row, portfolio, session)
                if signal:
                    signals.append(signal)

            # Calculate rebalance metrics
            buys = [s for s in signals if s.action in (TradeAction.BUY, TradeAction.INCREASE)]
            sells = [s for s in signals if s.action in (TradeAction.SELL, TradeAction.DECREASE)]
            holds = [s for s in signals if s.action == TradeAction.HOLD]
            # One-sided turnover: sum of buys only (or sells only — they mirror).
            # Exclude HOLDs with zero weight change to avoid inflating the metric.
            turnover = sum(abs(s.weight_change) for s in signals if s.action != TradeAction.HOLD) / 2.0

            result = RebalanceResult(
                portfolio_id=portfolio.id,
                signals=signals,
                total_buys=len(buys),
                total_sells=len(sells),
                total_holds=len(holds),
                turnover=turnover,
            )

            logger.info(
                "Generated %d signals: %d buys, %d sells, %d holds, %.1f%% turnover",
                len(signals), len(buys), len(sells), len(holds), turnover * 100,
            )

            return result
        finally:
            if not self.session:
                session.close()

    def _score_to_signal(
        self, score_row: Any, portfolio: Portfolio, session,
    ) -> TradeSignal | None:
        """Convert a single company score to a trade signal."""
        # score_row is a dict from get_latest_scores
        ticker = score_row.get("ticker")
        company_name = score_row.get("company_name")
        opportunity = score_row.get("opportunity", 0) or 0
        realization = score_row.get("realization", 0) or 0
        quadrant = score_row.get("quadrant")
        quadrant_label = score_row.get("quadrant_label")
        ai_index_usd = score_row.get("ai_index_usd")
        flags = score_row.get("flags") or []

        if not ticker:
            return None

        current_weight = portfolio.positions.get(ticker, 0.0)

        # Determine action and target weight
        action, target_weight, strength = self._determine_action(
            opportunity, realization, current_weight, flags,
        )

        if action == TradeAction.HOLD and current_weight == 0:
            return None  # Skip non-positions with HOLD

        # Build rationale chain
        rationale = self._build_rationale(
            ticker, company_name, action, opportunity, realization,
            quadrant, quadrant_label, ai_index_usd, flags, session,
        )

        # Generate summary
        summary = self._generate_summary(
            ticker, company_name, action, strength, opportunity, realization,
            quadrant_label,
        )

        # Risk factors
        risk_factors = self._assess_risks(opportunity, realization, flags, ticker)

        signal = TradeSignal(
            ticker=ticker,
            company_name=company_name,
            action=action,
            strength=strength,
            target_weight=target_weight,
            current_weight=current_weight,
            weight_change=target_weight - current_weight,
            opportunity_score=opportunity,
            realization_score=realization,
            ai_index_usd=ai_index_usd,
            quadrant=quadrant,
            rationale=rationale,
            rationale_summary=summary,
            risk_factors=risk_factors,
            flags=list(flags) if flags else [],
            portfolio_id=portfolio.id,
            expires_at=datetime.utcnow() + timedelta(days=SIGNAL_EXPIRY_DAYS),
        )

        return signal

    def _determine_action(
        self,
        opportunity: float,
        realization: float,
        current_weight: float,
        flags: list,
    ) -> tuple[TradeAction, float, SignalStrength]:
        """Determine trade action based on scores."""
        combined = (opportunity + realization) / 2

        # Strong BUY: high opportunity AND decent realization
        if opportunity >= BUY_OPPORTUNITY_THRESHOLD and realization >= BUY_REALIZATION_THRESHOLD:
            target = min(MAX_WEIGHT, BASE_WEIGHT + SCORE_SCALING * combined)
            strength = (
                SignalStrength.STRONG
                if combined >= STRONG_SIGNAL_THRESHOLD
                else SignalStrength.MODERATE
            )

            if current_weight > 0:
                if target > current_weight * 1.1:  # More than 10% increase needed
                    return TradeAction.INCREASE, target, strength
                else:
                    return TradeAction.HOLD, current_weight, SignalStrength.WEAK
            return TradeAction.BUY, target, strength

        # SELL: low scores on both dimensions
        if opportunity < SELL_OPPORTUNITY_THRESHOLD and realization < SELL_REALIZATION_THRESHOLD:
            if current_weight > 0:
                return TradeAction.SELL, 0.0, SignalStrength.MODERATE
            return TradeAction.HOLD, 0.0, SignalStrength.WEAK

        # HOLD or gradual adjustment
        if current_weight > 0:
            target = min(MAX_WEIGHT, BASE_WEIGHT + SCORE_SCALING * combined)
            if target < current_weight * 0.8:  # More than 20% decrease
                return TradeAction.DECREASE, target, SignalStrength.WEAK
            return TradeAction.HOLD, current_weight, SignalStrength.WEAK

        # No position, moderate scores -- weak buy
        if opportunity >= 0.5 or realization >= 0.5:
            target = BASE_WEIGHT
            return TradeAction.BUY, target, SignalStrength.WEAK

        return TradeAction.HOLD, 0.0, SignalStrength.WEAK

    def _build_rationale(
        self,
        ticker: str,
        company_name: str | None,
        action: TradeAction,
        opportunity: float,
        realization: float,
        quadrant: str | None,
        quadrant_label: str | None,
        ai_index_usd: float | None,
        flags: list,
        session,
    ) -> RationaleNode:
        """Build the full rationale tree from trade action to evidence sources."""

        # Level 1: Trade rationale
        trade_node = RationaleNode(
            level="trade",
            description=f"{action.value.upper()} {ticker}: {quadrant_label or 'unclassified'}",
            data={
                "action": action.value,
                "opportunity": opportunity,
                "realization": realization,
                "quadrant": quadrant,
                "ai_index_usd": ai_index_usd,
            },
        )

        # Level 2: Scoring insights
        opp_insight = RationaleNode(
            level="insight",
            description=f"Opportunity score: {opportunity:.2f} -- AI-addressable market potential",
            data={"dimension": "opportunity", "score": opportunity},
            confidence=opportunity,
        )

        real_insight = RationaleNode(
            level="insight",
            description=f"Realization score: {realization:.2f} -- evidence of AI capture",
            data={"dimension": "realization", "score": realization},
            confidence=realization,
        )

        # Level 3: Evidence groups with valuations and passages
        try:
            from ai_opportunity_index.storage.models import (
                CompanyModel,
                EvidenceGroupModel,
                EvidenceGroupPassageModel,
                EvidenceModel,
                ValuationModel,
            )
            from ai_opportunity_index.domains import ValuationStage

            company = session.query(CompanyModel).filter(
                CompanyModel.ticker == ticker,
            ).first()

            if company:
                # Get evidence groups with final valuations
                groups = (
                    session.query(EvidenceGroupModel)
                    .filter(EvidenceGroupModel.company_id == company.id)
                    .all()
                )
                group_ids = [g.id for g in groups]

                # Load valuations for these groups
                valuations_by_group: dict[int, list] = {}
                if group_ids:
                    vals = (
                        session.query(ValuationModel)
                        .filter(
                            ValuationModel.group_id.in_(group_ids),
                            ValuationModel.stage == ValuationStage.FINAL.value,
                        )
                        .all()
                    )
                    for v in vals:
                        valuations_by_group.setdefault(v.group_id, []).append(v)

                # Load top passages per group (limit 3 per group for rationale)
                passages_by_group: dict[int, list] = {}
                if group_ids:
                    passages = (
                        session.query(EvidenceGroupPassageModel)
                        .filter(EvidenceGroupPassageModel.group_id.in_(group_ids))
                        .all()
                    )
                    for p in passages:
                        passages_by_group.setdefault(p.group_id, []).append(p)

                # Build evidence group nodes
                for group in groups:
                    group_vals = valuations_by_group.get(group.id, [])
                    group_passages = passages_by_group.get(group.id, [])

                    # Build group description from valuations or passages
                    if group_vals:
                        val = group_vals[0]  # Use first (usually only) final valuation
                        dollar_text = ""
                        if val.dollar_mid is not None:
                            if abs(val.dollar_mid) >= 1_000_000_000:
                                dollar_text = f" (${val.dollar_mid / 1e9:.1f}B)"
                            elif abs(val.dollar_mid) >= 1_000_000:
                                dollar_text = f" (${val.dollar_mid / 1e6:.1f}M)"
                            elif abs(val.dollar_mid) >= 1_000:
                                dollar_text = f" (${val.dollar_mid / 1e3:.0f}K)"
                            else:
                                dollar_text = f" (${val.dollar_mid:.0f})"
                        desc = f"{val.narrative or 'Valued evidence'}{dollar_text}"
                        group_confidence = val.confidence
                    elif group.representative_text:
                        desc = group.representative_text[:150]
                        group_confidence = group.mean_confidence
                    else:
                        desc = f"{group.passage_count} passages ({group.target_dimension})"
                        group_confidence = group.mean_confidence

                    group_node = RationaleNode(
                        level="evidence",
                        description=desc,
                        data={
                            "group_id": group.id,
                            "target_dimension": group.target_dimension,
                            "evidence_type": group.evidence_type,
                            "passage_count": group.passage_count,
                            "dollar_mid": group_vals[0].dollar_mid if group_vals else None,
                            "dollar_low": group_vals[0].dollar_low if group_vals else None,
                            "dollar_high": group_vals[0].dollar_high if group_vals else None,
                        },
                        confidence=group_confidence,
                    )

                    # Add top passages as children (max 3 per group)
                    for passage in group_passages[:3]:
                        passage_node = RationaleNode(
                            level="source",
                            description=(
                                passage.passage_text[:200]
                                if passage.passage_text else "No text"
                            ),
                            data={
                                "source_type": passage.source_type,
                                "source_publisher": passage.source_publisher,
                                "source_authority": passage.source_authority,
                            },
                            source_url=passage.source_url,
                            source_date=passage.source_date,
                            confidence=passage.confidence,
                        )
                        group_node.children.append(passage_node)

                    # Attach to appropriate insight based on dimension
                    if group.target_dimension in ("cost", "revenue", "general"):
                        opp_insight.children.append(group_node)
                    else:
                        real_insight.children.append(group_node)

                # Fallback: if no evidence groups, use legacy evidence table
                if not groups:
                    evidence = (
                        session.query(EvidenceModel)
                        .filter(EvidenceModel.company_id == company.id)
                        .order_by(EvidenceModel.observed_at.desc())
                        .limit(20)
                        .all()
                    )
                    for ev in evidence:
                        ev_node = RationaleNode(
                            level="evidence",
                            description=(
                                f"[{ev.evidence_type}] "
                                f"{ev.source_excerpt[:100] if ev.source_excerpt else 'No excerpt'}"
                            ),
                            data={
                                "evidence_type": ev.evidence_type,
                                "target_dimension": ev.target_dimension,
                                "capture_stage": ev.capture_stage,
                                "signal_strength": ev.signal_strength,
                            },
                            source_url=ev.source_url,
                            source_date=ev.source_date,
                            confidence=ev.score_contribution,
                        )
                        if ev.target_dimension in ("cost", "revenue"):
                            opp_insight.children.append(ev_node)
                        else:
                            real_insight.children.append(ev_node)

        except Exception as e:
            logger.warning("Could not load evidence for rationale: %s", e)

        trade_node.children = [opp_insight, real_insight]

        # Add flag insights
        if flags:
            flag_node = RationaleNode(
                level="insight",
                description=f"Flags: {', '.join(flags)}",
                data={"flags": flags},
            )
            trade_node.children.append(flag_node)

        return trade_node

    def _generate_summary(
        self,
        ticker: str,
        company_name: str | None,
        action: TradeAction,
        strength: SignalStrength,
        opportunity: float,
        realization: float,
        quadrant_label: str | None,
    ) -> str:
        """Generate a one-line human-readable summary."""
        name = company_name or ticker

        if action == TradeAction.BUY:
            return (
                f"{strength.value.upper()} BUY {name} ({ticker}): "
                f"{quadrant_label} with {opportunity:.0%} opportunity, "
                f"{realization:.0%} realization"
            )
        elif action == TradeAction.SELL:
            return (
                f"SELL {name} ({ticker}): {quadrant_label} -- "
                f"low opportunity ({opportunity:.0%}) and realization ({realization:.0%})"
            )
        elif action == TradeAction.INCREASE:
            return (
                f"INCREASE {name} ({ticker}): improving scores -- "
                f"opportunity {opportunity:.0%}, realization {realization:.0%}"
            )
        elif action == TradeAction.DECREASE:
            return (
                f"DECREASE {name} ({ticker}): declining scores -- "
                f"opportunity {opportunity:.0%}, realization {realization:.0%}"
            )
        else:
            return f"HOLD {name} ({ticker}): {quadrant_label}"

    def _assess_risks(
        self,
        opportunity: float,
        realization: float,
        flags: list,
        ticker: str,
    ) -> list[str]:
        """Assess risk factors for this signal."""
        risks = []

        if opportunity > 0.8 and realization < 0.3:
            risks.append(
                "High opportunity but low realization -- thesis may not materialize"
            )
        if realization > 0.7 and opportunity < 0.3:
            risks.append(
                "High realization but limited opportunity -- may be fully priced in"
            )
        if flags:
            for flag in flags:
                if "discrepancy" in flag.lower():
                    risks.append(f"Score discrepancy detected: {flag}")
                if "thin" in flag.lower() or "sparse" in flag.lower():
                    risks.append(f"Limited evidence: {flag}")

        return risks
