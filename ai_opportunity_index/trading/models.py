"""Trading domain models -- dry-run trade signals with full rationale chains."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, Field

import uuid


class TradeAction(str, Enum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    INCREASE = "increase"   # Add to existing position
    DECREASE = "decrease"   # Reduce existing position


class TradeStatus(str, Enum):
    SIGNAL = "signal"       # Generated signal, not yet reviewed
    APPROVED = "approved"   # Human-approved for execution
    REJECTED = "rejected"   # Human-rejected
    EXECUTED = "executed"    # Trade was executed (future)
    EXPIRED = "expired"     # Signal expired without action


class SignalStrength(str, Enum):
    STRONG = "strong"       # High confidence, act now
    MODERATE = "moderate"   # Worth considering
    WEAK = "weak"           # Informational only


class RationaleNode(BaseModel):
    """A single node in the rationale chain -- traces from trade back to source."""
    level: str              # "trade", "insight", "evidence", "source"
    description: str        # Human-readable explanation
    data: dict = Field(default_factory=dict)  # Structured data at this level
    source_url: str | None = None
    source_author: str | None = None
    source_date: date | None = None
    confidence: float | None = None  # p(true) at this level
    children: list[RationaleNode] = Field(default_factory=list)


class TradeSignal(BaseModel):
    """A dry-run trade signal with full rationale chain."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ticker: str
    company_name: str | None = None
    action: TradeAction
    strength: SignalStrength

    # Position sizing (as fraction of portfolio)
    target_weight: float              # Target portfolio weight (0.0 - 1.0)
    current_weight: float = 0.0       # Current portfolio weight
    weight_change: float = 0.0        # Delta to apply

    # Scoring inputs
    opportunity_score: float = 0.0
    realization_score: float = 0.0
    ai_index_usd: float | None = None
    quadrant: str | None = None

    # Rationale chain -- the full provenance tree
    rationale: RationaleNode | None = None
    rationale_summary: str = ""       # One-line human-readable summary

    # Risk factors
    risk_factors: list[str] = Field(default_factory=list)
    flags: list[str] = Field(default_factory=list)

    # Metadata
    status: TradeStatus = TradeStatus.SIGNAL
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime | None = None
    reviewed_by: str | None = None
    review_notes: str | None = None

    # Portfolio context
    portfolio_id: str | None = None
    thesis_id: str | None = None


class Portfolio(BaseModel):
    """A portfolio of positions with trade history."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: str = ""
    owner: str = "system"

    # Positions: ticker -> weight
    positions: dict[str, float] = Field(default_factory=dict)
    cash_weight: float = 1.0  # Starts fully in cash

    # Constraints
    max_positions: int = 50
    max_single_position: float = 0.10   # Max 10% in one stock
    min_single_position: float = 0.005  # Min 0.5% per position

    # History
    signals: list[TradeSignal] = Field(default_factory=list)
    rebalance_count: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_rebalanced: datetime | None = None


class RebalanceResult(BaseModel):
    """Result of a portfolio rebalance operation."""
    portfolio_id: str
    signals: list[TradeSignal] = Field(default_factory=list)
    total_buys: int = 0
    total_sells: int = 0
    total_holds: int = 0
    turnover: float = 0.0  # Sum of absolute weight changes
    timestamp: datetime = Field(default_factory=datetime.utcnow)
