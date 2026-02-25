"""Data models for the evidence-to-dollar pipeline."""

from __future__ import annotations

from pydantic import BaseModel, Field

from ai_opportunity_index.scoring.evidence_classification import (
    CaptureStage,
    TargetDimension,
)


class EvidencePassage(BaseModel):
    """Output of the Extract stage — a relevant passage from a raw document."""

    source_type: str  # 'filing', 'news', 'patent', 'job'
    source_document: str  # filename or article title
    passage_text: str  # extracted relevant text
    target: TargetDimension  # COST, REVENUE, or GENERAL
    stage: CaptureStage  # PLANNED, INVESTED, REALIZED
    confidence: float = 1.0  # 0-1, how confident the extraction is
    metadata: dict = Field(default_factory=dict)  # source-specific details


class ValuedEvidence(BaseModel):
    """Output of the Value stage — a passage with dollar estimates."""

    passage: EvidencePassage
    dollar_year_1: float = 0.0  # estimated annual impact year 1
    dollar_year_2: float = 0.0  # year 2
    dollar_year_3: float = 0.0  # year 3
    total_3yr: float = 0.0  # sum of years 1-3
    horizon_shape: str = "flat"  # 'flat', 'linear_ramp', 's_curve', 'back_loaded'
    valuation_method: str = "formula"  # 'formula' or 'llm'
    valuation_rationale: str = ""  # human-readable explanation


class CompanyDollarScore(BaseModel):
    """Output of the Score stage — aggregated company-level dollar metrics."""

    # Dollar totals (3-year horizon)
    cost_opportunity_usd: float = 0.0
    revenue_opportunity_usd: float = 0.0
    total_opportunity_usd: float = 0.0

    # Per-stage breakdowns
    cost_realized_usd: float = 0.0
    cost_invested_usd: float = 0.0
    cost_planned_usd: float = 0.0
    revenue_realized_usd: float = 0.0
    revenue_invested_usd: float = 0.0
    revenue_planned_usd: float = 0.0

    # General AI investment (not clearly cost or revenue)
    general_investment_usd: float = 0.0

    # Evidence counts
    total_evidence_count: int = 0
    cost_evidence_count: int = 0
    revenue_evidence_count: int = 0
    general_evidence_count: int = 0

    # Quadrant assignment (preserved for compatibility)
    quadrant: str = ""
    quadrant_label: str = ""

    # All valued evidence items
    valued_evidence: list[ValuedEvidence] = Field(default_factory=list)

    # Legacy 0-1 scores (for backward compatibility during transition)
    opportunity: float = 0.0
    realization: float = 0.0
