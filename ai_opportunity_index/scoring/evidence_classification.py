"""Shared types for the 4-value scoring framework.

Every piece of evidence is classified on two axes:
- Target: cost, revenue, or general (unspecified AI investment)
- Stage: planned, invested, or realized
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class TargetDimension(str, Enum):
    COST = "cost"
    REVENUE = "revenue"
    GENERAL = "general"  # unspecified AI investment — does NOT inflate cost/revenue capture


class CaptureStage(str, Enum):
    PLANNED = "planned"    # announced plans/intentions
    INVESTED = "invested"  # actual spending/hiring/filing
    REALIZED = "realized"  # announced results/savings/revenue


class ClassifiedEvidence(BaseModel):
    source_type: str  # 'filing_nlp', 'product', 'job', 'patent'
    target: TargetDimension
    stage: CaptureStage
    raw_score: float  # 0-1
    weight: float = 1.0
    description: str = ""
    source_excerpt: str = ""  # verbatim excerpt from the source document supporting this evidence
    metadata: dict = Field(default_factory=dict)


class ClassifiedScorerOutput(BaseModel):
    overall_score: float  # legacy 0-1 composite
    cost_capture_score: float = 0.0  # from COST-targeted evidence only
    revenue_capture_score: float = 0.0  # from REVENUE-targeted evidence only
    general_investment_score: float = 0.0  # from GENERAL-targeted evidence only
    evidence_items: list[ClassifiedEvidence] = Field(default_factory=list)
    raw_details: dict = Field(default_factory=dict)
