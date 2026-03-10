"""Domain models for the journalism subsystem."""

import datetime as _dt
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from ai_opportunity_index.domains import (
    CaptureStage,
    CollectedItem,
    TargetDimension,
    ValuationEvidenceType,
)


class ResearchPriority(str, Enum):
    """Priority level for a research task."""
    CRITICAL = "critical"   # no evidence at all
    HIGH = "high"           # single-source dimensions
    MEDIUM = "medium"       # stale evidence (>60 days)
    LOW = "low"             # thin coverage on high-value companies


class ResearchTask(BaseModel):
    """An editor-assigned research task."""
    company_id: int
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    dimensions: list[str] = Field(
        default_factory=list,
        description="TargetDimension x CaptureStage combos to investigate, e.g. 'cost:planned'",
    )
    priority: ResearchPriority = ResearchPriority.MEDIUM
    deadline: Optional[_dt.datetime] = None
    coverage_gaps: list[str] = Field(
        default_factory=list,
        description="Human-readable descriptions of what evidence is missing",
    )


class DimensionCoverage(BaseModel):
    """Coverage status for a single dimension (and optional stage)."""
    dimension: TargetDimension
    stage: Optional[ValuationEvidenceType] = None
    source_count: int = 0
    distinct_source_types: list[str] = Field(default_factory=list)
    newest_date: Optional[_dt.date] = None
    oldest_date: Optional[_dt.date] = None
    is_stale: bool = False
    has_gap: bool = False


class CoverageReport(BaseModel):
    """Summary of evidence coverage for a company."""
    company_id: int
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    total_evidence_groups: int = 0
    total_passages: int = 0
    dimension_coverage: list[DimensionCoverage] = Field(default_factory=list)
    gaps: list[str] = Field(
        default_factory=list,
        description="Dimensions with fewer than 2 independent sources",
    )
    stale_dimensions: list[str] = Field(
        default_factory=list,
        description="Dimensions where newest evidence is older than 60 days",
    )
    overall_coverage_score: float = 0.0
    assessed_at: _dt.datetime = Field(default_factory=_dt.datetime.utcnow)


class ResearchResult(BaseModel):
    """What a researcher delivers after executing a task."""
    task: ResearchTask
    collected_items: list[CollectedItem] = Field(default_factory=list)
    sources_attempted: int = 0
    sources_succeeded: int = 0
    errors: list[str] = Field(default_factory=list)
    collected_at: _dt.datetime = Field(default_factory=_dt.datetime.utcnow)


class Citation(BaseModel):
    """Full provenance citation for a narrative passage."""
    source_url: Optional[str] = None
    author: Optional[str] = None
    publisher: Optional[str] = None
    source_date: Optional[_dt.date] = None
    authority: Optional[str] = None
    excerpt: Optional[str] = None


class NarrativeSection(BaseModel):
    """A section of a company narrative."""
    title: str
    body: str
    citations: list[Citation] = Field(default_factory=list)
    confidence: float = 0.0
    evidence_count: int = 0


class CompanyNarrative(BaseModel):
    """A structured narrative about a company's AI position."""
    company_id: int
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    summary: str = ""
    sections: list[NarrativeSection] = Field(default_factory=list)
    generated_at: _dt.datetime = Field(default_factory=_dt.datetime.utcnow)
    total_citations: int = 0
    overall_confidence: float = 0.0
