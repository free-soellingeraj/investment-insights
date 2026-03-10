"""GraphQL types mapped from Pydantic domain models."""

from __future__ import annotations

import strawberry
from datetime import date, datetime
from typing import Optional


@strawberry.type
class CompanyType:
    id: int
    ticker: Optional[str]
    slug: Optional[str]
    company_name: Optional[str]
    exchange: Optional[str]
    sector: Optional[str]
    industry: Optional[str]
    sic: Optional[str]
    naics: Optional[str]
    is_active: bool
    github_url: Optional[str] = None
    careers_url: Optional[str] = None
    ir_url: Optional[str] = None
    blog_url: Optional[str] = None


@strawberry.type
class CompanyScoreType:
    id: int
    company_id: int
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    opportunity: float
    realization: float
    quadrant: Optional[str] = None
    quadrant_label: Optional[str] = None
    cost_opp_score: Optional[float] = None
    revenue_opp_score: Optional[float] = None
    composite_opp_score: Optional[float] = None
    cost_capture_score: Optional[float] = None
    revenue_capture_score: Optional[float] = None
    filing_nlp_score: Optional[float] = None
    product_score: Optional[float] = None
    github_score: Optional[float] = None
    analyst_score: Optional[float] = None
    cost_roi: Optional[float] = None
    revenue_roi: Optional[float] = None
    combined_rank: Optional[int] = None
    ai_index_usd: Optional[float] = None
    capture_probability: Optional[float] = None
    opportunity_usd: Optional[float] = None
    evidence_dollars: Optional[float] = None
    flags: list[str]
    scored_at: datetime
    score_age_days: Optional[int] = None
    staleness_level: Optional[str] = None
    agreement_score: Optional[float] = None
    num_confirmations: Optional[int] = None
    num_contradictions: Optional[int] = None


@strawberry.type
class EvidenceType:
    id: int
    company_id: int
    evidence_type: str
    evidence_subtype: Optional[str] = None
    source_name: Optional[str] = None
    source_url: Optional[str] = None
    source_date: Optional[date] = None
    source_excerpt: Optional[str] = None
    target_dimension: Optional[str] = None
    capture_stage: Optional[str] = None
    signal_strength: Optional[str] = None
    dollar_estimate_usd: Optional[float] = None
    source_author: Optional[str] = None
    source_publisher: Optional[str] = None
    source_authority: Optional[str] = None
    observed_at: Optional[datetime] = None


@strawberry.type
class FinancialObservationType:
    id: int
    company_id: int
    metric: str
    value: float
    value_units: str
    source_name: Optional[str] = None
    fiscal_period: Optional[str] = None
    source_datetime: datetime


@strawberry.type
class PipelineRunType:
    id: int
    run_id: str
    task: str
    subtask: str
    run_type: str
    status: str
    tickers_succeeded: int
    tickers_failed: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


@strawberry.type
class EvidenceGroupType:
    id: int
    company_id: int
    target_dimension: str
    evidence_type: Optional[str] = None
    passage_count: int
    representative_text: Optional[str] = None
    mean_confidence: Optional[float] = None
    date_earliest: Optional[date] = None
    date_latest: Optional[date] = None


@strawberry.type
class ValuationType:
    id: int
    group_id: int
    stage: str
    evidence_type: str
    narrative: str
    confidence: float
    dollar_low: Optional[float] = None
    dollar_mid: Optional[float] = None
    dollar_high: Optional[float] = None
    specificity: Optional[float] = None
    magnitude: Optional[float] = None


@strawberry.type
class RationaleNodeType:
    """A node in the rationale provenance tree: trade -> insight -> evidence."""
    level: str           # "trade", "insight", "evidence"
    description: str
    data: strawberry.scalars.JSON
    source_url: Optional[str] = None
    source_date: Optional[date] = None
    confidence: Optional[float] = None
    children: list["RationaleNodeType"] = strawberry.field(default_factory=list)


@strawberry.type
class TradeSignalType:
    id: str
    ticker: str
    company_name: Optional[str] = None
    action: str
    strength: str
    target_weight: float
    current_weight: float
    weight_change: float
    opportunity_score: float
    realization_score: float
    quadrant: Optional[str] = None
    rationale_summary: str
    risk_factors: list[str]
    flags: list[str]
    status: str
    rationale: Optional[RationaleNodeType] = None


@strawberry.type
class RebalanceResultType:
    portfolio_id: str
    total_buys: int
    total_sells: int
    total_holds: int
    turnover: float
    signals: list[TradeSignalType]


@strawberry.type
class FactGraphStatsType:
    total_nodes: int
    total_edges: int
    total_attributes: int
    missing_values: int
    low_confidence_values: int
    counterfactual_branches: int
    completeness_pct: float
    nodes_by_type: strawberry.scalars.JSON


@strawberry.type
class ChangelogEntryType:
    description: str
    change_type: str
    component: str


@strawberry.type
class ReleaseType:
    version: str
    title: str
    date: datetime
    summary: str
    status: str
    changes: list[ChangelogEntryType]


@strawberry.type
class SourceAgreementType:
    source_a: str
    source_b: str
    dimension: str
    dollar_a: float
    dollar_b: float
    agreement_ratio: float


@strawberry.type
class SourceDisagreementType:
    source_a: str
    source_b: str
    dimension: str
    dollar_a: float
    dollar_b: float
    disagreement_ratio: float
    severity: str


@strawberry.type
class VerificationResultType:
    company_id: int
    ticker: str
    confirmations: list[SourceAgreementType]
    contradictions: list[SourceDisagreementType]
    agreement_score: float
    confidence_adjustment: float


@strawberry.type
class InvestmentProjectType:
    """A discrete AI investment project synthesized from evidence groups."""
    id: int
    company_id: int
    short_title: str
    description: str
    target_dimension: str
    target_subcategory: str
    target_detail: Optional[str] = None
    status: str
    dollar_total: Optional[float] = None
    dollar_low: Optional[float] = None
    dollar_high: Optional[float] = None
    confidence: float
    evidence_count: int
    date_start: Optional[date] = None
    date_end: Optional[date] = None
    technology_area: Optional[str] = None
    deployment_scope: Optional[str] = None
    evidence_group_ids: list[int]


@strawberry.type
class CompanyDetailType:
    """Rich company detail combining company info, scores, evidence, financials."""
    company: CompanyType
    latest_score: Optional[CompanyScoreType] = None
    evidence: list[EvidenceType]
    financials: list[FinancialObservationType]
    peers: list[CompanyType]
    evidence_groups: list[EvidenceGroupType]
    valuations: list[ValuationType]
    investment_projects: list[InvestmentProjectType]


@strawberry.type
class PipelineStatusType:
    total_companies: int
    companies_scored: int
    total_evidence: int
    last_run: Optional[PipelineRunType] = None
    recent_runs: list[PipelineRunType]


@strawberry.type
class AuditFindingType:
    severity: str
    category: str
    company_id: int
    ticker: str
    description: str
    expected: Optional[str] = None
    actual: Optional[str] = None


@strawberry.type
class AuditReportType:
    companies_audited: int
    clean_companies: int
    total_findings: int
    critical: int
    warning: int
    info: int
    pass_rate: float
    findings: list[AuditFindingType]


@strawberry.type
class InferenceResultType:
    """Result of running inference on the fact graph."""
    method: str
    facts_updated: int
    facts_created: int
    constraints_satisfied: int
    constraints_violated: int
    duration_ms: int
    reasoning_log: list[str]


@strawberry.type
class SystemInternalsType:
    db_status: str
    total_companies: int
    total_evidence: int
    total_scores: int
    fact_graph: FactGraphStatsType
    pipeline_status: PipelineStatusType
    changelog: list[ReleaseType]


# -- Journalism types ---------------------------------------------------------


@strawberry.type
class CitationType:
    source_url: Optional[str] = None
    author: Optional[str] = None
    publisher: Optional[str] = None
    date: Optional[date] = None
    authority: Optional[str] = None
    excerpt: Optional[str] = None


@strawberry.type
class NarrativeSectionType:
    title: str
    body: str
    citations: list[CitationType]
    confidence: float
    evidence_count: int


@strawberry.type
class CompanyNarrativeType:
    company_id: int
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    summary: str
    sections: list[NarrativeSectionType]
    generated_at: datetime
    total_citations: int
    overall_confidence: float


@strawberry.type
class DimensionCoverageType:
    dimension: str
    stage: Optional[str] = None
    source_count: int
    distinct_source_types: list[str]
    newest_date: Optional[date] = None
    oldest_date: Optional[date] = None
    is_stale: bool
    has_gap: bool


@strawberry.type
class CoverageReportType:
    company_id: int
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    total_evidence_groups: int
    total_passages: int
    dimension_coverage: list[DimensionCoverageType]
    gaps: list[str]
    stale_dimensions: list[str]
    overall_coverage_score: float
    assessed_at: datetime


@strawberry.type
class ResearchTaskType:
    company_id: int
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    dimensions: list[str]
    priority: str
    coverage_gaps: list[str]
