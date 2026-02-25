"""Central Pydantic domain models for the AI Opportunity Index.

Used everywhere: API responses, scoring pipeline, DB serialization.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, Field


class PipelineTask(str, Enum):
    """Top-level pipeline task."""
    COLLECT = "collect"
    EXTRACT = "extract"
    VALUE = "value"
    SCORE = "score"


class PipelineSubtask(str, Enum):
    """Specific operation within a pipeline task.

    Collect subtasks:
        LINKS           – Discover company URLs (GitHub, careers, IR, blog)
        YAHOO_FUNDAMENTALS – Market cap, revenue, net income, employees, sector/industry
        SEC_FILINGS     – 10-K and 10-Q filing documents from SEC EDGAR
        NEWS            – News articles from Google News RSS, GNews, SEC EFTS
        GITHUB          – GitHub organization signals/metrics
        ANALYSTS        – Analyst consensus ratings and recommendations
        WEB_ENRICHMENT  – AI signals from careers, IR, and blog pages via LLM

    Extract/Value/Score subtasks:
        ALL             – Full stage (these run as single monolithic operations)
    """
    # Collect subtasks
    LINKS = "links"
    YAHOO_FUNDAMENTALS = "yahoo_fundamentals"
    SEC_FILINGS = "sec_filings"
    NEWS = "news"
    GITHUB = "github"
    ANALYSTS = "analysts"
    WEB_ENRICHMENT = "web_enrichment"
    # Generic subtask for monolithic stages
    ALL = "all"


class Company(BaseModel):
    id: int | None = None
    ticker: str
    exchange: str | None = None
    company_name: str | None = None
    cik: int | None = None
    sic: str | None = None
    naics: str | None = None
    country: str = "US"
    sector: str | None = None
    industry: str | None = None
    is_active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None


class FinancialObservation(BaseModel):
    id: int | None = None
    company_id: int
    metric: str  # 'market_cap', 'revenue', 'net_income', 'employees'
    value: float
    value_units: str  # 'usd', 'millions_usd', 'billions_usd', 'count', etc.
    source_datetime: datetime
    source_link: str | None = None
    source_name: str | None = None  # 'yahoo_finance', 'sec_10k', 'sec_10q'
    fiscal_period: str | None = None  # 'FY2024', '2024-Q3', etc.
    created_at: datetime | None = None


class AIOpportunityEvidence(BaseModel):
    id: int | None = None
    company_id: int
    pipeline_run_id: int | None = None
    evidence_type: str  # 'filing_nlp', 'patent', 'product', 'job', 'revenue_opportunity', 'cost_opportunity'
    evidence_subtype: str | None = None  # 'keyword_match', 'ai_patent', 'product_launch', etc.
    source_name: str | None = None  # 'SEC EDGAR', 'USPTO', 'GNews', 'Adzuna', 'BLS'
    source_url: str | None = None
    source_date: date | None = None
    score_contribution: float | None = None
    weight: float | None = None
    signal_strength: str | None = None  # 'high', 'medium', 'low'
    target_dimension: str | None = None  # 'cost', 'revenue', 'general'
    capture_stage: str | None = None  # 'planned', 'invested', 'realized'
    source_excerpt: str | None = None  # verbatim excerpt from source supporting this evidence
    dollar_estimate_usd: float | None = None
    dollar_year_1: float | None = None
    dollar_year_2: float | None = None
    dollar_year_3: float | None = None
    payload: dict = Field(default_factory=dict)
    observed_at: datetime | None = None
    valid_from: date | None = None
    valid_to: date | None = None


class CompanyScore(BaseModel):
    id: int | None = None
    company_id: int
    pipeline_run_id: int
    revenue_opp_score: float | None = None
    cost_opp_score: float | None = None
    composite_opp_score: float | None = None
    filing_nlp_score: float | None = None
    product_score: float | None = None
    github_score: float | None = None
    analyst_score: float | None = None
    composite_real_score: float | None = None
    cost_capture_score: float | None = None
    revenue_capture_score: float | None = None
    general_investment_score: float | None = None
    cost_roi: float | None = None
    revenue_roi: float | None = None
    combined_roi: float | None = None
    cost_opp_usd: float | None = None
    revenue_opp_usd: float | None = None
    cost_capture_usd: float | None = None
    revenue_capture_usd: float | None = None
    total_investment_usd: float | None = None
    opportunity: float
    realization: float
    quadrant: str | None = None
    quadrant_label: str | None = None
    combined_rank: int | None = None
    flags: list[str] = Field(default_factory=list)
    data_as_of: datetime
    scored_at: datetime


class PipelineRun(BaseModel):
    id: int | None = None
    run_id: str  # UUID
    task: PipelineTask
    subtask: PipelineSubtask
    run_type: str  # 'full', 'partial', 'refresh_request'
    status: str = "running"  # 'running', 'completed', 'failed'
    parameters: dict = Field(default_factory=dict)
    tickers_requested: list[str] = Field(default_factory=list)
    tickers_succeeded: int = 0
    tickers_failed: int = 0
    parent_run_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None


class RefreshRequest(BaseModel):
    id: int | None = None
    subscriber_id: int
    company_id: int
    dimensions: list[str] = Field(default_factory=lambda: ["opportunity", "realization"])
    status: str = "pending"  # 'pending', 'processing', 'completed', 'failed'
    pipeline_run_id: int | None = None
    requested_at: datetime | None = None
    completed_at: datetime | None = None


class Notification(BaseModel):
    id: int | None = None
    subscriber_id: int
    notification_type: str  # 'refresh_complete', 'score_change'
    channel: str = "email"
    subject: str | None = None
    body: str | None = None
    payload: dict = Field(default_factory=dict)
    status: str = "pending"
    created_at: datetime | None = None


class Subscriber(BaseModel):
    id: int | None = None
    email: str
    stripe_customer_id: str | None = None
    stripe_subscription_id: str | None = None
    status: str = "active"
    plan_tier: str = "standard"
    access_token: str
    created_at: datetime | None = None


class ScoreChange(BaseModel):
    id: int | None = None
    company_id: int
    dimension: str
    old_score: float | None = None
    new_score: float | None = None
    old_quadrant: str | None = None
    new_quadrant: str | None = None
    changed_at: datetime | None = None


# ── Evidence Valuation Domain Models ──────────────────────────────────────


class EvidenceGroupPassage(BaseModel):
    """A single passage linked to an evidence group."""
    id: int | None = None
    group_id: int | None = None
    evidence_id: int | None = None
    passage_text: str
    source_type: str | None = None
    source_filename: str | None = None
    source_date: date | None = None
    confidence: float | None = None
    reasoning: str | None = None
    target_dimension: str | None = None
    capture_stage: str | None = None
    source_url: str | None = None
    source_author: str | None = None


class EvidenceGroup(BaseModel):
    """A munged group of related evidence passages."""
    id: int | None = None
    company_id: int
    pipeline_run_id: int | None = None
    target_dimension: str  # cost, revenue, general
    evidence_type: str | None = None  # plan, investment, capture (set after valuation)
    passage_count: int = 0
    source_types: list[str] = Field(default_factory=list)
    date_earliest: date | None = None
    date_latest: date | None = None
    mean_confidence: float | None = None
    max_confidence: float | None = None
    representative_text: str | None = None
    passages: list[EvidenceGroupPassage] = Field(default_factory=list)


class PlanDetails(BaseModel):
    """Type-specific fields for plan evidence."""
    timeframe: str = ""
    probability: float = 0.5
    strategic_rationale: str = ""
    contingencies: str = ""
    horizon_shape: str = "s_curve"
    year_1_pct: float = 0.15
    year_2_pct: float = 0.60
    year_3_pct: float = 1.0


class InvestmentDetails(BaseModel):
    """Type-specific fields for investment evidence."""
    actual_spend_usd: float | None = None
    deployment_scope: str = ""
    completion_pct: float = 0.5
    technology_area: str = ""
    vendor_partner: str = ""
    horizon_shape: str = "linear_ramp"
    year_1_pct: float = 0.33
    year_2_pct: float = 0.66
    year_3_pct: float = 1.0


class CaptureDetails(BaseModel):
    """Type-specific fields for capture (realized value) evidence."""
    metric_name: str = ""
    metric_value_before: str = ""
    metric_value_after: str = ""
    metric_delta: str = ""
    measurement_period: str = ""
    measured_dollar_impact: float | None = None
    horizon_shape: str = "flat"
    year_1_pct: float = 1.0
    year_2_pct: float = 1.0
    year_3_pct: float = 1.0


class Valuation(BaseModel):
    """Base valuation — shared fields for all evidence types."""
    id: int | None = None
    group_id: int
    pipeline_run_id: int | None = None
    stage: str  # "preliminary" or "final"
    preliminary_id: int | None = None  # final → preliminary link
    evidence_type: str  # plan, investment, capture
    narrative: str
    confidence: float
    dollar_low: float | None = None
    dollar_high: float | None = None
    dollar_mid: float | None = None
    dollar_rationale: str = ""
    specificity: float | None = None
    magnitude: float | None = None
    stage_weight: float | None = None
    recency: float | None = None
    factor_score: float | None = None
    adjusted_from_preliminary: bool = False
    adjustment_reason: str | None = None
    prior_groups_seen: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    model_name: str | None = None
    # Type-specific detail (only one will be populated)
    plan_detail: PlanDetails | None = None
    investment_detail: InvestmentDetails | None = None
    capture_detail: CaptureDetails | None = None


class ValuationDiscrepancy(BaseModel):
    """Flagged conflict between two evidence groups."""
    id: int | None = None
    company_id: int
    pipeline_run_id: int | None = None
    group_id_a: int
    group_id_b: int
    description: str
    resolution: str
    resolution_method: str | None = None
    source_search_result: str | None = None
    trusted_group_id: int | None = None
