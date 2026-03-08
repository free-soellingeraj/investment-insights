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


class TargetDimension(str, Enum):
    """Cost, revenue, or general AI investment dimension."""
    COST = "cost"
    REVENUE = "revenue"
    GENERAL = "general"


class CaptureStage(str, Enum):
    """Maturity stage of AI evidence."""
    PLANNED = "planned"
    INVESTED = "invested"
    REALIZED = "realized"


class EvidenceSourceType(str, Enum):
    """Type of evidence source."""
    FILING_NLP = "filing_nlp"
    PATENT = "patent"
    PRODUCT = "product"
    JOB = "job"
    REVENUE_OPPORTUNITY = "revenue_opportunity"
    COST_OPPORTUNITY = "cost_opportunity"
    SUBSIDIARY_DISCOVERY = "subsidiary_discovery"
    WEB_ENRICHMENT = "web_enrichment"
    GITHUB = "github"
    ANALYST = "analyst"


class SourceType(str, Enum):
    """Type of data source for unified collection/extraction."""
    NEWS = "news"
    FILING = "filing"
    GITHUB = "github"
    ANALYST = "analyst"
    WEB_CAREERS = "web_careers"
    WEB_IR = "web_ir"
    WEB_BLOG = "web_blog"


class SourceAuthority(str, Enum):
    """How the author knows what they claim — basis of knowledge.

    Determines base credibility weighting in scoring.
    """
    FIRST_PARTY_DISCLOSURE = "first_party_disclosure"  # company's own legal filing (10-K, proxy)
    FIRST_PARTY_PUBLIC = "first_party_public"           # company's own website, blog, press release
    FIRST_PARTY_CODE = "first_party_code"               # company's own code repositories
    PROFESSIONAL_ANALYSIS = "professional_analysis"     # sell-side/buy-side analyst, rating agency
    THIRD_PARTY_JOURNALISM = "third_party_journalism"   # news article, investigative report
    AGGREGATED_CONSENSUS = "aggregated_consensus"       # consensus estimates, aggregated ratings


class ValuationEvidenceType(str, Enum):
    """Evidence type used in valuations and evidence groups."""
    PLAN = "plan"
    INVESTMENT = "investment"
    CAPTURE = "capture"


class SignalStrength(str, Enum):
    """Strength of an evidence signal."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class HorizonShape(str, Enum):
    """Shape of the value realization horizon curve."""
    FLAT = "flat"
    LINEAR_RAMP = "linear_ramp"
    S_CURVE = "s_curve"
    BACK_LOADED = "back_loaded"


class RunStatus(str, Enum):
    """Status of a pipeline run."""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class RefreshStatus(str, Enum):
    """Status of a refresh request."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class NotificationStatus(str, Enum):
    """Status of a notification."""
    PENDING = "pending"
    SENT = "sent"


class NotificationChannel(str, Enum):
    """Delivery channel for notifications."""
    EMAIL = "email"


class SubscriberStatus(str, Enum):
    """Status of a subscriber account."""
    ACTIVE = "active"
    CANCELED = "canceled"
    PAST_DUE = "past_due"


class PlanTier(str, Enum):
    """Subscription plan tier."""
    STANDARD = "standard"


class RelationshipType(str, Enum):
    """Type of corporate relationship."""
    SUBSIDIARY = "subsidiary"
    JOINT_VENTURE = "joint_venture"
    STRATEGIC_INVESTMENT = "strategic_investment"


class Quadrant(str, Enum):
    """AI opportunity/realization quadrant."""
    HIGH_OPP_HIGH_REAL = "high_opp_high_real"
    HIGH_OPP_LOW_REAL = "high_opp_low_real"
    LOW_OPP_HIGH_REAL = "low_opp_high_real"
    LOW_OPP_LOW_REAL = "low_opp_low_real"


class ValuationStage(str, Enum):
    """Stage of a valuation (preliminary vs final)."""
    PRELIMINARY = "preliminary"
    FINAL = "final"


class FinancialMetric(str, Enum):
    """Type of financial observation metric."""
    MARKET_CAP = "market_cap"
    REVENUE = "revenue"
    NET_INCOME = "net_income"
    EMPLOYEES = "employees"


class FinancialUnits(str, Enum):
    """Units for financial observation values."""
    USD = "usd"
    COUNT = "count"


class RunType(str, Enum):
    """Type of pipeline run."""
    FULL = "full"
    PARTIAL = "partial"
    REFRESH_REQUEST = "refresh_request"


class Company(BaseModel):
    id: int | None = None
    ticker: str | None = None
    slug: str | None = None
    exchange: str | None = None
    company_name: str | None = None
    cik: int | None = None
    sic: str | None = None
    naics: str | None = None
    country: str = "US"
    sector: str | None = None
    industry: str | None = None
    is_active: bool = True
    child_ticker_refs: list[int] | None = None
    canonical_company_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CompanyLinks(BaseModel):
    """Discovered URLs for a company."""
    github_url: str | None = None
    careers_url: str | None = None
    ir_url: str | None = None
    blog_url: str | None = None


class CompanyUpdate(BaseModel):
    """Payload for updating a company record. Only provided fields are applied."""
    company_name: str | None = None
    ticker: str | None = None
    exchange: str | None = None
    sector: str | None = None
    industry: str | None = None
    github_url: str | None = None
    careers_url: str | None = None
    ir_url: str | None = None
    blog_url: str | None = None


class CompanyRecord(BaseModel):
    """Full company record returned after reads/updates."""
    id: int
    ticker: str | None = None
    slug: str | None = None
    company_name: str | None = None
    exchange: str | None = None
    sector: str | None = None
    industry: str | None = None
    github_url: str | None = None
    careers_url: str | None = None
    ir_url: str | None = None
    blog_url: str | None = None
    is_active: bool = True
    child_ticker_refs: list[int] | None = None
    canonical_company_id: int | None = None
    updated_at: datetime | None = None


class CompanyVenture(BaseModel):
    """Ownership relationship between a parent and subsidiary company."""
    id: int | None = None
    parent_id: int
    subsidiary_id: int
    ownership_pct: float | None = None
    relationship_type: RelationshipType = RelationshipType.SUBSIDIARY
    notes: str | None = None


class FinancialObservation(BaseModel):
    id: int | None = None
    company_id: int
    metric: FinancialMetric
    value: float
    value_units: FinancialUnits
    source_datetime: datetime
    source_link: str | None = None
    source_name: str | None = None  # 'yahoo_finance', 'sec_10k', 'sec_10q'
    fiscal_period: str | None = None  # 'FY2024', '2024-Q3', etc.
    created_at: datetime | None = None


class AIOpportunityEvidence(BaseModel):
    id: int | None = None
    company_id: int
    pipeline_run_id: int | None = None
    evidence_type: EvidenceSourceType
    evidence_subtype: str | None = None
    source_name: str | None = None
    source_url: str | None = None
    source_date: date | None = None
    score_contribution: float | None = None
    weight: float | None = None
    signal_strength: SignalStrength | None = None
    target_dimension: TargetDimension | None = None
    capture_stage: CaptureStage | None = None
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
    ai_index_usd: float | None = None
    capture_probability: float | None = None
    opportunity_usd: float | None = None
    evidence_dollars: float | None = None
    opportunity: float
    realization: float
    quadrant: Quadrant | None = None
    quadrant_label: str | None = None  # human-readable label derived from quadrant
    combined_rank: int | None = None
    flags: list[str] = Field(default_factory=list)
    data_as_of: datetime
    scored_at: datetime


class PipelineRun(BaseModel):
    id: int | None = None
    run_id: str  # UUID
    task: PipelineTask
    subtask: PipelineSubtask
    run_type: RunType
    status: RunStatus = RunStatus.RUNNING
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
    status: RefreshStatus = RefreshStatus.PENDING
    pipeline_run_id: int | None = None
    requested_at: datetime | None = None
    completed_at: datetime | None = None


class Notification(BaseModel):
    id: int | None = None
    subscriber_id: int
    notification_type: str  # open-ended: 'refresh_complete', 'score_change', etc.
    channel: NotificationChannel = NotificationChannel.EMAIL
    subject: str | None = None
    body: str | None = None
    payload: dict = Field(default_factory=dict)
    status: NotificationStatus = NotificationStatus.PENDING
    created_at: datetime | None = None


class Subscriber(BaseModel):
    id: int | None = None
    email: str
    stripe_customer_id: str | None = None
    stripe_subscription_id: str | None = None
    status: SubscriberStatus = SubscriberStatus.ACTIVE
    plan_tier: PlanTier = PlanTier.STANDARD
    access_token: str
    created_at: datetime | None = None


class ScoreChange(BaseModel):
    id: int | None = None
    company_id: int
    dimension: str
    old_score: float | None = None
    new_score: float | None = None
    old_quadrant: Quadrant | None = None
    new_quadrant: Quadrant | None = None
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
    target_dimension: TargetDimension | None = None
    capture_stage: CaptureStage | None = None
    source_url: str | None = None
    source_author: str | None = None
    # — Provenance fields (for citation and credibility) —
    source_author_role: str | None = None
    source_author_affiliation: str | None = None
    source_publisher: str | None = None
    source_access_date: date | None = None
    source_authority: SourceAuthority | None = None


class EvidenceGroup(BaseModel):
    """A munged group of related evidence passages."""
    id: int | None = None
    company_id: int
    pipeline_run_id: int | None = None
    target_dimension: TargetDimension
    evidence_type: ValuationEvidenceType | None = None
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
    horizon_shape: HorizonShape = HorizonShape.S_CURVE
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
    horizon_shape: HorizonShape = HorizonShape.LINEAR_RAMP
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
    horizon_shape: HorizonShape = HorizonShape.FLAT
    year_1_pct: float = 1.0
    year_2_pct: float = 1.0
    year_3_pct: float = 1.0


class Valuation(BaseModel):
    """Base valuation — shared fields for all evidence types."""
    id: int | None = None
    group_id: int
    pipeline_run_id: int | None = None
    stage: ValuationStage
    preliminary_id: int | None = None  # final → preliminary link
    evidence_type: ValuationEvidenceType
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


# ── Unified Source & Evidence Models ────────────────────────────────────────


class CollectedItem(BaseModel):
    """A single raw collected item from any source.

    Captures the full provenance chain: WHO said it, WHAT they claimed,
    WHEN it was published, WHERE it appeared, WHY they have authority,
    and HOW we obtained it. Enough metadata to reconstruct a proper
    MLA citation and assess credibility even if the original URL goes dead.
    """
    item_id: str              # dedup key (URL, accession number, org+date hash)

    # — WHAT was said —
    title: str | None = None  # article title, filing type, repo name, etc.
    content: str | None = None  # raw text for extraction (article body, page text)

    # — WHO said it —
    author: str | None = None           # person or entity who authored it
    author_role: str | None = None      # role/title establishing authority
    author_affiliation: str | None = None  # organization the author belongs to

    # — WHERE it appeared (publication venue) —
    publisher: str | None = None        # publication/outlet/platform
    url: str | None = None              # direct link to the content

    # — WHEN —
    source_date: date | None = None     # publication/creation date (the as-of date)
    access_date: date | None = None     # date we retrieved/scraped this content

    # — WHY they have authority (credibility basis) —
    authority: SourceAuthority | None = None

    # — Extra source-specific fields —
    metadata: dict = Field(default_factory=dict)  # raw API response fields


class CollectionManifest(BaseModel):
    """Tracks metadata about a collection run (not the items themselves).

    Written to sources/{TICKER}/{source_type}/{YYYY}/{MM}/_manifest.json.
    Used by daily refresh to determine since_date for next incremental fetch.
    """
    ticker: str
    company_name: str | None = None
    source_type: SourceType
    collected_at: datetime
    since_date: datetime | None = None  # date range start for this run
    items_found: int = 0                # how many new items were written
    item_ids: list[str] = Field(default_factory=list)  # item_ids collected


class ExtractedPassage(BaseModel):
    """A single extracted evidence passage (shared by all sources)."""
    passage_text: str
    target_dimension: TargetDimension | str
    capture_stage: CaptureStage | str
    confidence: float
    reasoning: str


class ExtractedItem(BaseModel):
    """Extraction results for a single source item.

    Carries forward full provenance from CollectedItem so downstream
    consumers (munger, valuation, web UI) can cite sources and assess credibility.
    """
    item_id: str              # matches CollectedItem.item_id
    title: str | None = None
    url: str | None = None
    # — Provenance (copied from CollectedItem) —
    author: str | None = None
    author_role: str | None = None
    author_affiliation: str | None = None
    publisher: str | None = None
    source_date: date | None = None
    access_date: date | None = None
    authority: SourceAuthority | None = None
    passages: list[ExtractedPassage] = Field(default_factory=list)
