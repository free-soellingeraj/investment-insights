"""SQLAlchemy 2.0 models for the AI Opportunity Index database.

Maps to Pydantic domain models in ai_opportunity_index.domains.
"""

from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from ai_opportunity_index.domains import (
    CaptureStage,
    EvidenceSourceType,
    FinancialMetric,
    FinancialUnits,
    HorizonShape,
    NotificationChannel,
    NotificationStatus,
    PlanTier,
    PipelineSubtask,
    PipelineTask,
    Quadrant,
    RefreshStatus,
    RelationshipType,
    RunStatus,
    RunType,
    SignalStrength,
    SourceAuthority,
    SubscriberStatus,
    TargetDimension,
    ValuationEvidenceType,
    ValuationStage,
)


class Base(DeclarativeBase):
    pass


class CompanyModel(Base):
    __tablename__ = "companies"
    __table_args__ = (
        Index(
            "uq_companies_ticker_exchange", "ticker", "exchange",
            unique=True,
            postgresql_where=text("ticker IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    slug: Mapped[str] = mapped_column(String(50), nullable=False, unique=True, index=True)
    exchange: Mapped[str | None] = mapped_column(String(20))
    company_name: Mapped[str | None] = mapped_column(String(500))
    cik: Mapped[int | None] = mapped_column(Integer, index=True)
    sic: Mapped[str | None] = mapped_column(String(10))
    naics: Mapped[str | None] = mapped_column(String(10))
    country: Mapped[str] = mapped_column(String(50), default="US")
    sector: Mapped[str | None] = mapped_column(String(100))
    industry: Mapped[str | None] = mapped_column(String(200))
    github_url: Mapped[str | None] = mapped_column(String(2048))
    careers_url: Mapped[str | None] = mapped_column(String(2048))
    ir_url: Mapped[str | None] = mapped_column(String(2048))
    blog_url: Mapped[str | None] = mapped_column(String(2048))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    child_ticker_refs: Mapped[list[int] | None] = mapped_column(
        ARRAY(Integer), nullable=True
    )
    canonical_company_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=True
    )
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    evidence: Mapped[list["EvidenceModel"]] = relationship(back_populates="company")
    scores: Mapped[list["CompanyScoreModel"]] = relationship(back_populates="company")
    financial_observations: Mapped[list["FinancialObservationModel"]] = relationship(
        back_populates="company"
    )
    refresh_requests: Mapped[list["RefreshRequestModel"]] = relationship(
        back_populates="company"
    )
    score_changes: Mapped[list["ScoreChangeModel"]] = relationship(
        back_populates="company"
    )
    ventures_as_parent: Mapped[list["CompanyVentureModel"]] = relationship(
        foreign_keys="CompanyVentureModel.parent_id", back_populates="parent"
    )
    ventures_as_subsidiary: Mapped[list["CompanyVentureModel"]] = relationship(
        foreign_keys="CompanyVentureModel.subsidiary_id", back_populates="subsidiary"
    )


class CompanyVentureModel(Base):
    __tablename__ = "company_ventures"
    __table_args__ = (
        UniqueConstraint("parent_id", "subsidiary_id", name="uq_company_ventures"),
        Index("ix_cv_parent", "parent_id"),
        Index("ix_cv_subsidiary", "subsidiary_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parent_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=False
    )
    subsidiary_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=False
    )
    ownership_pct: Mapped[float | None] = mapped_column(Float)
    relationship_type: Mapped[RelationshipType] = mapped_column(String(50), default="subsidiary")
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )

    parent: Mapped["CompanyModel"] = relationship(
        foreign_keys=[parent_id], back_populates="ventures_as_parent"
    )
    subsidiary: Mapped["CompanyModel"] = relationship(
        foreign_keys=[subsidiary_id], back_populates="ventures_as_subsidiary"
    )


class FinancialObservationModel(Base):
    __tablename__ = "financial_observations"
    __table_args__ = (
        Index("ix_finobs_company_metric", "company_id", "metric"),
        Index("ix_finobs_company_metric_date", "company_id", "metric", "source_datetime"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=False, index=True
    )
    metric: Mapped[FinancialMetric] = mapped_column(String(50), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    value_units: Mapped[FinancialUnits] = mapped_column(String(30), nullable=False)
    source_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    source_link: Mapped[str | None] = mapped_column(Text)
    source_name: Mapped[str | None] = mapped_column(String(100))
    fiscal_period: Mapped[str | None] = mapped_column(String(20))
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )

    company: Mapped["CompanyModel"] = relationship(back_populates="financial_observations")


class EvidenceModel(Base):
    __tablename__ = "evidence"
    __table_args__ = (
        Index("ix_evidence_company_type", "company_id", "evidence_type"),
        Index("ix_evidence_payload", "payload", postgresql_using="gin"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=False, index=True
    )
    pipeline_run_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("pipeline_runs.id")
    )
    evidence_type: Mapped[EvidenceSourceType] = mapped_column(String(50), nullable=False)
    evidence_subtype: Mapped[str | None] = mapped_column(String(100))
    source_name: Mapped[str | None] = mapped_column(String(100))
    source_url: Mapped[str | None] = mapped_column(Text)
    source_date: Mapped[date | None] = mapped_column(Date)
    score_contribution: Mapped[float | None] = mapped_column(Float)
    weight: Mapped[float | None] = mapped_column(Float)
    signal_strength: Mapped[SignalStrength | None] = mapped_column(String(20))
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    observed_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )
    target_dimension: Mapped[TargetDimension | None] = mapped_column(String(20))
    capture_stage: Mapped[CaptureStage | None] = mapped_column(String(20))
    source_excerpt: Mapped[str | None] = mapped_column(Text)  # verbatim excerpt from source
    source_author: Mapped[str | None] = mapped_column(String(200))
    source_publisher: Mapped[str | None] = mapped_column(String(200))
    source_access_date: Mapped[date | None] = mapped_column(Date)
    source_authority: Mapped[str | None] = mapped_column(String(50))
    dollar_estimate_usd: Mapped[float | None] = mapped_column(Float)
    dollar_year_1: Mapped[float | None] = mapped_column(Float)
    dollar_year_2: Mapped[float | None] = mapped_column(Float)
    dollar_year_3: Mapped[float | None] = mapped_column(Float)
    valid_from: Mapped[date | None] = mapped_column(Date)
    valid_to: Mapped[date | None] = mapped_column(Date)

    company: Mapped["CompanyModel"] = relationship(back_populates="evidence")
    pipeline_run: Mapped["PipelineRunModel | None"] = relationship(
        back_populates="evidence"
    )


class CompanyScoreModel(Base):
    __tablename__ = "company_scores"
    __table_args__ = (
        UniqueConstraint(
            "company_id", "pipeline_run_id", name="uq_company_scores_company_run"
        ),
        Index("ix_company_scores_company_scored", "company_id", "scored_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=False, index=True
    )
    pipeline_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pipeline_runs.id"), nullable=False
    )
    revenue_opp_score: Mapped[float | None] = mapped_column(Float)
    cost_opp_score: Mapped[float | None] = mapped_column(Float)
    composite_opp_score: Mapped[float | None] = mapped_column(Float)
    filing_nlp_score: Mapped[float | None] = mapped_column(Float)
    product_score: Mapped[float | None] = mapped_column(Float)
    github_score: Mapped[float | None] = mapped_column(Float)
    analyst_score: Mapped[float | None] = mapped_column(Float)
    composite_real_score: Mapped[float | None] = mapped_column(Float)
    cost_capture_score: Mapped[float | None] = mapped_column(Float)
    revenue_capture_score: Mapped[float | None] = mapped_column(Float)
    general_investment_score: Mapped[float | None] = mapped_column(Float)
    cost_roi: Mapped[float | None] = mapped_column(Float)
    revenue_roi: Mapped[float | None] = mapped_column(Float)
    combined_roi: Mapped[float | None] = mapped_column(Float)
    opportunity: Mapped[float] = mapped_column(Float, nullable=False)
    realization: Mapped[float] = mapped_column(Float, nullable=False)
    quadrant: Mapped[Quadrant | None] = mapped_column(String(50))
    quadrant_label: Mapped[str | None] = mapped_column(String(100))
    combined_rank: Mapped[int | None] = mapped_column(Integer)
    cost_opp_usd: Mapped[float | None] = mapped_column(Float)
    revenue_opp_usd: Mapped[float | None] = mapped_column(Float)
    cost_capture_usd: Mapped[float | None] = mapped_column(Float)
    revenue_capture_usd: Mapped[float | None] = mapped_column(Float)
    total_investment_usd: Mapped[float | None] = mapped_column(Float)
    ai_index_usd: Mapped[float | None] = mapped_column(Float)
    capture_probability: Mapped[float | None] = mapped_column(Float)
    opportunity_usd: Mapped[float | None] = mapped_column(Float)
    evidence_dollars: Mapped[float | None] = mapped_column(Float)
    flags: Mapped[list] = mapped_column(ARRAY(String), default=list)
    data_as_of: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    scored_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=func.now()
    )

    company: Mapped["CompanyModel"] = relationship(back_populates="scores")
    pipeline_run: Mapped["PipelineRunModel"] = relationship(back_populates="scores")


class PipelineRunModel(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(50), nullable=False, unique=True, index=True
    )
    task: Mapped[PipelineTask] = mapped_column(String(10), nullable=False)
    subtask: Mapped[PipelineSubtask] = mapped_column(String(25), nullable=False)
    run_type: Mapped[RunType] = mapped_column(String(30), nullable=False)
    status: Mapped[RunStatus] = mapped_column(String(20), default="running")
    parameters: Mapped[dict | None] = mapped_column(JSONB, default=dict)
    tickers_requested: Mapped[list | None] = mapped_column(ARRAY(String), default=list)
    tickers_succeeded: Mapped[int] = mapped_column(Integer, default=0)
    tickers_failed: Mapped[int] = mapped_column(Integer, default=0)
    parent_run_id: Mapped[str | None] = mapped_column(String(50))
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    error_message: Mapped[str | None] = mapped_column(Text)

    evidence: Mapped[list["EvidenceModel"]] = relationship(
        back_populates="pipeline_run"
    )
    scores: Mapped[list["CompanyScoreModel"]] = relationship(
        back_populates="pipeline_run"
    )


class RefreshRequestModel(Base):
    __tablename__ = "refresh_requests"
    __table_args__ = (
        Index(
            "ix_refresh_requests_pending",
            "status",
            "requested_at",
            postgresql_where=text("status = 'pending'"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False
    )
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=False
    )
    dimensions: Mapped[list] = mapped_column(
        ARRAY(String), default=lambda: ["opportunity", "realization"]
    )
    status: Mapped[RefreshStatus] = mapped_column(String(20), default="pending")
    pipeline_run_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("pipeline_runs.id")
    )
    requested_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)

    subscriber: Mapped["SubscriberModel"] = relationship(
        back_populates="refresh_requests"
    )
    company: Mapped["CompanyModel"] = relationship(back_populates="refresh_requests")


class NotificationModel(Base):
    __tablename__ = "notifications"
    __table_args__ = (
        Index(
            "ix_notifications_pending",
            "status",
            "created_at",
            postgresql_where=text("status = 'pending'"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False
    )
    notification_type: Mapped[str] = mapped_column(String(50), nullable=False)
    channel: Mapped[NotificationChannel] = mapped_column(String(20), default="email")
    subject: Mapped[str | None] = mapped_column(String(500))
    body: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    status: Mapped[NotificationStatus] = mapped_column(String(20), default="pending")
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )

    subscriber: Mapped["SubscriberModel"] = relationship(
        back_populates="notifications"
    )


class SubscriberModel(Base):
    __tablename__ = "subscribers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    stripe_customer_id: Mapped[str | None] = mapped_column(String(255))
    stripe_subscription_id: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[SubscriberStatus] = mapped_column(String(20), default="active")
    plan_tier: Mapped[PlanTier] = mapped_column(String(30), default="standard")
    access_token: Mapped[str] = mapped_column(
        String(64), unique=True, nullable=False, index=True
    )
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )

    refresh_requests: Mapped[list["RefreshRequestModel"]] = relationship(
        back_populates="subscriber"
    )
    notifications: Mapped[list["NotificationModel"]] = relationship(
        back_populates="subscriber"
    )


class EvidenceGroupModel(Base):
    __tablename__ = "evidence_groups"
    __table_args__ = (
        Index("ix_eg_company", "company_id"),
        Index("ix_eg_company_run", "company_id", "pipeline_run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=False
    )
    pipeline_run_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("pipeline_runs.id")
    )
    target_dimension: Mapped[TargetDimension] = mapped_column(String(20), nullable=False)
    evidence_type: Mapped[ValuationEvidenceType | None] = mapped_column(String(20))
    passage_count: Mapped[int] = mapped_column(Integer, default=0)
    source_types: Mapped[list] = mapped_column(ARRAY(String(20)), default=list)
    date_earliest: Mapped[date | None] = mapped_column(Date)
    date_latest: Mapped[date | None] = mapped_column(Date)
    mean_confidence: Mapped[float | None] = mapped_column(Float)
    max_confidence: Mapped[float | None] = mapped_column(Float)
    representative_text: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    company: Mapped["CompanyModel"] = relationship()
    passages: Mapped[list["EvidenceGroupPassageModel"]] = relationship(
        back_populates="group", cascade="all, delete-orphan"
    )
    valuations: Mapped[list["ValuationModel"]] = relationship(
        back_populates="group", cascade="all, delete-orphan"
    )


class EvidenceGroupPassageModel(Base):
    __tablename__ = "evidence_group_passages"
    __table_args__ = (
        Index("ix_egp_group", "group_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("evidence_groups.id", ondelete="CASCADE"), nullable=False
    )
    evidence_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("evidence.id")
    )
    passage_text: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str | None] = mapped_column(String(50))
    source_filename: Mapped[str | None] = mapped_column(String(255))
    source_date: Mapped[date | None] = mapped_column(Date)
    confidence: Mapped[float | None] = mapped_column(Float)
    reasoning: Mapped[str | None] = mapped_column(Text)
    target_dimension: Mapped[TargetDimension | None] = mapped_column(String(20))
    capture_stage: Mapped[CaptureStage | None] = mapped_column(String(20))
    source_url: Mapped[str | None] = mapped_column(Text)
    source_author: Mapped[str | None] = mapped_column(String(255))
    source_author_role: Mapped[str | None] = mapped_column(String(200))
    source_author_affiliation: Mapped[str | None] = mapped_column(String(200))
    source_publisher: Mapped[str | None] = mapped_column(String(200))
    source_access_date: Mapped[date | None] = mapped_column(Date)
    source_authority: Mapped[str | None] = mapped_column(String(50))
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )

    group: Mapped["EvidenceGroupModel"] = relationship(back_populates="passages")


class ValuationModel(Base):
    __tablename__ = "valuations"
    __table_args__ = (
        Index("ix_val_group", "group_id"),
        Index("ix_val_group_stage", "group_id", "stage"),
        UniqueConstraint("group_id", "pipeline_run_id", "stage", name="uq_val_group_run_stage"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("evidence_groups.id", ondelete="CASCADE"), nullable=False
    )
    pipeline_run_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("pipeline_runs.id")
    )
    stage: Mapped[ValuationStage] = mapped_column(String(20), nullable=False)
    preliminary_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("valuations.id")
    )
    evidence_type: Mapped[ValuationEvidenceType] = mapped_column(String(20), nullable=False)
    narrative: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    dollar_low: Mapped[float | None] = mapped_column(Float)
    dollar_high: Mapped[float | None] = mapped_column(Float)
    dollar_mid: Mapped[float | None] = mapped_column(Float)
    dollar_rationale: Mapped[str | None] = mapped_column(Text)
    specificity: Mapped[float | None] = mapped_column(Float)
    magnitude: Mapped[float | None] = mapped_column(Float)
    stage_weight: Mapped[float | None] = mapped_column(Float)
    recency: Mapped[float | None] = mapped_column(Float)
    factor_score: Mapped[float | None] = mapped_column(Float)
    adjusted_from_preliminary: Mapped[bool] = mapped_column(Boolean, default=False)
    adjustment_reason: Mapped[str | None] = mapped_column(Text)
    prior_groups_seen: Mapped[int] = mapped_column(Integer, default=0)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    model_name: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    group: Mapped["EvidenceGroupModel"] = relationship(back_populates="valuations")
    plan_detail: Mapped["PlanDetailModel | None"] = relationship(
        back_populates="valuation", cascade="all, delete-orphan", uselist=False
    )
    investment_detail: Mapped["InvestmentDetailModel | None"] = relationship(
        back_populates="valuation", cascade="all, delete-orphan", uselist=False
    )
    capture_detail: Mapped["CaptureDetailModel | None"] = relationship(
        back_populates="valuation", cascade="all, delete-orphan", uselist=False
    )


class PlanDetailModel(Base):
    __tablename__ = "plan_details"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    valuation_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("valuations.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    timeframe: Mapped[str | None] = mapped_column(String(50))
    probability: Mapped[float | None] = mapped_column(Float)
    strategic_rationale: Mapped[str | None] = mapped_column(Text)
    contingencies: Mapped[str | None] = mapped_column(Text)
    horizon_shape: Mapped[HorizonShape | None] = mapped_column(String(20))
    year_1_pct: Mapped[float | None] = mapped_column(Float)
    year_2_pct: Mapped[float | None] = mapped_column(Float)
    year_3_pct: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )

    valuation: Mapped["ValuationModel"] = relationship(back_populates="plan_detail")


class InvestmentDetailModel(Base):
    __tablename__ = "investment_details"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    valuation_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("valuations.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    actual_spend_usd: Mapped[float | None] = mapped_column(Float)
    deployment_scope: Mapped[str | None] = mapped_column(Text)
    completion_pct: Mapped[float | None] = mapped_column(Float)
    technology_area: Mapped[str | None] = mapped_column(Text)
    vendor_partner: Mapped[str | None] = mapped_column(String(200))
    horizon_shape: Mapped[HorizonShape | None] = mapped_column(String(20))
    year_1_pct: Mapped[float | None] = mapped_column(Float)
    year_2_pct: Mapped[float | None] = mapped_column(Float)
    year_3_pct: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )

    valuation: Mapped["ValuationModel"] = relationship(back_populates="investment_detail")


class CaptureDetailModel(Base):
    __tablename__ = "capture_details"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    valuation_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("valuations.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    metric_name: Mapped[str | None] = mapped_column(String(200))
    metric_value_before: Mapped[str | None] = mapped_column(Text)
    metric_value_after: Mapped[str | None] = mapped_column(Text)
    metric_delta: Mapped[str | None] = mapped_column(Text)
    measurement_period: Mapped[str | None] = mapped_column(Text)
    measured_dollar_impact: Mapped[float | None] = mapped_column(Float)
    horizon_shape: Mapped[HorizonShape | None] = mapped_column(String(20), default="flat")
    year_1_pct: Mapped[float | None] = mapped_column(Float, default=1.0)
    year_2_pct: Mapped[float | None] = mapped_column(Float, default=1.0)
    year_3_pct: Mapped[float | None] = mapped_column(Float, default=1.0)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )

    valuation: Mapped["ValuationModel"] = relationship(back_populates="capture_detail")


class ValuationDiscrepancyModel(Base):
    __tablename__ = "valuation_discrepancies"
    __table_args__ = (
        Index("ix_disc_company", "company_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=False
    )
    pipeline_run_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("pipeline_runs.id")
    )
    group_id_a: Mapped[int] = mapped_column(
        Integer, ForeignKey("evidence_groups.id"), nullable=False
    )
    group_id_b: Mapped[int] = mapped_column(
        Integer, ForeignKey("evidence_groups.id"), nullable=False
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    resolution: Mapped[str] = mapped_column(Text, nullable=False)
    resolution_method: Mapped[str | None] = mapped_column(String(50))
    source_search_result: Mapped[str | None] = mapped_column(Text)
    trusted_group_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("evidence_groups.id")
    )
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )


class ScoreChangeModel(Base):
    __tablename__ = "score_change_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("companies.id"), nullable=False, index=True
    )
    dimension: Mapped[str] = mapped_column(String(30), nullable=False)
    old_score: Mapped[float | None] = mapped_column(Float)
    new_score: Mapped[float | None] = mapped_column(Float)
    old_quadrant: Mapped[Quadrant | None] = mapped_column(String(50))
    new_quadrant: Mapped[Quadrant | None] = mapped_column(String(50))
    changed_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now()
    )

    company: Mapped["CompanyModel"] = relationship(back_populates="score_changes")


# SQL for materialized view — run after initial migration
LATEST_COMPANY_SCORES_VIEW = """
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_company_scores AS
SELECT DISTINCT ON (c.id)
    c.id AS company_id,
    c.ticker,
    c.exchange,
    c.company_name,
    c.sector,
    c.industry,
    c.is_active,
    cs.id AS score_id,
    cs.pipeline_run_id,
    cs.revenue_opp_score,
    cs.cost_opp_score,
    cs.composite_opp_score,
    cs.filing_nlp_score,
    cs.product_score,
    cs.composite_real_score,
    cs.cost_capture_score,
    cs.revenue_capture_score,
    cs.general_investment_score,
    cs.cost_roi,
    cs.revenue_roi,
    cs.combined_roi,
    cs.cost_opp_usd,
    cs.revenue_opp_usd,
    cs.cost_capture_usd,
    cs.revenue_capture_usd,
    cs.total_investment_usd,
    cs.ai_index_usd,
    cs.capture_probability,
    cs.opportunity_usd,
    cs.evidence_dollars,
    cs.opportunity,
    cs.realization,
    cs.quadrant,
    cs.quadrant_label,
    cs.combined_rank,
    cs.flags,
    cs.data_as_of,
    cs.scored_at
FROM companies c
JOIN company_scores cs ON cs.company_id = c.id
WHERE c.is_active = true
ORDER BY c.id, cs.scored_at DESC;

CREATE UNIQUE INDEX IF NOT EXISTS ix_latest_company_scores_company_id
    ON latest_company_scores (company_id);
CREATE INDEX IF NOT EXISTS ix_latest_company_scores_ticker
    ON latest_company_scores (ticker);
"""
