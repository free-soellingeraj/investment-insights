"""Database operations for the AI Opportunity Index.

All functions accept and return Pydantic domain models from ai_opportunity_index.domains.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, select, text, update, delete, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from ai_opportunity_index.config import DATABASE_URL, DB_MAX_OVERFLOW, DB_POOL_SIZE
from ai_opportunity_index.domains import (
    AIOpportunityEvidence,
    CaptureDetails,
    Company,
    CompanyScore,
    EvidenceGroup,
    EvidenceGroupPassage,
    FinancialObservation,
    InvestmentDetails,
    Notification,
    PipelineRun,
    PlanDetails,
    RefreshRequest,
    ScoreChange,
    Subscriber,
    Valuation,
    ValuationDiscrepancy,
)
from ai_opportunity_index.storage.models import (
    Base,
    CaptureDetailModel,
    CompanyModel,
    CompanyScoreModel,
    EvidenceGroupModel,
    EvidenceGroupPassageModel,
    EvidenceModel,
    FinancialObservationModel,
    InvestmentDetailModel,
    NotificationModel,
    PipelineRunModel,
    PlanDetailModel,
    RefreshRequestModel,
    ScoreChangeModel,
    SubscriberModel,
    ValuationDiscrepancyModel,
    ValuationModel,
)

logger = logging.getLogger(__name__)

_engine = None
_SessionLocal = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            echo=False,
            pool_size=DB_POOL_SIZE,
            max_overflow=DB_MAX_OVERFLOW,
        )
    return _engine


def get_session() -> Session:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine())
    return _SessionLocal()


def init_db():
    """Create all tables if they don't exist (dev convenience — use Alembic in prod)."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("Database initialized at %s", DATABASE_URL)


# ── Company CRUD ──────────────────────────────────────────────────────────


def upsert_company(data: Company | dict) -> Company:
    """Insert or update a company record."""
    if isinstance(data, dict):
        data = Company(**{k: v for k, v in data.items() if v is not None and k != "id"})

    session = get_session()
    try:
        existing = session.execute(
            select(CompanyModel).where(CompanyModel.ticker == data.ticker)
        ).scalar_one_or_none()

        if existing is None:
            model = CompanyModel(
                ticker=data.ticker,
                exchange=data.exchange,
                company_name=data.company_name,
                cik=data.cik,
                sic=data.sic,
                naics=data.naics,
                country=data.country,
                sector=data.sector,
                industry=data.industry,
                is_active=data.is_active,
            )
            session.add(model)
        else:
            model = existing
            for field in [
                "exchange", "company_name", "cik", "sic", "naics", "country",
                "sector", "industry", "is_active",
            ]:
                val = getattr(data, field)
                if val is not None:
                    setattr(model, field, val)

        session.commit()
        session.refresh(model)
        return _company_from_model(model)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def upsert_company_in_session(session: Session, data: dict) -> CompanyModel:
    """Insert or update a company within an existing session (for bulk ops)."""
    existing = session.execute(
        select(CompanyModel).where(CompanyModel.ticker == data["ticker"])
    ).scalar_one_or_none()

    if existing is None:
        model = CompanyModel(**{k: v for k, v in data.items() if k != "id"})
        session.add(model)
    else:
        model = existing
        for key, value in data.items():
            if key != "id" and value is not None:
                setattr(model, key, value)
    return model


def upsert_companies_bulk(df: pd.DataFrame) -> int:
    """Insert or update companies from a DataFrame."""
    session = get_session()
    count = 0
    try:
        for _, row in df.iterrows():
            data = {k: v for k, v in row.to_dict().items() if pd.notna(v)}
            if "ticker" not in data:
                continue
            upsert_company_in_session(session, data)
            count += 1

            if count % 500 == 0:
                session.commit()
                logger.info("Upserted %d companies", count)

        session.commit()
        logger.info("Total companies upserted: %d", count)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return count


def get_company_by_ticker(ticker: str) -> Company | None:
    """Look up a company by ticker."""
    session = get_session()
    try:
        model = session.execute(
            select(CompanyModel).where(CompanyModel.ticker == ticker.upper())
        ).scalar_one_or_none()
        return _company_from_model(model) if model else None
    finally:
        session.close()


def get_company_model_by_ticker(session: Session, ticker: str) -> CompanyModel | None:
    """Look up a CompanyModel by ticker within a session."""
    return session.execute(
        select(CompanyModel).where(CompanyModel.ticker == ticker.upper())
    ).scalar_one_or_none()


# ── Evidence ──────────────────────────────────────────────────────────────


def save_evidence(evidence: AIOpportunityEvidence) -> AIOpportunityEvidence:
    """Save a single evidence row."""
    session = get_session()
    try:
        model = EvidenceModel(
            company_id=evidence.company_id,
            pipeline_run_id=evidence.pipeline_run_id,
            evidence_type=evidence.evidence_type,
            evidence_subtype=evidence.evidence_subtype,
            source_name=evidence.source_name,
            source_url=evidence.source_url,
            source_date=evidence.source_date,
            score_contribution=evidence.score_contribution,
            weight=evidence.weight,
            signal_strength=evidence.signal_strength,
            target_dimension=evidence.target_dimension,
            capture_stage=evidence.capture_stage,
            source_excerpt=evidence.source_excerpt,
            payload=evidence.payload,
            valid_from=evidence.valid_from,
            valid_to=evidence.valid_to,
        )
        session.add(model)
        session.commit()
        session.refresh(model)
        return _evidence_from_model(model)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def save_evidence_batch(items: list[AIOpportunityEvidence], session: Session | None = None):
    """Bulk insert evidence rows."""
    own_session = session is None
    if own_session:
        session = get_session()
    try:
        for ev in items:
            model = EvidenceModel(
                company_id=ev.company_id,
                pipeline_run_id=ev.pipeline_run_id,
                evidence_type=ev.evidence_type,
                evidence_subtype=ev.evidence_subtype,
                source_name=ev.source_name,
                source_url=ev.source_url,
                source_date=ev.source_date,
                score_contribution=ev.score_contribution,
                weight=ev.weight,
                signal_strength=ev.signal_strength,
                target_dimension=ev.target_dimension,
                capture_stage=ev.capture_stage,
                source_excerpt=ev.source_excerpt,
                payload=ev.payload,
                valid_from=ev.valid_from,
                valid_to=ev.valid_to,
            )
            session.add(model)
        if own_session:
            session.commit()
    except Exception:
        if own_session:
            session.rollback()
        raise
    finally:
        if own_session:
            session.close()


def get_evidence_for_company(
    company_id: int, evidence_type: str | None = None
) -> list[AIOpportunityEvidence]:
    """Get evidence for a company, optionally filtered by type."""
    session = get_session()
    try:
        stmt = select(EvidenceModel).where(EvidenceModel.company_id == company_id)
        if evidence_type:
            stmt = stmt.where(EvidenceModel.evidence_type == evidence_type)
        stmt = stmt.order_by(EvidenceModel.observed_at.desc())
        models = session.execute(stmt).scalars().all()
        return [_evidence_from_model(m) for m in models]
    finally:
        session.close()


def delete_evidence_for_company(company_id: int, session: Session | None = None):
    """Remove all evidence for a company (called before re-scoring)."""
    own_session = session is None
    if own_session:
        session = get_session()
    try:
        result = session.execute(
            delete(EvidenceModel).where(EvidenceModel.company_id == company_id)
        )
        if own_session:
            session.commit()
        logger.info("Deleted %d evidence rows for company_id=%d", result.rowcount, company_id)
    except Exception:
        if own_session:
            session.rollback()
        raise
    finally:
        if own_session:
            session.close()


def delete_evidence_for_run(
    company_id: int, pipeline_run_id: int, evidence_type: str, session: Session | None = None
):
    """Delete evidence for a specific company/run/type before re-scoring."""
    own_session = session is None
    if own_session:
        session = get_session()
    try:
        session.execute(
            delete(EvidenceModel).where(
                EvidenceModel.company_id == company_id,
                EvidenceModel.pipeline_run_id == pipeline_run_id,
                EvidenceModel.evidence_type == evidence_type,
            )
        )
        if own_session:
            session.commit()
    except Exception:
        if own_session:
            session.rollback()
        raise
    finally:
        if own_session:
            session.close()


# ── Scores ────────────────────────────────────────────────────────────────


def save_company_score(score: CompanyScore, session: Session | None = None) -> CompanyScore:
    """Save a unified company score."""
    own_session = session is None
    if own_session:
        session = get_session()
    try:
        model = CompanyScoreModel(
            company_id=score.company_id,
            pipeline_run_id=score.pipeline_run_id,
            revenue_opp_score=score.revenue_opp_score,
            cost_opp_score=score.cost_opp_score,
            composite_opp_score=score.composite_opp_score,
            filing_nlp_score=score.filing_nlp_score,
            product_score=score.product_score,
            github_score=score.github_score,
            analyst_score=score.analyst_score,
            composite_real_score=score.composite_real_score,
            cost_capture_score=score.cost_capture_score,
            revenue_capture_score=score.revenue_capture_score,
            general_investment_score=score.general_investment_score,
            cost_roi=score.cost_roi,
            revenue_roi=score.revenue_roi,
            combined_roi=score.combined_roi,
            opportunity=score.opportunity,
            realization=score.realization,
            quadrant=score.quadrant,
            quadrant_label=score.quadrant_label,
            combined_rank=score.combined_rank,
            cost_opp_usd=score.cost_opp_usd,
            revenue_opp_usd=score.revenue_opp_usd,
            cost_capture_usd=score.cost_capture_usd,
            revenue_capture_usd=score.revenue_capture_usd,
            total_investment_usd=score.total_investment_usd,
            flags=score.flags,
            data_as_of=score.data_as_of,
            scored_at=score.scored_at,
        )
        session.add(model)
        if own_session:
            session.commit()
            session.refresh(model)
        result = _score_from_model(model)
        return result
    except Exception:
        if own_session:
            session.rollback()
        raise
    finally:
        if own_session:
            session.close()


def get_latest_score(company_id: int) -> CompanyScore | None:
    """Get the most recent score for a company."""
    session = get_session()
    try:
        model = session.execute(
            select(CompanyScoreModel)
            .where(CompanyScoreModel.company_id == company_id)
            .order_by(CompanyScoreModel.scored_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        return _score_from_model(model) if model else None
    finally:
        session.close()


def get_latest_scores(
    sector: str | None = None,
    quadrant: str | None = None,
    industry: str | None = None,
    sort_by: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Dashboard query — uses materialized view for fast reads."""
    session = get_session()
    try:
        params: dict = {}
        filters = []

        if sector:
            filters.append("sector = :sector")
            params["sector"] = sector
        if quadrant:
            filters.append("quadrant = :quadrant")
            params["quadrant"] = quadrant
        if industry:
            filters.append("industry = :industry")
            params["industry"] = industry

        # Validate sort_by to prevent SQL injection
        allowed_sort = {
            "opportunity", "realization", "cost_capture_score", "revenue_capture_score",
            "general_investment_score", "cost_roi", "revenue_roi", "combined_roi",
            "revenue_opp_score", "cost_opp_score", "ticker", "company_name",
        }
        order_col = sort_by if sort_by in allowed_sort else "opportunity"
        order_dir = "DESC" if order_col != "ticker" and order_col != "company_name" else "ASC"

        where_clause = " AND ".join(filters)
        if where_clause:
            sql = f"SELECT * FROM latest_company_scores WHERE {where_clause} ORDER BY {order_col} {order_dir} NULLS LAST LIMIT :limit OFFSET :offset"
        else:
            sql = f"SELECT * FROM latest_company_scores ORDER BY {order_col} {order_dir} NULLS LAST LIMIT :limit OFFSET :offset"

        params["limit"] = limit
        params["offset"] = offset

        result = session.execute(text(sql), params)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]
    finally:
        session.close()


def get_industry_peers(ticker: str, limit: int = 20) -> list[dict]:
    """Get companies in the same industry as the given ticker for peer comparison."""
    session = get_session()
    try:
        # First, get the company's industry
        company = session.execute(
            select(CompanyModel).where(CompanyModel.ticker == ticker.upper())
        ).scalar_one_or_none()

        if not company or not company.industry:
            return []

        result = session.execute(
            text(
                "SELECT * FROM latest_company_scores WHERE industry = :industry "
                "ORDER BY opportunity DESC LIMIT :limit"
            ),
            {"industry": company.industry, "limit": limit},
        )
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]
    finally:
        session.close()


def get_full_index() -> pd.DataFrame:
    """Retrieve the full index as a DataFrame (backwards compat for portfolio computation)."""
    session = get_session()
    try:
        # Use materialized view if available, fall back to join
        try:
            df = pd.read_sql(
                text("SELECT * FROM latest_company_scores"),
                session.bind,
            )
            return df
        except Exception:
            # Fall back to manual join if materialized view doesn't exist
            latest = (
                session.query(
                    CompanyScoreModel.company_id,
                    func.max(CompanyScoreModel.scored_at).label("max_scored"),
                )
                .group_by(CompanyScoreModel.company_id)
                .subquery()
            )

            query = (
                session.query(
                    CompanyModel.ticker,
                    CompanyModel.company_name,
                    CompanyModel.sic,
                    CompanyModel.exchange,
                    CompanyModel.sector,
                    CompanyModel.industry,
                    CompanyScoreModel.opportunity,
                    CompanyScoreModel.realization,
                    CompanyScoreModel.quadrant,
                    CompanyScoreModel.quadrant_label,
                    CompanyScoreModel.scored_at,
                )
                .join(CompanyScoreModel, CompanyModel.id == CompanyScoreModel.company_id)
                .join(
                    latest,
                    (CompanyScoreModel.company_id == latest.c.company_id)
                    & (CompanyScoreModel.scored_at == latest.c.max_scored),
                )
            )
            df = pd.read_sql(query.statement, session.bind)
            return df
    finally:
        session.close()


def refresh_latest_scores_view():
    """Refresh the materialized view after a scoring run."""
    session = get_session()
    try:
        session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY latest_company_scores"))
        session.commit()
        logger.info("Refreshed latest_company_scores materialized view")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── Pipeline Runs ─────────────────────────────────────────────────────────


def create_pipeline_run(run: PipelineRun) -> PipelineRun:
    """Create a new pipeline run record."""
    session = get_session()
    try:
        model = PipelineRunModel(
            run_id=run.run_id,
            task=run.task,
            subtask=run.subtask,
            run_type=run.run_type,
            status=run.status,
            parameters=run.parameters,
            tickers_requested=run.tickers_requested,
            tickers_succeeded=run.tickers_succeeded,
            tickers_failed=run.tickers_failed,
            parent_run_id=run.parent_run_id,
        )
        session.add(model)
        session.commit()
        session.refresh(model)
        return _pipeline_run_from_model(model)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def complete_pipeline_run(
    run_id: str,
    status: str = "completed",
    tickers_succeeded: int = 0,
    tickers_failed: int = 0,
    error_message: str | None = None,
):
    """Mark a pipeline run as completed or failed."""
    session = get_session()
    try:
        values: dict = {
            "status": status,
            "tickers_succeeded": tickers_succeeded,
            "tickers_failed": tickers_failed,
            "completed_at": datetime.utcnow(),
        }
        if error_message is not None:
            values["error_message"] = error_message
        session.execute(
            update(PipelineRunModel)
            .where(PipelineRunModel.run_id == run_id)
            .values(**values)
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_pipeline_run(run_id: str) -> PipelineRun | None:
    """Look up a pipeline run by its UUID run_id."""
    session = get_session()
    try:
        model = session.execute(
            select(PipelineRunModel).where(PipelineRunModel.run_id == run_id)
        ).scalar_one_or_none()
        return _pipeline_run_from_model(model) if model else None
    finally:
        session.close()


def get_pipeline_runs(
    task: str | None = None,
    subtask: str | None = None,
    limit: int = 10,
) -> list[PipelineRun]:
    """List pipeline runs, optionally filtered by task and/or subtask."""
    session = get_session()
    try:
        stmt = select(PipelineRunModel).order_by(PipelineRunModel.started_at.desc())
        if task:
            stmt = stmt.where(PipelineRunModel.task == task)
        if subtask:
            stmt = stmt.where(PipelineRunModel.subtask == subtask)
        stmt = stmt.limit(limit)
        models = session.execute(stmt).scalars().all()
        return [_pipeline_run_from_model(m) for m in models]
    finally:
        session.close()


# ── Refresh Requests ──────────────────────────────────────────────────────


def create_refresh_request(req: RefreshRequest) -> RefreshRequest:
    """Create a refresh request."""
    session = get_session()
    try:
        model = RefreshRequestModel(
            subscriber_id=req.subscriber_id,
            company_id=req.company_id,
            dimensions=req.dimensions,
            status=req.status,
        )
        session.add(model)
        session.commit()
        session.refresh(model)
        return _refresh_request_from_model(model)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_pending_refresh_requests(limit: int = 10) -> list[RefreshRequest]:
    """Get pending refresh requests ordered by requested_at."""
    session = get_session()
    try:
        models = (
            session.execute(
                select(RefreshRequestModel)
                .where(RefreshRequestModel.status == "pending")
                .order_by(RefreshRequestModel.requested_at)
                .limit(limit)
            )
            .scalars()
            .all()
        )
        return [_refresh_request_from_model(m) for m in models]
    finally:
        session.close()


def update_refresh_request_status(
    request_id: int, status: str, pipeline_run_id: int | None = None
):
    """Update a refresh request's status."""
    session = get_session()
    try:
        values: dict = {"status": status}
        if pipeline_run_id is not None:
            values["pipeline_run_id"] = pipeline_run_id
        if status in ("completed", "failed"):
            values["completed_at"] = datetime.utcnow()

        session.execute(
            update(RefreshRequestModel)
            .where(RefreshRequestModel.id == request_id)
            .values(**values)
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── Notifications ─────────────────────────────────────────────────────────


def create_notification(notif: Notification):
    """Create a notification."""
    session = get_session()
    try:
        model = NotificationModel(
            subscriber_id=notif.subscriber_id,
            notification_type=notif.notification_type,
            channel=notif.channel,
            subject=notif.subject,
            body=notif.body,
            payload=notif.payload,
            status=notif.status,
        )
        session.add(model)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_pending_notifications(limit: int = 50) -> list[Notification]:
    """Get pending notifications."""
    session = get_session()
    try:
        models = (
            session.execute(
                select(NotificationModel)
                .where(NotificationModel.status == "pending")
                .order_by(NotificationModel.created_at)
                .limit(limit)
            )
            .scalars()
            .all()
        )
        return [_notification_from_model(m) for m in models]
    finally:
        session.close()


def mark_notification_sent(notification_id: int):
    """Mark a notification as sent."""
    session = get_session()
    try:
        session.execute(
            update(NotificationModel)
            .where(NotificationModel.id == notification_id)
            .values(status="sent")
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── Score Changes ─────────────────────────────────────────────────────────


def save_score_change(change: ScoreChange):
    """Record a score change."""
    session = get_session()
    try:
        model = ScoreChangeModel(
            company_id=change.company_id,
            dimension=change.dimension,
            old_score=change.old_score,
            new_score=change.new_score,
            old_quadrant=change.old_quadrant,
            new_quadrant=change.new_quadrant,
        )
        session.add(model)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── Subscribers ───────────────────────────────────────────────────────────


def create_subscriber(
    email: str, stripe_customer_id: str | None, stripe_subscription_id: str | None
) -> str:
    """Create or update a subscriber, returning the access token."""
    session = get_session()
    try:
        existing = session.execute(
            select(SubscriberModel).where(SubscriberModel.email == email)
        ).scalar_one_or_none()

        if existing:
            existing.stripe_customer_id = stripe_customer_id
            existing.stripe_subscription_id = stripe_subscription_id
            existing.status = "active"
            session.commit()
            return existing.access_token

        token = uuid.uuid4().hex
        subscriber = SubscriberModel(
            email=email,
            stripe_customer_id=stripe_customer_id,
            stripe_subscription_id=stripe_subscription_id,
            status="active",
            access_token=token,
        )
        session.add(subscriber)
        session.commit()
        return token
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_subscriber_by_token(token: str) -> Subscriber | None:
    """Look up a subscriber by access token."""
    session = get_session()
    try:
        model = session.execute(
            select(SubscriberModel).where(SubscriberModel.access_token == token)
        ).scalar_one_or_none()
        return _subscriber_from_model(model) if model else None
    finally:
        session.close()


def get_subscriber_by_email(email: str) -> Subscriber | None:
    """Look up a subscriber by email."""
    session = get_session()
    try:
        model = session.execute(
            select(SubscriberModel).where(SubscriberModel.email == email)
        ).scalar_one_or_none()
        return _subscriber_from_model(model) if model else None
    finally:
        session.close()


def update_subscriber_status(stripe_subscription_id: str, new_status: str):
    """Update subscriber status by Stripe subscription ID."""
    session = get_session()
    try:
        model = session.execute(
            select(SubscriberModel).where(
                SubscriberModel.stripe_subscription_id == stripe_subscription_id
            )
        ).scalar_one_or_none()
        if model:
            model.status = new_status
            session.commit()
            logger.info("Subscriber %s status -> %s", model.email, new_status)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_company_detail(ticker: str) -> dict | None:
    """Get full detail for a single company including latest scores and evidence."""
    session = get_session()
    try:
        company = session.execute(
            select(CompanyModel).where(CompanyModel.ticker == ticker.upper())
        ).scalar_one_or_none()
        if not company:
            return None

        # Latest score
        score = session.execute(
            select(CompanyScoreModel)
            .where(CompanyScoreModel.company_id == company.id)
            .order_by(CompanyScoreModel.scored_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        # All evidence
        evidence_models = (
            session.execute(
                select(EvidenceModel)
                .where(EvidenceModel.company_id == company.id)
                .order_by(EvidenceModel.observed_at.desc())
            )
            .scalars()
            .all()
        )

        evidence_by_type: dict[str, list] = {}
        for ev in evidence_models:
            evidence_by_type.setdefault(ev.evidence_type, []).append({
                "id": ev.id,
                "subtype": ev.evidence_subtype,
                "source_name": ev.source_name,
                "source_url": ev.source_url,
                "source_date": str(ev.source_date) if ev.source_date else None,
                "score_contribution": ev.score_contribution,
                "weight": ev.weight,
                "signal_strength": ev.signal_strength,
                "target_dimension": ev.target_dimension,
                "capture_stage": ev.capture_stage,
                "source_excerpt": ev.source_excerpt,
                "dollar_estimate_usd": ev.dollar_estimate_usd,
                "dollar_year_1": ev.dollar_year_1,
                "dollar_year_2": ev.dollar_year_2,
                "dollar_year_3": ev.dollar_year_3,
                "payload": ev.payload,
                "valid_from": str(ev.valid_from) if ev.valid_from else None,
                "valid_to": str(ev.valid_to) if ev.valid_to else None,
                "pipeline_run_id": ev.pipeline_run_id,
            })

        # Fetch latest financial observations
        latest_financials = _get_latest_financials_in_session(session, company.id)

        financials_dict = {}
        for metric, obs_model in latest_financials.items():
            financials_dict[metric] = {
                "value": obs_model.value,
                "value_units": obs_model.value_units,
                "source_datetime": str(obs_model.source_datetime) if obs_model.source_datetime else None,
                "source_name": obs_model.source_name,
                "source_link": obs_model.source_link,
                "fiscal_period": obs_model.fiscal_period,
            }

        return {
            "ticker": company.ticker,
            "company_name": company.company_name,
            "exchange": company.exchange,
            "sic": company.sic,
            "sector": company.sector,
            "industry": company.industry,
            "financials": financials_dict,
            "scores": {
                "revenue_opp": score.revenue_opp_score if score else None,
                "cost_opp": score.cost_opp_score if score else None,
                "composite_opp": score.composite_opp_score if score else None,
                "filing_nlp": score.filing_nlp_score if score else None,
                "product": score.product_score if score else None,
                "github": score.github_score if score else None,
                "analyst": score.analyst_score if score else None,
                "composite_real": score.composite_real_score if score else None,
                "cost_capture": score.cost_capture_score if score else None,
                "revenue_capture": score.revenue_capture_score if score else None,
                "general_investment": score.general_investment_score if score else None,
                "cost_roi": score.cost_roi if score else None,
                "revenue_roi": score.revenue_roi if score else None,
                "combined_roi": score.combined_roi if score else None,
                "opportunity": score.opportunity if score else None,
                "realization": score.realization if score else None,
                "quadrant": score.quadrant if score else None,
                "quadrant_label": score.quadrant_label if score else None,
                "cost_opp_usd": score.cost_opp_usd if score else None,
                "revenue_opp_usd": score.revenue_opp_usd if score else None,
                "cost_capture_usd": score.cost_capture_usd if score else None,
                "revenue_capture_usd": score.revenue_capture_usd if score else None,
                "total_investment_usd": score.total_investment_usd if score else None,
                "data_as_of": str(score.data_as_of) if score else None,
                "scored_at": str(score.scored_at) if score else None,
            },
            "evidence": evidence_by_type,
        }
    finally:
        session.close()


# ── Financial Observations ─────────────────────────────────────────────────


def save_financial_observation(obs: FinancialObservation) -> FinancialObservation:
    """Save a single financial observation."""
    session = get_session()
    try:
        model = FinancialObservationModel(
            company_id=obs.company_id,
            metric=obs.metric,
            value=obs.value,
            value_units=obs.value_units,
            source_datetime=obs.source_datetime,
            source_link=obs.source_link,
            source_name=obs.source_name,
            fiscal_period=obs.fiscal_period,
        )
        session.add(model)
        session.commit()
        session.refresh(model)
        return _financial_observation_from_model(model)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def save_financial_observations_batch(items: list[FinancialObservation]):
    """Bulk insert financial observation rows."""
    session = get_session()
    try:
        for obs in items:
            model = FinancialObservationModel(
                company_id=obs.company_id,
                metric=obs.metric,
                value=obs.value,
                value_units=obs.value_units,
                source_datetime=obs.source_datetime,
                source_link=obs.source_link,
                source_name=obs.source_name,
                fiscal_period=obs.fiscal_period,
            )
            session.add(model)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_latest_financial(company_id: int, metric: str) -> FinancialObservation | None:
    """Get the most recent observation for a company+metric."""
    session = get_session()
    try:
        model = session.execute(
            select(FinancialObservationModel)
            .where(
                FinancialObservationModel.company_id == company_id,
                FinancialObservationModel.metric == metric,
            )
            .order_by(FinancialObservationModel.source_datetime.desc())
            .limit(1)
        ).scalar_one_or_none()
        return _financial_observation_from_model(model) if model else None
    finally:
        session.close()


def get_latest_financials(company_id: int) -> dict[str, FinancialObservation]:
    """Get the latest observation per metric for a company.

    Returns {'market_cap': obs, 'revenue': obs, ...}.
    """
    session = get_session()
    try:
        result = _get_latest_financials_in_session(session, company_id)
        return {
            metric: _financial_observation_from_model(model)
            for metric, model in result.items()
        }
    finally:
        session.close()


def _get_latest_financials_in_session(
    session: Session, company_id: int
) -> dict[str, FinancialObservationModel]:
    """Internal helper: get latest financial observation models within an existing session."""
    # Subquery for max source_datetime per metric
    latest_sub = (
        select(
            FinancialObservationModel.metric,
            func.max(FinancialObservationModel.source_datetime).label("max_dt"),
        )
        .where(FinancialObservationModel.company_id == company_id)
        .group_by(FinancialObservationModel.metric)
        .subquery()
    )

    models = (
        session.execute(
            select(FinancialObservationModel)
            .join(
                latest_sub,
                (FinancialObservationModel.metric == latest_sub.c.metric)
                & (FinancialObservationModel.source_datetime == latest_sub.c.max_dt),
            )
            .where(FinancialObservationModel.company_id == company_id)
        )
        .scalars()
        .all()
    )
    return {m.metric: m for m in models}


def get_financial_history(
    company_id: int, metric: str, limit: int = 20
) -> list[FinancialObservation]:
    """Get historical observations for a company+metric."""
    session = get_session()
    try:
        models = (
            session.execute(
                select(FinancialObservationModel)
                .where(
                    FinancialObservationModel.company_id == company_id,
                    FinancialObservationModel.metric == metric,
                )
                .order_by(FinancialObservationModel.source_datetime.desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )
        return [_financial_observation_from_model(m) for m in models]
    finally:
        session.close()


# ── Model Converters ──────────────────────────────────────────────────────


def _company_from_model(m: CompanyModel) -> Company:
    return Company(
        id=m.id,
        ticker=m.ticker,
        exchange=m.exchange,
        company_name=m.company_name,
        cik=m.cik,
        sic=m.sic,
        naics=m.naics,
        country=m.country,
        sector=m.sector,
        industry=m.industry,
        is_active=m.is_active,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


def _financial_observation_from_model(m: FinancialObservationModel) -> FinancialObservation:
    return FinancialObservation(
        id=m.id,
        company_id=m.company_id,
        metric=m.metric,
        value=m.value,
        value_units=m.value_units,
        source_datetime=m.source_datetime,
        source_link=m.source_link,
        source_name=m.source_name,
        fiscal_period=m.fiscal_period,
        created_at=m.created_at,
    )


def _evidence_from_model(m: EvidenceModel) -> AIOpportunityEvidence:
    return AIOpportunityEvidence(
        id=m.id,
        company_id=m.company_id,
        pipeline_run_id=m.pipeline_run_id,
        evidence_type=m.evidence_type,
        evidence_subtype=m.evidence_subtype,
        source_name=m.source_name,
        source_url=m.source_url,
        source_date=m.source_date,
        score_contribution=m.score_contribution,
        weight=m.weight,
        signal_strength=m.signal_strength,
        target_dimension=m.target_dimension,
        capture_stage=m.capture_stage,
        source_excerpt=m.source_excerpt,
        payload=m.payload or {},
        observed_at=m.observed_at,
        valid_from=m.valid_from,
        valid_to=m.valid_to,
    )


def _score_from_model(m: CompanyScoreModel) -> CompanyScore:
    return CompanyScore(
        id=m.id,
        company_id=m.company_id,
        pipeline_run_id=m.pipeline_run_id,
        revenue_opp_score=m.revenue_opp_score,
        cost_opp_score=m.cost_opp_score,
        composite_opp_score=m.composite_opp_score,
        filing_nlp_score=m.filing_nlp_score,
        product_score=m.product_score,
        github_score=m.github_score,
        analyst_score=m.analyst_score,
        composite_real_score=m.composite_real_score,
        cost_capture_score=m.cost_capture_score,
        revenue_capture_score=m.revenue_capture_score,
        general_investment_score=m.general_investment_score,
        cost_roi=m.cost_roi,
        revenue_roi=m.revenue_roi,
        combined_roi=m.combined_roi,
        cost_opp_usd=m.cost_opp_usd,
        revenue_opp_usd=m.revenue_opp_usd,
        cost_capture_usd=m.cost_capture_usd,
        revenue_capture_usd=m.revenue_capture_usd,
        total_investment_usd=m.total_investment_usd,
        opportunity=m.opportunity,
        realization=m.realization,
        quadrant=m.quadrant,
        quadrant_label=m.quadrant_label,
        combined_rank=m.combined_rank,
        flags=m.flags or [],
        data_as_of=m.data_as_of,
        scored_at=m.scored_at,
    )


def _pipeline_run_from_model(m: PipelineRunModel) -> PipelineRun:
    return PipelineRun(
        id=m.id,
        run_id=m.run_id,
        task=m.task,
        subtask=m.subtask,
        run_type=m.run_type,
        status=m.status,
        parameters=m.parameters or {},
        tickers_requested=m.tickers_requested or [],
        tickers_succeeded=m.tickers_succeeded,
        tickers_failed=m.tickers_failed,
        parent_run_id=m.parent_run_id,
        started_at=m.started_at,
        completed_at=m.completed_at,
        error_message=m.error_message,
    )


def _refresh_request_from_model(m: RefreshRequestModel) -> RefreshRequest:
    return RefreshRequest(
        id=m.id,
        subscriber_id=m.subscriber_id,
        company_id=m.company_id,
        dimensions=m.dimensions or [],
        status=m.status,
        pipeline_run_id=m.pipeline_run_id,
        requested_at=m.requested_at,
        completed_at=m.completed_at,
    )


def _notification_from_model(m: NotificationModel) -> Notification:
    return Notification(
        id=m.id,
        subscriber_id=m.subscriber_id,
        notification_type=m.notification_type,
        channel=m.channel,
        subject=m.subject,
        body=m.body,
        payload=m.payload or {},
        status=m.status,
        created_at=m.created_at,
    )


def _subscriber_from_model(m: SubscriberModel) -> Subscriber:
    return Subscriber(
        id=m.id,
        email=m.email,
        stripe_customer_id=m.stripe_customer_id,
        stripe_subscription_id=m.stripe_subscription_id,
        status=m.status,
        plan_tier=m.plan_tier,
        access_token=m.access_token,
        created_at=m.created_at,
    )


# ── Evidence Valuation CRUD ──────────────────────────────────────────────


def save_evidence_group(
    group: EvidenceGroup, session: Session | None = None,
) -> EvidenceGroup:
    """Save an evidence group and its passages. Returns domain model with id set."""
    s = session or get_session()
    try:
        m = EvidenceGroupModel(
            company_id=group.company_id,
            pipeline_run_id=group.pipeline_run_id,
            target_dimension=group.target_dimension,
            evidence_type=group.evidence_type,
            passage_count=group.passage_count,
            source_types=group.source_types,
            date_earliest=group.date_earliest,
            date_latest=group.date_latest,
            mean_confidence=group.mean_confidence,
            max_confidence=group.max_confidence,
            representative_text=group.representative_text,
        )
        s.add(m)
        s.flush()

        for p in group.passages:
            pm = EvidenceGroupPassageModel(
                group_id=m.id,
                evidence_id=p.evidence_id,
                passage_text=p.passage_text,
                source_type=p.source_type,
                source_filename=p.source_filename,
                source_date=p.source_date,
                confidence=p.confidence,
                reasoning=p.reasoning,
                target_dimension=p.target_dimension,
                capture_stage=p.capture_stage,
                source_url=p.source_url,
                source_author=p.source_author,
            )
            s.add(pm)

        if session is None:
            s.commit()

        group.id = m.id
        return group
    except Exception:
        if session is None:
            s.rollback()
        raise
    finally:
        if session is None:
            s.close()


def save_valuation(
    val: Valuation, session: Session | None = None,
) -> Valuation:
    """Save a valuation and its type-specific detail. Returns domain model with id set."""
    s = session or get_session()
    try:
        # Compute dollar_mid
        dollar_mid = None
        if val.dollar_low is not None and val.dollar_high is not None:
            dollar_mid = (val.dollar_low + val.dollar_high) / 2.0
        elif val.dollar_mid is not None:
            dollar_mid = val.dollar_mid

        m = ValuationModel(
            group_id=val.group_id,
            pipeline_run_id=val.pipeline_run_id,
            stage=val.stage,
            preliminary_id=val.preliminary_id,
            evidence_type=val.evidence_type,
            narrative=val.narrative,
            confidence=val.confidence,
            dollar_low=val.dollar_low,
            dollar_high=val.dollar_high,
            dollar_mid=dollar_mid,
            dollar_rationale=val.dollar_rationale,
            specificity=val.specificity,
            magnitude=val.magnitude,
            stage_weight=val.stage_weight,
            recency=val.recency,
            factor_score=val.factor_score,
            adjusted_from_preliminary=val.adjusted_from_preliminary,
            adjustment_reason=val.adjustment_reason,
            prior_groups_seen=val.prior_groups_seen,
            input_tokens=val.input_tokens,
            output_tokens=val.output_tokens,
            model_name=val.model_name,
        )
        s.add(m)
        s.flush()

        # Save type-specific detail
        if val.evidence_type == "plan" and val.plan_detail:
            d = val.plan_detail
            s.add(PlanDetailModel(
                valuation_id=m.id,
                timeframe=d.timeframe,
                probability=d.probability,
                strategic_rationale=d.strategic_rationale,
                contingencies=d.contingencies,
                horizon_shape=d.horizon_shape,
                year_1_pct=d.year_1_pct,
                year_2_pct=d.year_2_pct,
                year_3_pct=d.year_3_pct,
            ))
        elif val.evidence_type == "investment" and val.investment_detail:
            d = val.investment_detail
            s.add(InvestmentDetailModel(
                valuation_id=m.id,
                actual_spend_usd=d.actual_spend_usd,
                deployment_scope=d.deployment_scope,
                completion_pct=d.completion_pct,
                technology_area=d.technology_area,
                vendor_partner=d.vendor_partner,
                horizon_shape=d.horizon_shape,
                year_1_pct=d.year_1_pct,
                year_2_pct=d.year_2_pct,
                year_3_pct=d.year_3_pct,
            ))
        elif val.evidence_type == "capture" and val.capture_detail:
            d = val.capture_detail
            s.add(CaptureDetailModel(
                valuation_id=m.id,
                metric_name=d.metric_name,
                metric_value_before=d.metric_value_before,
                metric_value_after=d.metric_value_after,
                metric_delta=d.metric_delta,
                measurement_period=d.measurement_period,
                measured_dollar_impact=d.measured_dollar_impact,
                horizon_shape=d.horizon_shape,
                year_1_pct=d.year_1_pct,
                year_2_pct=d.year_2_pct,
                year_3_pct=d.year_3_pct,
            ))

        if session is None:
            s.commit()

        val.id = m.id
        val.dollar_mid = dollar_mid
        return val
    except Exception:
        if session is None:
            s.rollback()
        raise
    finally:
        if session is None:
            s.close()


def save_valuation_discrepancy(
    disc: ValuationDiscrepancy, session: Session | None = None,
) -> ValuationDiscrepancy:
    """Save a valuation discrepancy record."""
    s = session or get_session()
    try:
        m = ValuationDiscrepancyModel(
            company_id=disc.company_id,
            pipeline_run_id=disc.pipeline_run_id,
            group_id_a=disc.group_id_a,
            group_id_b=disc.group_id_b,
            description=disc.description,
            resolution=disc.resolution,
            resolution_method=disc.resolution_method,
            source_search_result=disc.source_search_result,
            trusted_group_id=disc.trusted_group_id,
        )
        s.add(m)
        if session is None:
            s.commit()
        disc.id = m.id
        return disc
    except Exception:
        if session is None:
            s.rollback()
        raise
    finally:
        if session is None:
            s.close()


def get_evidence_groups_for_company(
    company_id: int,
    pipeline_run_id: int | None = None,
    session: Session | None = None,
) -> list[EvidenceGroup]:
    """Get all evidence groups for a company, optionally filtered by pipeline run."""
    s = session or get_session()
    try:
        q = s.query(EvidenceGroupModel).filter(
            EvidenceGroupModel.company_id == company_id
        )
        if pipeline_run_id is not None:
            q = q.filter(EvidenceGroupModel.pipeline_run_id == pipeline_run_id)
        q = q.order_by(EvidenceGroupModel.id)

        groups = []
        for m in q.all():
            passages = [
                EvidenceGroupPassage(
                    id=p.id,
                    group_id=p.group_id,
                    evidence_id=p.evidence_id,
                    passage_text=p.passage_text,
                    source_type=p.source_type,
                    source_filename=p.source_filename,
                    source_date=p.source_date,
                    confidence=p.confidence,
                    reasoning=p.reasoning,
                    target_dimension=p.target_dimension,
                    capture_stage=p.capture_stage,
                    source_url=p.source_url,
                    source_author=p.source_author,
                )
                for p in m.passages
            ]
            groups.append(EvidenceGroup(
                id=m.id,
                company_id=m.company_id,
                pipeline_run_id=m.pipeline_run_id,
                target_dimension=m.target_dimension,
                evidence_type=m.evidence_type,
                passage_count=m.passage_count,
                source_types=m.source_types or [],
                date_earliest=m.date_earliest,
                date_latest=m.date_latest,
                mean_confidence=m.mean_confidence,
                max_confidence=m.max_confidence,
                representative_text=m.representative_text,
                passages=passages,
            ))
        return groups
    finally:
        if session is None:
            s.close()


def get_final_valuations_for_company(
    company_id: int,
    pipeline_run_id: int | None = None,
    session: Session | None = None,
) -> list[Valuation]:
    """Get all final-stage valuations for a company."""
    s = session or get_session()
    try:
        q = (
            s.query(ValuationModel)
            .join(EvidenceGroupModel)
            .filter(
                EvidenceGroupModel.company_id == company_id,
                ValuationModel.stage == "final",
            )
        )
        if pipeline_run_id is not None:
            q = q.filter(ValuationModel.pipeline_run_id == pipeline_run_id)
        q = q.order_by(ValuationModel.id)

        valuations = []
        for m in q.all():
            val = _valuation_from_model(m)
            valuations.append(val)
        return valuations
    finally:
        if session is None:
            s.close()


def delete_evidence_groups_for_company(
    company_id: int,
    pipeline_run_id: int | None = None,
    session: Session | None = None,
) -> int:
    """Delete evidence groups (and cascading valuations/passages) for a company.

    Deletes valuation_discrepancies first since they reference evidence_groups
    without ON DELETE CASCADE.

    Returns the number of groups deleted.
    """
    s = session or get_session()
    try:
        # Delete discrepancies first (they reference groups without CASCADE)
        disc_q = s.query(ValuationDiscrepancyModel).filter(
            ValuationDiscrepancyModel.company_id == company_id
        )
        if pipeline_run_id is not None:
            disc_q = disc_q.filter(ValuationDiscrepancyModel.pipeline_run_id == pipeline_run_id)
        disc_q.delete(synchronize_session="fetch")

        # Now delete groups (valuations + passages cascade via ON DELETE CASCADE)
        q = s.query(EvidenceGroupModel).filter(
            EvidenceGroupModel.company_id == company_id
        )
        if pipeline_run_id is not None:
            q = q.filter(EvidenceGroupModel.pipeline_run_id == pipeline_run_id)
        count = q.delete(synchronize_session="fetch")
        if session is None:
            s.commit()
        return count
    except Exception:
        if session is None:
            s.rollback()
        raise
    finally:
        if session is None:
            s.close()


def get_company_valuation_detail(ticker: str) -> dict | None:
    """Get structured valuation data for a company's evidence viewer.

    Returns dict organized by dimension -> groups -> valuation + passages,
    with dimension aggregates and pipeline summary counts.
    """
    session = get_session()
    try:
        company = session.execute(
            select(CompanyModel).where(CompanyModel.ticker == ticker.upper())
        ).scalar_one_or_none()
        if not company:
            return None

        # Load evidence groups with passages
        group_models = (
            session.query(EvidenceGroupModel)
            .filter(EvidenceGroupModel.company_id == company.id)
            .order_by(EvidenceGroupModel.id)
            .all()
        )
        if not group_models:
            return None

        # Load final valuations keyed by group_id
        val_models = (
            session.query(ValuationModel)
            .join(EvidenceGroupModel)
            .filter(
                EvidenceGroupModel.company_id == company.id,
                ValuationModel.stage == "final",
            )
            .all()
        )
        val_by_group: dict[int, ValuationModel] = {}
        for vm in val_models:
            val_by_group[vm.group_id] = vm

        # Load discrepancies
        disc_models = (
            session.query(ValuationDiscrepancyModel)
            .filter(ValuationDiscrepancyModel.company_id == company.id)
            .all()
        )
        # Index discrepancies by group_id (either side)
        disc_by_group: dict[int, list] = {}
        for dm in disc_models:
            for gid in (dm.group_id_a, dm.group_id_b):
                disc_by_group.setdefault(gid, []).append({
                    "id": dm.id,
                    "group_id_a": dm.group_id_a,
                    "group_id_b": dm.group_id_b,
                    "description": dm.description,
                    "resolution": dm.resolution,
                    "resolution_method": dm.resolution_method,
                    "trusted_group_id": dm.trusted_group_id,
                })

        # Build structured output by dimension
        dimensions: dict[str, dict] = {}
        total_passages = 0
        total_groups = 0
        type_counts = {"plan": 0, "investment": 0, "capture": 0}

        for gm in group_models:
            dim = gm.target_dimension or "general"
            if dim not in dimensions:
                dimensions[dim] = {
                    "groups": [],
                    "raw_sum": 0.0,
                    "potential_usd": 0.0,
                    "actual_usd": 0.0,
                }

            total_groups += 1
            total_passages += gm.passage_count or 0

            # Passages
            passages = []
            for p in gm.passages:
                passages.append({
                    "id": p.id,
                    "passage_text": p.passage_text,
                    "source_type": p.source_type,
                    "source_filename": p.source_filename,
                    "source_date": str(p.source_date) if p.source_date else None,
                    "confidence": p.confidence,
                    "reasoning": p.reasoning,
                    "target_dimension": p.target_dimension,
                    "capture_stage": p.capture_stage,
                    "source_url": p.source_url,
                    "source_author": p.source_author,
                    "scraped_at": p.created_at.isoformat() if p.created_at else None,
                })

            # Valuation for this group
            vm = val_by_group.get(gm.id)
            valuation = None
            if vm:
                ev_type = vm.evidence_type or ""
                if ev_type in type_counts:
                    type_counts[ev_type] += 1

                # Type-specific detail
                type_detail = None
                if vm.evidence_type == "plan" and vm.plan_detail:
                    d = vm.plan_detail
                    type_detail = {
                        "timeframe": d.timeframe,
                        "probability": d.probability,
                        "strategic_rationale": d.strategic_rationale,
                        "contingencies": d.contingencies,
                        "horizon_shape": d.horizon_shape,
                        "year_1_pct": d.year_1_pct,
                        "year_2_pct": d.year_2_pct,
                        "year_3_pct": d.year_3_pct,
                    }
                elif vm.evidence_type == "investment" and vm.investment_detail:
                    d = vm.investment_detail
                    type_detail = {
                        "actual_spend_usd": d.actual_spend_usd,
                        "deployment_scope": d.deployment_scope,
                        "completion_pct": d.completion_pct,
                        "technology_area": d.technology_area,
                        "vendor_partner": d.vendor_partner,
                        "horizon_shape": d.horizon_shape,
                        "year_1_pct": d.year_1_pct,
                        "year_2_pct": d.year_2_pct,
                        "year_3_pct": d.year_3_pct,
                    }
                elif vm.evidence_type == "capture" and vm.capture_detail:
                    d = vm.capture_detail
                    type_detail = {
                        "metric_name": d.metric_name,
                        "metric_value_before": d.metric_value_before,
                        "metric_value_after": d.metric_value_after,
                        "metric_delta": d.metric_delta,
                        "measurement_period": d.measurement_period,
                        "measured_dollar_impact": d.measured_dollar_impact,
                        "horizon_shape": d.horizon_shape,
                        "year_1_pct": d.year_1_pct,
                        "year_2_pct": d.year_2_pct,
                        "year_3_pct": d.year_3_pct,
                    }

                valuation = {
                    "id": vm.id,
                    "evidence_type": vm.evidence_type,
                    "valued_at": vm.created_at.isoformat() if vm.created_at else None,
                    "narrative": vm.narrative,
                    "confidence": vm.confidence,
                    "dollar_low": vm.dollar_low,
                    "dollar_high": vm.dollar_high,
                    "dollar_mid": vm.dollar_mid,
                    "dollar_rationale": vm.dollar_rationale,
                    "specificity": vm.specificity,
                    "magnitude": vm.magnitude,
                    "stage_weight": vm.stage_weight,
                    "recency": vm.recency,
                    "factor_score": vm.factor_score,
                    "adjusted_from_preliminary": vm.adjusted_from_preliminary,
                    "adjustment_reason": vm.adjustment_reason,
                    "type_detail": type_detail,
                }

                # Aggregate into dimension
                if vm.factor_score is not None:
                    dimensions[dim]["raw_sum"] += vm.factor_score
                if vm.dollar_high is not None:
                    dimensions[dim]["potential_usd"] += vm.dollar_high
                if vm.dollar_mid is not None:
                    dimensions[dim]["actual_usd"] += vm.dollar_mid

            # Discrepancies for this group
            discrepancies = disc_by_group.get(gm.id, [])

            group_dict = {
                "id": gm.id,
                "target_dimension": dim,
                "evidence_type": gm.evidence_type,
                "passage_count": gm.passage_count,
                "source_types": gm.source_types or [],
                "date_earliest": str(gm.date_earliest) if gm.date_earliest else None,
                "date_latest": str(gm.date_latest) if gm.date_latest else None,
                "mean_confidence": gm.mean_confidence,
                "max_confidence": gm.max_confidence,
                "representative_text": gm.representative_text,
                "grouped_at": gm.created_at.isoformat() if gm.created_at else None,
                "valuation": valuation,
                "passages": passages,
                "discrepancies": discrepancies,
            }
            dimensions[dim]["groups"].append(group_dict)

        # Compute dimension scores (normalize raw_sum)
        for dim_data in dimensions.values():
            groups = dim_data["groups"]
            if groups:
                factor_scores = [
                    g["valuation"]["factor_score"]
                    for g in groups
                    if g["valuation"] and g["valuation"]["factor_score"] is not None
                ]
                dim_data["dimension_score"] = (
                    sum(factor_scores) / len(factor_scores) if factor_scores else 0.0
                )
                dim_data["group_count"] = len(groups)
            else:
                dim_data["dimension_score"] = 0.0
                dim_data["group_count"] = 0

        return {
            "ticker": ticker.upper(),
            "total_passages": total_passages,
            "total_groups": total_groups,
            "type_counts": type_counts,
            "dimensions": dimensions,
        }
    finally:
        session.close()


def _valuation_from_model(m: ValuationModel) -> Valuation:
    """Convert a ValuationModel to a Valuation domain model."""
    plan_detail = None
    investment_detail = None
    capture_detail = None

    if m.evidence_type == "plan" and m.plan_detail:
        d = m.plan_detail
        plan_detail = PlanDetails(
            timeframe=d.timeframe or "",
            probability=d.probability or 0.5,
            strategic_rationale=d.strategic_rationale or "",
            contingencies=d.contingencies or "",
            horizon_shape=d.horizon_shape or "s_curve",
            year_1_pct=d.year_1_pct or 0.15,
            year_2_pct=d.year_2_pct or 0.60,
            year_3_pct=d.year_3_pct or 1.0,
        )
    elif m.evidence_type == "investment" and m.investment_detail:
        d = m.investment_detail
        investment_detail = InvestmentDetails(
            actual_spend_usd=d.actual_spend_usd,
            deployment_scope=d.deployment_scope or "",
            completion_pct=d.completion_pct or 0.5,
            technology_area=d.technology_area or "",
            vendor_partner=d.vendor_partner or "",
            horizon_shape=d.horizon_shape or "linear_ramp",
            year_1_pct=d.year_1_pct or 0.33,
            year_2_pct=d.year_2_pct or 0.66,
            year_3_pct=d.year_3_pct or 1.0,
        )
    elif m.evidence_type == "capture" and m.capture_detail:
        d = m.capture_detail
        capture_detail = CaptureDetails(
            metric_name=d.metric_name or "",
            metric_value_before=d.metric_value_before or "",
            metric_value_after=d.metric_value_after or "",
            metric_delta=d.metric_delta or "",
            measurement_period=d.measurement_period or "",
            measured_dollar_impact=d.measured_dollar_impact,
            horizon_shape=d.horizon_shape or "flat",
            year_1_pct=d.year_1_pct if d.year_1_pct is not None else 1.0,
            year_2_pct=d.year_2_pct if d.year_2_pct is not None else 1.0,
            year_3_pct=d.year_3_pct if d.year_3_pct is not None else 1.0,
        )

    return Valuation(
        id=m.id,
        group_id=m.group_id,
        pipeline_run_id=m.pipeline_run_id,
        stage=m.stage,
        preliminary_id=m.preliminary_id,
        evidence_type=m.evidence_type,
        narrative=m.narrative,
        confidence=m.confidence,
        dollar_low=m.dollar_low,
        dollar_high=m.dollar_high,
        dollar_mid=m.dollar_mid,
        dollar_rationale=m.dollar_rationale or "",
        specificity=m.specificity,
        magnitude=m.magnitude,
        stage_weight=m.stage_weight,
        recency=m.recency,
        factor_score=m.factor_score,
        adjusted_from_preliminary=m.adjusted_from_preliminary,
        adjustment_reason=m.adjustment_reason,
        prior_groups_seen=m.prior_groups_seen or 0,
        input_tokens=m.input_tokens or 0,
        output_tokens=m.output_tokens or 0,
        model_name=m.model_name,
        plan_detail=plan_detail,
        investment_detail=investment_detail,
        capture_detail=capture_detail,
    )
