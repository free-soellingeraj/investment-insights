"""Initial schema — all tables for AI Opportunity Index v2.

Revision ID: 001
Revises:
Create Date: 2026-02-21
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # subscribers (must be created before refresh_requests which references it)
    op.create_table(
        "subscribers",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("stripe_customer_id", sa.String(255)),
        sa.Column("stripe_subscription_id", sa.String(255)),
        sa.Column("status", sa.String(20), server_default="active"),
        sa.Column("plan_tier", sa.String(30), server_default="standard"),
        sa.Column("access_token", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
        sa.UniqueConstraint("access_token"),
    )
    op.create_index("ix_subscribers_email", "subscribers", ["email"])
    op.create_index("ix_subscribers_access_token", "subscribers", ["access_token"])

    # companies
    op.create_table(
        "companies",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column("exchange", sa.String(20)),
        sa.Column("company_name", sa.String(500)),
        sa.Column("cik", sa.Integer()),
        sa.Column("sic", sa.String(10)),
        sa.Column("naics", sa.String(10)),
        sa.Column("country", sa.String(50), server_default="US"),
        sa.Column("sector", sa.String(100)),
        sa.Column("industry", sa.String(200)),
        sa.Column("market_cap", sa.Float()),
        sa.Column("revenue", sa.Float()),
        sa.Column("net_income", sa.Float()),
        sa.Column("employees", sa.Integer()),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true")),
        sa.Column("financials_as_of", sa.Date()),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", "exchange", name="uq_companies_ticker_exchange"),
    )
    op.create_index("ix_companies_ticker", "companies", ["ticker"])
    op.create_index("ix_companies_cik", "companies", ["cik"])

    # scoring_runs
    op.create_table(
        "scoring_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(50), nullable=False),
        sa.Column("run_type", sa.String(30), nullable=False),
        sa.Column("status", sa.String(20), server_default="running"),
        sa.Column("scoring_version", sa.String(50)),
        sa.Column("parameters", postgresql.JSONB(), server_default="{}"),
        sa.Column(
            "dimensions",
            postgresql.ARRAY(sa.String()),
            server_default=sa.text("ARRAY['opportunity','realization']"),
        ),
        sa.Column("companies_requested", sa.Integer(), server_default="0"),
        sa.Column("companies_scored", sa.Integer(), server_default="0"),
        sa.Column("companies_failed", sa.Integer(), server_default="0"),
        sa.Column("started_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime()),
        sa.Column("refresh_request_id", sa.Integer()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id"),
    )
    op.create_index("ix_scoring_runs_run_id", "scoring_runs", ["run_id"])

    # refresh_requests
    op.create_table(
        "refresh_requests",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "subscriber_id",
            sa.Integer(),
            sa.ForeignKey("subscribers.id"),
            nullable=False,
        ),
        sa.Column(
            "company_id",
            sa.Integer(),
            sa.ForeignKey("companies.id"),
            nullable=False,
        ),
        sa.Column(
            "dimensions",
            postgresql.ARRAY(sa.String()),
            server_default=sa.text("ARRAY['opportunity','realization']"),
        ),
        sa.Column("status", sa.String(20), server_default="pending"),
        sa.Column("scoring_run_id", sa.Integer(), sa.ForeignKey("scoring_runs.id")),
        sa.Column("requested_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_refresh_requests_pending",
        "refresh_requests",
        ["status", "requested_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )

    # Add FK from scoring_runs.refresh_request_id -> refresh_requests.id
    op.create_foreign_key(
        "fk_scoring_runs_refresh_request",
        "scoring_runs",
        "refresh_requests",
        ["refresh_request_id"],
        ["id"],
    )

    # evidence
    op.create_table(
        "evidence",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "company_id",
            sa.Integer(),
            sa.ForeignKey("companies.id"),
            nullable=False,
        ),
        sa.Column(
            "scoring_run_id", sa.Integer(), sa.ForeignKey("scoring_runs.id")
        ),
        sa.Column("evidence_type", sa.String(50), nullable=False),
        sa.Column("evidence_subtype", sa.String(100)),
        sa.Column("source_name", sa.String(100)),
        sa.Column("source_url", sa.Text()),
        sa.Column("source_date", sa.Date()),
        sa.Column("score_contribution", sa.Float()),
        sa.Column("weight", sa.Float()),
        sa.Column("signal_strength", sa.String(20)),
        sa.Column("payload", postgresql.JSONB(), server_default="{}"),
        sa.Column("observed_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("valid_from", sa.Date()),
        sa.Column("valid_to", sa.Date()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_evidence_company_id", "evidence", ["company_id"])
    op.create_index(
        "ix_evidence_company_type", "evidence", ["company_id", "evidence_type"]
    )
    op.create_index(
        "ix_evidence_payload", "evidence", ["payload"], postgresql_using="gin"
    )

    # company_scores
    op.create_table(
        "company_scores",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "company_id",
            sa.Integer(),
            sa.ForeignKey("companies.id"),
            nullable=False,
        ),
        sa.Column(
            "scoring_run_id",
            sa.Integer(),
            sa.ForeignKey("scoring_runs.id"),
            nullable=False,
        ),
        sa.Column("revenue_opp_score", sa.Float()),
        sa.Column("cost_opp_score", sa.Float()),
        sa.Column("composite_opp_score", sa.Float()),
        sa.Column("filing_nlp_score", sa.Float()),
        sa.Column("product_score", sa.Float()),
        sa.Column("job_score", sa.Float()),
        sa.Column("patent_score", sa.Float()),
        sa.Column("composite_real_score", sa.Float()),
        sa.Column("opportunity", sa.Float(), nullable=False),
        sa.Column("realization", sa.Float(), nullable=False),
        sa.Column("quadrant", sa.String(50)),
        sa.Column("quadrant_label", sa.String(100)),
        sa.Column("combined_rank", sa.Integer()),
        sa.Column("flags", postgresql.ARRAY(sa.String()), server_default="{}"),
        sa.Column("data_as_of", sa.DateTime(), nullable=False),
        sa.Column("scored_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "company_id", "scoring_run_id", name="uq_company_scores_company_run"
        ),
    )
    op.create_index("ix_company_scores_company_id", "company_scores", ["company_id"])
    op.create_index(
        "ix_company_scores_company_scored",
        "company_scores",
        ["company_id", "scored_at"],
    )

    # notifications
    op.create_table(
        "notifications",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "subscriber_id",
            sa.Integer(),
            sa.ForeignKey("subscribers.id"),
            nullable=False,
        ),
        sa.Column("notification_type", sa.String(50), nullable=False),
        sa.Column("channel", sa.String(20), server_default="email"),
        sa.Column("subject", sa.String(500)),
        sa.Column("body", sa.Text()),
        sa.Column("payload", postgresql.JSONB(), server_default="{}"),
        sa.Column("status", sa.String(20), server_default="pending"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_notifications_pending",
        "notifications",
        ["status", "created_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )

    # score_change_log
    op.create_table(
        "score_change_log",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "company_id",
            sa.Integer(),
            sa.ForeignKey("companies.id"),
            nullable=False,
        ),
        sa.Column("dimension", sa.String(30), nullable=False),
        sa.Column("old_score", sa.Float()),
        sa.Column("new_score", sa.Float()),
        sa.Column("old_quadrant", sa.String(50)),
        sa.Column("new_quadrant", sa.String(50)),
        sa.Column("changed_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_score_change_log_company_id", "score_change_log", ["company_id"]
    )

    # Materialized view for latest company scores
    op.execute("""
        CREATE MATERIALIZED VIEW latest_company_scores AS
        SELECT DISTINCT ON (c.id)
            c.id AS company_id,
            c.ticker,
            c.exchange,
            c.company_name,
            c.sector,
            c.industry,
            c.market_cap,
            c.revenue,
            c.employees,
            c.is_active,
            c.financials_as_of,
            cs.id AS score_id,
            cs.scoring_run_id,
            cs.revenue_opp_score,
            cs.cost_opp_score,
            cs.composite_opp_score,
            cs.filing_nlp_score,
            cs.product_score,
            cs.job_score,
            cs.patent_score,
            cs.composite_real_score,
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
        ORDER BY c.id, cs.scored_at DESC
    """)
    op.execute(
        "CREATE UNIQUE INDEX ix_latest_company_scores_company_id "
        "ON latest_company_scores (company_id)"
    )
    op.execute(
        "CREATE INDEX ix_latest_company_scores_ticker "
        "ON latest_company_scores (ticker)"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_company_scores")
    op.drop_table("score_change_log")
    op.drop_table("notifications")
    op.drop_table("company_scores")
    op.drop_table("evidence")
    op.drop_constraint(
        "fk_scoring_runs_refresh_request", "scoring_runs", type_="foreignkey"
    )
    op.drop_table("refresh_requests")
    op.drop_table("scoring_runs")
    op.drop_table("companies")
    op.drop_table("subscribers")
