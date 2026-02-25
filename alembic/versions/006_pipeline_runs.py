"""Replace scoring_runs with pipeline_runs for per-stage tracking.

Revision ID: 006
Revises: 005
Create Date: 2026-02-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 0. Drop pipeline_runs if it was auto-created by init_db/create_all
    op.execute("DROP TABLE IF EXISTS pipeline_runs CASCADE")

    # 1. Create the new pipeline_runs table
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(50), nullable=False),
        sa.Column("task", sa.String(10), nullable=False),
        sa.Column("subtask", sa.String(25), nullable=False),
        sa.Column("run_type", sa.String(30), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="running"),
        sa.Column("parameters", postgresql.JSONB(), nullable=True),
        sa.Column("tickers_requested", postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column("tickers_succeeded", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("tickers_failed", sa.Integer(), nullable=True, server_default="0"),
        sa.Column("parent_run_id", sa.String(50), nullable=True),
        sa.Column("started_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", name="uq_pipeline_runs_run_id"),
    )
    op.create_index("ix_pipeline_runs_run_id", "pipeline_runs", ["run_id"])
    op.create_index("ix_pipeline_runs_task", "pipeline_runs", ["task"])
    op.create_index("ix_pipeline_runs_subtask", "pipeline_runs", ["subtask"])
    op.create_index("ix_pipeline_runs_status", "pipeline_runs", ["status"])

    # 2. Migrate data from scoring_runs → pipeline_runs
    op.execute("""
        INSERT INTO pipeline_runs (id, run_id, task, subtask, run_type, status, parameters,
                                   tickers_succeeded, tickers_failed,
                                   started_at, completed_at)
        SELECT id, run_id, 'score', 'all', run_type, status, parameters,
               companies_scored, companies_failed,
               started_at, completed_at
        FROM scoring_runs
    """)

    # 3. Drop FK constraints on evidence and company_scores that point to scoring_runs
    op.drop_constraint("evidence_scoring_run_id_fkey", "evidence", type_="foreignkey")
    op.drop_constraint(
        "company_scores_scoring_run_id_fkey", "company_scores", type_="foreignkey"
    )
    op.drop_constraint(
        "uq_company_scores_company_run", "company_scores", type_="unique"
    )

    # 4. Rename columns
    op.alter_column("evidence", "scoring_run_id", new_column_name="pipeline_run_id")
    op.alter_column(
        "company_scores", "scoring_run_id", new_column_name="pipeline_run_id"
    )

    # 5. Add new FK constraints pointing to pipeline_runs
    op.create_foreign_key(
        "evidence_pipeline_run_id_fkey",
        "evidence",
        "pipeline_runs",
        ["pipeline_run_id"],
        ["id"],
    )
    op.create_foreign_key(
        "company_scores_pipeline_run_id_fkey",
        "company_scores",
        "pipeline_runs",
        ["pipeline_run_id"],
        ["id"],
    )
    op.create_unique_constraint(
        "uq_company_scores_company_run",
        "company_scores",
        ["company_id", "pipeline_run_id"],
    )

    # 6. Update refresh_requests FK
    op.drop_constraint(
        "refresh_requests_scoring_run_id_fkey", "refresh_requests", type_="foreignkey"
    )
    op.alter_column(
        "refresh_requests", "scoring_run_id", new_column_name="pipeline_run_id"
    )
    op.create_foreign_key(
        "refresh_requests_pipeline_run_id_fkey",
        "refresh_requests",
        "pipeline_runs",
        ["pipeline_run_id"],
        ["id"],
    )

    # 7. Drop old scoring_runs table
    op.drop_table("scoring_runs")

    # 8. Recreate materialized view with new column name
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_company_scores")
    op.execute("""
        CREATE MATERIALIZED VIEW latest_company_scores AS
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
            cs.job_score,
            cs.patent_score,
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
    # Recreate scoring_runs
    op.create_table(
        "scoring_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(50), nullable=False),
        sa.Column("run_type", sa.String(30), nullable=False),
        sa.Column("status", sa.String(20), server_default="running"),
        sa.Column("scoring_version", sa.String(50), nullable=True),
        sa.Column("parameters", postgresql.JSONB(), nullable=True),
        sa.Column(
            "dimensions", postgresql.ARRAY(sa.String()), nullable=True
        ),
        sa.Column("companies_requested", sa.Integer(), server_default="0"),
        sa.Column("companies_scored", sa.Integer(), server_default="0"),
        sa.Column("companies_failed", sa.Integer(), server_default="0"),
        sa.Column("started_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("refresh_request_id", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id"),
        sa.ForeignKeyConstraint(["refresh_request_id"], ["refresh_requests.id"]),
    )

    # Migrate data back
    op.execute("""
        INSERT INTO scoring_runs (id, run_id, run_type, status, parameters,
                                  companies_scored, companies_failed,
                                  started_at, completed_at)
        SELECT id, run_id, run_type, status, parameters,
               tickers_succeeded, tickers_failed,
               started_at, completed_at
        FROM pipeline_runs
        WHERE stage = 'score'
    """)

    # Rename columns back
    op.drop_constraint("evidence_pipeline_run_id_fkey", "evidence", type_="foreignkey")
    op.drop_constraint(
        "company_scores_pipeline_run_id_fkey", "company_scores", type_="foreignkey"
    )
    op.drop_constraint(
        "uq_company_scores_company_run", "company_scores", type_="unique"
    )
    op.drop_constraint(
        "refresh_requests_pipeline_run_id_fkey",
        "refresh_requests",
        type_="foreignkey",
    )

    op.alter_column("evidence", "pipeline_run_id", new_column_name="scoring_run_id")
    op.alter_column(
        "company_scores", "pipeline_run_id", new_column_name="scoring_run_id"
    )
    op.alter_column(
        "refresh_requests", "pipeline_run_id", new_column_name="scoring_run_id"
    )

    op.create_foreign_key(
        "evidence_scoring_run_id_fkey",
        "evidence",
        "scoring_runs",
        ["scoring_run_id"],
        ["id"],
    )
    op.create_foreign_key(
        "company_scores_scoring_run_id_fkey",
        "company_scores",
        "scoring_runs",
        ["scoring_run_id"],
        ["id"],
    )
    op.create_unique_constraint(
        "uq_company_scores_company_run",
        "company_scores",
        ["company_id", "scoring_run_id"],
    )
    op.create_foreign_key(
        "refresh_requests_scoring_run_id_fkey",
        "refresh_requests",
        "scoring_runs",
        ["scoring_run_id"],
        ["id"],
    )

    # Drop pipeline_runs
    op.drop_table("pipeline_runs")

    # Recreate materialized view with old column name
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_company_scores")
    op.execute("""
        CREATE MATERIALIZED VIEW latest_company_scores AS
        SELECT DISTINCT ON (c.id)
            c.id AS company_id,
            c.ticker,
            c.exchange,
            c.company_name,
            c.sector,
            c.industry,
            c.is_active,
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
