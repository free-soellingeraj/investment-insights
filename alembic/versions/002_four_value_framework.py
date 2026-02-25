"""Four-value scoring framework — cost/revenue x opportunity/capture.

Adds cost_capture_score, revenue_capture_score, general_investment_score,
cost_roi, revenue_roi, combined_roi to company_scores.
Adds target_dimension, capture_stage to evidence.
Recreates materialized view with new columns.

Revision ID: 002
Revises: 001
Create Date: 2026-02-21
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new columns to company_scores
    op.add_column("company_scores", sa.Column("cost_capture_score", sa.Float()))
    op.add_column("company_scores", sa.Column("revenue_capture_score", sa.Float()))
    op.add_column("company_scores", sa.Column("general_investment_score", sa.Float()))
    op.add_column("company_scores", sa.Column("cost_roi", sa.Float()))
    op.add_column("company_scores", sa.Column("revenue_roi", sa.Float()))
    op.add_column("company_scores", sa.Column("combined_roi", sa.Float()))

    # Add new columns to evidence
    op.add_column("evidence", sa.Column("target_dimension", sa.String(20)))
    op.add_column("evidence", sa.Column("capture_stage", sa.String(20)))

    # Add indexes on new evidence columns
    op.create_index(
        "ix_evidence_target_dimension",
        "evidence",
        ["target_dimension"],
    )
    op.create_index(
        "ix_evidence_capture_stage",
        "evidence",
        ["capture_stage"],
    )
    op.create_index(
        "ix_evidence_company_target",
        "evidence",
        ["company_id", "target_dimension"],
    )

    # Drop and recreate materialized view with new columns
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
            cs.cost_capture_score,
            cs.revenue_capture_score,
            cs.general_investment_score,
            cs.cost_roi,
            cs.revenue_roi,
            cs.combined_roi,
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
    # Drop and recreate materialized view without new columns
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_company_scores")

    # Drop indexes
    op.drop_index("ix_evidence_company_target", table_name="evidence")
    op.drop_index("ix_evidence_capture_stage", table_name="evidence")
    op.drop_index("ix_evidence_target_dimension", table_name="evidence")

    # Drop evidence columns
    op.drop_column("evidence", "capture_stage")
    op.drop_column("evidence", "target_dimension")

    # Drop company_scores columns
    op.drop_column("company_scores", "combined_roi")
    op.drop_column("company_scores", "revenue_roi")
    op.drop_column("company_scores", "cost_roi")
    op.drop_column("company_scores", "general_investment_score")
    op.drop_column("company_scores", "revenue_capture_score")
    op.drop_column("company_scores", "cost_capture_score")

    # Recreate original materialized view
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
