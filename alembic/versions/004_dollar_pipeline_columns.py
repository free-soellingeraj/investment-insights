"""Add dollar estimation columns to company_scores and evidence tables.

Supports the 4-stage evidence-to-dollar pipeline.

Revision ID: 004
Revises: 003
Create Date: 2026-02-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add dollar columns to company_scores
    op.add_column("company_scores", sa.Column("cost_opp_usd", sa.Float(), nullable=True))
    op.add_column("company_scores", sa.Column("revenue_opp_usd", sa.Float(), nullable=True))
    op.add_column("company_scores", sa.Column("cost_capture_usd", sa.Float(), nullable=True))
    op.add_column("company_scores", sa.Column("revenue_capture_usd", sa.Float(), nullable=True))
    op.add_column("company_scores", sa.Column("total_investment_usd", sa.Float(), nullable=True))

    # Add dollar columns to evidence
    op.add_column("evidence", sa.Column("dollar_estimate_usd", sa.Float(), nullable=True))
    op.add_column("evidence", sa.Column("dollar_year_1", sa.Float(), nullable=True))
    op.add_column("evidence", sa.Column("dollar_year_2", sa.Float(), nullable=True))
    op.add_column("evidence", sa.Column("dollar_year_3", sa.Float(), nullable=True))

    # Recreate materialized view with new columns
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
    op.execute("""
        CREATE UNIQUE INDEX ix_latest_company_scores_company_id
            ON latest_company_scores (company_id)
    """)
    op.execute("""
        CREATE INDEX ix_latest_company_scores_ticker
            ON latest_company_scores (ticker)
    """)


def downgrade() -> None:
    # Recreate materialized view without dollar columns
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
    op.execute("""
        CREATE UNIQUE INDEX ix_latest_company_scores_company_id
            ON latest_company_scores (company_id)
    """)
    op.execute("""
        CREATE INDEX ix_latest_company_scores_ticker
            ON latest_company_scores (ticker)
    """)

    # Remove dollar columns
    op.drop_column("evidence", "dollar_year_3")
    op.drop_column("evidence", "dollar_year_2")
    op.drop_column("evidence", "dollar_year_1")
    op.drop_column("evidence", "dollar_estimate_usd")
    op.drop_column("company_scores", "total_investment_usd")
    op.drop_column("company_scores", "revenue_capture_usd")
    op.drop_column("company_scores", "cost_capture_usd")
    op.drop_column("company_scores", "revenue_opp_usd")
    op.drop_column("company_scores", "cost_opp_usd")
