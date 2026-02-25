"""Drop job_score and patent_score columns from company_scores.

These sub-scorers have been removed from the pipeline — careers page
data (via web enrichment) replaces job signals, and patent data had
no reliable source.

Revision ID: 007
Revises: 006
"""

from alembic import op
import sqlalchemy as sa

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the materialized view first (it references the columns)
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_company_scores CASCADE")

    # Drop columns
    op.drop_column("company_scores", "job_score")
    op.drop_column("company_scores", "patent_score")

    # Recreate materialized view without job_score/patent_score
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
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_company_scores CASCADE")

    op.add_column("company_scores", sa.Column("job_score", sa.Float(), nullable=True))
    op.add_column("company_scores", sa.Column("patent_score", sa.Float(), nullable=True))

    # Recreate view with columns restored
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
