"""Financial observations table — time-series financial data.

Moves market_cap, revenue, net_income, employees from companies table
into a separate financial_observations table with per-observation
metadata (source, timestamp, units, fiscal period).

Revision ID: 003
Revises: 002
Create Date: 2026-02-21
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UPDATED_VIEW_SQL = """
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
"""


def upgrade() -> None:
    # Create financial_observations table
    op.create_table(
        "financial_observations",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("company_id", sa.Integer, sa.ForeignKey("companies.id"), nullable=False),
        sa.Column("metric", sa.String(50), nullable=False),
        sa.Column("value", sa.Float, nullable=False),
        sa.Column("value_units", sa.String(30), nullable=False),
        sa.Column("source_datetime", sa.DateTime, nullable=False),
        sa.Column("source_link", sa.Text),
        sa.Column("source_name", sa.String(100)),
        sa.Column("fiscal_period", sa.String(20)),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
    )
    op.create_index("ix_finobs_company_id", "financial_observations", ["company_id"])
    op.create_index("ix_finobs_company_metric", "financial_observations", ["company_id", "metric"])
    op.create_index(
        "ix_finobs_company_metric_date",
        "financial_observations",
        ["company_id", "metric", "source_datetime"],
    )

    # Add source_excerpt column to evidence table
    op.add_column("evidence", sa.Column("source_excerpt", sa.Text))

    # Migrate existing financial data from companies table into financial_observations
    op.execute("""
        INSERT INTO financial_observations (company_id, metric, value, value_units, source_datetime, source_name)
        SELECT id, 'market_cap', market_cap, 'usd', COALESCE(financials_as_of, NOW()), 'yahoo_finance'
        FROM companies WHERE market_cap IS NOT NULL
    """)
    op.execute("""
        INSERT INTO financial_observations (company_id, metric, value, value_units, source_datetime, source_name)
        SELECT id, 'revenue', revenue, 'usd', COALESCE(financials_as_of, NOW()), 'yahoo_finance'
        FROM companies WHERE revenue IS NOT NULL
    """)
    op.execute("""
        INSERT INTO financial_observations (company_id, metric, value, value_units, source_datetime, source_name)
        SELECT id, 'net_income', net_income, 'usd', COALESCE(financials_as_of, NOW()), 'yahoo_finance'
        FROM companies WHERE net_income IS NOT NULL
    """)
    op.execute("""
        INSERT INTO financial_observations (company_id, metric, value, value_units, source_datetime, source_name)
        SELECT id, 'employees', employees, 'count', COALESCE(financials_as_of, NOW()), 'yahoo_finance'
        FROM companies WHERE employees IS NOT NULL
    """)

    # Drop materialized view before removing columns it depends on
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_company_scores")

    # Remove financial columns from companies
    op.drop_column("companies", "market_cap")
    op.drop_column("companies", "revenue")
    op.drop_column("companies", "net_income")
    op.drop_column("companies", "employees")
    op.drop_column("companies", "financials_as_of")

    # Rebuild materialized view without financial columns
    op.execute(UPDATED_VIEW_SQL)
    op.execute(
        "CREATE UNIQUE INDEX ix_latest_company_scores_company_id "
        "ON latest_company_scores (company_id)"
    )
    op.execute(
        "CREATE INDEX ix_latest_company_scores_ticker "
        "ON latest_company_scores (ticker)"
    )


def downgrade() -> None:
    # Remove source_excerpt from evidence
    op.drop_column("evidence", "source_excerpt")

    # Re-add financial columns to companies
    op.add_column("companies", sa.Column("market_cap", sa.Float))
    op.add_column("companies", sa.Column("revenue", sa.Float))
    op.add_column("companies", sa.Column("net_income", sa.Float))
    op.add_column("companies", sa.Column("employees", sa.Integer))
    op.add_column("companies", sa.Column("financials_as_of", sa.Date))

    # Restore latest financial data back to companies table
    op.execute("""
        UPDATE companies c SET
            market_cap = fo.value
        FROM (
            SELECT DISTINCT ON (company_id) company_id, value
            FROM financial_observations
            WHERE metric = 'market_cap'
            ORDER BY company_id, source_datetime DESC
        ) fo
        WHERE c.id = fo.company_id
    """)
    op.execute("""
        UPDATE companies c SET
            revenue = fo.value
        FROM (
            SELECT DISTINCT ON (company_id) company_id, value
            FROM financial_observations
            WHERE metric = 'revenue'
            ORDER BY company_id, source_datetime DESC
        ) fo
        WHERE c.id = fo.company_id
    """)
    op.execute("""
        UPDATE companies c SET
            net_income = fo.value
        FROM (
            SELECT DISTINCT ON (company_id) company_id, value
            FROM financial_observations
            WHERE metric = 'net_income'
            ORDER BY company_id, source_datetime DESC
        ) fo
        WHERE c.id = fo.company_id
    """)
    op.execute("""
        UPDATE companies c SET
            employees = fo.value::integer
        FROM (
            SELECT DISTINCT ON (company_id) company_id, value
            FROM financial_observations
            WHERE metric = 'employees'
            ORDER BY company_id, source_datetime DESC
        ) fo
        WHERE c.id = fo.company_id
    """)
    op.execute("""
        UPDATE companies c SET
            financials_as_of = fo.source_datetime::date
        FROM (
            SELECT DISTINCT ON (company_id) company_id, source_datetime
            FROM financial_observations
            ORDER BY company_id, source_datetime DESC
        ) fo
        WHERE c.id = fo.company_id
    """)

    # Drop financial_observations table
    op.drop_index("ix_finobs_company_metric_date", table_name="financial_observations")
    op.drop_index("ix_finobs_company_metric", table_name="financial_observations")
    op.drop_index("ix_finobs_company_id", table_name="financial_observations")
    op.drop_table("financial_observations")

    # Rebuild materialized view with financial columns
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
