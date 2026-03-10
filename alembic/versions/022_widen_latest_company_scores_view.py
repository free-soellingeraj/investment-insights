"""Rebuild latest_company_scores materialized view with missing columns

Adds slug, sic, naics, country from companies table and
opportunity_usd, evidence_dollars, github_score, analyst_score from
company_scores so that the peers query returns complete data.

Revision ID: 022
Revises: 021
"""
from alembic import op

revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None

NEW_VIEW = """\
CREATE MATERIALIZED VIEW latest_company_scores AS
SELECT DISTINCT ON (c.id)
    c.id            AS company_id,
    c.ticker,
    c.slug,
    c.exchange,
    c.company_name,
    c.sector,
    c.industry,
    c.sic,
    c.naics,
    c.country,
    c.is_active,
    cs.id           AS score_id,
    cs.pipeline_run_id,
    cs.revenue_opp_score,
    cs.cost_opp_score,
    cs.composite_opp_score,
    cs.filing_nlp_score,
    cs.product_score,
    cs.github_score,
    cs.analyst_score,
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
ORDER BY c.id, cs.scored_at DESC
"""

OLD_VIEW = """\
CREATE MATERIALIZED VIEW latest_company_scores AS
SELECT DISTINCT ON (c.id)
    c.id            AS company_id,
    c.ticker,
    c.exchange,
    c.company_name,
    c.sector,
    c.industry,
    c.is_active,
    cs.id           AS score_id,
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


def upgrade():
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_company_scores")
    op.execute(NEW_VIEW)
    op.execute(
        "CREATE UNIQUE INDEX ix_lcs_company_id ON latest_company_scores (company_id)"
    )


def downgrade():
    op.execute("DROP MATERIALIZED VIEW IF EXISTS latest_company_scores")
    op.execute(OLD_VIEW)
    op.execute(
        "CREATE UNIQUE INDEX ix_lcs_company_id ON latest_company_scores (company_id)"
    )
