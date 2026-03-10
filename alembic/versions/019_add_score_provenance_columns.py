"""Add evidence_group_ids and valuation_ids to company_scores

Revision ID: 019
Revises: 018
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

revision = "019"
down_revision = "018"


def upgrade():
    op.add_column(
        "company_scores",
        sa.Column("evidence_group_ids", ARRAY(sa.Integer()), nullable=True),
    )
    op.add_column(
        "company_scores",
        sa.Column("valuation_ids", ARRAY(sa.Integer()), nullable=True),
    )


def downgrade():
    op.drop_column("company_scores", "valuation_ids")
    op.drop_column("company_scores", "evidence_group_ids")
