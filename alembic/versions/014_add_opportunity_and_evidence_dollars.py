"""Add opportunity_usd and evidence_dollars columns to company_scores.

Revision ID: 014
Revises: 013
Create Date: 2026-02-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '014'
down_revision: Union[str, None] = '013'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('company_scores', sa.Column('opportunity_usd', sa.Float(), nullable=True))
    op.add_column('company_scores', sa.Column('evidence_dollars', sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column('company_scores', 'evidence_dollars')
    op.drop_column('company_scores', 'opportunity_usd')
