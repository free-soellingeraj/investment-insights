"""Add ai_index_usd and capture_probability columns to company_scores.

Revision ID: 013
Revises: 012
Create Date: 2026-02-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '013'
down_revision: Union[str, None] = '012'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('company_scores', sa.Column('ai_index_usd', sa.Float(), nullable=True))
    op.add_column('company_scores', sa.Column('capture_probability', sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column('company_scores', 'capture_probability')
    op.drop_column('company_scores', 'ai_index_usd')
