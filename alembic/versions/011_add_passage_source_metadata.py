"""Add source_url and source_author to evidence_group_passages.

Revision ID: 011
Revises: 010
Create Date: 2026-02-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '011'
down_revision: Union[str, None] = '010'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('evidence_group_passages', sa.Column('source_url', sa.Text(), nullable=True))
    op.add_column('evidence_group_passages', sa.Column('source_author', sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column('evidence_group_passages', 'source_author')
    op.drop_column('evidence_group_passages', 'source_url')
