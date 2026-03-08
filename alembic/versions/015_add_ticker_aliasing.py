"""Add child_ticker_refs and canonical_company_id to companies for share-class aliasing.

Revision ID: 015
Revises: 014
Create Date: 2026-02-27
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

# revision identifiers, used by Alembic.
revision: str = '015'
down_revision: Union[str, None] = '014'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('companies', sa.Column('child_ticker_refs', ARRAY(sa.Integer()), nullable=True))
    op.add_column('companies', sa.Column('canonical_company_id', sa.Integer(), sa.ForeignKey('companies.id'), nullable=True))


def downgrade() -> None:
    op.drop_column('companies', 'canonical_company_id')
    op.drop_column('companies', 'child_ticker_refs')
