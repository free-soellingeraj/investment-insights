"""Widen technology_area column to Text.

Revision ID: 012
Revises: 011
Create Date: 2026-02-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '012'
down_revision: Union[str, None] = '011'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'investment_details',
        'technology_area',
        type_=sa.Text(),
        existing_type=sa.String(100),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        'investment_details',
        'technology_area',
        type_=sa.String(100),
        existing_type=sa.Text(),
        existing_nullable=True,
    )
