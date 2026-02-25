"""Add github_url, careers_url, ir_url columns to companies table.

Revision ID: 005
Revises: 004
Create Date: 2026-02-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("github_url", sa.String(500), nullable=True))
    op.add_column("companies", sa.Column("careers_url", sa.String(500), nullable=True))
    op.add_column("companies", sa.Column("ir_url", sa.String(500), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "ir_url")
    op.drop_column("companies", "careers_url")
    op.drop_column("companies", "github_url")
