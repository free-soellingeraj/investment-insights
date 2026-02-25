"""Add blog_url column to companies table.

Promotes blog_url from JSON cache to a first-class DB column,
consistent with github_url, careers_url, and ir_url.

Revision ID: 008
Revises: a623efa8e39b
Create Date: 2026-02-24
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "008"
down_revision: Union[str, None] = "a623efa8e39b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("blog_url", sa.String(2048), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "blog_url")
