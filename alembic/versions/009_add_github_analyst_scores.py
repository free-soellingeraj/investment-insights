"""Add github_score and analyst_score columns to company_scores.

New sub-scorers for GitHub AI activity and analyst consensus signals.

Revision ID: 009
Revises: 008
Create Date: 2026-02-24
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "009"
down_revision: Union[str, None] = "008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("company_scores", sa.Column("github_score", sa.Float(), nullable=True))
    op.add_column("company_scores", sa.Column("analyst_score", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("company_scores", "analyst_score")
    op.drop_column("company_scores", "github_score")
