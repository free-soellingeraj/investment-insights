"""Add PR tracking columns to plans/projects, add review fields to projects.

Revision ID: 024
Revises: 023
"""
from alembic import op
import sqlalchemy as sa

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade():
    # PR tracking on plans
    op.add_column("agent_plans", sa.Column("pr_number", sa.Integer(), nullable=True))
    op.add_column("agent_plans", sa.Column("pr_branch", sa.String(200), nullable=True))
    op.add_column("agent_plans", sa.Column("pr_url", sa.String(500), nullable=True))

    # PR tracking + review fields on projects
    op.add_column("agent_projects", sa.Column("pr_number", sa.Integer(), nullable=True))
    op.add_column("agent_projects", sa.Column("pr_branch", sa.String(200), nullable=True))
    op.add_column("agent_projects", sa.Column("pr_url", sa.String(500), nullable=True))
    op.add_column("agent_projects", sa.Column("code_impact", sa.Text(), nullable=True))
    op.add_column("agent_projects", sa.Column("test_instructions", sa.Text(), nullable=True))
    op.add_column("agent_projects", sa.Column("human_review_notes", sa.Text(), nullable=True))
    op.add_column("agent_projects", sa.Column("human_review_status", sa.String(20), nullable=True))


def downgrade():
    op.drop_column("agent_projects", "human_review_status")
    op.drop_column("agent_projects", "human_review_notes")
    op.drop_column("agent_projects", "test_instructions")
    op.drop_column("agent_projects", "code_impact")
    op.drop_column("agent_projects", "pr_url")
    op.drop_column("agent_projects", "pr_branch")
    op.drop_column("agent_projects", "pr_number")
    op.drop_column("agent_plans", "pr_url")
    op.drop_column("agent_plans", "pr_branch")
    op.drop_column("agent_plans", "pr_number")
