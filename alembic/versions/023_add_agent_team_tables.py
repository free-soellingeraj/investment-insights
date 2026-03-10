"""Add agent team tables for multi-team agent system

Creates: agent_teams, agents, agent_channels, agent_messages,
agent_plans, agent_plan_comments, agent_projects.

Revision ID: 023
Revises: 022
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade():
    # ── agent_teams ──
    op.create_table(
        "agent_teams",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(100), unique=True, nullable=False),
        sa.Column("display_name", sa.String(200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # ── agents ──
    op.create_table(
        "agents",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("team_id", sa.Integer(), sa.ForeignKey("agent_teams.id"), nullable=True),
        sa.Column("role", sa.String(50), nullable=False),
        sa.Column("display_name", sa.String(200), nullable=False),
        sa.Column("status", sa.String(20), server_default="idle"),
        sa.Column("pid", sa.Integer(), nullable=True),
        sa.Column("last_heartbeat", sa.DateTime(), nullable=True),
        sa.Column("cycle_count", sa.Integer(), server_default="0"),
        sa.Column("fix_count", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # ── agent_channels ──
    op.create_table(
        "agent_channels",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(100), unique=True, nullable=False),
        sa.Column("channel_type", sa.String(20), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # ── agent_messages ──
    op.create_table(
        "agent_messages",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("channel_id", sa.Integer(), sa.ForeignKey("agent_channels.id"), nullable=False),
        sa.Column("agent_id", sa.Integer(), sa.ForeignKey("agents.id"), nullable=True),
        sa.Column("sender_name", sa.String(100), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("message_type", sa.String(20), server_default="chat"),
        sa.Column("metadata", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index(
        "ix_agent_messages_channel_created", "agent_messages", ["channel_id", "created_at"]
    )

    # ── agent_plans ──
    op.create_table(
        "agent_plans",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("team_id", sa.Integer(), sa.ForeignKey("agent_teams.id"), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("plan_text", sa.Text(), nullable=False),
        sa.Column("status", sa.String(20), server_default="draft"),
        sa.Column("created_by", sa.Integer(), sa.ForeignKey("agents.id"), nullable=True),
        sa.Column("reviewed_by", sa.Integer(), sa.ForeignKey("agents.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # ── agent_plan_comments ──
    op.create_table(
        "agent_plan_comments",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("plan_id", sa.Integer(), sa.ForeignKey("agent_plans.id"), nullable=False),
        sa.Column("line_number", sa.Integer(), nullable=True),
        sa.Column("author_name", sa.String(100), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("resolved", sa.Boolean(), server_default="false"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_plan_comments_plan", "agent_plan_comments", ["plan_id"])

    # ── agent_projects ──
    op.create_table(
        "agent_projects",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("plan_id", sa.Integer(), sa.ForeignKey("agent_plans.id"), nullable=False),
        sa.Column("team_id", sa.Integer(), sa.ForeignKey("agent_teams.id"), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("status", sa.String(20), server_default="active"),
        sa.Column("assigned_to", sa.Integer(), sa.ForeignKey("agents.id"), nullable=True),
        sa.Column("reviewer_id", sa.Integer(), sa.ForeignKey("agents.id"), nullable=True),
        sa.Column("files_changed", JSONB, nullable=True),
        sa.Column("test_results", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
    )


def downgrade():
    op.drop_table("agent_projects")
    op.drop_table("agent_plan_comments")
    op.drop_table("agent_plans")
    op.drop_index("ix_agent_messages_channel_created", table_name="agent_messages")
    op.drop_table("agent_messages")
    op.drop_table("agent_channels")
    op.drop_table("agents")
    op.drop_table("agent_teams")
