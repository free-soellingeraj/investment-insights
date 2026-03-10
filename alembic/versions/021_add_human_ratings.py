"""Add human_ratings table for user feedback on any entity

Revision ID: 021
Revises: 020
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "021"
down_revision = "020"


def upgrade():
    op.create_table(
        "human_ratings",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("entity_type", sa.String(30), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("rating", sa.Integer(), nullable=True),
        sa.Column("dimension", sa.String(30), nullable=False, server_default="overall"),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("action", sa.String(30), nullable=True),
        sa.Column("metadata", JSONB(), server_default="{}"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index(
        "ix_hr_entity",
        "human_ratings",
        ["entity_type", "entity_id"],
    )
    op.create_index(
        "ix_hr_created",
        "human_ratings",
        ["created_at"],
    )
    op.create_index(
        "ix_hr_action",
        "human_ratings",
        ["action"],
        postgresql_where=sa.text("action IS NOT NULL"),
    )


def downgrade():
    op.drop_index("ix_hr_action")
    op.drop_index("ix_hr_created")
    op.drop_index("ix_hr_entity")
    op.drop_table("human_ratings")
