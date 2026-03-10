"""Add investment_projects table for synthesized project records

Revision ID: 020
Revises: 019
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

revision = "020"
down_revision = "019"


def upgrade():
    op.create_table(
        "investment_projects",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "company_id",
            sa.Integer(),
            sa.ForeignKey("companies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("pipeline_run_id", sa.Integer(), sa.ForeignKey("pipeline_runs.id")),
        sa.Column("short_title", sa.String(200), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("target_dimension", sa.String(20), nullable=False),
        sa.Column("target_subcategory", sa.String(100), nullable=False),
        sa.Column("target_detail", sa.String(200), server_default=""),
        sa.Column("status", sa.String(20), server_default="planned"),
        sa.Column("dollar_total", sa.Float()),
        sa.Column("dollar_low", sa.Float()),
        sa.Column("dollar_high", sa.Float()),
        sa.Column("confidence", sa.Float(), server_default="0"),
        sa.Column("evidence_count", sa.Integer(), server_default="0"),
        sa.Column("date_start", sa.Date()),
        sa.Column("date_end", sa.Date()),
        sa.Column("technology_area", sa.String(100), server_default=""),
        sa.Column("deployment_scope", sa.String(200), server_default=""),
        sa.Column("evidence_group_ids", ARRAY(sa.Integer())),
        sa.Column("valuation_ids", ARRAY(sa.Integer())),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_ip_company", "investment_projects", ["company_id"])
    op.create_index(
        "ix_ip_company_run",
        "investment_projects",
        ["company_id", "pipeline_run_id"],
    )


def downgrade():
    op.drop_index("ix_ip_company_run")
    op.drop_index("ix_ip_company")
    op.drop_table("investment_projects")
