"""Evidence valuation pipeline tables.

New tables for the factor index scoring pipeline:
evidence_groups, evidence_group_passages, valuations,
plan_details, investment_details, capture_details,
valuation_discrepancies.

Revision ID: 010
Revises: 009
Create Date: 2026-02-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

revision: str = "010"
down_revision: Union[str, None] = "009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # evidence_groups
    op.create_table(
        "evidence_groups",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("company_id", sa.Integer(), sa.ForeignKey("companies.id"), nullable=False),
        sa.Column("pipeline_run_id", sa.Integer(), sa.ForeignKey("pipeline_runs.id"), nullable=True),
        sa.Column("target_dimension", sa.String(20), nullable=False),
        sa.Column("evidence_type", sa.String(20), nullable=True),
        sa.Column("passage_count", sa.Integer(), server_default="0"),
        sa.Column("source_types", ARRAY(sa.String(20)), server_default="{}"),
        sa.Column("date_earliest", sa.Date(), nullable=True),
        sa.Column("date_latest", sa.Date(), nullable=True),
        sa.Column("mean_confidence", sa.Float(), nullable=True),
        sa.Column("max_confidence", sa.Float(), nullable=True),
        sa.Column("representative_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_eg_company", "evidence_groups", ["company_id"])
    op.create_index("ix_eg_company_run", "evidence_groups", ["company_id", "pipeline_run_id"])

    # evidence_group_passages
    op.create_table(
        "evidence_group_passages",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "group_id", sa.Integer(),
            sa.ForeignKey("evidence_groups.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("evidence_id", sa.Integer(), sa.ForeignKey("evidence.id"), nullable=True),
        sa.Column("passage_text", sa.Text(), nullable=False),
        sa.Column("source_type", sa.String(50), nullable=True),
        sa.Column("source_filename", sa.String(255), nullable=True),
        sa.Column("source_date", sa.Date(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("reasoning", sa.Text(), nullable=True),
        sa.Column("target_dimension", sa.String(20), nullable=True),
        sa.Column("capture_stage", sa.String(20), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_egp_group", "evidence_group_passages", ["group_id"])

    # valuations
    op.create_table(
        "valuations",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "group_id", sa.Integer(),
            sa.ForeignKey("evidence_groups.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("pipeline_run_id", sa.Integer(), sa.ForeignKey("pipeline_runs.id"), nullable=True),
        sa.Column("stage", sa.String(20), nullable=False),
        sa.Column("preliminary_id", sa.Integer(), sa.ForeignKey("valuations.id"), nullable=True),
        sa.Column("evidence_type", sa.String(20), nullable=False),
        sa.Column("narrative", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("dollar_low", sa.Float(), nullable=True),
        sa.Column("dollar_high", sa.Float(), nullable=True),
        sa.Column("dollar_mid", sa.Float(), nullable=True),
        sa.Column("dollar_rationale", sa.Text(), nullable=True),
        sa.Column("specificity", sa.Float(), nullable=True),
        sa.Column("magnitude", sa.Float(), nullable=True),
        sa.Column("stage_weight", sa.Float(), nullable=True),
        sa.Column("recency", sa.Float(), nullable=True),
        sa.Column("factor_score", sa.Float(), nullable=True),
        sa.Column("adjusted_from_preliminary", sa.Boolean(), server_default="false"),
        sa.Column("adjustment_reason", sa.Text(), nullable=True),
        sa.Column("prior_groups_seen", sa.Integer(), server_default="0"),
        sa.Column("input_tokens", sa.Integer(), server_default="0"),
        sa.Column("output_tokens", sa.Integer(), server_default="0"),
        sa.Column("model_name", sa.String(100), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_val_group", "valuations", ["group_id"])
    op.create_index("ix_val_group_stage", "valuations", ["group_id", "stage"])
    op.create_unique_constraint("uq_val_group_run_stage", "valuations", ["group_id", "pipeline_run_id", "stage"])

    # plan_details
    op.create_table(
        "plan_details",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "valuation_id", sa.Integer(),
            sa.ForeignKey("valuations.id", ondelete="CASCADE"),
            nullable=False, unique=True,
        ),
        sa.Column("timeframe", sa.String(50), nullable=True),
        sa.Column("probability", sa.Float(), nullable=True),
        sa.Column("strategic_rationale", sa.Text(), nullable=True),
        sa.Column("contingencies", sa.Text(), nullable=True),
        sa.Column("horizon_shape", sa.String(20), nullable=True),
        sa.Column("year_1_pct", sa.Float(), nullable=True),
        sa.Column("year_2_pct", sa.Float(), nullable=True),
        sa.Column("year_3_pct", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # investment_details
    op.create_table(
        "investment_details",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "valuation_id", sa.Integer(),
            sa.ForeignKey("valuations.id", ondelete="CASCADE"),
            nullable=False, unique=True,
        ),
        sa.Column("actual_spend_usd", sa.Float(), nullable=True),
        sa.Column("deployment_scope", sa.Text(), nullable=True),
        sa.Column("completion_pct", sa.Float(), nullable=True),
        sa.Column("technology_area", sa.String(100), nullable=True),
        sa.Column("vendor_partner", sa.String(200), nullable=True),
        sa.Column("horizon_shape", sa.String(20), nullable=True),
        sa.Column("year_1_pct", sa.Float(), nullable=True),
        sa.Column("year_2_pct", sa.Float(), nullable=True),
        sa.Column("year_3_pct", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # capture_details
    op.create_table(
        "capture_details",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "valuation_id", sa.Integer(),
            sa.ForeignKey("valuations.id", ondelete="CASCADE"),
            nullable=False, unique=True,
        ),
        sa.Column("metric_name", sa.String(200), nullable=True),
        sa.Column("metric_value_before", sa.Text(), nullable=True),
        sa.Column("metric_value_after", sa.Text(), nullable=True),
        sa.Column("metric_delta", sa.Text(), nullable=True),
        sa.Column("measurement_period", sa.String(50), nullable=True),
        sa.Column("measured_dollar_impact", sa.Float(), nullable=True),
        sa.Column("horizon_shape", sa.String(20), server_default="flat"),
        sa.Column("year_1_pct", sa.Float(), server_default="1.0"),
        sa.Column("year_2_pct", sa.Float(), server_default="1.0"),
        sa.Column("year_3_pct", sa.Float(), server_default="1.0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )

    # valuation_discrepancies
    op.create_table(
        "valuation_discrepancies",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("company_id", sa.Integer(), sa.ForeignKey("companies.id"), nullable=False),
        sa.Column("pipeline_run_id", sa.Integer(), sa.ForeignKey("pipeline_runs.id"), nullable=True),
        sa.Column("group_id_a", sa.Integer(), sa.ForeignKey("evidence_groups.id"), nullable=False),
        sa.Column("group_id_b", sa.Integer(), sa.ForeignKey("evidence_groups.id"), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("resolution", sa.Text(), nullable=False),
        sa.Column("resolution_method", sa.String(50), nullable=True),
        sa.Column("source_search_result", sa.Text(), nullable=True),
        sa.Column("trusted_group_id", sa.Integer(), sa.ForeignKey("evidence_groups.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_disc_company", "valuation_discrepancies", ["company_id"])


def downgrade() -> None:
    op.drop_table("valuation_discrepancies")
    op.drop_table("capture_details")
    op.drop_table("investment_details")
    op.drop_table("plan_details")
    op.drop_table("valuations")
    op.drop_table("evidence_group_passages")
    op.drop_table("evidence_groups")
