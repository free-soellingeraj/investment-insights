"""Widen capture_details.measurement_period to text

Revision ID: 017
Revises: 016
"""
from alembic import op
import sqlalchemy as sa

revision = "017"
down_revision = "016"

def upgrade():
    op.alter_column(
        "capture_details",
        "measurement_period",
        type_=sa.Text(),
        existing_type=sa.String(50),
    )

def downgrade():
    op.alter_column(
        "capture_details",
        "measurement_period",
        type_=sa.String(50),
        existing_type=sa.Text(),
    )
