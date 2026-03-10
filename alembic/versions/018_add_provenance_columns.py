"""Add provenance columns for citation metadata

Revision ID: 018
Revises: 017
"""
from alembic import op
import sqlalchemy as sa

revision = "018"
down_revision = "017"


def upgrade():
    # EvidenceGroupPassageModel — provenance fields
    op.add_column("evidence_group_passages", sa.Column("source_author_role", sa.String(200)))
    op.add_column("evidence_group_passages", sa.Column("source_author_affiliation", sa.String(200)))
    op.add_column("evidence_group_passages", sa.Column("source_publisher", sa.String(200)))
    op.add_column("evidence_group_passages", sa.Column("source_access_date", sa.Date()))
    op.add_column("evidence_group_passages", sa.Column("source_authority", sa.String(50)))

    # EvidenceModel — provenance fields
    op.add_column("evidence", sa.Column("source_author", sa.String(200)))
    op.add_column("evidence", sa.Column("source_publisher", sa.String(200)))
    op.add_column("evidence", sa.Column("source_access_date", sa.Date()))
    op.add_column("evidence", sa.Column("source_authority", sa.String(50)))


def downgrade():
    op.drop_column("evidence", "source_authority")
    op.drop_column("evidence", "source_access_date")
    op.drop_column("evidence", "source_publisher")
    op.drop_column("evidence", "source_author")

    op.drop_column("evidence_group_passages", "source_authority")
    op.drop_column("evidence_group_passages", "source_access_date")
    op.drop_column("evidence_group_passages", "source_publisher")
    op.drop_column("evidence_group_passages", "source_author_affiliation")
    op.drop_column("evidence_group_passages", "source_author_role")
