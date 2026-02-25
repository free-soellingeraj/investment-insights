"""increase url column lengths to 2048

Revision ID: a623efa8e39b
Revises: 007
Create Date: 2026-02-24 08:23:33.013393
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'a623efa8e39b'
down_revision: Union[str, None] = '007'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('companies', 'github_url',
               existing_type=sa.VARCHAR(length=500),
               type_=sa.String(length=2048),
               existing_nullable=True)
    op.alter_column('companies', 'careers_url',
               existing_type=sa.VARCHAR(length=500),
               type_=sa.String(length=2048),
               existing_nullable=True)
    op.alter_column('companies', 'ir_url',
               existing_type=sa.VARCHAR(length=500),
               type_=sa.String(length=2048),
               existing_nullable=True)


def downgrade() -> None:
    op.alter_column('companies', 'ir_url',
               existing_type=sa.String(length=2048),
               type_=sa.VARCHAR(length=500),
               existing_nullable=True)
    op.alter_column('companies', 'careers_url',
               existing_type=sa.String(length=2048),
               type_=sa.VARCHAR(length=500),
               existing_nullable=True)
    op.alter_column('companies', 'github_url',
               existing_type=sa.String(length=2048),
               type_=sa.VARCHAR(length=500),
               existing_nullable=True)
