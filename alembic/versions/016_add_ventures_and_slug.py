"""Add company_ventures table, slug column, and make ticker nullable.

Revision ID: 016
Revises: 015
Create Date: 2026-02-27
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '016'
down_revision: Union[str, None] = '015'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create company_ventures table
    op.create_table(
        'company_ventures',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('parent_id', sa.Integer(), sa.ForeignKey('companies.id'), nullable=False),
        sa.Column('subsidiary_id', sa.Integer(), sa.ForeignKey('companies.id'), nullable=False),
        sa.Column('ownership_pct', sa.Float(), nullable=True),
        sa.Column('relationship_type', sa.String(50), server_default='subsidiary', nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_unique_constraint('uq_company_ventures', 'company_ventures', ['parent_id', 'subsidiary_id'])
    op.create_index('ix_cv_parent', 'company_ventures', ['parent_id'])
    op.create_index('ix_cv_subsidiary', 'company_ventures', ['subsidiary_id'])

    # 2. Add slug column (nullable initially for backfill)
    op.add_column('companies', sa.Column('slug', sa.String(50), nullable=True))

    # 3. Backfill slug = UPPER(ticker) for existing rows
    op.execute("UPDATE companies SET slug = UPPER(ticker) WHERE slug IS NULL AND ticker IS NOT NULL")

    # 4. Make slug NOT NULL and add unique index
    op.alter_column('companies', 'slug', nullable=False)
    op.create_index('ix_companies_slug', 'companies', ['slug'], unique=True)

    # 5. Make ticker nullable
    op.alter_column('companies', 'ticker', nullable=True)

    # 6. Drop old unique constraint, recreate as partial index (ticker IS NOT NULL)
    op.drop_constraint('uq_companies_ticker_exchange', 'companies', type_='unique')
    op.execute(
        "CREATE UNIQUE INDEX uq_companies_ticker_exchange "
        "ON companies (ticker, exchange) WHERE ticker IS NOT NULL"
    )


def downgrade() -> None:
    # Reverse partial index
    op.execute("DROP INDEX IF EXISTS uq_companies_ticker_exchange")
    op.create_unique_constraint('uq_companies_ticker_exchange', 'companies', ['ticker', 'exchange'])

    # Make ticker NOT NULL again (requires backfill from slug)
    op.execute("UPDATE companies SET ticker = slug WHERE ticker IS NULL")
    op.alter_column('companies', 'ticker', nullable=False)

    # Drop slug
    op.drop_index('ix_companies_slug', 'companies')
    op.drop_column('companies', 'slug')

    # Drop company_ventures
    op.drop_index('ix_cv_subsidiary', 'company_ventures')
    op.drop_index('ix_cv_parent', 'company_ventures')
    op.drop_constraint('uq_company_ventures', 'company_ventures', type_='unique')
    op.drop_table('company_ventures')
