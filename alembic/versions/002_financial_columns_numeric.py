"""Change financial columns from FLOAT to NUMERIC(18,8)

Revision ID: 002
Revises: 001
Create Date: 2026-05-12
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_NUMERIC = sa.Numeric(18, 8)


def upgrade() -> None:
    op.alter_column(
        "bot_sessions", "entry_price",
        type_=_NUMERIC,
        existing_type=sa.Float(),
        existing_nullable=True,
    )
    op.alter_column(
        "bot_trades", "quantity",
        type_=_NUMERIC,
        existing_type=sa.Float(),
        existing_nullable=False,
    )
    op.alter_column(
        "bot_trades", "price",
        type_=_NUMERIC,
        existing_type=sa.Float(),
        existing_nullable=False,
    )
    op.alter_column(
        "bot_trades", "pnl",
        type_=_NUMERIC,
        existing_type=sa.Float(),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "bot_sessions", "entry_price",
        type_=sa.Float(),
        existing_type=_NUMERIC,
        existing_nullable=True,
    )
    op.alter_column(
        "bot_trades", "quantity",
        type_=sa.Float(),
        existing_type=_NUMERIC,
        existing_nullable=False,
    )
    op.alter_column(
        "bot_trades", "price",
        type_=sa.Float(),
        existing_type=_NUMERIC,
        existing_nullable=False,
    )
    op.alter_column(
        "bot_trades", "pnl",
        type_=sa.Float(),
        existing_type=_NUMERIC,
        existing_nullable=True,
    )
