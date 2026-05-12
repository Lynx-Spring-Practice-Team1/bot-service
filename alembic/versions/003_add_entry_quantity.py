"""Add entry_quantity to bot_sessions

Revision ID: 003
Revises: 002
Create Date: 2026-05-12
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_NUMERIC = sa.Numeric(18, 8)


def upgrade() -> None:
    op.add_column(
        "bot_sessions",
        sa.Column("entry_quantity", _NUMERIC, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("bot_sessions", "entry_quantity")
