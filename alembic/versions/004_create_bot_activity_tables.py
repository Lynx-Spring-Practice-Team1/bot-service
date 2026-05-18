"""create bot decision and event log tables

Revision ID: 004
Revises: 003
Create Date: 2026-05-17

Adds two tables that power the admin Bot Service dashboard:

- bot_decision_logs: tick-level indicator + strategy snapshots
  (throttled writes — ~6 rows/min/bot under steady state)

- bot_event_logs: lifecycle, error, and admin-action events
  (low volume, longer retention)
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_MONEY = sa.Numeric(18, 8)


def upgrade() -> None:
    op.create_table(
        "bot_decision_logs",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "session_id",
            UUID(as_uuid=True),
            sa.ForeignKey("bot_sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("user_id", sa.String(128), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column(
            "tick_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("current_price", _MONEY, nullable=True),
        sa.Column("momentum", sa.Float, nullable=True),
        sa.Column("volatility", sa.Float, nullable=True),
        sa.Column("pressure_ratio", sa.Float, nullable=True),
        sa.Column("bid_ratio", sa.Float, nullable=True),
        sa.Column("ema_signal", sa.String(16), nullable=True),
        sa.Column("market_event", sa.String(32), nullable=True),
        sa.Column("selected_strategy", sa.String(16), nullable=False),
        sa.Column("strategy_switched", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("signals_count", sa.SmallInteger, nullable=False, server_default="0"),
        sa.Column("signal_summary", sa.String(128), nullable=True),
        sa.Column("position_side", sa.String(8), nullable=True),
        sa.Column("entry_price", _MONEY, nullable=True),
        sa.Column("daily_pnl", _MONEY, nullable=True),
    )
    op.create_index(
        "ix_bot_decision_logs_session_tick",
        "bot_decision_logs",
        ["session_id", sa.text("tick_at DESC")],
    )
    op.create_index(
        "ix_bot_decision_logs_user_tick",
        "bot_decision_logs",
        ["user_id", sa.text("tick_at DESC")],
    )
    op.create_index(
        "ix_bot_decision_logs_strategy",
        "bot_decision_logs",
        ["selected_strategy", sa.text("tick_at DESC")],
    )

    op.create_table(
        "bot_event_logs",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "session_id",
            UUID(as_uuid=True),
            sa.ForeignKey("bot_sessions.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("user_id", sa.String(128), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("context", JSONB, nullable=True),
        sa.Column("admin_user", sa.String(128), nullable=True),
    )
    op.create_index(
        "ix_bot_event_logs_session_created",
        "bot_event_logs",
        ["session_id", sa.text("created_at DESC")],
    )
    op.create_index(
        "ix_bot_event_logs_severity",
        "bot_event_logs",
        ["severity", sa.text("created_at DESC")],
    )
    op.create_index(
        "ix_bot_event_logs_event_type",
        "bot_event_logs",
        ["event_type", sa.text("created_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_bot_event_logs_event_type", table_name="bot_event_logs")
    op.drop_index("ix_bot_event_logs_severity", table_name="bot_event_logs")
    op.drop_index("ix_bot_event_logs_session_created", table_name="bot_event_logs")
    op.drop_table("bot_event_logs")

    op.drop_index("ix_bot_decision_logs_strategy", table_name="bot_decision_logs")
    op.drop_index("ix_bot_decision_logs_user_tick", table_name="bot_decision_logs")
    op.drop_index("ix_bot_decision_logs_session_tick", table_name="bot_decision_logs")
    op.drop_table("bot_decision_logs")
