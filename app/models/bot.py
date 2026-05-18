from datetime import datetime, UTC
from uuid import uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    JSON,
    Numeric,
    SmallInteger,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.database import Base

# Precision/scale for all financial columns: 18 digits, 8 decimal places.
# Matches the NUMERIC(18,8) columns created by migration 002.
_MONEY = Numeric(18, 8)


class BotSession(Base):
    __tablename__ = "bot_sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id = Column(String(128), nullable=False, index=True)
    jwt_token = Column(Text, nullable=False)  # Fernet-encrypted; see crypto.py
    symbol = Column(String(32), nullable=False)
    strategy_config = Column(JSON, nullable=False, default=dict)
    # active | paused | halted | deactivated | error
    status = Column(String(32), nullable=False, default="active")
    entry_price = Column(_MONEY, nullable=True)
    entry_quantity = Column(_MONEY, nullable=True)
    position_side = Column(String(8), nullable=True)  # BUY | SELL
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))


class BotTrade(Base):
    __tablename__ = "bot_trades"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    session_id = Column(UUID(as_uuid=True), ForeignKey("bot_sessions.id"), nullable=False, index=True)
    user_id = Column(String(128), nullable=False, index=True)
    broker_order_id = Column(String(128), nullable=False)
    symbol = Column(String(32), nullable=False)
    side = Column(String(8), nullable=False)  # BUY | SELL
    quantity = Column(_MONEY, nullable=False)
    price = Column(_MONEY, nullable=False)
    # open | closed | cancelled
    status = Column(String(32), nullable=False, default="open")
    pnl = Column(_MONEY, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    closed_at = Column(DateTime(timezone=True), nullable=True)


class BotDecisionLog(Base):
    """Tick-level snapshot of indicators + strategy selection + signals.

    Written by activity_logger with a throttle: every tick that switches
    strategy or fires a signal, otherwise at most once per DECISION_LOG_THROTTLE_SECS.
    """

    __tablename__ = "bot_decision_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    session_id = Column(UUID(as_uuid=True), ForeignKey("bot_sessions.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(String(128), nullable=False)
    symbol = Column(String(32), nullable=False)
    tick_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    current_price = Column(_MONEY, nullable=True)
    momentum = Column(Float, nullable=True)
    volatility = Column(Float, nullable=True)
    pressure_ratio = Column(Float, nullable=True)
    bid_ratio = Column(Float, nullable=True)
    ema_signal = Column(String(16), nullable=True)        # BUY | SELL | NONE
    market_event = Column(String(32), nullable=True)
    selected_strategy = Column(String(16), nullable=False)  # event_driven | martingale | grid | hold
    strategy_switched = Column(Boolean, nullable=False, default=False)
    signals_count = Column(SmallInteger, nullable=False, default=0)
    signal_summary = Column(String(128), nullable=True)
    position_side = Column(String(8), nullable=True)
    entry_price = Column(_MONEY, nullable=True)
    daily_pnl = Column(_MONEY, nullable=True)


class BotEventLog(Base):
    """Lifecycle, error, and admin-action events.

    Lower volume than decision logs; longer retention. Includes
    admin_user when an admin endpoint triggers the event for audit.
    """

    __tablename__ = "bot_event_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    session_id = Column(UUID(as_uuid=True), ForeignKey("bot_sessions.id", ondelete="CASCADE"), nullable=True)
    user_id = Column(String(128), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    severity = Column(String(16), nullable=False)   # DEBUG | INFO | WARNING | ERROR | CRITICAL
    event_type = Column(String(64), nullable=False)
    message = Column(Text, nullable=False)
    context = Column(JSONB, nullable=True)
    admin_user = Column(String(128), nullable=True)
