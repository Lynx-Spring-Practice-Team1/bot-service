from datetime import datetime, UTC
from uuid import uuid4

from sqlalchemy import Column, String, Numeric, JSON, DateTime, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

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
