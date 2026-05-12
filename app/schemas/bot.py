from typing import Any, Optional
from uuid import UUID
from datetime import datetime

from pydantic import BaseModel, field_validator


class ActivateRequest(BaseModel):
    # jwt_token intentionally absent — the bot reads the caller's Bearer token
    # from the Authorization header so it never travels in the request body.
    symbol: str
    strategy_config: dict[str, Any] = {}


class ConfigUpdate(BaseModel):
    symbol: Optional[str] = None
    strategy_config: Optional[dict[str, Any]] = None
    # User-settable values only.  'halted' and 'error' are system-set;
    # posting status='active' also serves as a manual halt-override.
    status: Optional[str] = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in ("active", "paused"):
            raise ValueError("status must be 'active' or 'paused'")
        return v


class TradeOut(BaseModel):
    id: UUID
    broker_order_id: str
    side: str
    quantity: float
    price: float
    status: str
    pnl: Optional[float]
    created_at: datetime

    model_config = {"from_attributes": True}


class BotStatusOut(BaseModel):
    session_id: UUID
    # active | paused | halted | error | deactivated
    status: str
    symbol: str
    entry_price: Optional[float]
    position_side: Optional[str]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class BotPerformanceOut(BaseModel):
    session_id: UUID
    symbol: str
    total_trades: int
    open_trades: int
    closed_trades: int
    total_pnl: float
    trades: list[TradeOut]
