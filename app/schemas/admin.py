"""Pydantic response models for /internal/admin/bots/* endpoints."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel


class BotSummary(BaseModel):
    """Row for the admin list view."""
    session_id: UUID
    user_id: str
    symbol: str
    status: str                       # active | paused | halted | deactivated | error
    runtime_state: Optional[str] = None  # in-memory ephemeral state
    current_strategy: Optional[str] = None
    is_running: bool
    position_side: Optional[str] = None
    entry_price: Optional[float] = None
    entry_quantity: Optional[float] = None
    total_pnl: float = 0.0
    daily_pnl: float = 0.0
    trades_count: int = 0
    open_trades: int = 0
    closed_trades: int = 0
    win_rate: float = 0.0
    last_decision_at: Optional[datetime] = None
    last_error_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class BotListResponse(BaseModel):
    items: list[BotSummary]
    total: int


class BotDetail(BotSummary):
    strategy_config: dict[str, Any] = {}
    peak_balance: Optional[float] = None
    error_count_session: int = 0
    last_error_message: Optional[str] = None


class TradeRow(BaseModel):
    id: UUID
    broker_order_id: str
    symbol: str
    side: str
    quantity: float
    price: float
    status: str
    pnl: Optional[float] = None
    created_at: datetime
    closed_at: Optional[datetime] = None


class TradeListResponse(BaseModel):
    items: list[TradeRow]
    total: int


class HoldingRow(BaseModel):
    symbol: str
    side: str                         # BUY | SELL
    quantity: float
    entry_price: float
    current_price: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    unrealized_pct: Optional[float] = None


class HoldingsResponse(BaseModel):
    items: list[HoldingRow]


class DecisionRow(BaseModel):
    id: int
    tick_at: datetime
    current_price: Optional[float] = None
    momentum: Optional[float] = None
    volatility: Optional[float] = None
    pressure_ratio: Optional[float] = None
    bid_ratio: Optional[float] = None
    ema_signal: Optional[str] = None
    market_event: Optional[str] = None
    selected_strategy: str
    strategy_switched: bool
    signals_count: int
    signal_summary: Optional[str] = None
    position_side: Optional[str] = None
    entry_price: Optional[float] = None
    daily_pnl: Optional[float] = None


class DecisionListResponse(BaseModel):
    items: list[DecisionRow]


class EventRow(BaseModel):
    id: int
    created_at: datetime
    severity: str
    event_type: str
    message: str
    context: Optional[dict[str, Any]] = None
    admin_user: Optional[str] = None


class EventListResponse(BaseModel):
    items: list[EventRow]


class EquityPoint(BaseModel):
    ts: datetime
    value: float


class EquityResponse(BaseModel):
    items: list[EquityPoint]


class StrategyMetric(BaseModel):
    strategy: str
    count: int


class BotMetricsResponse(BaseModel):
    trades_total: int
    trades_open: int
    trades_closed: int
    wins: int
    losses: int
    win_rate: float
    gross_profit: float
    gross_loss: float
    total_pnl: float
    daily_pnl: float
    peak_balance: Optional[float] = None
    avg_trade_pnl: float
    best_trade: Optional[float] = None
    worst_trade: Optional[float] = None
    decisions_count: int
    signals_per_strategy: list[StrategyMetric]
    errors_24h: int


class GlobalMetrics(BaseModel):
    running_count: int
    sessions_by_status: dict[str, int]
    total_open_trades: int
    total_pnl_today: float
    total_pnl_all_time: float
    errors_last_24h: int


class ActionResponse(BaseModel):
    session_id: UUID
    status: str
    message: str | None = None


class RestartRequest(BaseModel):
    close_positions: bool = False


class ClosePositionsResponse(BaseModel):
    closed: int
    total_pnl: float
