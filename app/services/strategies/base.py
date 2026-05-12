"""Shared dataclasses for strategy input/output."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class MarketState:
    symbol: str
    prices: list[float]
    current_price: float
    momentum: float        # (upTicks - downTicks) / totalTicks, last 5 ticks
    volatility: float      # σ of percentage price changes, last 10 ticks
    pressure_ratio: float  # (bid_vol - ask_vol) / total; -1.0 to 1.0
    ema_signal: str        # "BUY" | "SELL" | "NONE"
    market_event: dict | None = None  # latest MARKET_EVENT payload or None


@dataclass
class TradeSignal:
    action: str      # "BUY" | "SELL" | "CLOSE" | "HOLD"
    order_type: str  # "LIMIT" | "MARKET"
    quantity: float
    price: float
    reason: str = ""
    meta: dict = field(default_factory=dict)
