"""Event-Driven strategy — reacts to BULL_RUN, BEAR_CRASH, SECTOR_BOOM, etc."""
from __future__ import annotations

from .base import MarketState, TradeSignal

_BASE_SIZE_PCT = 0.01   # 1% of available cash
_MIN_MAGNITUDE = 1.2

_BULL_TYPES = frozenset({"BULL_RUN", "SECTOR_BOOM"})
_BEAR_TYPES = frozenset({"BEAR_CRASH", "SECTOR_SLUMP"})
_SHOCK_TYPES = frozenset({"STOCK_SHOCK"})


def generate_signals(
    state: MarketState,
    available_cash: float,
    has_position: bool,
) -> list[TradeSignal]:
    """Return trade signals based on the active market event."""
    event = state.market_event
    if not event:
        return []

    event_type = str(event.get("event_type") or event.get("type", "")).upper()
    magnitude = float(event.get("magnitude", 1.0))

    if magnitude < _MIN_MAGNITUDE:
        return []

    if event_type in _BULL_TYPES:
        size_pct = _BASE_SIZE_PCT * magnitude
        qty = (available_cash * size_pct) / state.current_price
        if qty <= 0:
            return []
        return [TradeSignal(
            action="BUY",
            order_type="MARKET",
            quantity=round(qty, 2),
            price=state.current_price,
            reason=f"EVENT {event_type} mag={magnitude:.2f}",
        )]

    if event_type in _BEAR_TYPES:
        if has_position:
            return [TradeSignal(
                action="CLOSE",
                order_type="MARKET",
                quantity=0,
                price=state.current_price,
                reason=f"EVENT {event_type} closing longs",
            )]
        return []

    if event_type in _SHOCK_TYPES and not has_position:
        size_pct = _BASE_SIZE_PCT * 0.5  # half size on shock
        qty = (available_cash * size_pct) / state.current_price
        if qty <= 0:
            return []
        return [TradeSignal(
            action="BUY",
            order_type="LIMIT",
            quantity=round(qty, 2),
            price=state.current_price,
            reason="EVENT STOCK_SHOCK half-size",
            meta={"tight_stop": True},
        )]

    return []
