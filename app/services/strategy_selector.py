"""Select the active trading strategy based on current market state.

Priority order (matches trading_bot_extended.txt Part E):
  1. MARKET_EVENT active (non-HALT) → event_driven
  2. momentum > 0.4 AND ema_signal != NONE → martingale
  3. volatility > 0.02 AND |pressure_ratio| < 0.2 → grid
  4. otherwise → hold
"""
from __future__ import annotations

from app.services.strategies.base import MarketState

_MOMENTUM_THRESHOLD = 0.4
_VOLATILITY_THRESHOLD = 0.02
_PRESSURE_THRESHOLD = 0.2


def select_strategy(state: MarketState) -> str:
    """Return one of: 'event_driven' | 'martingale' | 'grid' | 'hold'."""
    if state.market_event:
        event_type = str(
            state.market_event.get("event_type") or state.market_event.get("type", "")
        ).upper()
        if event_type not in ("HALT", "PRICE_FEED", ""):
            return "event_driven"

    if state.momentum > _MOMENTUM_THRESHOLD and state.ema_signal != "NONE":
        return "martingale"

    if (
        state.volatility > _VOLATILITY_THRESHOLD
        and abs(state.pressure_ratio) < _PRESSURE_THRESHOLD
    ):
        return "grid"

    return "hold"
