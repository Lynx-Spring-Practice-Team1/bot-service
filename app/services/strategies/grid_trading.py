"""Grid Trading strategy — symmetric buy/sell levels around current price.

Places 5 LIMIT buy orders below current price and watches for price to hit
sell levels above.  Exits when volatility drops, pressure spikes, or momentum
signals a trend forming.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .base import MarketState, TradeSignal

_NUM_LEVELS = 5
_POSITION_PCT = 0.01     # 1% of available cash per level
_VOLATILITY_RANGE = 0.02 # 2% total grid range
_MAX_ORDERS = 5
_EXIT_VOL_LOW = 0.015    # exit grid when volatility drops below this
_EXIT_PRESSURE_HIGH = 0.25
_EXIT_LOW_TICKS = 10
_EXIT_PRESSURE_TICKS = 3


@dataclass
class GridState:
    active: bool = False
    buy_levels: list[float] = field(default_factory=list)
    sell_levels: list[float] = field(default_factory=list)
    active_order_ids: list[str] = field(default_factory=list)
    ordered_sell_levels: set = field(default_factory=set)  # sell levels already ordered
    low_vol_ticks: int = 0
    high_pressure_ticks: int = 0


def _build_levels(price: float, volatility: float) -> tuple[list[float], list[float]]:
    grid_size = (price * _VOLATILITY_RANGE) / _NUM_LEVELS
    buys = [round(price - grid_size * i, 4) for i in range(1, _NUM_LEVELS + 1)]
    sells = [round(price + grid_size * i, 4) for i in range(1, _NUM_LEVELS + 1)]
    return buys, sells


def generate_signals(
    state: MarketState,
    available_cash: float,
    gs: GridState,
    open_order_count: int,
) -> tuple[list[TradeSignal], GridState]:
    signals: list[TradeSignal] = []

    if gs.active:
        # Check exit conditions
        if state.volatility < _EXIT_VOL_LOW:
            gs.low_vol_ticks += 1
        else:
            gs.low_vol_ticks = 0

        if abs(state.pressure_ratio) > _EXIT_PRESSURE_HIGH:
            gs.high_pressure_ticks += 1
        else:
            gs.high_pressure_ticks = 0

        if (
            gs.low_vol_ticks >= _EXIT_LOW_TICKS
            or gs.high_pressure_ticks >= _EXIT_PRESSURE_TICKS
            or state.momentum > 0.4
        ):
            signals.append(TradeSignal(
                "CLOSE", "MARKET", 0, state.current_price,
                "GRID exit: low-vol / high-pressure / trend",
            ))
            return signals, GridState()

        # Place sell orders when price reaches sell levels (once per level)
        for level_price in gs.sell_levels:
            if (
                state.current_price >= level_price
                and level_price not in gs.ordered_sell_levels
                and open_order_count < _MAX_ORDERS
            ):
                qty = (available_cash * _POSITION_PCT) / level_price
                if qty > 0:
                    signals.append(TradeSignal(
                        "SELL", "LIMIT", round(qty, 2), level_price,
                        f"GRID sell @ {level_price:.4f}",
                    ))
                    gs.ordered_sell_levels.add(level_price)
                    open_order_count += 1

        return signals, gs

    # Build a fresh grid
    buys, sells = _build_levels(state.current_price, state.volatility)
    gs = GridState(active=True, buy_levels=buys, sell_levels=sells)

    # Place buy orders up to available order slots
    slots = _MAX_ORDERS - open_order_count
    for level_price in gs.buy_levels[:slots]:
        qty = (available_cash * _POSITION_PCT) / max(level_price, 0.0001)
        if qty > 0:
            signals.append(TradeSignal(
                "BUY", "LIMIT", round(qty, 2), level_price,
                f"GRID buy @ {level_price:.4f}",
            ))

    return signals, gs
