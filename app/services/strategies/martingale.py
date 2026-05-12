"""Martingale + EMA confirmation strategy.

Position sizing: 1% → 2% → 4% of portfolio on consecutive stop-losses.
Exits: +3% profit (all levels), Level-3 stop-loss (forced exit), 60-tick
max hold, or 5 consecutive ticks of weak momentum (<0.3).
"""
from __future__ import annotations

from dataclasses import dataclass

from .base import MarketState, TradeSignal

_MAX_LEVELS = 3
_LEVEL_SIZES = (0.01, 0.02, 0.04)   # portfolio % per level
_TAKE_PROFIT_PCT = 0.03              # +3% from original entry
_STOP_PCT_PER_LEVEL = 0.01          # -1% per level cumulative
_MAX_HOLD_TICKS = 60
_ENTRY_OFFSET = 0.0015              # ±0.15% limit offset
_CONFIDENCE_THRESHOLD = 0.0006     # 0.06% EMA separation minimum
_DEAD_MOMENTUM = 0.3
_DEAD_TICKS = 5


@dataclass
class MartingaleState:
    level: int = 0
    original_entry: float = 0.0
    side: str = ""
    hold_ticks: int = 0
    weak_ticks: int = 0


def _ema_confidence(prices: list[float], fast: int = 5, slow: int = 20) -> float:
    if len(prices) < slow:
        return 0.0
    k_f, k_s = 2.0 / (fast + 1), 2.0 / (slow + 1)
    ema_f = ema_s = sum(prices[:slow]) / slow
    for p in prices[slow:]:
        ema_f = p * k_f + ema_f * (1 - k_f)
        ema_s = p * k_s + ema_s * (1 - k_s)
    if ema_s == 0:
        return 0.0
    return abs(ema_f - ema_s) / ema_s  # ratio (0–~0.10)


def generate_signals(
    state: MarketState,
    available_cash: float,
    portfolio_value: float,
    ms: MartingaleState,
    bid_ratio: float | None,
) -> tuple[list[TradeSignal], MartingaleState]:
    signals: list[TradeSignal] = []

    if ms.level > 0:
        ms.hold_ticks += 1

        if ms.side == "BUY":
            pnl_pct = (state.current_price - ms.original_entry) / ms.original_entry
        else:
            pnl_pct = (ms.original_entry - state.current_price) / ms.original_entry

        # Take profit at +3% from original entry
        if pnl_pct >= _TAKE_PROFIT_PCT:
            signals.append(TradeSignal("CLOSE", "LIMIT", 0, state.current_price, "MART take-profit +3%"))
            return signals, MartingaleState()

        # Cumulative stop-loss check
        stop_pct = _STOP_PCT_PER_LEVEL * ms.level
        if pnl_pct <= -stop_pct:
            if ms.level < _MAX_LEVELS:
                next_lvl = ms.level + 1
                size_pct = _LEVEL_SIZES[next_lvl - 1]
                # Entry price is at original_entry - level * 1% for BUY
                if ms.side == "BUY":
                    entry = ms.original_entry * (1.0 - _STOP_PCT_PER_LEVEL * ms.level)
                else:
                    entry = ms.original_entry * (1.0 + _STOP_PCT_PER_LEVEL * ms.level)
                qty = (portfolio_value * size_pct) / max(entry, 0.0001)
                if qty > 0:
                    signals.append(TradeSignal(
                        ms.side, "LIMIT", round(qty, 2), round(entry, 4),
                        f"MART level {next_lvl} doubling",
                    ))
                    ms.level = next_lvl
            else:
                signals.append(TradeSignal("CLOSE", "MARKET", 0, state.current_price, "MART L3 forced exit"))
                return signals, MartingaleState()
            return signals, ms

        # Max hold time
        if ms.hold_ticks >= _MAX_HOLD_TICKS:
            signals.append(TradeSignal("CLOSE", "LIMIT", 0, state.current_price, "MART max-hold-time"))
            return signals, MartingaleState()

        # Trend death check
        if abs(state.momentum) < _DEAD_MOMENTUM:
            ms.weak_ticks += 1
            if ms.weak_ticks >= _DEAD_TICKS:
                signals.append(TradeSignal("CLOSE", "LIMIT", 0, state.current_price, "MART trend dead"))
                return signals, MartingaleState()
        else:
            ms.weak_ticks = 0

        return signals, ms

    # No position — check entry
    if state.ema_signal == "NONE":
        return signals, ms

    confidence = _ema_confidence(state.prices)
    if confidence < _CONFIDENCE_THRESHOLD:
        return signals, ms

    # bid_ratio is None when the orderbook endpoint is unavailable.
    # In that case skip the confirmation and trust EMA + momentum alone.
    ob_confirms_buy = bid_ratio is None or bid_ratio > 0.55
    ob_confirms_sell = bid_ratio is None or bid_ratio < 0.45

    if state.ema_signal == "BUY" and ob_confirms_buy:
        side = "BUY"
        price = round(state.current_price * (1.0 - _ENTRY_OFFSET), 4)
    elif state.ema_signal == "SELL" and ob_confirms_sell:
        side = "SELL"
        price = round(state.current_price * (1.0 + _ENTRY_OFFSET), 4)
    else:
        return signals, ms

    qty = (portfolio_value * _LEVEL_SIZES[0]) / max(price, 0.0001)
    if qty <= 0:
        return signals, ms

    signals.append(TradeSignal(side, "LIMIT", round(qty, 2), price, "MART L1 entry"))
    new_ms = MartingaleState(
        level=1,
        original_entry=price,
        side=side,
        hold_ticks=0,
        weak_ticks=0,
    )
    return signals, new_ms
