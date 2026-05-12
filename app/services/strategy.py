"""EMA crossover + order-book imbalance strategy helpers.

Also provides market indicator calculations (momentum, volatility, pressure)
used by the multi-strategy bot engine.
"""

from __future__ import annotations

import math


def _ema(prices: list[float], period: int) -> float | None:
    if len(prices) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema


def extract_prices(events: list[dict]) -> list[float]:
    """Pull price values from PRICE_FEED event objects.

    Tries several field layouts emitted by different exchange implementations.
    """
    prices: list[float] = []
    for ev in events:
        raw = (
            ev.get("price")
            or ev.get("data", {}).get("price")
            or ev.get("payload", {}).get("price")
        )
        if raw is not None:
            try:
                prices.append(float(raw))
            except (TypeError, ValueError):
                pass
    return prices


def detect_crossover(
    prices: list[float],
    fast: int = 5,
    slow: int = 20,
) -> str | None:
    """Return 'bullish', 'bearish', or None.

    Requires at least slow+2 data points to detect a genuine cross between
    the previous and current EMA pair.
    """
    if len(prices) < slow + 2:
        return None

    fast_cur = _ema(prices, fast)
    slow_cur = _ema(prices, slow)
    fast_prv = _ema(prices[:-1], fast)
    slow_prv = _ema(prices[:-1], slow)

    if None in (fast_cur, slow_cur, fast_prv, slow_prv):
        return None

    if fast_prv <= slow_prv and fast_cur > slow_cur:
        return "bullish"
    if fast_prv >= slow_prv and fast_cur < slow_cur:
        return "bearish"
    return None


def orderbook_bias(
    orderbook: dict,
    bullish_threshold: float = 0.6,
    bearish_threshold: float = 0.4,
) -> str | None:
    """Return 'bullish' (bid-heavy), 'bearish' (ask-heavy), or None (neutral)."""
    bids: list[dict] = orderbook.get("bids", [])
    asks: list[dict] = orderbook.get("asks", [])

    bid_vol = sum(float(b.get("qty", 0)) for b in bids)
    ask_vol = sum(float(a.get("qty", 0)) for a in asks)
    total = bid_vol + ask_vol

    if total == 0:
        return None

    ratio = bid_vol / total
    if ratio > bullish_threshold:
        return "bullish"
    if ratio < bearish_threshold:
        return "bearish"
    return None


# ── multi-strategy indicator helpers ─────────────────────────────────────────

def ema_signal(prices: list[float], fast: int = 5, slow: int = 20) -> str:
    """Return current EMA position: 'BUY', 'SELL', or 'NONE'.

    Unlike detect_crossover this returns a value every tick (not just on the
    crossover event), which is what the strategy selector needs.
    """
    if len(prices) < slow:
        return "NONE"
    fast_val = _ema(prices, fast)
    slow_val = _ema(prices, slow)
    if fast_val is None or slow_val is None:
        return "NONE"
    if fast_val > slow_val:
        return "BUY"
    if fast_val < slow_val:
        return "SELL"
    return "NONE"


def calculate_momentum(prices: list[float], window: int = 5) -> float:
    """(upTicks - downTicks) / totalTicks over the last `window` price changes.

    Returns a value in [-1.0, 1.0].
    """
    if len(prices) < 2:
        return 0.0
    recent = prices[-min(window + 1, len(prices)):]
    deltas = [recent[i] - recent[i - 1] for i in range(1, len(recent))]
    if not deltas:
        return 0.0
    up = sum(1 for d in deltas if d > 0)
    down = sum(1 for d in deltas if d < 0)
    return (up - down) / len(deltas)


def calculate_volatility(prices: list[float], window: int = 10) -> float:
    """Standard deviation of percentage price changes over the last `window` ticks."""
    if len(prices) < 2:
        return 0.0
    recent = prices[-min(window + 1, len(prices)):]
    changes = [
        (recent[i] - recent[i - 1]) / recent[i - 1]
        for i in range(1, len(recent))
        if recent[i - 1] != 0
    ]
    if not changes:
        return 0.0
    mean = sum(changes) / len(changes)
    variance = sum((c - mean) ** 2 for c in changes) / len(changes)
    return math.sqrt(variance)


def calculate_pressure(orderbook: dict) -> float:
    """(bid_vol - ask_vol) / total — ranges from -1.0 (all asks) to +1.0 (all bids)."""
    bids: list[dict] = orderbook.get("bids", [])
    asks: list[dict] = orderbook.get("asks", [])
    bid_vol = sum(float(b.get("qty", 0)) for b in bids)
    ask_vol = sum(float(a.get("qty", 0)) for a in asks)
    total = bid_vol + ask_vol
    if total == 0:
        return 0.0
    return (bid_vol - ask_vol) / total


def calculate_bid_ratio(orderbook: dict) -> float | None:
    """bid_vol / total — ranges from 0.0 to 1.0; 0.5 is balanced.

    Returns None when the orderbook is empty (endpoint unavailable), so
    callers can skip confirmation checks that require real depth data.
    """
    bids: list[dict] = orderbook.get("bids", [])
    asks: list[dict] = orderbook.get("asks", [])
    bid_vol = sum(float(b.get("qty", 0)) for b in bids)
    ask_vol = sum(float(a.get("qty", 0)) for a in asks)
    total = bid_vol + ask_vol
    if total == 0:
        return None  # no data
    return bid_vol / total
