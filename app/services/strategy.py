"""EMA crossover + order-book imbalance strategy helpers."""

from __future__ import annotations


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
