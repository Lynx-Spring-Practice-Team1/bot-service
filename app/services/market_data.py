"""Thin caching layer over broker market-data endpoints.

A single shared PriceCache instance deduplicates identical symbol fetches
that land within the same second when multiple users trade the same ticker.
The cache is TTL-based and never grows unbounded (one entry per symbol).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from app.services.broker_client import BrokerClient
from app.services.strategy import extract_prices

_PRICE_TTL = 0.8   # seconds — keeps consecutive same-user ticks fresh
_HALT_TTL = 1.0    # seconds
_EVENT_TTL = 1.0   # seconds — market events change at most every tick

# Event types that are NOT trading signals — filtered out from get_active_market_event
_NON_TRADING_TYPES: frozenset[str] = frozenset({"PRICE_FEED", "HALT", ""})


def _is_event_active(event: dict) -> bool:
    """Return False when the event's duration window has already elapsed.

    Each tick is 1 simulated market minute = 1 real second, so
    duration_ticks maps directly to seconds of wall-clock time.
    """
    duration_ticks = int(event.get("duration_ticks", 0))
    if duration_ticks <= 0:
        return True  # no duration info — assume still active

    market_time_raw = event.get("market_time")
    if not market_time_raw:
        return True

    try:
        market_time = datetime.fromisoformat(
            str(market_time_raw).replace("Z", "+00:00")
        )
        expiry = market_time + timedelta(seconds=duration_ticks)
        return datetime.now(UTC) <= expiry
    except (ValueError, TypeError):
        return True  # can't parse — don't block on bad data


class _Cache:
    def __init__(self) -> None:
        self._prices: dict[str, tuple[float, list[float]]] = {}
        self._halt: dict[str, tuple[float, bool]] = {}
        self._event: tuple[float, dict | None] | None = None  # single global slot

    async def get_prices(
        self,
        client: BrokerClient,
        symbol: str,
        limit: int = 30,
    ) -> list[float]:
        now = time.monotonic()
        cached = self._prices.get(symbol)
        if cached and now - cached[0] < _PRICE_TTL:
            return cached[1]

        events = await client.get_price_events(symbol, limit=limit)
        prices = extract_prices(events)
        self._prices[symbol] = (now, prices)
        return prices

    async def is_halted(self, client: BrokerClient, symbol: str) -> bool:
        now = time.monotonic()
        cached = self._halt.get(symbol)
        if cached and now - cached[0] < _HALT_TTL:
            return cached[1]

        events = await client.get_halt_events(symbol, limit=5)
        halted = bool(events)
        self._halt[symbol] = (now, halted)
        return halted

    async def get_active_market_event(self, client: BrokerClient) -> dict | None:
        """Return the most recent active trading event or None.

        Fetches all recent events unfiltered, then skips:
        - PRICE_FEED and HALT rows (not trading signals)
        - Events whose duration window has already elapsed
        """
        now = time.monotonic()
        if self._event is not None and now - self._event[0] < _EVENT_TTL:
            return self._event[1]

        events = await client.get_market_events()
        event = next(
            (
                e for e in events
                if str(e.get("event_type") or e.get("type", "")).upper()
                not in _NON_TRADING_TYPES
                and _is_event_active(e)
            ),
            None,
        )
        self._event = (now, event)
        return event

    def invalidate(self, symbol: str) -> None:
        self._prices.pop(symbol, None)
        self._halt.pop(symbol, None)

    def get_last_price(self, symbol: str) -> float | None:
        """Return the most recent cached price for the symbol, or None.

        Used by admin endpoints that need a mark price without driving a
        fresh broker fetch. Returns None when the cache hasn't seen this
        symbol or the prices list is empty.
        """
        cached = self._prices.get(symbol)
        if not cached or not cached[1]:
            return None
        return float(cached[1][-1])


# module-level singleton shared by all bot tasks
price_cache = _Cache()
