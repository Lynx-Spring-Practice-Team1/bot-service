"""Thin caching layer over broker market-data endpoints.

A single shared PriceCache instance deduplicates identical symbol fetches
that land within the same second when multiple users trade the same ticker.
The cache is TTL-based and never grows unbounded (one entry per symbol).
"""

from __future__ import annotations

import time

from app.services.broker_client import BrokerClient
from app.services.strategy import extract_prices

_PRICE_TTL = 0.8   # seconds — keeps consecutive same-user ticks fresh
_HALT_TTL = 1.0    # seconds


class _Cache:
    def __init__(self) -> None:
        self._prices: dict[str, tuple[float, list[float]]] = {}
        self._halt: dict[str, tuple[float, bool]] = {}

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

    def invalidate(self, symbol: str) -> None:
        self._prices.pop(symbol, None)
        self._halt.pop(symbol, None)


# module-level singleton shared by all bot tasks
price_cache = _Cache()
