"""WebSocket feed client for real-time market data.

Connects to the market-notifications relay at /api/ws and provides:
- Rolling price buffer per symbol (replaces REST PRICE_FEED polling)
- ORDER_UPDATE notifications (order status changes from the exchange)

The feed runs as a background asyncio task alongside the bot tick loop.
When disconnected it retries with exponential backoff; the tick loop falls
back to REST polling in the meantime so the bot never stalls.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque

import websockets
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from app.config import settings

logger = logging.getLogger(__name__)

_PRICE_BUFFER = 60      # max prices kept per symbol
_RECONNECT_BASE = 1.0   # seconds, doubles on each failure
_RECONNECT_MAX = 30.0   # seconds ceiling


def _ws_url_from_http(http_url: str) -> str:
    url = http_url.rstrip("/")
    if url.startswith("https://"):
        return "wss://" + url[8:] + "/api/ws"
    return "ws://" + url[7:] + "/api/ws"


def _extract_price(msg: dict) -> float | None:
    payload = msg.get("payload") or {}
    for container in (payload, msg):
        for key in ("price", "last_price", "last", "magnitude"):
            val = container.get(key)
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    return None


def _extract_symbol(msg: dict) -> str | None:
    payload = msg.get("payload") or {}
    for container in (payload, msg):
        for key in ("symbol", "ticker", "target"):
            val = container.get(key)
            if val:
                return str(val).upper()
    return None


class BotWebSocketFeed:
    """Per-session WebSocket feed.

    One instance is created per active bot session and runs as a sibling
    asyncio task alongside the tick loop.
    """

    def __init__(self, jwt_token: str) -> None:
        self._jwt = jwt_token
        self._url = _ws_url_from_http(settings.BROKER_API_URL)
        self._prices: dict[str, deque[float]] = {}
        self._order_updates: dict[str, dict] = {}  # order_id → latest payload
        self._price_events: dict[str, asyncio.Event] = {}  # symbol → event
        self._connected = False
        self._stop = asyncio.Event()

    # ── public API ────────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._connected

    def get_prices(self, symbol: str) -> list[float]:
        buf = self._prices.get(symbol.upper())
        return list(buf) if buf else []

    def inject_prices(self, symbol: str, prices: list[float]) -> None:
        """Seed the buffer with historical prices fetched from REST on startup."""
        sym = symbol.upper()
        buf = self._prices.setdefault(sym, deque(maxlen=_PRICE_BUFFER))
        if not buf:
            buf.extend(prices[-_PRICE_BUFFER:])

    async def wait_for_price(self, symbol: str, timeout: float = 2.0) -> bool:
        """Block until the next PRICE_UPDATE for this symbol arrives, or timeout.

        Returns True if a new price arrived, False on timeout (WS disconnected
        or no data). Either way the caller should proceed — this replaces fixed
        asyncio.sleep() calls so the bot reacts immediately to new prices.
        """
        sym = symbol.upper()
        if not sym:
            try:
                await asyncio.wait_for(asyncio.sleep(timeout), timeout=timeout)
            except asyncio.TimeoutError:
                pass
            return False

        event = self._price_events.setdefault(sym, asyncio.Event())
        event.clear()
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def pop_order_updates(self) -> dict[str, dict]:
        """Return and clear accumulated order status updates."""
        updates = dict(self._order_updates)
        self._order_updates.clear()
        return updates

    def stop(self) -> None:
        self._stop.set()

    # ── background task ───────────────────────────────────────────────────────

    async def run(self) -> None:
        url = f"{self._url}?token={self._jwt}"
        delay = _RECONNECT_BASE
        while not self._stop.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, open_timeout=10) as ws:
                    self._connected = True
                    delay = _RECONNECT_BASE
                    logger.info("WS feed connected to %s", self._url)
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        self._handle(raw)
            except asyncio.CancelledError:
                break
            except (ConnectionClosed, InvalidHandshake, OSError) as exc:
                self._connected = False
                logger.warning("WS feed disconnected: %s — retry in %.0fs", exc, delay)
            except Exception as exc:
                self._connected = False
                logger.error("WS feed error: %s — retry in %.0fs", exc, delay)

            if not self._stop.is_set():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                delay = min(delay * 2, _RECONNECT_MAX)

        self._connected = False
        logger.info("WS feed stopped")

    def _handle(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        msg_type = str(msg.get("type", "")).upper()

        if msg_type == "PRICE_UPDATE":
            symbol = _extract_symbol(msg)
            price = _extract_price(msg)
            if symbol and price is not None:
                buf = self._prices.setdefault(symbol, deque(maxlen=_PRICE_BUFFER))
                buf.append(price)
                event = self._price_events.get(symbol)
                if event:
                    event.set()

        elif msg_type == "ORDER_UPDATE":
            payload = msg.get("payload") or msg
            order_id = str(payload.get("order_id", "")).strip()
            if order_id:
                self._order_updates[order_id] = payload
