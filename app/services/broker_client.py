import asyncio

import httpx

from app.config import settings

_RETRIES = 2       # up to 2 retries (3 total attempts)
_BACKOFF = 0.5     # seconds; multiplied by attempt number (0.5s, 1.0s)


class BrokerAPIError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"Broker API error {status_code}: {detail}")


class BrokerClient:
    """Async HTTP client wrapping the broker API gateway.

    Transient failures (5xx, network errors) are retried up to _RETRIES times
    with linear backoff.  4xx errors surface immediately — retrying them would
    not help.
    """

    def __init__(self, jwt_token: str):
        self._base = settings.BROKER_API_URL.rstrip("/")
        self._headers = {"Authorization": f"Bearer {jwt_token}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
    ) -> dict | list:
        url = f"{self._base}{path}"
        last_exc: Exception | None = None

        for attempt in range(1 + _RETRIES):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    r = await client.request(
                        method, url,
                        headers=self._headers,
                        params=params,
                        json=json,
                    )

                if r.status_code >= 400:
                    err = BrokerAPIError(r.status_code, r.text)
                    if r.status_code < 500:
                        raise err  # 4xx — no retry, surface immediately
                    # 5xx — fall through to backoff + retry
                    last_exc = err
                else:
                    return r.json()

            except BrokerAPIError:
                raise  # re-raises 4xx without retry
            except (httpx.TransientError, httpx.TimeoutException) as exc:
                last_exc = exc

            if attempt < _RETRIES:
                await asyncio.sleep(_BACKOFF * (attempt + 1))

        raise last_exc  # type: ignore[misc]

    # ── market data ───────────────────────────────────────────────────────────

    async def get_price_events(self, symbol: str, limit: int = 30) -> list[dict]:
        return await self._request(
            "GET", "/api/market/events",
            params={"event_type": "PRICE_FEED", "target": symbol, "limit": limit},
        )

    async def get_halt_events(self, symbol: str, limit: int = 5) -> list[dict]:
        return await self._request(
            "GET", "/api/market/events",
            params={"event_type": "HALT", "target": symbol, "limit": limit},
        )

    async def get_market_events(self, limit: int = 10) -> list[dict]:
        """Fetch recent market events of all types.

        Returns unfiltered so the caller can exclude PRICE_FEED / HALT.
        A higher default limit gives the cache enough events to find the
        most recent non-price, non-halt entry.
        """
        return await self._request(
            "GET", "/api/market/events",
            params={"limit": limit},
        )

    async def get_orderbook(self, symbol: str) -> dict:
        return await self._request("GET", f"/api/market/stocks/{symbol}/orderbook")

    async def get_stocks(self) -> list[dict]:
        return await self._request("GET", "/api/market/stocks")

    # ── account ───────────────────────────────────────────────────────────────

    async def get_balance(self) -> dict:
        return await self._request("GET", "/api/wallet/balance")

    async def get_portfolio(self) -> dict:
        return await self._request("GET", "/api/portfolio")

    # ── orders ────────────────────────────────────────────────────────────────

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None = None,
    ) -> dict:
        # order-service expects quantity as int (OrderCreate schema)
        payload: dict = {
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "quantity": max(1, int(quantity)),
        }
        if price is not None:
            payload["price"] = price
        return await self._request("POST", "/api/orders", json=payload)

    async def get_orders(self) -> list[dict]:
        return await self._request("GET", "/api/orders")

    async def cancel_order(self, order_id: str) -> dict:
        return await self._request("DELETE", f"/api/orders/{order_id}")
