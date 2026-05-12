import httpx
from app.config import settings


class BrokerAPIError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"Broker API error {status_code}: {detail}")


class BrokerClient:
    """Async HTTP client wrapping the broker API gateway."""

    def __init__(self, jwt_token: str):
        self._base = settings.BROKER_API_URL.rstrip("/")
        self._headers = {"Authorization": f"Bearer {jwt_token}"}

    async def _get(self, path: str, params: dict | None = None) -> dict | list:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self._base}{path}", headers=self._headers, params=params)
            if r.status_code >= 400:
                raise BrokerAPIError(r.status_code, r.text)
            return r.json()

    async def _post(self, path: str, body: dict) -> dict:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{self._base}{path}", headers=self._headers, json=body)
            if r.status_code >= 400:
                raise BrokerAPIError(r.status_code, r.text)
            return r.json()

    async def _delete(self, path: str) -> dict:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.delete(f"{self._base}{path}", headers=self._headers)
            if r.status_code >= 400:
                raise BrokerAPIError(r.status_code, r.text)
            return r.json()

    # --- market data ---

    async def get_price_events(self, symbol: str, limit: int = 30) -> list[dict]:
        return await self._get(
            "/api/market/events",
            params={"event_type": "PRICE_FEED", "target": symbol, "limit": limit},
        )

    async def get_halt_events(self, symbol: str, limit: int = 5) -> list[dict]:
        return await self._get(
            "/api/market/events",
            params={"event_type": "HALT", "target": symbol, "limit": limit},
        )

    async def get_orderbook(self, symbol: str) -> dict:
        return await self._get(f"/api/market/stocks/{symbol}/orderbook")

    async def get_stocks(self) -> list[dict]:
        return await self._get("/api/market/stocks")

    # --- account ---

    async def get_balance(self) -> dict:
        return await self._get("/api/wallet/balance")

    async def get_portfolio(self) -> dict:
        return await self._get("/api/portfolio")

    # --- orders ---

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float,
    ) -> dict:
        return await self._post(
            "/api/orders",
            {"symbol": symbol, "side": side, "order_type": order_type, "quantity": quantity, "price": price},
        )

    async def get_orders(self) -> list[dict]:
        return await self._get("/api/orders")

    async def cancel_order(self, order_id: str) -> dict:
        return await self._delete(f"/api/orders/{order_id}")
