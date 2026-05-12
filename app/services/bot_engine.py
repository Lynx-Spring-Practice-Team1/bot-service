"""Bot tick loop.

Each active session gets one asyncio.Task (managed by bot_manager).
Risk rules live in risk_manager; market data fetching/caching in market_data;
strategy signal logic in strategy.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, UTC
from decimal import Decimal
from uuid import UUID

from app.database import AsyncSessionLocal
from app.models.bot import BotSession, BotTrade
from app.services import bot_manager, risk_manager
from app.services.broker_client import BrokerAPIError, BrokerClient
from app.services.crypto import InvalidToken, decrypt_token
from app.services.market_data import price_cache
from app.services.strategy import detect_crossover, orderbook_bias

logger = logging.getLogger(__name__)

TICK_INTERVAL = 1.0  # seconds

# Statuses that mean the broker rejected the order and it will never fill.
_REJECTED_STATUSES: frozenset[str] = frozenset(
    {"REJECTED", "CANCELLED", "CANCELED", "FAILED", "EXPIRED", "DENIED"}
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _order_id(order: dict) -> str:
    return str(order.get("id") or order.get("order_id") or order.get("orderId") or "")


def _read_config(cfg: dict) -> dict:
    """Extract strategy params from session.strategy_config with safe defaults."""
    return {
        "fast_period": int(cfg.get("fast_period", 5)),
        "slow_period": int(cfg.get("slow_period", 20)),
        "ob_bullish": float(cfg.get("ob_bullish_threshold", 0.6)),
        "ob_bearish": float(cfg.get("ob_bearish_threshold", 0.4)),
        "stop_loss_pct": float(cfg.get("stop_loss_pct", risk_manager.DEFAULT_STOP_LOSS_PCT)),
        "max_balance_pct": float(cfg.get("max_balance_pct", risk_manager.DEFAULT_MAX_BALANCE_PCT)),
        "max_open_orders": int(cfg.get("max_open_orders", risk_manager.DEFAULT_MAX_OPEN_ORDERS)),
    }


async def _close_position(
    client: BrokerClient,
    session: BotSession,
    current_price: float,
    db,
    reason: str,
) -> None:
    close_side = "SELL" if session.position_side == "BUY" else "BUY"
    qty = 1.0
    try:
        order = await client.place_order(
            symbol=session.symbol,
            side=close_side,
            order_type="LIMIT",
            quantity=qty,
            price=current_price,
        )
        if session.entry_price and session.position_side:
            entry = Decimal(str(session.entry_price))
            cur = Decimal(str(current_price))
            q = Decimal(str(qty))
            pnl = float(
                (cur - entry) * q if session.position_side == "BUY" else (entry - cur) * q
            )
        else:
            pnl = 0.0

        db.add(
            BotTrade(
                session_id=session.id,
                user_id=session.user_id,
                broker_order_id=_order_id(order),
                symbol=session.symbol,
                side=close_side,
                quantity=qty,
                price=current_price,
                status="open",
                pnl=pnl,
            )
        )
        session.entry_price = None
        session.position_side = None
        session.updated_at = datetime.now(UTC)
        logger.info(
            "Closed position (%s)  user=%s  symbol=%s  price=%.4f  pnl=%.4f",
            reason, session.user_id, session.symbol, current_price, pnl,
        )
    except BrokerAPIError as exc:
        logger.error("Failed to close position: %s", exc)


# ── main tick coroutine ───────────────────────────────────────────────────────

async def _bot_tick(session_id: UUID, user_id: str) -> None:
    logger.info("Bot started  user=%s  session=%s", user_id, session_id)

    while True:
        try:
            async with AsyncSessionLocal() as db:
                session: BotSession | None = await db.get(BotSession, session_id)

                if session is None or session.status in ("deactivated", "error"):
                    logger.info("Session %s inactive – exiting task", session_id)
                    return

                if session.status == "paused":
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                try:
                    raw_token = decrypt_token(session.jwt_token)
                except InvalidToken:
                    logger.error(
                        "Cannot decrypt JWT for session %s (secret rotated?) – marking error",
                        session_id,
                    )
                    session.status = "error"
                    await db.commit()
                    return

                client = BrokerClient(raw_token)
                symbol = session.symbol
                p = _read_config(session.strategy_config or {})

                # ── 1. HALT check ─────────────────────────────────────────
                try:
                    halted = await price_cache.is_halted(client, symbol)
                    if halted:
                        if session.status != "halted":
                            session.status = "halted"
                            session.updated_at = datetime.now(UTC)
                            await db.commit()
                            logger.warning("HALT detected for %s – bot halted", symbol)
                        await asyncio.sleep(TICK_INTERVAL)
                        continue
                    elif session.status == "halted":
                        session.status = "active"
                        session.updated_at = datetime.now(UTC)
                        await db.commit()
                        logger.info("HALT cleared for %s – bot resuming", symbol)
                except BrokerAPIError as exc:
                    if exc.status_code == 401:
                        logger.error("JWT rejected for user=%s – marking session error", user_id)
                        session.status = "error"
                        await db.commit()
                        return
                    logger.warning("Halt check failed: %s", exc)

                # ── 2. Price history ──────────────────────────────────────
                try:
                    prices = await price_cache.get_prices(client, symbol, limit=30)
                except BrokerAPIError as exc:
                    if exc.status_code == 401:
                        session.status = "error"
                        await db.commit()
                        return
                    logger.error("Price fetch failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                if len(prices) < p["slow_period"] + 2:
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                current_price = prices[-1]

                # ── 3. Stop-loss ──────────────────────────────────────────
                if session.entry_price and session.position_side:
                    if risk_manager.is_stop_loss_triggered(
                        session.entry_price, current_price,
                        session.position_side, p["stop_loss_pct"],
                    ):
                        logger.warning(
                            "Stop-loss triggered  user=%s  symbol=%s", user_id, symbol
                        )
                        await _close_position(client, session, current_price, db, "stop-loss")
                        await db.commit()
                        await asyncio.sleep(TICK_INTERVAL)
                        continue

                # ── 4. EMA crossover ──────────────────────────────────────
                signal = detect_crossover(prices, fast=p["fast_period"], slow=p["slow_period"])
                if not signal:
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                # ── 5. Order-book confirmation ────────────────────────────
                try:
                    book = await client.get_orderbook(symbol)
                    bias = orderbook_bias(
                        book,
                        bullish_threshold=p["ob_bullish"],
                        bearish_threshold=p["ob_bearish"],
                    )
                except BrokerAPIError as exc:
                    logger.error("Orderbook fetch failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                if bias != signal:
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                # ── 6. Max open orders ────────────────────────────────────
                try:
                    all_orders = await client.get_orders()
                except BrokerAPIError as exc:
                    logger.error("Orders fetch failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                if not risk_manager.can_place_order(all_orders, p["max_open_orders"]):
                    logger.debug("Max open orders reached – skipping")
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                # ── 7. Position sizing ────────────────────────────────────
                try:
                    balance = await client.get_balance()
                    available = float(balance.get("available", 0))
                except BrokerAPIError as exc:
                    logger.error("Balance fetch failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                quantity = risk_manager.compute_quantity(available, current_price, p["max_balance_pct"])
                if quantity <= 0:
                    logger.info("Insufficient balance for %s", symbol)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                # ── 8. Place LIMIT order ──────────────────────────────────
                side = "BUY" if signal == "bullish" else "SELL"
                try:
                    order = await client.place_order(symbol, side, "LIMIT", quantity, current_price)
                except BrokerAPIError as exc:
                    logger.error("Order placement failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                # Validate that the broker actually accepted the order.
                # Some brokers return 200 with status=REJECTED on validation
                # failures (insufficient margin, price bands, etc.).
                order_status = str(order.get("status", "")).upper()
                if order_status in _REJECTED_STATUSES:
                    logger.warning(
                        "Order rejected by broker  user=%s  symbol=%s  "
                        "broker_status=%s  reason=%s",
                        user_id, symbol, order_status,
                        order.get("message") or order.get("reason", "unknown"),
                    )
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                db.add(
                    BotTrade(
                        session_id=session.id,
                        user_id=user_id,
                        broker_order_id=_order_id(order),
                        symbol=symbol,
                        side=side,
                        quantity=quantity,
                        price=current_price,
                        status="open",
                    )
                )
                session.entry_price = current_price
                session.position_side = side
                session.updated_at = datetime.now(UTC)
                await db.commit()

                logger.info(
                    "Placed %s LIMIT  user=%s  symbol=%s  qty=%.2f  price=%.4f  broker_status=%s",
                    side, user_id, symbol, quantity, current_price, order_status or "unknown",
                )

        except asyncio.CancelledError:
            logger.info("Bot task cancelled  user=%s", user_id)
            return
        except Exception:
            logger.exception("Unexpected error in bot tick  user=%s", user_id)

        await asyncio.sleep(TICK_INTERVAL)


# ── public API (used by routers and main) ─────────────────────────────────────

def start_bot(session_id: UUID, user_id: str) -> asyncio.Task:
    return bot_manager.start(session_id, user_id, _bot_tick)


def stop_bot(user_id: str) -> None:
    bot_manager.stop(user_id)


def is_bot_running(user_id: str) -> bool:
    return bot_manager.is_running(user_id)


def stop_all_bots() -> None:
    bot_manager.stop_all()
