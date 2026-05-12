"""Asyncio-based bot engine.

One asyncio.Task runs per active user session.  Each task ticks every second,
fetches market data, evaluates the EMA-crossover + order-book strategy, and
places LIMIT orders when a confirmed signal fires, respecting all risk rules.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, UTC
from uuid import UUID

from app.database import AsyncSessionLocal
from app.models.bot import BotSession, BotTrade
from app.services.broker_client import BrokerAPIError, BrokerClient
from app.services.strategy import detect_crossover, extract_prices, orderbook_bias

logger = logging.getLogger(__name__)

# ── risk constants ────────────────────────────────────────────────────────────
FAST_PERIOD = 5
SLOW_PERIOD = 20
MAX_BOT_OPEN_ORDERS = 3
STOP_LOSS_PCT = 0.02      # 2 %
MAX_BALANCE_PCT = 0.20    # 20 % of available balance per order
TICK_INTERVAL = 1.0       # seconds

# ── global task registry  user_id → Task ──────────────────────────────────────
_tasks: dict[str, asyncio.Task] = {}


# ── internal helpers ──────────────────────────────────────────────────────────

def _order_id(order: dict) -> str:
    return str(order.get("id") or order.get("order_id") or order.get("orderId") or "")


def _open_order_statuses() -> set[str]:
    return {"open", "pending", "NEW", "PENDING", "OPEN", "submitted"}


async def _close_position(
    client: BrokerClient,
    session: BotSession,
    current_price: float,
    db,
    reason: str,
) -> None:
    """Place the opposite side order to close the current position."""
    close_side = "SELL" if session.position_side == "BUY" else "BUY"
    qty = 1.0  # minimal close – real sizing would need portfolio data
    try:
        order = await client.place_order(
            symbol=session.symbol,
            side=close_side,
            order_type="LIMIT",
            quantity=qty,
            price=current_price,
        )
        if session.entry_price and session.position_side:
            if session.position_side == "BUY":
                pnl = (current_price - session.entry_price) * qty
            else:
                pnl = (session.entry_price - current_price) * qty
        else:
            pnl = 0.0

        trade = BotTrade(
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
        db.add(trade)
        session.entry_price = None
        session.position_side = None
        session.updated_at = datetime.now(UTC)
        logger.info(
            "Closed position (%s) for user=%s symbol=%s at %.4f pnl=%.4f",
            reason, session.user_id, session.symbol, current_price, pnl,
        )
    except BrokerAPIError as exc:
        logger.error("Failed to close position: %s", exc)


async def _bot_tick(session_id: UUID, user_id: str) -> None:
    logger.info("Bot started  user=%s  session=%s", user_id, session_id)

    while True:
        try:
            async with AsyncSessionLocal() as db:
                session: BotSession | None = await db.get(BotSession, session_id)

                if session is None or session.status == "deactivated":
                    logger.info("Session %s deactivated – exiting task", session_id)
                    return

                if session.status == "paused":
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                client = BrokerClient(session.jwt_token)
                symbol = session.symbol

                # ── 1. check for HALT events ──────────────────────────────
                try:
                    halt_events = await client.get_halt_events(symbol, limit=5)
                    if halt_events:
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
                        logger.error("JWT expired for user=%s – deactivating bot", user_id)
                        session.status = "deactivated"
                        await db.commit()
                        return
                    logger.warning("Could not fetch halt events: %s", exc)

                # ── 2. fetch price history ────────────────────────────────
                try:
                    price_events = await client.get_price_events(symbol, limit=30)
                except BrokerAPIError as exc:
                    if exc.status_code == 401:
                        session.status = "deactivated"
                        await db.commit()
                        return
                    logger.error("Price fetch failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                prices = extract_prices(price_events)
                if len(prices) < SLOW_PERIOD + 2:
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                current_price = prices[-1]

                # ── 3. stop-loss check ────────────────────────────────────
                if session.entry_price and session.position_side:
                    entry = session.entry_price
                    if session.position_side == "BUY":
                        loss_pct = (entry - current_price) / entry
                    else:
                        loss_pct = (current_price - entry) / entry

                    if loss_pct >= STOP_LOSS_PCT:
                        logger.warning(
                            "Stop-loss triggered for user=%s symbol=%s loss=%.2f%%",
                            user_id, symbol, loss_pct * 100,
                        )
                        await _close_position(client, session, current_price, db, "stop-loss")
                        await db.commit()
                        await asyncio.sleep(TICK_INTERVAL)
                        continue

                # ── 4. EMA crossover signal ───────────────────────────────
                signal = detect_crossover(prices, fast=FAST_PERIOD, slow=SLOW_PERIOD)
                if not signal:
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                # ── 5. order-book confirmation ────────────────────────────
                try:
                    orderbook = await client.get_orderbook(symbol)
                    bias = orderbook_bias(orderbook)
                except BrokerAPIError as exc:
                    logger.error("Orderbook fetch failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                if bias != signal:
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                # ── 6. risk: max open bot orders ──────────────────────────
                try:
                    all_orders = await client.get_orders()
                except BrokerAPIError as exc:
                    logger.error("Orders fetch failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                open_count = sum(
                    1 for o in all_orders if o.get("status") in _open_order_statuses()
                )
                if open_count >= MAX_BOT_OPEN_ORDERS:
                    logger.debug("Max open orders (%d) reached – skipping", MAX_BOT_OPEN_ORDERS)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                # ── 7. risk: position sizing (max 20 % of available cash) ─
                try:
                    balance = await client.get_balance()
                    available = float(balance.get("available", 0))
                except BrokerAPIError as exc:
                    logger.error("Balance fetch failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                max_spend = available * MAX_BALANCE_PCT
                if max_spend <= 0 or current_price <= 0:
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                quantity = round(max_spend / current_price, 2)
                if quantity <= 0:
                    logger.info("Insufficient balance for %s", symbol)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                # ── 8. place LIMIT order ──────────────────────────────────
                side = "BUY" if signal == "bullish" else "SELL"
                try:
                    order = await client.place_order(
                        symbol=symbol,
                        side=side,
                        order_type="LIMIT",
                        quantity=quantity,
                        price=current_price,
                    )
                except BrokerAPIError as exc:
                    logger.error("Order placement failed: %s", exc)
                    await asyncio.sleep(TICK_INTERVAL)
                    continue

                trade = BotTrade(
                    session_id=session.id,
                    user_id=user_id,
                    broker_order_id=_order_id(order),
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    price=current_price,
                    status="open",
                )
                db.add(trade)

                session.entry_price = current_price
                session.position_side = side
                session.updated_at = datetime.now(UTC)
                await db.commit()

                logger.info(
                    "Placed %s LIMIT order  user=%s  symbol=%s  qty=%.2f  price=%.4f",
                    side, user_id, symbol, quantity, current_price,
                )

        except asyncio.CancelledError:
            logger.info("Bot task cancelled  user=%s", user_id)
            return
        except Exception:
            logger.exception("Unexpected error in bot tick  user=%s", user_id)

        await asyncio.sleep(TICK_INTERVAL)


# ── public API ────────────────────────────────────────────────────────────────

def start_bot(session_id: UUID, user_id: str) -> asyncio.Task:
    stop_bot(user_id)
    task = asyncio.create_task(_bot_tick(session_id, user_id), name=f"bot-{user_id}")
    _tasks[user_id] = task
    return task


def stop_bot(user_id: str) -> None:
    task = _tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()


def is_bot_running(user_id: str) -> bool:
    task = _tasks.get(user_id)
    return task is not None and not task.done()


def stop_all_bots() -> None:
    for uid in list(_tasks.keys()):
        stop_bot(uid)
