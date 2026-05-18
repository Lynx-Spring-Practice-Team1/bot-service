"""Bot tick loop — multi-strategy edition.

Each active session gets one asyncio.Task (managed by bot_manager).
Every tick the engine:
  1. Checks for HALT
  2. Fetches price history, orderbook, market events, balance
  3. Calculates momentum / volatility / pressure / EMA signal
  4. Selects the active strategy (event_driven | martingale | grid | hold)
  5. Closes previous strategy positions when switching
  6. Generates trade signals from the active strategy
  7. Applies portfolio-level risk rules (daily loss, drawdown, max orders)
  8. Executes approved signals via the broker API
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from app.database import AsyncSessionLocal
from app.models.bot import BotSession, BotTrade
from app.services import activity_logger, bot_manager, risk_manager, runtime_state
from app.services.broker_client import BrokerAPIError, BrokerClient
from app.services.crypto import InvalidToken, decrypt_token
from app.services.market_data import price_cache
from app.services.strategies import event_driven, grid_trading, martingale
from app.services.strategies.base import MarketState, TradeSignal
from app.services.strategies.grid_trading import GridState
from app.services.strategies.martingale import MartingaleState
from app.services.strategy import (
    calculate_bid_ratio,
    calculate_momentum,
    calculate_pressure,
    calculate_volatility,
    ema_signal,
)
from app.services.strategy_selector import select_strategy
from app.services.ws_feed import BotWebSocketFeed

logger = logging.getLogger(__name__)

TICK_INTERVAL = 1.0  # seconds

# Matches OrderStatus enum in order-service/app/models.py
_REJECTED_STATUSES: frozenset[str] = frozenset({"REJECTED", "CANCELLED"})
_OPEN_STATUSES: frozenset[str] = frozenset({"PENDING", "ACCEPTED"})
_FILLED_STATUSES: frozenset[str] = frozenset({"FILLED", "COMPLETE", "COMPLETED"})


# ── helpers ───────────────────────────────────────────────────────────────────

_GLOBAL_SCOPES: frozenset[str] = frozenset({"GLOBAL", "MARKET", ""})


def _filter_event_for_symbol(event: dict | None, symbol: str) -> dict | None:
    """Return the event only if it is relevant to this bot's symbol.

    Global-scope events (BULL_RUN, BEAR_CRASH) affect all symbols.
    Stock-scope events (STOCK_SHOCK) only affect the target symbol.
    Sector events pass through — we don't have sector→symbol mapping.
    """
    if event is None:
        return None
    scope = str(event.get("scope", "")).upper()
    target = str(event.get("target", "")).upper().strip()
    if scope in _GLOBAL_SCOPES:
        return event
    if scope == "STOCK" and target != symbol.upper():
        return None  # event is for a different stock
    return event


def _order_id(order: dict) -> str:
    # order-service returns id as int; avoid falsy-0 bug with `or` chaining
    for key in ("id", "order_id", "orderId"):
        val = order.get(key)
        if val is not None:
            return str(val)
    return ""


def _read_config(cfg: dict) -> dict:
    return {
        "fast_period": int(cfg.get("fast_period", 5)),
        "slow_period": int(cfg.get("slow_period", 20)),
        "ob_bullish": float(cfg.get("ob_bullish_threshold", 0.6)),
        "ob_bearish": float(cfg.get("ob_bearish_threshold", 0.4)),
        "stop_loss_pct": float(cfg.get("stop_loss_pct", risk_manager.DEFAULT_STOP_LOSS_PCT)),
        "max_balance_pct": float(cfg.get("max_balance_pct", risk_manager.DEFAULT_MAX_BALANCE_PCT)),
        "max_open_orders": int(cfg.get("max_open_orders", risk_manager.DEFAULT_MAX_OPEN_ORDERS)),
        "daily_loss_limit_pct": float(cfg.get("daily_loss_limit_pct", risk_manager.DEFAULT_DAILY_LOSS_LIMIT)),
        "max_drawdown_pct": float(cfg.get("max_drawdown_pct", risk_manager.DEFAULT_MAX_DRAWDOWN)),
    }


async def _close_position(
    client: BrokerClient,
    session: BotSession,
    current_price: float,
    db,
    reason: str,
    order_type: str = "LIMIT",
) -> float:
    """Place a closing order for the current session position. Returns realized PnL."""
    if not session.position_side:
        return 0.0

    close_side = "SELL" if session.position_side == "BUY" else "BUY"
    qty = float(session.entry_quantity) if session.entry_quantity else 1.0
    pnl = 0.0
    price_arg = current_price if order_type == "LIMIT" else None
    try:
        order = await client.place_order(
            symbol=session.symbol,
            side=close_side,
            order_type=order_type,
            quantity=qty,
            price=price_arg,
        )
        if session.entry_price:
            entry = Decimal(str(session.entry_price))
            cur = Decimal(str(current_price))
            q = Decimal(str(qty))
            pnl = float(
                (cur - entry) * q if session.position_side == "BUY" else (entry - cur) * q
            )

        db.add(BotTrade(
            session_id=session.id,
            user_id=session.user_id,
            broker_order_id=_order_id(order),
            symbol=session.symbol,
            side=close_side,
            quantity=qty,
            price=current_price,
            status="open",
            pnl=pnl,
        ))
        session.entry_price = None
        session.entry_quantity = None
        session.position_side = None
        session.updated_at = datetime.now(UTC)
        logger.info(
            "Closed position (%s)  user=%s  symbol=%s  price=%.4f  pnl=%.4f",
            reason, session.user_id, session.symbol, current_price, pnl,
        )
    except BrokerAPIError as exc:
        logger.error("Failed to close position: %s", exc)
    return pnl


async def _cancel_orders(client: BrokerClient, order_ids: list[str]) -> None:
    for oid in order_ids:
        try:
            await client.cancel_order(oid)
        except BrokerAPIError as exc:
            logger.warning("Failed to cancel order %s: %s", oid, exc)


# ── main tick coroutine ───────────────────────────────────────────────────────

async def _bot_tick(session_id: UUID, user_id: str) -> None:
    """Top-level entry: starts the WS feed then runs the tick loop."""
    async with AsyncSessionLocal() as db:
        session: BotSession | None = await db.get(BotSession, session_id)
        if session is None:
            return
        try:
            raw_token = decrypt_token(session.jwt_token)
        except InvalidToken:
            logger.error("Cannot decrypt JWT for session %s on startup – marking error", session_id)
            session.status = "error"
            await db.commit()
            return

    feed = BotWebSocketFeed(raw_token)
    feed_task = asyncio.create_task(feed.run(), name=f"ws-feed-{user_id}")
    try:
        await _bot_loop(session_id, user_id, feed)
    finally:
        feed.stop()
        with suppress(asyncio.CancelledError):
            await feed_task


async def _bot_loop(session_id: UUID, user_id: str, feed: BotWebSocketFeed) -> None:
    logger.info("Bot started  user=%s  session=%s", user_id, session_id)

    # Seed in-memory runtime snapshot for the admin dashboard.
    runtime_state.update(
        session_id,
        runtime_state="starting",
        current_strategy="hold",
        last_decision_at=datetime.now(UTC),
    )

    try:
        async with AsyncSessionLocal() as start_db:
            await activity_logger.log_event(
                start_db, session_id=session_id, user_id=user_id,
                severity="INFO", event_type="BOT_STARTED",
                message=f"Bot loop started for {user_id}",
                publish_lifecycle=True,
            )
            await start_db.commit()
    except Exception:
        logger.exception("Failed to publish BOT_STARTED event")

    # In-memory per-task state (reset on task restart)
    mart_state = MartingaleState()
    grid_state = GridState()
    current_strategy: str = "hold"
    symbol: str = ""  # guards end-of-loop wait before first DB load

    daily_pnl: float = 0.0
    peak_balance: float | None = None
    last_reset: date | None = None

    # Maps exchange_order_id → platform_order_id so WebSocket ORDER_UPDATE
    # fill notifications (keyed by exchange UUID) can be matched back to
    # BotTrade records (stored with the platform integer ID).
    exchange_to_platform: dict[str, str] = {}

    while True:
        try:
            async with AsyncSessionLocal() as db:
                session: BotSession | None = await db.get(BotSession, session_id)

                if session is None or session.status in ("deactivated", "error"):
                    logger.info("Session %s inactive – exiting task", session_id)
                    return

                symbol = session.symbol
                p = _read_config(session.strategy_config or {})

                if session.status == "paused":
                    # Auto-resume on a new trading day: daily P&L and peak
                    # balance counters reset at midnight, so a pause that was
                    # triggered by the daily-loss or drawdown limit is no longer
                    # valid once the day rolls over.
                    today_check = datetime.now(UTC).date()
                    if last_reset is not None and last_reset < today_check:
                        daily_pnl = 0.0
                        peak_balance = None
                        last_reset = today_check
                        session.status = "active"
                        session.updated_at = datetime.now(UTC)
                        await db.commit()
                        logger.info(
                            "Bot auto-resumed on new trading day  user=%s  session=%s",
                            user_id, session_id,
                        )
                        # Fall through — let this tick execute normally.
                    else:
                        await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)
                        continue

                try:
                    raw_token = decrypt_token(session.jwt_token)
                except InvalidToken:
                    logger.error(
                        "Cannot decrypt JWT for session %s (secret rotated?) – marking error",
                        session_id,
                    )
                    session.status = "error"
                    runtime_state.update(session_id, runtime_state="error")
                    runtime_state.record_error(session_id, "JWT decryption failed")
                    await activity_logger.log_event(
                        db, session_id=session_id, user_id=user_id,
                        severity="ERROR", event_type="TOKEN_EXPIRED",
                        message="Cannot decrypt stored JWT (secret rotated?)",
                        publish_lifecycle=True,
                    )
                    await db.commit()
                    return

                client = BrokerClient(raw_token)

                # ── 1. HALT check ─────────────────────────────────────────
                try:
                    halted = await price_cache.is_halted(client, symbol)
                    if halted:
                        if session.status != "halted":
                            session.status = "halted"
                            session.updated_at = datetime.now(UTC)
                            runtime_state.update(session_id, runtime_state="halted")
                            await activity_logger.log_event(
                                db, session_id=session_id, user_id=user_id,
                                severity="WARNING", event_type="HALT_DETECTED",
                                message=f"HALT detected for {symbol}",
                                context={"symbol": symbol},
                                publish_lifecycle=True,
                            )
                            await db.commit()
                            logger.warning("HALT detected for %s – bot halted", symbol)
                        await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)
                        continue
                    elif session.status == "halted":
                        session.status = "active"
                        session.updated_at = datetime.now(UTC)
                        await activity_logger.log_event(
                            db, session_id=session_id, user_id=user_id,
                            severity="INFO", event_type="HALT_CLEARED",
                            message=f"HALT cleared for {symbol}",
                            context={"symbol": symbol},
                            publish_lifecycle=True,
                        )
                        await db.commit()
                        logger.info("HALT cleared for %s – bot resuming", symbol)
                except BrokerAPIError as exc:
                    if exc.status_code == 401:
                        logger.error("JWT rejected for user=%s – marking session error", user_id)
                        session.status = "error"
                        runtime_state.update(session_id, runtime_state="error")
                        runtime_state.record_error(session_id, "Broker rejected JWT (401)")
                        await activity_logger.log_event(
                            db, session_id=session_id, user_id=user_id,
                            severity="ERROR", event_type="BROKER_401",
                            message="Broker rejected JWT — marking session error",
                            publish_lifecycle=True,
                        )
                        await db.commit()
                        return
                    logger.warning("Halt check failed: %s", exc)

                # ── 2. Price history ──────────────────────────────────────
                ws_prices = feed.get_prices(symbol)
                if len(ws_prices) >= p["slow_period"] + 2:
                    prices = ws_prices
                else:
                    # WS buffer not ready yet — fall back to REST and seed feed
                    try:
                        prices = await price_cache.get_prices(client, symbol, limit=30)
                        feed.inject_prices(symbol, prices)
                    except BrokerAPIError as exc:
                        if exc.status_code == 401:
                            session.status = "error"
                            await db.commit()
                            return
                        logger.error("Price fetch failed: %s", exc)
                        await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)
                        continue

                if len(prices) < p["slow_period"] + 2:
                    await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)
                    continue

                current_price = prices[-1]

                # ── 3. Supporting market data ─────────────────────────────
                try:
                    book = await client.get_orderbook(symbol)
                except BrokerAPIError as exc:
                    if exc.status_code == 404:
                        # Orderbook endpoint not available — use neutral defaults.
                        # pressure=0.0 and bid_ratio=None signal "no data" downstream.
                        book = {}
                    elif exc.status_code == 401:
                        session.status = "error"
                        await db.commit()
                        return
                    else:
                        logger.warning("Orderbook fetch failed (%s): %s", exc.status_code, exc)
                        book = {}

                try:
                    market_event = await price_cache.get_active_market_event(client)
                    market_event = _filter_event_for_symbol(market_event, symbol)
                except BrokerAPIError as exc:
                    logger.warning("Market event fetch failed: %s", exc)
                    market_event = None

                try:
                    balance_info = await client.get_balance()
                    available = float(balance_info.get("available", 0))
                    # wallet-service: {"balance": <total>, "reserved": <held>, "available": <free>}
                    total_balance = float(balance_info.get("balance", available))
                except BrokerAPIError as exc:
                    logger.error("Balance fetch failed: %s", exc)
                    await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)
                    continue

                # ── 4. Daily tracker reset ────────────────────────────────
                today = datetime.now(UTC).date()
                if last_reset != today:
                    daily_pnl = 0.0
                    peak_balance = total_balance
                    last_reset = today
                if peak_balance is not None and total_balance > peak_balance:
                    peak_balance = total_balance

                # ── 5. Portfolio-level risk gates ─────────────────────────
                if risk_manager.is_daily_loss_exceeded(daily_pnl, total_balance, p["daily_loss_limit_pct"]):
                    if session.status != "paused":
                        session.status = "paused"
                        session.updated_at = datetime.now(UTC)
                        runtime_state.update(session_id, runtime_state="paused")
                        await activity_logger.log_event(
                            db, session_id=session_id, user_id=user_id,
                            severity="WARNING", event_type="DAILY_LOSS_PAUSE",
                            message="Daily loss limit reached — bot paused",
                            context={"daily_pnl": daily_pnl, "total_balance": total_balance,
                                     "limit_pct": p["daily_loss_limit_pct"]},
                            publish_lifecycle=True,
                        )
                        await db.commit()
                        logger.warning("Daily loss limit hit – pausing bot  user=%s", user_id)
                    await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)
                    continue

                # Guard: only check drawdown when we actually have realized
                # losses today.  Without this guard, buying stocks causes the
                # wallet's cash balance to drop, which looks like a drawdown even
                # though the money is still held as stock value — the primary
                # reason all bots were pausing while showing a profit.
                if daily_pnl < 0 and peak_balance is not None and risk_manager.is_max_drawdown_exceeded(
                    total_balance, peak_balance, p["max_drawdown_pct"]
                ):
                    if session.status != "paused":
                        session.status = "paused"
                        session.updated_at = datetime.now(UTC)
                        runtime_state.update(session_id, runtime_state="paused")
                        await activity_logger.log_event(
                            db, session_id=session_id, user_id=user_id,
                            severity="WARNING", event_type="DRAWDOWN_PAUSE",
                            message="Max drawdown reached — bot paused",
                            context={"total_balance": total_balance, "peak_balance": peak_balance,
                                     "daily_pnl": daily_pnl,
                                     "drawdown_limit_pct": p["max_drawdown_pct"]},
                            publish_lifecycle=True,
                        )
                        await db.commit()
                        logger.warning("Max drawdown hit – pausing bot  user=%s", user_id)
                    await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)
                    continue

                # ── 6. Build market state ─────────────────────────────────
                momentum = calculate_momentum(prices)
                volatility = calculate_volatility(prices)
                pressure = calculate_pressure(book)
                bid_ratio = calculate_bid_ratio(book)
                sig = ema_signal(prices, p["fast_period"], p["slow_period"])

                state = MarketState(
                    symbol=symbol,
                    prices=prices,
                    current_price=current_price,
                    momentum=momentum,
                    volatility=volatility,
                    pressure_ratio=pressure,
                    ema_signal=sig,
                    market_event=market_event,
                )

                # ── 7. Strategy selection + switch handling ───────────────
                new_strategy = select_strategy(state)
                strategy_switched_this_tick = new_strategy != current_strategy

                if strategy_switched_this_tick:
                    logger.info(
                        "Strategy switch: %s → %s  user=%s  symbol=%s",
                        current_strategy, new_strategy, user_id, symbol,
                    )
                    await activity_logger.log_event(
                        db, session_id=session_id, user_id=user_id,
                        severity="INFO", event_type="STRATEGY_SWITCH",
                        message=f"Strategy {current_strategy} → {new_strategy}",
                        context={"from": current_strategy, "to": new_strategy,
                                 "symbol": symbol, "momentum": momentum,
                                 "volatility": volatility, "ema_signal": sig},
                    )
                    # Close single-position strategies on switch-away
                    if current_strategy in ("event_driven", "martingale") and session.position_side:
                        pnl = await _close_position(
                            client, session, current_price, db,
                            f"strategy-switch-to-{new_strategy}",
                        )
                        daily_pnl += pnl

                    # Cancel all grid orders on switch-away
                    if current_strategy == "grid" and grid_state.active_order_ids:
                        await _cancel_orders(client, grid_state.active_order_ids)
                        grid_state = GridState()

                    if current_strategy == "martingale":
                        mart_state = MartingaleState()

                    current_strategy = new_strategy

                # ── 8. Event-driven stop-loss (tight during shock) ────────
                if current_strategy == "event_driven" and session.entry_price and session.position_side:
                    sl_pct = (
                        0.01  # -1% tight stop for STOCK_SHOCK
                        if market_event and str(
                            market_event.get("event_type") or market_event.get("type", "")
                        ).upper() == "STOCK_SHOCK"
                        else p["stop_loss_pct"]
                    )
                    if risk_manager.is_stop_loss_triggered(
                        float(session.entry_price), current_price,
                        session.position_side, sl_pct,
                    ):
                        logger.warning("Stop-loss triggered  user=%s  symbol=%s", user_id, symbol)
                        pnl = await _close_position(client, session, current_price, db, "stop-loss", "MARKET")
                        daily_pnl += pnl
                        await db.commit()
                        await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)
                        continue

                # ── 9. Fetch open orders ──────────────────────────────────
                try:
                    all_orders = await client.get_orders()
                except BrokerAPIError as exc:
                    logger.error("Orders fetch failed: %s", exc)
                    await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)
                    continue

                # Apply any WS order status updates on top of the polled list
                ws_updates = feed.pop_order_updates()
                if ws_updates:
                    for order in all_orders:
                        update = ws_updates.get(_order_id(order))
                        if update:
                            order["status"] = update.get("status", order.get("status"))
                            order["filled_quantity"] = update.get(
                                "filled_quantity", order.get("filled_quantity")
                            )

                    # Persist confirmed fills back to BotTrade so closed_trades
                    # reflects reality.  ws_updates is keyed by the exchange's
                    # order UUID; BotTrade.broker_order_id stores the platform
                    # integer ID — use exchange_to_platform to translate.
                    for ex_oid, upd in ws_updates.items():
                        if str(upd.get("status", "")).upper() not in _FILLED_STATUSES:
                            continue
                        platform_oid = exchange_to_platform.get(ex_oid, ex_oid)
                        trade_row = (await db.execute(
                            select(BotTrade).where(
                                BotTrade.session_id == session_id,
                                BotTrade.broker_order_id == platform_oid,
                                BotTrade.status == "open",
                            )
                        )).scalar_one_or_none()
                        if trade_row is not None:
                            trade_row.status = "closed"
                            trade_row.closed_at = datetime.now(UTC)
                            fill_price = (
                                upd.get("fill_price")
                                or upd.get("average_price")
                                or upd.get("price")
                            )
                            if fill_price:
                                try:
                                    trade_row.price = float(fill_price)
                                except (TypeError, ValueError):
                                    pass

                # Bug fix: filter to THIS session's symbol only.
                # all_orders contains every open order for the user across ALL
                # running bots.  Using the global list for can_place_order() means
                # that once any 3 orders are open (default max) across all bots,
                # every other bot is silently blocked — the most common reason for
                # "bot generates signals but never places orders" in multi-ticker mode.
                symbol_orders = [
                    o for o in all_orders
                    if str(o.get("symbol", "")).upper() == symbol.upper()
                ]
                open_count = sum(1 for o in symbol_orders if o.get("status") in _OPEN_STATUSES)

                # ── 10. Generate signals from active strategy ─────────────
                signals: list[TradeSignal] = []
                mart_level_before: int = mart_state.level

                if current_strategy == "event_driven":
                    signals = event_driven.generate_signals(
                        state, available, bool(session.position_side)
                    )
                elif current_strategy == "martingale":
                    signals, mart_state = martingale.generate_signals(
                        state, available, total_balance, mart_state, bid_ratio
                    )
                elif current_strategy == "grid":
                    signals, grid_state = grid_trading.generate_signals(
                        state, available, grid_state, open_count
                    )
                # "hold" → no signals

                # ── 11. Execute signals ───────────────────────────────────
                placed_buy_count = 0
                placed_sell_count = 0

                for signal in signals:
                    if signal.action == "CLOSE":
                        if current_strategy == "grid":
                            if grid_state.active_order_ids:
                                await _cancel_orders(client, grid_state.active_order_ids)
                            grid_state = GridState()
                        elif current_strategy == "martingale":
                            if session.entry_price and session.position_side:
                                pnl = await _close_position(
                                    client, session, current_price, db,
                                    signal.reason, signal.order_type,
                                )
                                daily_pnl += pnl
                            mart_state = MartingaleState()
                        elif session.entry_price and session.position_side:
                            pnl = await _close_position(
                                client, session, current_price, db,
                                signal.reason, signal.order_type,
                            )
                            daily_pnl += pnl
                        continue

                    if signal.action not in ("BUY", "SELL"):
                        continue

                    # Use per-symbol order count, not global user order count.
                    if not risk_manager.can_place_order(symbol_orders, p["max_open_orders"]):
                        logger.debug("Max open orders for %s – skipping signal from %s", symbol, current_strategy)
                        continue

                    try:
                        order = await client.place_order(
                            symbol, signal.action, signal.order_type,
                            signal.quantity, signal.price,
                        )
                    except BrokerAPIError as exc:
                        if exc.status_code == 401:
                            session.status = "error"
                            await db.commit()
                            return
                        # Surface order failures in the admin action stream so
                        # they are visible — previously only logger.error() fired.
                        await activity_logger.log_event(
                            db, session_id=session_id, user_id=user_id,
                            severity="WARNING", event_type="ORDER_FAILED",
                            message=f"{signal.action} {signal.order_type} {symbol} failed ({exc.status_code}): {exc.detail}",
                            context={"symbol": symbol, "side": signal.action,
                                     "order_type": signal.order_type,
                                     "quantity": signal.quantity, "price": signal.price,
                                     "status_code": exc.status_code},
                        )
                        logger.error("Order placement failed: %s", exc)
                        continue

                    order_status = str(order.get("status", "")).upper()
                    if order_status in _REJECTED_STATUSES:
                        reason = (
                            order.get("reject_reason")
                            or order.get("message")
                            or order.get("reason", "unknown")
                        )
                        logger.warning(
                            "Order rejected  user=%s  symbol=%s  status=%s  reason=%s",
                            user_id, symbol, order_status, reason,
                        )
                        await activity_logger.log_event(
                            db, session_id=session_id, user_id=user_id,
                            severity="WARNING", event_type="ORDER_REJECTED",
                            message=f"{signal.action} {signal.order_type} on {symbol} rejected: {reason}",
                            context={"symbol": symbol, "side": signal.action,
                                     "quantity": signal.quantity, "price": signal.price,
                                     "reason": str(reason)},
                        )
                        continue

                    oid = _order_id(order)  # platform integer ID — used for cancel
                    # Record exchange_order_id → platform_id so fill notifications
                    # arriving via WebSocket (keyed by exchange UUID) can find the
                    # matching BotTrade row (stored with the platform ID).
                    exchange_oid = str(order.get("exchange_order_id", "") or "")
                    if exchange_oid:
                        exchange_to_platform[exchange_oid] = oid

                    db.add(BotTrade(
                        session_id=session.id,
                        user_id=user_id,
                        broker_order_id=oid,
                        symbol=symbol,
                        side=signal.action,
                        quantity=signal.quantity,
                        price=signal.price,
                        status="open",
                    ))

                    if current_strategy == "grid":
                        grid_state.active_order_ids.append(oid)
                    elif current_strategy == "martingale" and mart_level_before > 0:
                        # Doubling up — accumulate total position quantity
                        prev_qty = float(session.entry_quantity or 0)
                        session.entry_quantity = prev_qty + signal.quantity
                        session.entry_price = signal.price
                        session.position_side = signal.action
                    else:
                        session.entry_price = signal.price
                        session.entry_quantity = signal.quantity
                        session.position_side = signal.action

                    session.updated_at = datetime.now(UTC)
                    # Keep both lists consistent so subsequent signals in this
                    # tick see the correct per-symbol and global order counts.
                    all_orders.append(order)
                    symbol_orders.append(order)

                    if signal.action == "BUY":
                        placed_buy_count += 1
                    else:
                        placed_sell_count += 1

                    logger.info(
                        "Placed %s %s  user=%s  symbol=%s  qty=%.2f  price=%.4f  strategy=%s",
                        signal.action, signal.order_type, user_id, symbol,
                        signal.quantity, signal.price, current_strategy,
                    )
                    await activity_logger.log_event(
                        db, session_id=session_id, user_id=user_id,
                        severity="INFO", event_type="ORDER_PLACED",
                        message=f"{signal.action} {signal.order_type} {symbol} qty={signal.quantity:.4f} price={signal.price:.4f}",
                        context={"symbol": symbol, "side": signal.action,
                                 "order_type": signal.order_type,
                                 "quantity": signal.quantity, "price": signal.price,
                                 "strategy": current_strategy,
                                 "broker_order_id": oid},
                    )
                    runtime_state.update(
                        session_id,
                        runtime_state=("buying" if signal.action == "BUY" else "selling"),
                    )

                # Decision log: use PLACED counts, not GENERATED signal counts.
                # Previously "BUY x1" appeared even when the order silently failed,
                # making it impossible to tell from logs whether trades actually executed.
                summary_parts: list[str] = []
                if placed_buy_count:
                    summary_parts.append(f"BUY x{placed_buy_count}")
                if placed_sell_count:
                    summary_parts.append(f"SELL x{placed_sell_count}")
                signal_summary = ", ".join(summary_parts) if summary_parts else None

                # Compute the steady-state runtime indicator
                # (transient buying/selling already set above; revert here
                # when no orders just placed)
                if not placed_buy_count and not placed_sell_count:
                    if session.position_side:
                        runtime_state.update(session_id, runtime_state="managing")
                    else:
                        runtime_state.update(session_id, runtime_state="thinking")

                runtime_state.update(
                    session_id,
                    current_strategy=current_strategy,
                    last_decision_at=datetime.now(UTC),
                    daily_pnl=daily_pnl,
                    peak_balance=peak_balance,
                )

                await activity_logger.log_decision(
                    db,
                    session=session,
                    selected_strategy=current_strategy,
                    strategy_switched=strategy_switched_this_tick,
                    signals_count=len(signals),
                    signal_summary=signal_summary,
                    current_price=current_price,
                    momentum=momentum,
                    volatility=volatility,
                    pressure_ratio=pressure,
                    bid_ratio=bid_ratio,
                    ema_signal=sig,
                    market_event_type=(
                        str(market_event.get("event_type") or market_event.get("type", ""))
                        if market_event else None
                    ),
                    daily_pnl=daily_pnl,
                )

                await db.commit()

        except asyncio.CancelledError:
            logger.info("Bot task cancelled  user=%s", user_id)
            runtime_state.update(session_id, runtime_state="stopped")
            return
        except Exception as exc:
            logger.exception("Unexpected error in bot tick  user=%s", user_id)
            runtime_state.record_error(session_id, f"tick exception: {exc}")
            try:
                async with AsyncSessionLocal() as err_db:
                    await activity_logger.log_event(
                        err_db, session_id=session_id, user_id=user_id,
                        severity="ERROR", event_type="TICK_EXCEPTION",
                        message=f"Unexpected error in bot tick: {exc}",
                    )
                    await err_db.commit()
            except Exception:
                logger.exception("Failed to persist TICK_EXCEPTION event")

        await feed.wait_for_price(symbol, timeout=TICK_INTERVAL)


# ── public API (used by routers and main) ─────────────────────────────────────

def start_bot(session_id: UUID, user_id: str) -> asyncio.Task:
    return bot_manager.start(session_id, user_id, _bot_tick)


def stop_bot(session_id: UUID) -> None:
    """Stop one specific session's task."""
    bot_manager.stop(session_id)


def stop_bots_for_user(user_id: str) -> None:
    """Stop every running task belonging to a user."""
    bot_manager.stop_all_for_user(user_id)


def is_bot_running(session_id: UUID) -> bool:
    return bot_manager.is_running(session_id)


def stop_all_bots() -> None:
    bot_manager.stop_all()


async def force_close_position(session_id: UUID, *, reason: str = "admin-force-close") -> float | None:
    """Drive a single MARKET close for the session's current open position.

    Returns the realized PnL, or None if the session has no position or the
    JWT cannot be decrypted. Used by admin restart and close-positions
    endpoints. Catches all errors — the caller decides how to respond.
    """
    async with AsyncSessionLocal() as db:
        session: BotSession | None = await db.get(BotSession, session_id)
        if session is None or not session.position_side:
            return None
        try:
            token = decrypt_token(session.jwt_token)
        except InvalidToken:
            logger.error("force_close_position: cannot decrypt JWT for %s", session_id)
            return None

        client = BrokerClient(token)
        try:
            # Use last seen price if cached; broker will fill at market anyway.
            current_price = price_cache.get_last_price(session.symbol) or 0.0
            pnl = await _close_position(client, session, current_price, db, reason, "MARKET")
            await db.commit()
            return pnl
        except Exception:
            logger.exception("force_close_position failed for %s", session_id)
            await db.rollback()
            return None
