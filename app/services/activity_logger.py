"""Chokepoint for persisting bot activity to Postgres + Kafka.

Two write APIs:
- log_decision: tick-level indicator snapshot + selected strategy. Throttled
  internally — writes only when (a) the strategy switched, (b) signals fired,
  or (c) DECISION_LOG_THROTTLE_SECS elapsed since the last persisted row.
- log_event: lifecycle / error / admin-action events. Always written.

Both paths also publish to Kafka (bot.activity.*), making them the single
fan-out point for realtime admin observation.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.bot import BotDecisionLog, BotEventLog, BotSession
from app.services import kafka_producer

logger = logging.getLogger(__name__)

# session_id → last persisted decision timestamp (epoch seconds).
# Process-local; resets on restart. That's fine — the throttle is only meant
# to bound DB write rate, not provide cross-restart determinism.
_last_decision_write: dict[UUID, float] = {}


def _ensure_aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _should_persist_decision(session_id: UUID, switched: bool, signals_count: int) -> bool:
    if switched or signals_count > 0:
        return True
    last = _last_decision_write.get(session_id, 0.0)
    return (time.monotonic() - last) >= settings.DECISION_LOG_THROTTLE_SECS


async def log_decision(
    db: AsyncSession,
    *,
    session: BotSession,
    selected_strategy: str,
    strategy_switched: bool,
    signals_count: int,
    signal_summary: str | None,
    current_price: float | None,
    momentum: float | None,
    volatility: float | None,
    pressure_ratio: float | None,
    bid_ratio: float | None,
    ema_signal: str | None,
    market_event_type: str | None,
    daily_pnl: float | None,
) -> None:
    """Persist a tick decision row (throttled) and publish to Kafka (always)."""
    persist = _should_persist_decision(session.id, strategy_switched, signals_count)

    payload: dict[str, Any] = {
        "session_id": str(session.id),
        "user_id": session.user_id,
        "symbol": session.symbol,
        "tick_at": datetime.now(timezone.utc).isoformat(),
        "current_price": current_price,
        "momentum": momentum,
        "volatility": volatility,
        "pressure_ratio": pressure_ratio,
        "bid_ratio": bid_ratio,
        "ema_signal": ema_signal,
        "market_event": market_event_type,
        "selected_strategy": selected_strategy,
        "strategy_switched": strategy_switched,
        "signals_count": signals_count,
        "signal_summary": signal_summary,
        "position_side": session.position_side,
        "entry_price": float(session.entry_price) if session.entry_price is not None else None,
        "daily_pnl": daily_pnl,
        "persisted": persist,
    }

    if persist:
        try:
            db.add(BotDecisionLog(
                session_id=session.id,
                user_id=session.user_id,
                symbol=session.symbol,
                current_price=current_price,
                momentum=momentum,
                volatility=volatility,
                pressure_ratio=pressure_ratio,
                bid_ratio=bid_ratio,
                ema_signal=ema_signal,
                market_event=market_event_type,
                selected_strategy=selected_strategy,
                strategy_switched=strategy_switched,
                signals_count=signals_count,
                signal_summary=signal_summary,
                position_side=session.position_side,
                entry_price=session.entry_price,
                daily_pnl=daily_pnl,
            ))
            _last_decision_write[session.id] = time.monotonic()
        except Exception:
            logger.exception("Failed to enqueue BotDecisionLog row")

    # Kafka publish is best-effort; never let it break the bot.
    await kafka_producer.publish(kafka_producer.TOPIC_DECISIONS, payload)


async def log_event(
    db: AsyncSession,
    *,
    session_id: UUID | None,
    user_id: str | None,
    severity: str,
    event_type: str,
    message: str,
    context: dict[str, Any] | None = None,
    admin_user: str | None = None,
    publish_lifecycle: bool = False,
) -> None:
    """Persist an event row and publish to Kafka.

    publish_lifecycle=True routes the event to bot.activity.lifecycle as well
    (used by start/stop/pause/resume transitions). Errors always go on
    bot.activity.events.
    """
    payload: dict[str, Any] = {
        "session_id": str(session_id) if session_id else None,
        "user_id": user_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "severity": severity,
        "event_type": event_type,
        "message": message,
        "context": context or {},
        "admin_user": admin_user,
    }

    try:
        db.add(BotEventLog(
            session_id=session_id,
            user_id=user_id,
            severity=severity,
            event_type=event_type,
            message=message,
            context=context,
            admin_user=admin_user,
        ))
    except Exception:
        logger.exception("Failed to enqueue BotEventLog row")

    await kafka_producer.publish(kafka_producer.TOPIC_EVENTS, payload)
    if publish_lifecycle:
        await kafka_producer.publish(kafka_producer.TOPIC_LIFECYCLE, payload)


def reset_throttle(session_id: UUID) -> None:
    _last_decision_write.pop(session_id, None)
