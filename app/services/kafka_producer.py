"""Lazy Kafka producer for bot.activity.* topics.

Mirrors the pattern in order-service/app/services/kafka_producer.py.
Publishing is best-effort — failures are logged but never crash the bot loop.
Kafka can be disabled entirely via KAFKA_ENABLED=false (useful for local
runs without redpanda).
"""
from __future__ import annotations

import json
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from app.config import settings

logger = logging.getLogger(__name__)

# Topic constants — single source of truth, also used by the docker-compose
# redpanda-init bootstrap script.
TOPIC_DECISIONS = "bot.activity.decisions"
TOPIC_EVENTS = "bot.activity.events"
TOPIC_LIFECYCLE = "bot.activity.lifecycle"
ALL_TOPICS: tuple[str, ...] = (TOPIC_DECISIONS, TOPIC_EVENTS, TOPIC_LIFECYCLE)

_producer: AIOKafkaProducer | None = None


def _json_default(value):
    """Serialise UUID, datetime, Decimal, etc. as strings for JSON."""
    return str(value)


async def get_producer() -> AIOKafkaProducer | None:
    global _producer
    if not settings.KAFKA_ENABLED:
        return None
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=_json_default).encode(),
        )
        await _producer.start()
    return _producer


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        try:
            await _producer.stop()
        except Exception:
            logger.exception("Failed to stop Kafka producer cleanly")
        _producer = None


async def publish(topic: str, payload: dict) -> None:
    """Send-and-forget publish. Errors are logged, never re-raised.

    The bot tick loop calls this on every decision/event — we never want a
    Kafka outage to break trading. The activity logs are still persisted to
    Postgres regardless.
    """
    producer = await get_producer()
    if producer is None:
        return
    try:
        await producer.send_and_wait(topic, payload)
    except KafkaError as exc:
        logger.warning("Kafka publish failed (topic=%s): %s", topic, exc)
    except Exception:
        logger.exception("Unexpected Kafka publish error (topic=%s)", topic)
