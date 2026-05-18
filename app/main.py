import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import timedelta

import jwt as pyjwt
import sqlalchemy as sa
from fastapi import FastAPI
from sqlalchemy import select

from app.config import settings
from app.database import AsyncSessionLocal
from app.models.bot import BotDecisionLog, BotEventLog, BotSession
from app.routers import admin_bots, bots
from app.services import kafka_producer
from app.services.bot_engine import start_bot, stop_all_bots
from app.services.crypto import InvalidToken, decrypt_token

logger = logging.getLogger(__name__)


async def _restore_active_sessions() -> int:
    """Re-spawn tasks for non-deactivated sessions that survived a service restart.

    Skips (and marks as error) any session whose stored token can no longer be
    decrypted (key rotation) or whose JWT has already expired — starting a bot
    with a dead token would just generate 401 errors on the first tick.
    """
    async with AsyncSessionLocal() as db:
        stmt = select(BotSession).where(
            BotSession.status.not_in(["deactivated", "error"])
        )
        sessions = (await db.execute(stmt)).scalars().all()
        restored = 0
        needs_commit = False

        for s in sessions:
            try:
                token = decrypt_token(s.jwt_token)
                # Verify expiry without re-checking the signature — the token
                # was already validated by the broker when the session was created.
                pyjwt.decode(
                    token,
                    options={"verify_signature": False},
                    algorithms=["HS256", "HS512"],
                )
                start_bot(s.id, s.user_id)
                restored += 1
            except InvalidToken:
                logger.warning(
                    "Session %s: cannot decrypt token (secret rotated?) — marking error", s.id
                )
                s.status = "error"
                needs_commit = True
            except pyjwt.ExpiredSignatureError:
                logger.warning("Session %s: JWT expired on restore — marking error", s.id)
                s.status = "error"
                needs_commit = True
            except Exception:
                logger.exception("Session %s: unexpected error during restore", s.id)

        if needs_commit:
            await db.commit()

        return restored


async def _run_async_migrations() -> None:
    """Run migrations using asyncio.to_thread to avoid blocking the event loop."""
    from alembic.config import Config
    from alembic import command
    
    def run_migrations():
        # Alembic works with sync connections, so we need to convert the async URL
        sync_db_url = settings.async_database_url.replace("asyncpg", "psycopg2")
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", sync_db_url)
        
        try:
            command.upgrade(config, "head")
            logger.info("Database migrations completed successfully")
        except Exception as e:
            logger.error("Failed to run migrations: %s", e, exc_info=True)
            raise
    
    # Run migrations in a thread pool to avoid blocking the event loop
    await asyncio.to_thread(run_migrations)


async def _prune_activity_logs() -> None:
    """Periodically delete old decision and event log rows.

    Runs forever as a background task. Failures are logged but don't crash —
    a single failed prune cycle is recoverable when the next cycle runs.
    """
    while True:
        try:
            async with AsyncSessionLocal() as db:
                dec_cutoff = sa.func.now() - sa.text(
                    f"interval '{settings.DECISION_LOG_RETENTION_DAYS} days'"
                )
                evt_cutoff = sa.func.now() - sa.text(
                    f"interval '{settings.EVENT_LOG_RETENTION_DAYS} days'"
                )
                dec_del = sa.delete(BotDecisionLog).where(BotDecisionLog.tick_at < dec_cutoff)
                evt_del = sa.delete(BotEventLog).where(BotEventLog.created_at < evt_cutoff)
                dec_result = await db.execute(dec_del)
                evt_result = await db.execute(evt_del)
                await db.commit()
                if dec_result.rowcount or evt_result.rowcount:
                    logger.info(
                        "Pruner: removed %d decision rows, %d event rows",
                        dec_result.rowcount or 0, evt_result.rowcount or 0,
                    )
        except Exception:
            logger.exception("Activity-log pruner cycle failed")
        await asyncio.sleep(max(1, settings.ACTIVITY_LOG_PRUNE_HOURS) * 3600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── startup ───────────────────────────────────────────────────────────────
    await _run_async_migrations()

    async with AsyncSessionLocal() as db:
        await db.execute(sa.text("SELECT 1"))  # warm connection pool

    # Best-effort Kafka producer warm-up — never block startup on Kafka.
    try:
        await kafka_producer.get_producer()
    except Exception:
        logger.warning("Kafka producer warm-up failed; will retry on first publish", exc_info=True)

    count = await _restore_active_sessions()
    if count:
        logger.info("Restored %d bot session(s) from DB on startup", count)

    pruner_task = asyncio.create_task(_prune_activity_logs(), name="bot-activity-pruner")

    try:
        yield
    finally:
        # ── shutdown ──────────────────────────────────────────────────────────
        pruner_task.cancel()
        stop_all_bots()
        await kafka_producer.stop_producer()


app = FastAPI(title="Bot Service", version="1.1.0", lifespan=lifespan)

app.include_router(bots.router)
app.include_router(admin_bots.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
