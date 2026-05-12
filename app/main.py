import logging
from contextlib import asynccontextmanager

import sqlalchemy as sa
from fastapi import FastAPI
from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models.bot import BotSession
from app.routers import bots
from app.services.bot_engine import start_bot, stop_all_bots

logger = logging.getLogger(__name__)


async def _restore_active_sessions() -> int:
    """Re-spawn tasks for sessions that were running when the service last stopped."""
    async with AsyncSessionLocal() as db:
        stmt = select(BotSession).where(
            BotSession.status.not_in(["deactivated", "error"])
        )
        sessions = (await db.execute(stmt)).scalars().all()
        for s in sessions:
            start_bot(s.id, s.user_id)
        return len(sessions)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── startup ───────────────────────────────────────────────────────────────
    # Warm the connection pool with a trivial query so the first real request
    # doesn't pay the pool-initialisation latency.
    async with AsyncSessionLocal() as db:
        await db.execute(sa.text("SELECT 1"))

    count = await _restore_active_sessions()
    if count:
        logger.info("Restored %d bot session(s) from DB on startup", count)

    yield

    # ── shutdown ──────────────────────────────────────────────────────────────
    stop_all_bots()


app = FastAPI(title="Bot Service", version="1.0.0", lifespan=lifespan)

app.include_router(bots.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
