import logging
from contextlib import asynccontextmanager

import jwt as pyjwt
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models.bot import BotSession
from app.routers import bots
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


def _run_migrations() -> None:
    cfg = Config("alembic.ini")
    command.upgrade(cfg, "head")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── startup ───────────────────────────────────────────────────────────────
    _run_migrations()

    async with AsyncSessionLocal() as db:
        await db.execute(sa.text("SELECT 1"))  # warm connection pool

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
