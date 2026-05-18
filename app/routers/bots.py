import asyncio
import logging
from collections import defaultdict
from datetime import datetime, UTC
from typing import List, Optional
from uuid import UUID, uuid4

import jwt as pyjwt
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models.bot import BotSession, BotTrade
from app.schemas.bot import (
    ActivateRequest,
    BotPerformanceOut,
    BotSessionListResponse,
    BotSessionOut,
    BotStatusOut,
    ConfigUpdate,
    TradeOut,
)
from app.services import bot_manager
from app.services.bot_engine import is_bot_running, start_bot, stop_bot, stop_bots_for_user
from app.services.broker_client import BrokerAPIError, BrokerClient
from app.services.crypto import encrypt_token

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/bots", tags=["bots"])


# ── auth helpers ──────────────────────────────────────────────────────────────

def _require_user_id(request: Request) -> str:
    uid = request.headers.get("X-User-Id")
    if not uid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="X-User-Id header missing")
    return uid


def _extract_and_verify_token(request: Request, user_id: str) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization: Bearer <token> header is required",
        )
    token = auth[len("Bearer "):]
    try:
        claims = pyjwt.decode(token, settings.JWT_SECRET, algorithms=["HS256", "HS512"])
        token_sub = str(claims.get("sub", ""))
        if token_sub and token_sub != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Token sub does not match the authenticated user")
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except pyjwt.InvalidTokenError as exc:
        logger.debug("Local JWT decode skipped (%s); broker will validate", exc)
    return token


async def _session_or_404(db: AsyncSession, session_id: UUID, user_id: str) -> BotSession:
    stmt = select(BotSession).where(BotSession.id == session_id, BotSession.user_id == user_id)
    session = (await db.execute(stmt)).scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bot session not found")
    return session


# ── activate (single symbol) ──────────────────────────────────────────────────

@router.post("/activate", status_code=status.HTTP_201_CREATED)
async def activate(
    payload: ActivateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    symbol = payload.symbol.upper()

    async with bot_manager.get_activation_lock(user_id):
        # Reject duplicate active session for the same symbol
        dup = (await db.execute(
            select(BotSession).where(
                BotSession.user_id == user_id,
                BotSession.symbol == symbol,
                BotSession.status.in_(["active", "paused", "halted"]),
            )
        )).scalar_one_or_none()
        if dup:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Bot already active for {symbol}")

        jwt_token = _extract_and_verify_token(request, user_id)

        client = BrokerClient(jwt_token)
        try:
            await client.get_balance()
        except BrokerAPIError as exc:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"JWT validation failed: {exc.detail}")

        session = BotSession(
            id=uuid4(),
            user_id=user_id,
            jwt_token=encrypt_token(jwt_token),
            symbol=symbol,
            strategy_config=payload.strategy_config,
            status="active",
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        db.add(session)
        await db.commit()
        await db.refresh(session)
        start_bot(session.id, user_id)

    return {"session_id": str(session.id), "status": "active", "symbol": session.symbol}


# ── activate-all (multiple symbols at once) ───────────────────────────────────

class ActivateAllRequest(ActivateRequest.__class__):
    pass


from pydantic import BaseModel

class ActivateAllBody(BaseModel):
    symbols: List[str]
    strategy_config: dict = {}


@router.post("/activate-all", status_code=status.HTTP_201_CREATED)
async def activate_all(
    payload: ActivateAllBody,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    jwt_token = _extract_and_verify_token(request, user_id)

    client = BrokerClient(jwt_token)
    try:
        await client.get_balance()
    except BrokerAPIError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"JWT validation failed: {exc.detail}")

    activated = []
    skipped = []
    to_start: list[tuple[UUID, str]] = []  # (session_id, user_id) — started after commit

    async with bot_manager.get_activation_lock(user_id):
        for raw_symbol in payload.symbols:
            symbol = raw_symbol.upper().strip()
            if not symbol:
                continue

            dup = (await db.execute(
                select(BotSession).where(
                    BotSession.user_id == user_id,
                    BotSession.symbol == symbol,
                    BotSession.status.in_(["active", "paused", "halted"]),
                )
            )).scalar_one_or_none()

            if dup:
                skipped.append(symbol)
                continue

            session = BotSession(
                id=uuid4(),
                user_id=user_id,
                jwt_token=encrypt_token(jwt_token),
                symbol=symbol,
                strategy_config=payload.strategy_config,
                status="active",
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
            db.add(session)
            await db.flush()
            to_start.append((session.id, user_id))
            activated.append({"session_id": str(session.id), "symbol": symbol})

        # Commit ALL sessions before starting any task — bot tasks open their
        # own DB connections and must be able to find the session on first tick.
        await db.commit()

    # Start bots one by one with a small stagger so 36 tasks don't all slam
    # the broker API (balance check, price fetch) in the same millisecond.
    for session_id, uid in to_start:
        start_bot(session_id, uid)
        await asyncio.sleep(0.15)

    return {"activated": activated, "skipped": skipped}


# ── deactivate ────────────────────────────────────────────────────────────────

@router.delete("/deactivate")
async def deactivate(
    request: Request,
    session_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    if session.status not in ("active", "paused", "halted"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session is not active")

    stop_bot(session.id)
    session.status = "deactivated"
    session.updated_at = datetime.now(UTC)
    await db.commit()
    return {"message": "Bot deactivated", "session_id": str(session.id)}


# ── read endpoints ────────────────────────────────────────────────────────────

# Route must be "/" not "" — the API gateway RewritePath sends GET /bots/ (with
# trailing slash). @router.get("") creates /bots without a slash, so FastAPI
# returns a 307 redirect to /bots which the browser follows outside the proxy,
# hitting the SPA and getting HTML instead of JSON.
@router.get("/", response_model=BotSessionListResponse)
async def list_sessions(
    request: Request,
    include_inactive: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    stmt = select(BotSession).where(BotSession.user_id == user_id)
    if not include_inactive:
        # Hide deactivated sessions by default — users with 100+ past sessions
        # would otherwise cause an N+1 explosion and a very slow response.
        stmt = stmt.where(BotSession.status.not_in(["deactivated"]))
    stmt = stmt.order_by(BotSession.created_at.desc())
    sessions = (await db.execute(stmt)).scalars().all()

    if not sessions:
        return BotSessionListResponse(items=[])

    # Batch load all trades in ONE query to avoid the N+1 that caused the
    # list endpoint to hang when a user had 100+ sessions.
    all_trades = (await db.execute(
        select(BotTrade).where(BotTrade.session_id.in_([s.id for s in sessions]))
    )).scalars().all()

    trades_map: dict = defaultdict(list)
    for t in all_trades:
        trades_map[t.session_id].append(t)

    today = datetime.now(UTC).date()
    items = []
    for session in sessions:
        trades = trades_map[session.id]
        daily_pnl = sum(
            float(t.pnl or 0) for t in trades
            if t.created_at and t.created_at.date() == today
        )
        closed = [t for t in trades if t.status == "closed"]
        wins = [t for t in closed if t.pnl is not None and float(t.pnl) > 0]
        win_rate = len(wins) / len(closed) if closed else 0.0

        items.append(BotSessionOut(
            session_id=session.id,
            symbol=session.symbol,
            status=session.status,
            daily_pnl=daily_pnl,
            trades_count=len(trades),
            win_rate=round(win_rate, 4),
            position_side=session.position_side,
            entry_price=float(session.entry_price) if session.entry_price is not None else None,
            entry_quantity=float(session.entry_quantity) if session.entry_quantity is not None else None,
            created_at=session.created_at,
            updated_at=session.updated_at,
        ))

    return BotSessionListResponse(items=items)


@router.get("/status", response_model=BotStatusOut)
async def get_status(
    request: Request,
    session_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    if is_bot_running(session.id) and session.status not in ("paused", "halted"):
        session.status = "active"

    return BotStatusOut(
        session_id=session.id,
        status=session.status,
        symbol=session.symbol,
        entry_price=session.entry_price,
        position_side=session.position_side,
        created_at=session.created_at,
        updated_at=session.updated_at,
    )


@router.get("/performance", response_model=BotPerformanceOut)
async def get_performance(
    request: Request,
    session_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    trades = (await db.execute(
        select(BotTrade).where(BotTrade.session_id == session.id).order_by(BotTrade.created_at.desc())
    )).scalars().all()

    open_trades = sum(1 for t in trades if t.status == "open")
    closed_trades = sum(1 for t in trades if t.status == "closed")
    total_pnl = sum(float(t.pnl or 0) for t in trades)

    return BotPerformanceOut(
        session_id=session.id,
        symbol=session.symbol,
        total_trades=len(trades),
        open_trades=open_trades,
        closed_trades=closed_trades,
        total_pnl=total_pnl,
        trades=[
            TradeOut(
                id=t.id,
                broker_order_id=t.broker_order_id,
                side=t.side,
                quantity=float(t.quantity),
                price=float(t.price),
                status=t.status,
                pnl=float(t.pnl) if t.pnl is not None else None,
                created_at=t.created_at,
            )
            for t in trades
        ],
    )


# ── action endpoints (all require session_id query param) ─────────────────────

@router.put("/config")
async def update_config(
    payload: ConfigUpdate,
    request: Request,
    session_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    if payload.symbol is not None:
        session.symbol = payload.symbol.upper()
    if payload.strategy_config is not None:
        session.strategy_config = payload.strategy_config
    if payload.status is not None:
        session.status = payload.status

    session.updated_at = datetime.now(UTC)
    await db.commit()
    return {"message": "Config updated", "session_id": str(session.id), "status": session.status}


@router.post("/start")
async def start_session(
    request: Request,
    session_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    if is_bot_running(session.id):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Bot already running")

    session.status = "active"
    session.updated_at = datetime.now(UTC)
    await db.commit()
    start_bot(session.id, user_id)
    return {"session_id": str(session.id), "status": "active"}


@router.post("/stop")
async def stop_session(
    request: Request,
    session_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    stop_bot(session.id)
    session.status = "deactivated"
    session.updated_at = datetime.now(UTC)
    await db.commit()
    return {"session_id": str(session.id), "status": "deactivated"}


@router.post("/pause")
async def pause_session(
    request: Request,
    session_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    session.status = "paused"
    session.updated_at = datetime.now(UTC)
    await db.commit()
    return {"session_id": str(session.id), "status": "paused"}


@router.post("/resume")
async def resume_session(
    request: Request,
    session_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    session.status = "active"
    session.updated_at = datetime.now(UTC)
    await db.commit()
    return {"session_id": str(session.id), "status": "active"}


@router.post("/restart")
async def restart_session(
    request: Request,
    session_id: UUID = Query(...),
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    stop_bot(session.id)
    session.status = "active"
    session.updated_at = datetime.now(UTC)
    await db.commit()
    start_bot(session.id, user_id)
    return {"session_id": str(session.id), "status": "active", "message": "Bot restarting"}


@router.post("/start-all")
async def start_all_sessions(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Start (resume) all paused/halted/error sessions for this user."""
    user_id = _require_user_id(request)
    stmt = select(BotSession).where(
        BotSession.user_id == user_id,
        BotSession.status.in_(["paused", "halted", "error"]),
    )
    sessions = (await db.execute(stmt)).scalars().all()

    to_start = []
    for session in sessions:
        if is_bot_running(session.id):
            continue
        session.status = "active"
        session.updated_at = datetime.now(UTC)
        to_start.append(session.id)

    await db.commit()

    for sid in to_start:
        start_bot(sid, user_id)
        await asyncio.sleep(0.1)

    return {"started": len(to_start)}


@router.post("/stop-all")
async def stop_all_sessions(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Stop and deactivate every active/paused/halted session for this user."""
    user_id = _require_user_id(request)
    stmt = select(BotSession).where(
        BotSession.user_id == user_id,
        BotSession.status.in_(["active", "paused", "halted"]),
    )
    sessions = (await db.execute(stmt)).scalars().all()

    for session in sessions:
        stop_bot(session.id)
        session.status = "deactivated"
        session.updated_at = datetime.now(UTC)

    await db.commit()
    return {"stopped": len(sessions)}


@router.delete("/remove/{session_id}")
async def remove_session(
    session_id: UUID,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)
    session = await _session_or_404(db, session_id, user_id)

    # Stop only THIS session's task — other sessions are unaffected
    stop_bot(session.id)
    session.status = "deactivated"
    session.updated_at = datetime.now(UTC)
    await db.commit()
    return {"message": "Session removed", "session_id": str(session_id)}
