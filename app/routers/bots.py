from datetime import datetime, UTC
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.bot import BotSession, BotTrade
from app.schemas.bot import (
    ActivateRequest,
    BotPerformanceOut,
    BotStatusOut,
    ConfigUpdate,
    TradeOut,
)
from app.services import bot_manager
from app.services.bot_engine import is_bot_running, start_bot, stop_bot
from app.services.broker_client import BrokerAPIError, BrokerClient

router = APIRouter(prefix="/bots", tags=["bots"])


# ── auth helper ───────────────────────────────────────────────────────────────

def _require_user_id(request: Request) -> str:
    uid = request.headers.get("X-User-Id")
    if not uid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="X-User-Id header missing")
    return uid


# ── routes ────────────────────────────────────────────────────────────────────

@router.post("/activate", status_code=status.HTTP_201_CREATED)
async def activate(
    payload: ActivateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)

    # Acquire the per-user lock before the is_running check so that two
    # concurrent activate requests for the same user cannot both pass the
    # guard and spawn duplicate tasks.
    async with bot_manager.get_activation_lock(user_id):
        if is_bot_running(user_id):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Bot already active")

        # validate JWT against the broker before persisting it
        client = BrokerClient(payload.jwt_token)
        try:
            await client.get_balance()
        except BrokerAPIError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"JWT validation failed: {exc.detail}",
            )

        # deactivate any previous sessions for this user
        stmt = select(BotSession).where(
            BotSession.user_id == user_id,
            BotSession.status != "deactivated",
        )
        for old in (await db.execute(stmt)).scalars().all():
            old.status = "deactivated"

        session = BotSession(
            id=uuid4(),
            user_id=user_id,
            jwt_token=payload.jwt_token,
            symbol=payload.symbol.upper(),
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


@router.delete("/deactivate")
async def deactivate(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)

    stmt = select(BotSession).where(
        BotSession.user_id == user_id,
        BotSession.status.in_(["active", "paused", "halted"]),
    )
    session = (await db.execute(stmt)).scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active bot session")

    stop_bot(user_id)
    session.status = "deactivated"
    session.updated_at = datetime.now(UTC)
    await db.commit()

    return {"message": "Bot deactivated", "session_id": str(session.id)}


@router.get("/status", response_model=BotStatusOut)
async def get_status(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)

    stmt = (
        select(BotSession)
        .where(BotSession.user_id == user_id)
        .order_by(BotSession.created_at.desc())
    )
    session = (await db.execute(stmt)).scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No bot session found")

    # reflect live task state
    if is_bot_running(user_id) and session.status not in ("paused", "halted"):
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
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)

    sess_stmt = (
        select(BotSession)
        .where(BotSession.user_id == user_id)
        .order_by(BotSession.created_at.desc())
    )
    session = (await db.execute(sess_stmt)).scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No bot session found")

    trades_stmt = (
        select(BotTrade)
        .where(BotTrade.session_id == session.id)
        .order_by(BotTrade.created_at.desc())
    )
    trades = (await db.execute(trades_stmt)).scalars().all()

    open_trades = sum(1 for t in trades if t.status == "open")
    closed_trades = sum(1 for t in trades if t.status == "closed")
    total_pnl = sum(t.pnl or 0.0 for t in trades)

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
                quantity=t.quantity,
                price=t.price,
                status=t.status,
                pnl=t.pnl,
                created_at=t.created_at,
            )
            for t in trades
        ],
    )


@router.put("/config")
async def update_config(
    payload: ConfigUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    user_id = _require_user_id(request)

    stmt = select(BotSession).where(
        BotSession.user_id == user_id,
        BotSession.status.in_(["active", "paused", "halted"]),
    )
    session = (await db.execute(stmt)).scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active bot session")

    if payload.symbol is not None:
        session.symbol = payload.symbol.upper()
    if payload.strategy_config is not None:
        session.strategy_config = payload.strategy_config
    if payload.status is not None:
        session.status = payload.status

    session.updated_at = datetime.now(UTC)
    await db.commit()

    return {"message": "Config updated", "session_id": str(session.id), "status": session.status}
