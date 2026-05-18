"""Admin endpoints for cross-user bot observation and control.

All routes are guarded by require_internal_token; the admin-panel proxy
attaches X-Internal-Token (and optionally X-Admin-User for audit).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID

import jwt as pyjwt
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.bot import (
    BotDecisionLog,
    BotEventLog,
    BotSession,
    BotTrade,
)
from app.schemas.admin import (
    ActionResponse,
    BotDetail,
    BotListResponse,
    BotMetricsResponse,
    BotSummary,
    ClosePositionsResponse,
    DecisionListResponse,
    DecisionRow,
    EquityPoint,
    EquityResponse,
    EventListResponse,
    EventRow,
    GlobalMetrics,
    HoldingRow,
    HoldingsResponse,
    RestartRequest,
    StrategyMetric,
    TradeListResponse,
    TradeRow,
)
from app.security import get_admin_user, require_internal_token
from app.services import activity_logger, bot_manager, runtime_state
from app.services.bot_engine import force_close_position, start_bot, stop_bot, stop_bots_for_user
from app.services.crypto import InvalidToken, decrypt_token
from app.services.market_data import price_cache

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/internal/admin",
    tags=["admin-bots"],
    dependencies=[Depends(require_internal_token)],
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _trade_stats(trades: list[BotTrade]) -> dict:
    closed = [t for t in trades if t.status == "closed"]
    wins = [t for t in closed if t.pnl is not None and float(t.pnl) > 0]
    losses = [t for t in closed if t.pnl is not None and float(t.pnl) < 0]
    total_pnl = float(sum((t.pnl or 0) for t in trades))
    today = _utcnow().date()
    daily_pnl = float(sum(
        (t.pnl or 0) for t in trades
        if t.created_at and t.created_at.date() == today
    ))
    win_rate = (len(wins) / len(closed)) if closed else 0.0
    return {
        "total_trades": len(trades),
        "open_trades": sum(1 for t in trades if t.status == "open"),
        "closed_trades": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 4),
        "total_pnl": total_pnl,
        "daily_pnl": daily_pnl,
        "gross_profit": float(sum(float(t.pnl) for t in wins)),
        "gross_loss": float(sum(float(t.pnl) for t in losses)),
        "avg_trade_pnl": (total_pnl / len(closed)) if closed else 0.0,
        "best_trade": max((float(t.pnl) for t in closed if t.pnl is not None), default=None),
        "worst_trade": min((float(t.pnl) for t in closed if t.pnl is not None), default=None),
    }


async def _build_summary(db: AsyncSession, session: BotSession) -> BotSummary:
    trades = (await db.execute(
        select(BotTrade).where(BotTrade.session_id == session.id)
    )).scalars().all()
    stats = _trade_stats(trades)

    snap = runtime_state.get(session.id)
    is_running = bot_manager.is_running(session.id)

    return BotSummary(
        session_id=session.id,
        user_id=session.user_id,
        symbol=session.symbol,
        status=session.status,
        runtime_state=(snap.runtime_state if snap else ("stopped" if not is_running else None)),
        current_strategy=(snap.current_strategy if snap else None),
        is_running=is_running,
        position_side=session.position_side,
        entry_price=float(session.entry_price) if session.entry_price is not None else None,
        entry_quantity=float(session.entry_quantity) if session.entry_quantity is not None else None,
        total_pnl=stats["total_pnl"],
        daily_pnl=stats["daily_pnl"],
        trades_count=stats["total_trades"],
        open_trades=stats["open_trades"],
        closed_trades=stats["closed_trades"],
        win_rate=stats["win_rate"],
        last_decision_at=(snap.last_decision_at if snap else None),
        last_error_at=(snap.last_error_at if snap else None),
        created_at=session.created_at,
        updated_at=session.updated_at,
    )


async def _session_or_404(db: AsyncSession, session_id: UUID) -> BotSession:
    s: BotSession | None = await db.get(BotSession, session_id)
    if s is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bot session not found")
    return s


def _verify_token_alive(token: str) -> None:
    """Best-effort check that the stored JWT isn't expired.

    We don't have access to the signing key here in all cases (the gateway
    uses HS256 with the shared JWT_SECRET, but the bot may have been
    activated with a token signed by a different algorithm). Decode without
    signature verification just to inspect the `exp` claim.
    """
    try:
        pyjwt.decode(token, options={"verify_signature": False}, algorithms=["HS256", "HS512"])
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="stored_token_expired: the user must re-activate the bot",
        )
    except pyjwt.InvalidTokenError as exc:
        # Don't fail-closed on parse error — broker call will detect it.
        logger.debug("admin token expiry check skipped: %s", exc)


# ── list & detail ─────────────────────────────────────────────────────────────

@router.get("/bots", response_model=BotListResponse)
async def list_bots(
    db: AsyncSession = Depends(get_db),
    status_filter: Optional[str] = Query(default=None, alias="status",
                                         description="csv of statuses to include"),
    q: Optional[str] = Query(default=None, description="substring match on user_id or symbol"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> BotListResponse:
    stmt = select(BotSession)

    if status_filter:
        wanted = [s.strip() for s in status_filter.split(",") if s.strip()]
        if wanted:
            stmt = stmt.where(BotSession.status.in_(wanted))

    if q:
        like = f"%{q.lower()}%"
        stmt = stmt.where(or_(
            func.lower(BotSession.user_id).like(like),
            func.lower(BotSession.symbol).like(like),
        ))

    count = (await db.execute(
        select(func.count()).select_from(stmt.subquery())
    )).scalar() or 0

    stmt = stmt.order_by(BotSession.updated_at.desc()).limit(limit).offset(offset)
    sessions = (await db.execute(stmt)).scalars().all()

    if not sessions:
        return BotListResponse(items=[], total=int(count))

    # Batch load all trades in ONE query instead of calling _build_summary
    # per session, which ran N separate SELECT queries and caused the admin
    # panel proxy to 502-timeout on lists with 100+ sessions.
    all_trades = (await db.execute(
        select(BotTrade).where(BotTrade.session_id.in_([s.id for s in sessions]))
    )).scalars().all()

    trades_map: dict = {}
    for t in all_trades:
        trades_map.setdefault(t.session_id, []).append(t)

    items = []
    for s in sessions:
        stats = _trade_stats(trades_map.get(s.id, []))
        snap = runtime_state.get(s.id)
        is_running = bot_manager.is_running(s.id)
        items.append(BotSummary(
            session_id=s.id,
            user_id=s.user_id,
            symbol=s.symbol,
            status=s.status,
            runtime_state=(snap.runtime_state if snap else ("stopped" if not is_running else None)),
            current_strategy=(snap.current_strategy if snap else None),
            is_running=is_running,
            position_side=s.position_side,
            entry_price=float(s.entry_price) if s.entry_price is not None else None,
            entry_quantity=float(s.entry_quantity) if s.entry_quantity is not None else None,
            total_pnl=stats["total_pnl"],
            daily_pnl=stats["daily_pnl"],
            trades_count=stats["total_trades"],
            open_trades=stats["open_trades"],
            closed_trades=stats["closed_trades"],
            win_rate=stats["win_rate"],
            last_decision_at=(snap.last_decision_at if snap else None),
            last_error_at=(snap.last_error_at if snap else None),
            created_at=s.created_at,
            updated_at=s.updated_at,
        ))

    return BotListResponse(items=items, total=int(count))


@router.get("/metrics", response_model=GlobalMetrics)
async def global_metrics(db: AsyncSession = Depends(get_db)) -> GlobalMetrics:
    sessions = (await db.execute(select(BotSession))).scalars().all()
    sessions_by_status: dict[str, int] = {}
    for s in sessions:
        sessions_by_status[s.status] = sessions_by_status.get(s.status, 0) + 1

    today = _utcnow().date()

    pnl_today = float((await db.execute(
        select(func.coalesce(func.sum(BotTrade.pnl), 0)).where(
            func.date(BotTrade.created_at) == today
        )
    )).scalar() or 0)

    pnl_all = float((await db.execute(
        select(func.coalesce(func.sum(BotTrade.pnl), 0))
    )).scalar() or 0)

    open_trades = int((await db.execute(
        select(func.count()).select_from(BotTrade).where(BotTrade.status == "open")
    )).scalar() or 0)

    since_24h = _utcnow() - timedelta(hours=24)
    errors_24h = int((await db.execute(
        select(func.count()).select_from(BotEventLog).where(
            and_(
                BotEventLog.created_at >= since_24h,
                BotEventLog.severity.in_(["ERROR", "CRITICAL"]),
            )
        )
    )).scalar() or 0)

    return GlobalMetrics(
        running_count=bot_manager.running_count(),
        sessions_by_status=sessions_by_status,
        total_open_trades=open_trades,
        total_pnl_today=pnl_today,
        total_pnl_all_time=pnl_all,
        errors_last_24h=errors_24h,
    )


@router.post("/bots/stop-all")
async def stop_all_running_bots(
    db: AsyncSession = Depends(get_db),
    admin_user: str | None = Depends(get_admin_user),
) -> dict:
    """Cancel every active/paused/halted bot task and mark sessions deactivated."""
    stmt = select(BotSession).where(
        BotSession.status.in_(["active", "paused", "halted"])
    )
    sessions = (await db.execute(stmt)).scalars().all()
    stopped = 0
    for session in sessions:
        stop_bot(session.id)
        runtime_state.update(session.id, runtime_state="stopped")
        session.status = "deactivated"
        session.updated_at = _utcnow()
        stopped += 1
    if stopped:
        await activity_logger.log_event(
            db, session_id=None, user_id="admin",
            severity="WARNING", event_type="ADMIN_STOP_ALL",
            message=f"Admin stopped all {stopped} running bot(s)",
            admin_user=admin_user,
            publish_lifecycle=True,
        )
        await db.commit()
    return {"stopped": stopped}


@router.get("/bots/{session_id}", response_model=BotDetail)
async def bot_detail(session_id: UUID, db: AsyncSession = Depends(get_db)) -> BotDetail:
    session = await _session_or_404(db, session_id)
    base = await _build_summary(db, session)
    snap = runtime_state.get(session_id)
    return BotDetail(
        **base.model_dump(),
        strategy_config=session.strategy_config or {},
        peak_balance=(snap.peak_balance if snap else None),
        error_count_session=(snap.error_count_session if snap else 0),
        last_error_message=(snap.last_error_message if snap else None),
    )


@router.get("/bots/{session_id}/trades", response_model=TradeListResponse)
async def bot_trades(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    side: Optional[str] = Query(default=None),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> TradeListResponse:
    await _session_or_404(db, session_id)
    stmt = select(BotTrade).where(BotTrade.session_id == session_id)
    if side:
        stmt = stmt.where(BotTrade.side == side.upper())
    if status_filter:
        stmt = stmt.where(BotTrade.status == status_filter)

    total = (await db.execute(
        select(func.count()).select_from(stmt.subquery())
    )).scalar() or 0

    rows = (await db.execute(
        stmt.order_by(BotTrade.created_at.desc()).limit(limit).offset(offset)
    )).scalars().all()

    return TradeListResponse(
        items=[
            TradeRow(
                id=t.id,
                broker_order_id=t.broker_order_id,
                symbol=t.symbol,
                side=t.side,
                quantity=float(t.quantity),
                price=float(t.price),
                status=t.status,
                pnl=float(t.pnl) if t.pnl is not None else None,
                created_at=t.created_at,
                closed_at=t.closed_at,
            ) for t in rows
        ],
        total=int(total),
    )


@router.get("/bots/{session_id}/holdings", response_model=HoldingsResponse)
async def bot_holdings(session_id: UUID, db: AsyncSession = Depends(get_db)) -> HoldingsResponse:
    session = await _session_or_404(db, session_id)
    items: list[HoldingRow] = []
    if session.position_side and session.entry_price and session.entry_quantity:
        entry = float(session.entry_price)
        qty = float(session.entry_quantity)
        # Best-effort: market_data.price_cache caches the last seen price.
        current_price = price_cache.get_last_price(session.symbol)
        unrealized_pnl = None
        unrealized_pct = None
        if current_price is not None and entry > 0 and qty > 0:
            if session.position_side == "BUY":
                unrealized_pnl = (current_price - entry) * qty
            else:
                unrealized_pnl = (entry - current_price) * qty
            unrealized_pct = (unrealized_pnl / (entry * qty)) * 100
        items.append(HoldingRow(
            symbol=session.symbol,
            side=session.position_side,
            quantity=qty,
            entry_price=entry,
            current_price=current_price,
            unrealized_pnl=unrealized_pnl,
            unrealized_pct=unrealized_pct,
        ))
    return HoldingsResponse(items=items)


@router.get("/bots/{session_id}/decisions", response_model=DecisionListResponse)
async def bot_decisions(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    since: Optional[datetime] = Query(default=None),
    strategy: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> DecisionListResponse:
    await _session_or_404(db, session_id)
    stmt = select(BotDecisionLog).where(BotDecisionLog.session_id == session_id)
    if since is not None:
        stmt = stmt.where(BotDecisionLog.tick_at > since)
    if strategy:
        stmt = stmt.where(BotDecisionLog.selected_strategy == strategy)

    rows = (await db.execute(
        stmt.order_by(BotDecisionLog.tick_at.desc()).limit(limit)
    )).scalars().all()

    return DecisionListResponse(items=[
        DecisionRow(
            id=r.id,
            tick_at=r.tick_at,
            current_price=float(r.current_price) if r.current_price is not None else None,
            momentum=r.momentum,
            volatility=r.volatility,
            pressure_ratio=r.pressure_ratio,
            bid_ratio=r.bid_ratio,
            ema_signal=r.ema_signal,
            market_event=r.market_event,
            selected_strategy=r.selected_strategy,
            strategy_switched=bool(r.strategy_switched),
            signals_count=int(r.signals_count or 0),
            signal_summary=r.signal_summary,
            position_side=r.position_side,
            entry_price=float(r.entry_price) if r.entry_price is not None else None,
            daily_pnl=float(r.daily_pnl) if r.daily_pnl is not None else None,
        ) for r in rows
    ])


@router.get("/bots/{session_id}/events", response_model=EventListResponse)
async def bot_events(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    severity: Optional[str] = Query(default=None),
    event_type: Optional[str] = Query(default=None),
    since: Optional[datetime] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> EventListResponse:
    await _session_or_404(db, session_id)
    stmt = select(BotEventLog).where(BotEventLog.session_id == session_id)
    if severity:
        wanted = [s.strip() for s in severity.split(",") if s.strip()]
        if wanted:
            stmt = stmt.where(BotEventLog.severity.in_(wanted))
    if event_type:
        stmt = stmt.where(BotEventLog.event_type == event_type)
    if since is not None:
        stmt = stmt.where(BotEventLog.created_at > since)

    rows = (await db.execute(
        stmt.order_by(BotEventLog.created_at.desc()).limit(limit)
    )).scalars().all()

    return EventListResponse(items=[
        EventRow(
            id=r.id,
            created_at=r.created_at,
            severity=r.severity,
            event_type=r.event_type,
            message=r.message,
            context=r.context,
            admin_user=r.admin_user,
        ) for r in rows
    ])


@router.get("/bots/{session_id}/metrics", response_model=BotMetricsResponse)
async def bot_metrics(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    window: str = Query(default="all", pattern="^(24h|7d|all)$"),
) -> BotMetricsResponse:
    session = await _session_or_404(db, session_id)

    trade_stmt = select(BotTrade).where(BotTrade.session_id == session_id)
    if window != "all":
        cutoff = _utcnow() - (timedelta(hours=24) if window == "24h" else timedelta(days=7))
        trade_stmt = trade_stmt.where(BotTrade.created_at >= cutoff)

    trades = (await db.execute(trade_stmt)).scalars().all()
    stats = _trade_stats(trades)

    decision_stmt = (
        select(BotDecisionLog.selected_strategy, func.count())
        .where(BotDecisionLog.session_id == session_id)
        .group_by(BotDecisionLog.selected_strategy)
    )
    if window != "all":
        cutoff = _utcnow() - (timedelta(hours=24) if window == "24h" else timedelta(days=7))
        decision_stmt = decision_stmt.where(BotDecisionLog.tick_at >= cutoff)

    rows = (await db.execute(decision_stmt)).all()
    signals_per_strategy = [StrategyMetric(strategy=r[0], count=int(r[1])) for r in rows]
    decisions_count = sum(m.count for m in signals_per_strategy)

    since_24h = _utcnow() - timedelta(hours=24)
    errors_24h = int((await db.execute(
        select(func.count()).select_from(BotEventLog).where(
            and_(
                BotEventLog.session_id == session_id,
                BotEventLog.created_at >= since_24h,
                BotEventLog.severity.in_(["ERROR", "CRITICAL"]),
            )
        )
    )).scalar() or 0)

    snap = runtime_state.get(session_id)
    return BotMetricsResponse(
        trades_total=stats["total_trades"],
        trades_open=stats["open_trades"],
        trades_closed=stats["closed_trades"],
        wins=stats["wins"],
        losses=stats["losses"],
        win_rate=stats["win_rate"],
        gross_profit=stats["gross_profit"],
        gross_loss=stats["gross_loss"],
        total_pnl=stats["total_pnl"],
        daily_pnl=(snap.daily_pnl if snap else stats["daily_pnl"]),
        peak_balance=(snap.peak_balance if snap else None),
        avg_trade_pnl=stats["avg_trade_pnl"],
        best_trade=stats["best_trade"],
        worst_trade=stats["worst_trade"],
        decisions_count=decisions_count,
        signals_per_strategy=signals_per_strategy,
        errors_24h=errors_24h,
    )


@router.get("/bots/{session_id}/equity", response_model=EquityResponse)
async def bot_equity(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    range_: str = Query(default="24h", alias="range", pattern="^(24h|7d|30d)$"),
) -> EquityResponse:
    """Cumulative realized P&L over time, derived from closed trades."""
    await _session_or_404(db, session_id)
    cutoff = {
        "24h": _utcnow() - timedelta(hours=24),
        "7d": _utcnow() - timedelta(days=7),
        "30d": _utcnow() - timedelta(days=30),
    }[range_]

    rows = (await db.execute(
        select(BotTrade)
        .where(and_(
            BotTrade.session_id == session_id,
            BotTrade.pnl.is_not(None),
            BotTrade.created_at >= cutoff,
        ))
        .order_by(BotTrade.created_at.asc())
    )).scalars().all()

    cumulative = 0.0
    points: list[EquityPoint] = []
    for t in rows:
        cumulative += float(t.pnl or 0)
        points.append(EquityPoint(ts=t.created_at, value=cumulative))
    return EquityResponse(items=points)


# ── controls ──────────────────────────────────────────────────────────────────

@router.post("/bots/{session_id}/start", response_model=ActionResponse)
async def admin_start(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    admin_user: str | None = Depends(get_admin_user),
) -> ActionResponse:
    session = await _session_or_404(db, session_id)
    try:
        token = decrypt_token(session.jwt_token)
    except InvalidToken:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="stored_token_unreadable: the user must re-activate the bot",
        )
    _verify_token_alive(token)

    session.status = "active"
    session.updated_at = _utcnow()
    await activity_logger.log_event(
        db,
        session_id=session.id,
        user_id=session.user_id,
        severity="INFO",
        event_type="ADMIN_START",
        message="Bot started by admin",
        admin_user=admin_user,
        publish_lifecycle=True,
    )
    await db.commit()
    start_bot(session.id, session.user_id)
    return ActionResponse(session_id=session.id, status="active", message="Bot starting")


@router.post("/bots/{session_id}/stop", response_model=ActionResponse)
async def admin_stop(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    admin_user: str | None = Depends(get_admin_user),
) -> ActionResponse:
    session = await _session_or_404(db, session_id)
    stop_bot(session.id)
    runtime_state.update(session.id, runtime_state="stopped")
    session.status = "deactivated"
    session.updated_at = _utcnow()
    await activity_logger.log_event(
        db,
        session_id=session.id,
        user_id=session.user_id,
        severity="INFO",
        event_type="ADMIN_STOP",
        message="Bot stopped by admin",
        admin_user=admin_user,
        publish_lifecycle=True,
    )
    await db.commit()
    return ActionResponse(session_id=session.id, status="deactivated", message="Bot stopped")


@router.post("/bots/{session_id}/restart", response_model=ActionResponse)
async def admin_restart(
    session_id: UUID,
    body: RestartRequest = RestartRequest(),
    db: AsyncSession = Depends(get_db),
    admin_user: str | None = Depends(get_admin_user),
) -> ActionResponse:
    session = await _session_or_404(db, session_id)
    try:
        token = decrypt_token(session.jwt_token)
    except InvalidToken:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="stored_token_unreadable",
        )
    _verify_token_alive(token)

    closed = 0
    if body.close_positions and session.position_side:
        closed_pnl = await force_close_position(session_id, reason="admin-restart-close")
        if closed_pnl is not None:
            closed = 1

    stop_bot(session.id)
    session.status = "active"
    session.updated_at = _utcnow()
    activity_logger.reset_throttle(session_id)
    await activity_logger.log_event(
        db,
        session_id=session.id,
        user_id=session.user_id,
        severity="INFO",
        event_type="ADMIN_RESTART",
        message="Bot restarted by admin",
        context={"close_positions": body.close_positions, "closed": closed},
        admin_user=admin_user,
        publish_lifecycle=True,
    )
    await db.commit()
    start_bot(session.id, session.user_id)
    return ActionResponse(session_id=session.id, status="active", message="Bot restarting")


@router.post("/bots/{session_id}/pause", response_model=ActionResponse)
async def admin_pause(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    admin_user: str | None = Depends(get_admin_user),
) -> ActionResponse:
    session = await _session_or_404(db, session_id)
    session.status = "paused"
    session.updated_at = _utcnow()
    runtime_state.update(session.id, runtime_state="paused")
    await activity_logger.log_event(
        db,
        session_id=session.id,
        user_id=session.user_id,
        severity="INFO",
        event_type="ADMIN_PAUSE",
        message="Bot paused by admin",
        admin_user=admin_user,
        publish_lifecycle=True,
    )
    await db.commit()
    return ActionResponse(session_id=session.id, status="paused")


@router.post("/bots/{session_id}/resume", response_model=ActionResponse)
async def admin_resume(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    admin_user: str | None = Depends(get_admin_user),
) -> ActionResponse:
    session = await _session_or_404(db, session_id)
    session.status = "active"
    session.updated_at = _utcnow()
    await activity_logger.log_event(
        db,
        session_id=session.id,
        user_id=session.user_id,
        severity="INFO",
        event_type="ADMIN_RESUME",
        message="Bot resumed by admin",
        admin_user=admin_user,
        publish_lifecycle=True,
    )
    await db.commit()
    # If the task isn't running (e.g. crashed), the user must reactivate.
    return ActionResponse(session_id=session.id, status="active")


@router.post("/bots/{session_id}/close-positions", response_model=ClosePositionsResponse)
async def admin_close_positions(
    session_id: UUID,
    db: AsyncSession = Depends(get_db),
    admin_user: str | None = Depends(get_admin_user),
) -> ClosePositionsResponse:
    session = await _session_or_404(db, session_id)
    pnl_total = 0.0
    closed = 0
    if session.position_side:
        result = await force_close_position(session_id, reason="admin-close-positions")
        if result is not None:
            pnl_total += float(result)
            closed += 1
    await activity_logger.log_event(
        db,
        session_id=session.id,
        user_id=session.user_id,
        severity="WARNING",
        event_type="ADMIN_CLOSE_POSITIONS",
        message=f"Admin forced close of {closed} position(s)",
        context={"pnl": pnl_total},
        admin_user=admin_user,
    )
    await db.commit()
    return ClosePositionsResponse(closed=closed, total_pnl=pnl_total)
