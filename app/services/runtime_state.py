"""In-memory per-session runtime snapshot.

The DB-persisted bot_sessions.status is too coarse for the admin UI's
fine-grained indicator (thinking / buying / selling / managing). This module
holds a transient snapshot updated by bot_engine each tick.

Snapshots are ephemeral — they reset when the bot-service process restarts,
which is fine: the UI can read the persisted DB status until the next tick
populates the runtime snapshot again.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Literal
from uuid import UUID

# 9 mutually-exclusive states surfaced to the admin UI.
RuntimeState = Literal[
    "stopped",
    "starting",
    "thinking",
    "buying",
    "selling",
    "managing",
    "paused",
    "halted",
    "error",
]


@dataclass
class RuntimeSnapshot:
    runtime_state: RuntimeState = "starting"
    current_strategy: str = "hold"
    last_decision_at: datetime | None = None
    daily_pnl: float = 0.0
    peak_balance: float | None = None
    error_count_session: int = 0
    last_error_at: datetime | None = None
    last_error_message: str | None = None
    # Set by bot_engine when a new admin user triggers an action; the
    # admin_bots router reads it back so audit columns get populated.
    last_admin_user: str | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        for k in ("last_decision_at", "last_error_at"):
            if d[k] is not None:
                d[k] = d[k].isoformat()
        return d


_snapshots: dict[UUID, RuntimeSnapshot] = {}


def update(session_id: UUID, **changes) -> RuntimeSnapshot:
    """Patch-update the snapshot for a session; create if missing."""
    snap = _snapshots.get(session_id)
    if snap is None:
        snap = RuntimeSnapshot()
        _snapshots[session_id] = snap
    for key, value in changes.items():
        if hasattr(snap, key):
            setattr(snap, key, value)
    return snap


def record_error(session_id: UUID, message: str) -> None:
    snap = _snapshots.get(session_id) or _snapshots.setdefault(session_id, RuntimeSnapshot())
    snap.error_count_session += 1
    snap.last_error_at = datetime.utcnow()
    snap.last_error_message = message


def get(session_id: UUID) -> RuntimeSnapshot | None:
    return _snapshots.get(session_id)


def clear(session_id: UUID) -> None:
    _snapshots.pop(session_id, None)


def all_snapshots() -> dict[UUID, RuntimeSnapshot]:
    return dict(_snapshots)
