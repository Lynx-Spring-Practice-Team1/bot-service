"""Per-session asyncio task registry.

Owns the mapping of session_id → running Task and a reverse index of
user_id → {session_ids} so we can stop all bots for a user at once.
Per-user activation locks serialise concurrent POST /bots/activate requests.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

# session_id (str) → Task
_tasks: dict[str, asyncio.Task] = {}

# user_id → set of session_id strings currently tracked
_user_sessions: dict[str, set[str]] = {}

# per-user lock used by activate to prevent racing duplicate spawns
_activation_locks: dict[str, asyncio.Lock] = {}


def get_activation_lock(user_id: str) -> asyncio.Lock:
    if user_id not in _activation_locks:
        _activation_locks[user_id] = asyncio.Lock()
    return _activation_locks[user_id]


def start(
    session_id: UUID,
    user_id: str,
    coro_factory,
) -> asyncio.Task:
    """Launch a new task for this session.

    Unlike the old design, this does NOT cancel any existing task — each
    session_id gets its own independent task. Call stop() first if you
    want to bounce a specific session.
    """
    key = str(session_id)
    # Cancel a stale task for this session if one exists (e.g. restart).
    _cancel_key(key)

    task: asyncio.Task = asyncio.create_task(
        coro_factory(session_id, user_id),
        name=f"bot-{key[:8]}",
    )
    _tasks[key] = task
    _user_sessions.setdefault(user_id, set()).add(key)
    return task


def stop(session_id: UUID) -> None:
    """Cancel the task for a specific session only."""
    _cancel_key(str(session_id))


def stop_all_for_user(user_id: str) -> None:
    """Cancel every running task belonging to this user."""
    for key in list(_user_sessions.get(user_id, set())):
        _cancel_key(key)


def is_running(session_id: UUID) -> bool:
    task = _tasks.get(str(session_id))
    return task is not None and not task.done()


def is_any_running_for_user(user_id: str) -> bool:
    return any(
        not _tasks[k].done()
        for k in _user_sessions.get(user_id, set())
        if k in _tasks
    )


def stop_all() -> None:
    for key in list(_tasks.keys()):
        _cancel_key(key)


def running_count() -> int:
    return sum(1 for t in _tasks.values() if not t.done())


# ── internal ──────────────────────────────────────────────────────────────────

def _cancel_key(key: str) -> None:
    task = _tasks.pop(key, None)
    if task and not task.done():
        task.cancel()
    # Remove from reverse index
    for sessions in _user_sessions.values():
        sessions.discard(key)
