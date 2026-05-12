"""Per-user asyncio task registry.

Owns the mapping of user_id → running Task and the per-user activation
locks that prevent two concurrent POST /bots/activate requests from
racing and spawning duplicate tasks for the same user.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

_tasks: dict[str, asyncio.Task] = {}
_activation_locks: dict[str, asyncio.Lock] = {}


def get_activation_lock(user_id: str) -> asyncio.Lock:
    """Return (creating if needed) the per-user Lock for activate serialisation.

    Callers should use it as ``async with bot_manager.get_activation_lock(uid)``
    around the full check-then-start window to ensure only one task is ever
    spawned per user at a time.
    """
    if user_id not in _activation_locks:
        _activation_locks[user_id] = asyncio.Lock()
    return _activation_locks[user_id]


def start(
    session_id: UUID,
    user_id: str,
    coro_factory,
) -> asyncio.Task:
    """Cancel any existing task for the user, then launch a new one.

    Must be called while the caller holds ``get_activation_lock(user_id)``
    to guarantee mutual exclusion across concurrent activate requests.
    """
    stop(user_id)
    task: asyncio.Task = asyncio.create_task(
        coro_factory(session_id, user_id),
        name=f"bot-{user_id}",
    )
    _tasks[user_id] = task
    return task


def stop(user_id: str) -> None:
    task = _tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()


def is_running(user_id: str) -> bool:
    task = _tasks.get(user_id)
    return task is not None and not task.done()


def stop_all() -> None:
    for uid in list(_tasks.keys()):
        stop(uid)


def running_count() -> int:
    return sum(1 for t in _tasks.values() if not t.done())
