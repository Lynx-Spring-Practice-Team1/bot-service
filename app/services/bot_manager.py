"""Per-user asyncio task registry.

Owns the mapping of user_id → running Task so every other module can
call start / stop / is_running without touching asyncio internals.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any
from uuid import UUID

_tasks: dict[str, asyncio.Task] = {}


def start(
    session_id: UUID,
    user_id: str,
    coro_factory,
) -> asyncio.Task:
    """Cancel any existing task for the user, then launch a new one."""
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
