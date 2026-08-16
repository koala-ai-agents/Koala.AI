"""Dict-backed in-memory conversation storage.

Zero dependencies, non-persistent. Ideal for tests, notebooks, ephemeral
sessions where restart-safety doesn't matter. All operations happen behind
an ``asyncio.Lock`` so concurrent coroutines on the same event loop stay
safe.
"""

from __future__ import annotations

import asyncio

from ..core.messages import Message
from .base import BaseMemory


class InMemoryMemory(BaseMemory):
    """Non-persistent, dict-backed conversation memory."""

    def __init__(self) -> None:
        self._sessions: dict[str, list[Message]] = {}
        self._lock = asyncio.Lock()

    async def append(
        self, session_id: str, messages: list[Message]
    ) -> None:
        if not messages:
            return
        async with self._lock:
            self._sessions.setdefault(session_id, []).extend(messages)

    async def get(
        self, session_id: str, *, limit: int | None = None
    ) -> list[Message]:
        async with self._lock:
            history = self._sessions.get(session_id, [])
            if limit is None:
                return list(history)
            if limit <= 0:
                return []
            return list(history[-limit:])

    async def clear(self, session_id: str) -> None:
        async with self._lock:
            self._sessions.pop(session_id, None)

    async def sessions(self) -> list[str]:
        async with self._lock:
            return sorted(self._sessions.keys())
