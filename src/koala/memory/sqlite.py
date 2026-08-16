"""SQLite-backed conversation memory.

Persistent across process restarts. Uses stdlib ``sqlite3`` wrapped in
``asyncio.to_thread`` so no extra async-sqlite dependency is required.

Schema is single-table, append-only:
    memory_messages(id INTEGER PK, session_id TEXT, message JSON, created_at REAL)

Each message is JSON-encoded on write via ``_serialize`` and reconstructed
via ``_deserialize`` on read, so all koala Message content-block variants
round-trip losslessly.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from typing import Any

from ..core.messages import (
    ContentBlock,
    ImageBlock,
    Message,
    TextBlock,
    ThinkingBlock,
    ToolCallBlock,
    ToolResultBlock,
)
from .base import BaseMemory

# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _block_to_dict(b: ContentBlock) -> dict[str, Any]:
    if isinstance(b, TextBlock):
        return {"kind": "text", "text": b.text}
    if isinstance(b, ThinkingBlock):
        return {"kind": "thinking", "text": b.text}
    if isinstance(b, ImageBlock):
        return {
            "kind": "image",
            "source": b.source,
            "media_type": b.media_type,
        }
    if isinstance(b, ToolCallBlock):
        return {
            "kind": "tool_call",
            "id": b.id,
            "name": b.name,
            "arguments": b.arguments,
        }
    if isinstance(b, ToolResultBlock):
        return {
            "kind": "tool_result",
            "tool_call_id": b.tool_call_id,
            "content": b.content,
            "is_error": b.is_error,
        }
    raise TypeError(f"Cannot serialize content block of type {type(b).__name__}")


def _dict_to_block(d: dict[str, Any]) -> ContentBlock:
    kind = d.get("kind")
    if kind == "text":
        return TextBlock(text=d["text"])
    if kind == "thinking":
        return ThinkingBlock(text=d["text"])
    if kind == "image":
        return ImageBlock(
            source=d["source"], media_type=d.get("media_type", "image/png")
        )
    if kind == "tool_call":
        return ToolCallBlock(
            id=d["id"], name=d["name"], arguments=d.get("arguments", {})
        )
    if kind == "tool_result":
        return ToolResultBlock(
            tool_call_id=d["tool_call_id"],
            content=d["content"],
            is_error=d.get("is_error", False),
        )
    raise ValueError(f"Unknown content block kind: {kind!r}")


def _serialize(m: Message) -> str:
    return json.dumps(
        {
            "role": m.role,
            "content": [_block_to_dict(b) for b in m.content],
            "name": m.name,
        }
    )


def _deserialize(row: str) -> Message:
    data = json.loads(row)
    return Message(
        role=data["role"],
        content=[_dict_to_block(b) for b in data.get("content", [])],
        name=data.get("name"),
    )


# ---------------------------------------------------------------------------
# SQLiteMemory
# ---------------------------------------------------------------------------


class SQLiteMemory(BaseMemory):
    """SQLite-backed persistent conversation memory.

    Args:
        path: Filesystem path to the SQLite database. Use ``":memory:"`` for
            a per-process in-memory database (useful for tests that want to
            exercise the SQL path without touching disk).
    """

    def __init__(self, path: str) -> None:
        self.path = path
        if path != ":memory:":
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        # check_same_thread=False so asyncio.to_thread can dispatch from any
        # worker thread. Access is serialized via self._lock.
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS memory_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_memory_session "
            "ON memory_messages(session_id, id)"
        )
        self._conn.commit()
        self._lock = asyncio.Lock()

    # ---- sync helpers (run on the thread executor) ----------------------

    def _append_sync(
        self, session_id: str, messages: list[Message]
    ) -> None:
        rows = [
            (session_id, _serialize(m), time.time()) for m in messages
        ]
        cur = self._conn.cursor()
        cur.executemany(
            "INSERT INTO memory_messages (session_id, message, created_at) "
            "VALUES (?, ?, ?)",
            rows,
        )
        self._conn.commit()

    def _get_sync(
        self, session_id: str, limit: int | None
    ) -> list[Message]:
        cur = self._conn.cursor()
        if limit is not None and limit >= 0:
            if limit == 0:
                return []
            cur.execute(
                "SELECT message FROM memory_messages WHERE session_id = ? "
                "ORDER BY id DESC LIMIT ?",
                (session_id, limit),
            )
            rows = list(reversed(cur.fetchall()))
        else:
            cur.execute(
                "SELECT message FROM memory_messages WHERE session_id = ? "
                "ORDER BY id ASC",
                (session_id,),
            )
            rows = cur.fetchall()
        return [_deserialize(row[0]) for row in rows]

    def _clear_sync(self, session_id: str) -> None:
        cur = self._conn.cursor()
        cur.execute(
            "DELETE FROM memory_messages WHERE session_id = ?",
            (session_id,),
        )
        self._conn.commit()

    def _sessions_sync(self) -> list[str]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT DISTINCT session_id FROM memory_messages "
            "ORDER BY session_id"
        )
        return [r[0] for r in cur.fetchall()]

    def _close_sync(self) -> None:
        self._conn.close()

    # ---- async surface -------------------------------------------------

    async def append(
        self, session_id: str, messages: list[Message]
    ) -> None:
        if not messages:
            return
        async with self._lock:
            await asyncio.to_thread(self._append_sync, session_id, messages)

    async def get(
        self, session_id: str, *, limit: int | None = None
    ) -> list[Message]:
        async with self._lock:
            return await asyncio.to_thread(self._get_sync, session_id, limit)

    async def clear(self, session_id: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._clear_sync, session_id)

    async def sessions(self) -> list[str]:
        async with self._lock:
            return await asyncio.to_thread(self._sessions_sync)

    async def close(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._close_sync)
