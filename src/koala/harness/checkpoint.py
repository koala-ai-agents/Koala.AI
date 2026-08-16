"""Durable checkpointing for :class:`~koala.harness.AgentSession`.

A ``Checkpointer`` snapshots the per-session state that must survive a
process restart — the full conversation history plus any HITL approvals
that were awaiting a human reply. Two backends ship in-tree:

    * :class:`InMemoryCheckpointer` — dict-backed, for tests.
    * :class:`SQLiteCheckpointer` — single JSON row per session, uses
      stdlib ``sqlite3`` wrapped in ``asyncio.to_thread``.

For richer durability guarantees (Postgres, S3, Redis) implement the
``Checkpointer`` protocol yourself — three async methods and you're done.

Scope
-----
This is *turn-boundary* checkpointing, not mid-turn. Koala saves a
checkpoint after every terminal event (``Output`` / ``Error``) on a session
worker's turn. If a process dies during a running turn, the previous
successful turn's messages plus any pending approvals from that turn are
preserved; the interrupted turn itself is not.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from ..core.messages import Message, ToolCallBlock
from ..memory.sqlite import _block_to_dict, _deserialize, _serialize

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PendingApproval:
    """A tool call awaiting a human reply at checkpoint time."""

    request_id: str
    tool_call: ToolCallBlock
    reason: str = ""
    created_at: float = field(default_factory=time.time)


@dataclass
class Checkpoint:
    """One session's durable state.

    Attributes:
        session_id: The session key.
        messages: Complete conversation history in chronological order.
            May be empty for a brand-new session.
        pending_approvals: HITL requests that were still awaiting a reply
            when this checkpoint was written. Callers of
            :meth:`AgentSession.resume` can inspect these on the returned
            session via ``session.pending_approvals``.
        metadata: Arbitrary user-defined data — passed through unchanged.
        created_at: Unix timestamp of the session's first save.
        updated_at: Unix timestamp of the most recent save.
    """

    session_id: str
    messages: list[Message] = field(default_factory=list)
    pending_approvals: list[PendingApproval] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Checkpointer(Protocol):
    """Durable backing store for session state.

    All methods are async. Custom implementations only need to provide
    ``save`` / ``load`` / ``delete`` / ``sessions``; ``close`` defaults to
    a no-op.
    """

    async def save(self, checkpoint: Checkpoint) -> None:
        """Persist a checkpoint, overwriting any prior state for the same session."""
        ...

    async def load(self, session_id: str) -> Checkpoint | None:
        """Return the stored checkpoint for a session, or ``None`` if unknown."""
        ...

    async def delete(self, session_id: str) -> None:
        """Remove a session's checkpoint. Idempotent."""
        ...

    async def sessions(self) -> list[str]:
        """List every session id with a stored checkpoint."""
        ...

    async def close(self) -> None:
        """Release backend resources. Default: no-op."""
        ...


# ---------------------------------------------------------------------------
# JSON serialization for pending approvals
# ---------------------------------------------------------------------------


def _pending_to_dict(p: PendingApproval) -> dict[str, Any]:
    return {
        "request_id": p.request_id,
        "reason": p.reason,
        "created_at": p.created_at,
        "tool_call": {
            "id": p.tool_call.id,
            "name": p.tool_call.name,
            "arguments": p.tool_call.arguments,
        },
    }


def _pending_from_dict(d: dict[str, Any]) -> PendingApproval:
    tc = d["tool_call"]
    return PendingApproval(
        request_id=d["request_id"],
        tool_call=ToolCallBlock(
            id=tc["id"], name=tc["name"], arguments=tc.get("arguments", {})
        ),
        reason=d.get("reason", ""),
        created_at=d.get("created_at", time.time()),
    )


def _checkpoint_to_json(c: Checkpoint) -> str:
    return json.dumps(
        {
            "session_id": c.session_id,
            "messages": [
                {
                    "role": m.role,
                    "content": [_block_to_dict(b) for b in m.content],
                    "name": m.name,
                }
                for m in c.messages
            ],
            "pending_approvals": [
                _pending_to_dict(p) for p in c.pending_approvals
            ],
            "metadata": c.metadata,
            "created_at": c.created_at,
            "updated_at": c.updated_at,
        }
    )


def _checkpoint_from_json(payload: str) -> Checkpoint:
    d = json.loads(payload)
    # Reuse SQLiteMemory's deserializer for messages so all ContentBlock
    # variants round-trip identically.
    messages: list[Message] = []
    for m_dict in d.get("messages", []):
        messages.append(_deserialize(json.dumps(m_dict)))
    return Checkpoint(
        session_id=d["session_id"],
        messages=messages,
        pending_approvals=[
            _pending_from_dict(p) for p in d.get("pending_approvals", [])
        ],
        metadata=d.get("metadata") or {},
        created_at=d.get("created_at", time.time()),
        updated_at=d.get("updated_at", time.time()),
    )


# ---------------------------------------------------------------------------
# InMemoryCheckpointer
# ---------------------------------------------------------------------------


class InMemoryCheckpointer:
    """Dict-backed checkpointer. Not durable across processes.

    Useful for tests, ephemeral sessions, and as a template when implementing
    a custom backend.
    """

    def __init__(self) -> None:
        self._store: dict[str, Checkpoint] = {}
        self._lock = asyncio.Lock()

    async def save(self, checkpoint: Checkpoint) -> None:
        async with self._lock:
            # Preserve the original created_at across saves.
            existing = self._store.get(checkpoint.session_id)
            if existing is not None:
                checkpoint.created_at = existing.created_at
            checkpoint.updated_at = time.time()
            self._store[checkpoint.session_id] = checkpoint

    async def load(self, session_id: str) -> Checkpoint | None:
        async with self._lock:
            return self._store.get(session_id)

    async def delete(self, session_id: str) -> None:
        async with self._lock:
            self._store.pop(session_id, None)

    async def sessions(self) -> list[str]:
        async with self._lock:
            return sorted(self._store.keys())

    async def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# SQLiteCheckpointer
# ---------------------------------------------------------------------------


class SQLiteCheckpointer:
    """SQLite-backed durable checkpointer.

    Schema is single-table, one row per session::

        checkpoints(session_id TEXT PRIMARY KEY,
                    payload TEXT,
                    created_at REAL,
                    updated_at REAL)

    Args:
        path: Filesystem path to the SQLite database. Use ``":memory:"`` for
            a per-process in-memory database (useful for tests).
    """

    def __init__(self, path: str) -> None:
        self.path = path
        if path != ":memory:":
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                session_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        self._conn.commit()
        self._lock = asyncio.Lock()

    # ---- sync helpers dispatched to asyncio.to_thread ------------------

    def _save_sync(self, checkpoint: Checkpoint) -> None:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT created_at FROM checkpoints WHERE session_id = ?",
            (checkpoint.session_id,),
        )
        row = cur.fetchone()
        if row is not None:
            checkpoint.created_at = row[0]
        checkpoint.updated_at = time.time()
        cur.execute(
            "INSERT OR REPLACE INTO checkpoints "
            "(session_id, payload, created_at, updated_at) "
            "VALUES (?, ?, ?, ?)",
            (
                checkpoint.session_id,
                _checkpoint_to_json(checkpoint),
                checkpoint.created_at,
                checkpoint.updated_at,
            ),
        )
        self._conn.commit()

    def _load_sync(self, session_id: str) -> Checkpoint | None:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT payload FROM checkpoints WHERE session_id = ?",
            (session_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return _checkpoint_from_json(row[0])

    def _delete_sync(self, session_id: str) -> None:
        cur = self._conn.cursor()
        cur.execute(
            "DELETE FROM checkpoints WHERE session_id = ?", (session_id,)
        )
        self._conn.commit()

    def _sessions_sync(self) -> list[str]:
        cur = self._conn.cursor()
        cur.execute("SELECT session_id FROM checkpoints ORDER BY session_id")
        return [r[0] for r in cur.fetchall()]

    def _close_sync(self) -> None:
        self._conn.close()

    # ---- async surface -------------------------------------------------

    async def save(self, checkpoint: Checkpoint) -> None:
        async with self._lock:
            await asyncio.to_thread(self._save_sync, checkpoint)

    async def load(self, session_id: str) -> Checkpoint | None:
        async with self._lock:
            return await asyncio.to_thread(self._load_sync, session_id)

    async def delete(self, session_id: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._delete_sync, session_id)

    async def sessions(self) -> list[str]:
        async with self._lock:
            return await asyncio.to_thread(self._sessions_sync)

    async def close(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._close_sync)


__all__ = [
    "Checkpoint",
    "Checkpointer",
    "InMemoryCheckpointer",
    "PendingApproval",
    "SQLiteCheckpointer",
]


# Ensure _serialize is referenced so it's not flagged as unused — it's part
# of the Message-JSON contract shared with SQLiteMemory.
_ = _serialize
