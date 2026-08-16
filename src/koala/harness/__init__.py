"""Koala harness package (L8).

The harness closes the loop on interactive agent work:
    - ``AgentSession`` implements L1 ``Channel`` — bidirectional streaming
      conversations with HITL, interrupt, and multi-turn memory.
    - Error hierarchy for session / resolver failures.

Users typically don't import from ``koala.harness`` directly; call
``agent.session(...)`` on an ``Agent`` instead.
"""

from __future__ import annotations

from .checkpoint import (
    Checkpoint,
    Checkpointer,
    InMemoryCheckpointer,
    PendingApproval,
    SQLiteCheckpointer,
)
from .errors import ResolverTimeoutError, SessionClosedError, SessionError
from .session import AgentSession

__all__ = [
    "AgentSession",
    "SessionError",
    "SessionClosedError",
    "ResolverTimeoutError",
    "Checkpoint",
    "Checkpointer",
    "InMemoryCheckpointer",
    "PendingApproval",
    "SQLiteCheckpointer",
]
