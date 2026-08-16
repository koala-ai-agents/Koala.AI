"""Koala memory package (L4).

Conversation history storage keyed by ``session_id``. Two built-in backends:
    - ``InMemoryMemory``  — dict-backed, non-persistent
    - ``SQLiteMemory``    — file-backed, persistent

The legacy KV store lives at ``koala.memory_legacy`` and is a different
abstraction (generic key/value, not conversation history).
"""

from __future__ import annotations

from .base import BaseMemory
from .in_memory import InMemoryMemory
from .sqlite import SQLiteMemory

__all__ = [
    "BaseMemory",
    "InMemoryMemory",
    "SQLiteMemory",
]
