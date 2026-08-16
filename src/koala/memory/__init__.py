"""Koala memory package (L4).

Conversation history storage keyed by ``session_id``. Two built-in backends:
    - ``InMemoryMemory``  — dict-backed, non-persistent
    - ``SQLiteMemory``    — file-backed, persistent
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
