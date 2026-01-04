"""Queue adapter placeholders for Kola.

This module provides a small, pluggable interface for queue adapters and a
local in-memory implementation suitable for tests and demos. It intentionally
avoids dependencies on Kafka/Redis so the project remains easy to run.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List


class QueueError(Exception):
    pass


class QueueAdapter:
    """Abstract interface for queue adapters."""

    def publish(self, topic: str, message: Dict[str, Any]) -> None:
        raise NotImplementedError()

    def subscribe(self, topic: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        raise NotImplementedError()


@dataclass
class InMemoryQueue(QueueAdapter):
    _topics: Dict[str, List[Callable[[Dict[str, Any]], None]]] = field(
        default_factory=dict
    )
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def publish(self, topic: str, message: Dict[str, Any]) -> None:
        with self._lock:
            handlers = list(self._topics.get(topic, []))
        for h in handlers:
            # dispatch synchronously for simplicity; in real adapter we'd use background workers
            try:
                h(message)
            except Exception:
                # swallow handler exceptions; production code should log
                pass

    def subscribe(self, topic: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        with self._lock:
            self._topics.setdefault(topic, []).append(handler)
