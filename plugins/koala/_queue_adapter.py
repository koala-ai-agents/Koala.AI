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
            try:
                h(message)
            except Exception:
                pass

    def subscribe(self, topic: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        with self._lock:
            self._topics.setdefault(topic, []).append(handler)
