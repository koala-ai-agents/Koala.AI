"""Compat queue module for Airflow plugins.

This file serves two purposes:
- Provide Kola's lightweight `InMemoryQueue` adapter used in examples/tests.
- Avoid shadowing Python's stdlib `queue` module by exposing compatible
    `Queue`, `Empty`, and `Full` symbols expected by dependencies like AnyIO.

Keeping this file inside the Airflow plugins tree means Python could attempt
to import it when a library does `from queue import Queue`. To prevent
breakage, we implement minimal, thread-safe equivalents of the stdlib
queue API.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


class QueueError(Exception):
    pass


class Empty(Exception):
    pass


class Full(Exception):
    pass


class QueueAdapter:
    """Abstract interface for queue adapters."""

    def publish(self, topic: str, message: Dict[str, Any]) -> None:
        raise NotImplementedError()

    def subscribe(self, topic: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        raise NotImplementedError()


class Queue:
    """Minimal stdlib-compatible FIFO queue with blocking put/get.

    This is not a full drop-in replacement but supports methods commonly
    used by AnyIO and similar libraries: `put`, `get`, `qsize`, `empty`,
    and `full`. Maxsize semantics are respected when `maxsize > 0`.
    """

    def __init__(self, maxsize: int = 0):
        self._maxsize = int(maxsize)
        self._q = deque()
        self._cond = threading.Condition()

    def qsize(self) -> int:
        with self._cond:
            return len(self._q)

    def empty(self) -> bool:
        return self.qsize() == 0

    def full(self) -> bool:
        with self._cond:
            return self._maxsize > 0 and len(self._q) >= self._maxsize

    def put(
        self, item: Any, block: bool = True, timeout: Optional[float] = None
    ) -> None:
        with self._cond:
            if not block and self.full():
                raise Full()
            if block:
                end = None if timeout is None else time.time() + timeout
                while self.full():
                    remaining = None if end is None else end - time.time()
                    if remaining is not None and remaining <= 0:
                        raise Full()
                    self._cond.wait(timeout=remaining)
            self._q.append(item)
            self._cond.notify()

    def put_nowait(self, item: Any) -> None:
        """Non-blocking put raising `Full` when the queue is full."""
        return self.put(item, block=False)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        with self._cond:
            if not block and not self._q:
                raise Empty()
            if block:
                end = None if timeout is None else time.time() + timeout
                while not self._q:
                    remaining = None if end is None else end - time.time()
                    if remaining is not None and remaining <= 0:
                        raise Empty()
                    self._cond.wait(timeout=remaining)
            item = self._q.popleft()
            self._cond.notify()
            return item

    def get_nowait(self) -> Any:
        """Non-blocking get raising `Empty` when the queue is empty."""
        return self.get(block=False)


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
