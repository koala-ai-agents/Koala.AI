"""Simple observability utilities: structured logging, in-memory metrics and tracing.

This is intentionally lightweight for local development and tests. It provides:
- StructuredLogger: emits JSON-like dicts via the standard logging module.
- MetricsCollector: in-memory counters and timers with a Prometheus-like text export.
- Tracer: simple in-memory trace store for events per trace id.

These primitives are useful for tests and early-stage observability before
integrating a full metrics/tracing stack.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any, Dict, List, Optional


class StructuredLogger:
    def __init__(self, name: str = "kola"):
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter("%(message)s")
            h.setFormatter(fmt)
            self._logger.addHandler(h)
        self._logger.setLevel(logging.INFO)

    def info(self, event: str, **kwargs: Any) -> None:
        payload = {"ts": time.time(), "event": event, **kwargs}
        try:
            self._logger.info(json.dumps(payload))
        except Exception:
            # fallback to plain string
            self._logger.info(str(payload))


# module-level default logger
logger = StructuredLogger()


class MetricsCollector:
    def __init__(self) -> None:
        self._counters: Dict[str, int] = {}
        self._timings: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + amount

    def timing(self, name: str, seconds: float) -> None:
        with self._lock:
            self._timings.setdefault(name, []).append(seconds)

    def export_prometheus(self) -> str:
        """Return a small Prometheus-like exposition format string."""
        lines: List[str] = []
        with self._lock:
            for k, v in sorted(self._counters.items()):
                lines.append(f"{k} {v}")
            for k, vals in sorted(self._timings.items()):
                if vals:
                    avg = sum(vals) / len(vals)
                    lines.append(f"{k}_count {len(vals)}")
                    lines.append(f"{k}_avg {avg:.6f}")
        return "\n".join(lines)


# module-level default metrics collector
metrics = MetricsCollector()


class Tracer:
    """A tiny in-memory tracer that records events per trace id.

    API:
      - start_trace() -> trace_id
      - record(trace_id, event, **kwargs)
      - get_trace(trace_id) -> list of events
    """

    def __init__(self) -> None:
        self._store: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def start_trace(self) -> str:
        tid = uuid.uuid4().hex
        with self._lock:
            self._store[tid] = []
        return tid

    def record(self, trace_id: str, event: str, **kwargs: Any) -> None:
        with self._lock:
            lst = self._store.setdefault(trace_id, [])
            lst.append({"ts": time.time(), "event": event, **kwargs})

    def get_trace(self, trace_id: str) -> Optional[List[Dict[str, Any]]]:
        return self._store.get(trace_id)


# module-level tracer
tracer = Tracer()


def retry(func, retries: int = 3, delay: float = 0.1):
    """Simple retry decorator for synchronous callables."""

    def wrapper(*args, **kwargs):
        last = None
        for _ in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last = e
                time.sleep(delay)
        raise last

    return wrapper


class TimeoutError(Exception):
    pass


def run_with_timeout(func, timeout: Optional[float], *args, **kwargs):
    """Run func in a thread and enforce timeout (thread-based). Returns func result or raises TimeoutError."""
    if timeout is None:
        return func(*args, **kwargs)

    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FutTimeout

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(func, *args, **kwargs)
        try:
            return fut.result(timeout=timeout)
        except FutTimeout as e:
            fut.cancel()
            raise TimeoutError(f"Function timed out after {timeout} seconds") from e


class RetryPolicy:
    def __init__(
        self, max_attempts: int = 3, backoff: float = 0.1, multiplier: float = 2.0
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.max_attempts = max_attempts
        self.backoff = backoff
        self.multiplier = multiplier

    @staticmethod
    def from_dict(d: Optional[Dict[str, Any]]):
        if not d:
            return None
        return RetryPolicy(
            max_attempts=int(d.get("max_attempts", 3)),
            backoff=float(d.get("backoff", 0.1)),
            multiplier=float(d.get("multiplier", 2.0)),
        )


def redact(
    data: Dict[str, Any], fields: List[str], replacement: str = "***"
) -> Dict[str, Any]:
    """Return a shallow-copied dict with selected fields redacted.

    Example:
        safe = redact({"name": "Alice", "ssn": "123-45-6789"}, ["ssn"])  # {"name": "Alice", "ssn": "***"}
    """
    out = dict(data)
    for f in fields:
        if f in out and out[f] is not None:
            out[f] = replacement
    return out
