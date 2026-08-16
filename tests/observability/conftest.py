"""Shared OTel fixtures for observability tests.

The OpenTelemetry SDK enforces set-once semantics on the global tracer
provider, so we install one session-scoped provider + in-memory exporter and
``.clear()`` the exporter between tests instead of swapping providers.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

_EXPORTER = InMemorySpanExporter()
_INSTALLED = False


def _ensure_provider() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(_EXPORTER))
    try:
        trace.set_tracer_provider(provider)
    except Exception:  # noqa: BLE001 — some other test already set one
        pass
    _INSTALLED = True


@pytest.fixture
def span_exporter() -> Iterator[InMemorySpanExporter]:
    """In-memory exporter, cleared per test."""
    _ensure_provider()
    _EXPORTER.clear()
    yield _EXPORTER
    _EXPORTER.clear()
