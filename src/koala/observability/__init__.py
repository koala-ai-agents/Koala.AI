"""Koala observability — OpenTelemetry emitter with GenAI semantic conventions.

The framework instruments three hot paths:

    1. ``Model.astream``          -> ``chat {model}`` span
    2. ``Agent.astream``          -> ``invoke_agent {name}`` span
    3. Tool execution inside Agent -> ``execute_tool {name}`` span

Every span uses the OpenTelemetry ``gen_ai.*`` semantic conventions so any
compliant backend (Grafana Tempo, Jaeger, Datadog, Honeycomb, W&B Weave,
Langfuse, Traceloop, ...) can display Koala traces without custom mappers.

OpenTelemetry is a soft-optional dependency. If ``opentelemetry-api`` is not
installed, the helpers here return no-op context managers — the framework
keeps working, just doesn't emit spans. Install with::

    pip install koala[otel]

then configure a provider + exporter in your application entry-point:

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
"""

from __future__ import annotations

from .otel import (
    agent_span,
    is_available,
    model_span,
    tool_span,
)

__all__ = [
    "is_available",
    "model_span",
    "agent_span",
    "tool_span",
]
