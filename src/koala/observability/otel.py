"""OpenTelemetry GenAI-semconv emitter for Koala.

Public entry points are three context managers:

    * ``model_span(system, model, settings=...)`` — one LLM request.
    * ``agent_span(name, description=...)`` — a full agent invocation.
    * ``tool_span(name, call_id=..., arguments=...)`` — one tool execution.

Each returns a small handle with methods to record the response (usage,
finish reason, tool result) after the underlying work completes. If the
OpenTelemetry API is not installed, all three degrade to a no-op context
manager so calling code stays unchanged.

Design note
-----------

Attribute names are hard-coded string literals rather than imported from
``opentelemetry.semconv._incubating.attributes.gen_ai_attributes``. The
Python package's constant names have shifted between versions
(``GEN_AI_SYSTEM`` vs ``GEN_AI_PROVIDER_NAME``, ``GEN_AI_AGENT_NAME``
only added in ~0.55b) and different environments — including
``apache/airflow:3.1.5`` — pin different versions. The semconv spec's
attribute *names* are stable; we track those instead of the package's
Python constants.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from ..models.settings import ChatSettings

# ---------------------------------------------------------------------------
# Optional OpenTelemetry import
# ---------------------------------------------------------------------------

try:  # pragma: no cover - trivial import guard
    from opentelemetry import trace as _otel_trace
    from opentelemetry.trace import Status, StatusCode

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _otel_trace = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment,misc]
    StatusCode = None  # type: ignore[assignment,misc]
    _OTEL_AVAILABLE = False


def is_available() -> bool:
    """True when the OpenTelemetry API is importable."""
    return _OTEL_AVAILABLE


_TRACER_NAME = "koala"


def _get_tracer() -> Any:
    """Return the framework tracer, or None when OTel isn't installed."""
    if not _OTEL_AVAILABLE:
        return None
    return _otel_trace.get_tracer(_TRACER_NAME)


# ---------------------------------------------------------------------------
# GenAI semantic-convention attribute names (string literals — stable per
# spec, decoupled from opentelemetry-semantic-conventions package version).
# ---------------------------------------------------------------------------

# Operation identity
_ATTR_OPERATION_NAME = "gen_ai.operation.name"
_ATTR_SYSTEM = "gen_ai.system"
_ATTR_PROVIDER_NAME = "gen_ai.provider.name"

# Request-side
_ATTR_REQUEST_MODEL = "gen_ai.request.model"
_ATTR_REQUEST_STREAM = "gen_ai.request.stream"
_ATTR_REQUEST_TEMPERATURE = "gen_ai.request.temperature"
_ATTR_REQUEST_MAX_TOKENS = "gen_ai.request.max_tokens"
_ATTR_REQUEST_TOP_P = "gen_ai.request.top_p"
_ATTR_REQUEST_STOP_SEQUENCES = "gen_ai.request.stop_sequences"
_ATTR_REQUEST_SEED = "gen_ai.request.seed"
_ATTR_REQUEST_FREQUENCY_PENALTY = "gen_ai.request.frequency_penalty"
_ATTR_REQUEST_PRESENCE_PENALTY = "gen_ai.request.presence_penalty"

# Response-side
_ATTR_RESPONSE_MODEL = "gen_ai.response.model"
_ATTR_RESPONSE_ID = "gen_ai.response.id"
_ATTR_RESPONSE_FINISH_REASONS = "gen_ai.response.finish_reasons"

# Usage
_ATTR_USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
_ATTR_USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"

# Agent
_ATTR_AGENT_NAME = "gen_ai.agent.name"
_ATTR_AGENT_DESCRIPTION = "gen_ai.agent.description"
_ATTR_CONVERSATION_ID = "gen_ai.conversation.id"

# Tool
_ATTR_TOOL_NAME = "gen_ai.tool.name"
_ATTR_TOOL_CALL_ID = "gen_ai.tool.call.id"
_ATTR_TOOL_DESCRIPTION = "gen_ai.tool.description"
_ATTR_TOOL_CALL_ARGUMENTS = "gen_ai.tool.call.arguments"
_ATTR_TOOL_CALL_RESULT = "gen_ai.tool.call.result"


# ---------------------------------------------------------------------------
# Model span
# ---------------------------------------------------------------------------


class ModelSpan:
    """Handle returned by ``model_span`` — call ``record_*`` to attach post-hoc data."""

    def __init__(self, span: Any) -> None:
        self._span = span

    def record_response(
        self,
        *,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        finish_reason: str | None = None,
        response_model: str | None = None,
        response_id: str | None = None,
    ) -> None:
        """Attach response-side attributes after the LLM call completes."""
        span = self._span
        if span is None:
            return
        if input_tokens is not None:
            span.set_attribute(_ATTR_USAGE_INPUT_TOKENS, int(input_tokens))
        if output_tokens is not None:
            span.set_attribute(_ATTR_USAGE_OUTPUT_TOKENS, int(output_tokens))
        if finish_reason:
            # OTel spec: this attribute is a list even when only one reason.
            span.set_attribute(_ATTR_RESPONSE_FINISH_REASONS, [finish_reason])
        if response_model:
            span.set_attribute(_ATTR_RESPONSE_MODEL, response_model)
        if response_id:
            span.set_attribute(_ATTR_RESPONSE_ID, response_id)

    def record_error(self, exc: BaseException) -> None:
        """Mark the span as errored and record the exception."""
        span = self._span
        if span is None:
            return
        span.set_status(Status(StatusCode.ERROR, str(exc)))
        span.record_exception(exc)


@contextmanager
def model_span(
    *,
    system: str,
    model: str,
    settings: "ChatSettings | None" = None,
    operation: str = "chat",
) -> Iterator[ModelSpan]:
    """Open a span for one LLM request.

    Args:
        system: Provider slug (``"openai"``, ``"groq"``, ...). Emitted as
            both ``gen_ai.system`` (legacy) and ``gen_ai.provider.name``
            (current) for maximum backend compatibility.
        model: Model name (``"gpt-4o-mini"``, ``"llama-3.3-70b-versatile"``).
        settings: Optional ``ChatSettings`` — request-side attributes like
            temperature / max_tokens / stop are copied onto the span.
        operation: Operation name — ``"chat"``, ``"text_completion"``,
            ``"embeddings"``. Defaults to ``"chat"``.
    """
    tracer = _get_tracer()
    if tracer is None:
        yield ModelSpan(span=None)
        return

    span_name = f"{operation} {model}"
    with tracer.start_as_current_span(span_name) as span:
        span.set_attribute(_ATTR_OPERATION_NAME, operation)
        span.set_attribute(_ATTR_SYSTEM, system)
        span.set_attribute(_ATTR_PROVIDER_NAME, system)
        span.set_attribute(_ATTR_REQUEST_MODEL, model)
        span.set_attribute(_ATTR_REQUEST_STREAM, True)
        if settings is not None:
            _apply_settings_attrs(span, settings)
        handle = ModelSpan(span=span)
        try:
            yield handle
        except BaseException as e:  # noqa: BLE001
            handle.record_error(e)
            raise


def _apply_settings_attrs(span: Any, settings: "ChatSettings") -> None:
    """Copy request-side ChatSettings values onto the current span."""
    if settings.temperature is not None:
        span.set_attribute(_ATTR_REQUEST_TEMPERATURE, float(settings.temperature))
    if settings.max_tokens is not None:
        span.set_attribute(_ATTR_REQUEST_MAX_TOKENS, int(settings.max_tokens))
    if settings.top_p is not None:
        span.set_attribute(_ATTR_REQUEST_TOP_P, float(settings.top_p))
    if settings.stop:
        span.set_attribute(_ATTR_REQUEST_STOP_SEQUENCES, list(settings.stop))
    if settings.seed is not None:
        span.set_attribute(_ATTR_REQUEST_SEED, int(settings.seed))
    if settings.frequency_penalty is not None:
        span.set_attribute(
            _ATTR_REQUEST_FREQUENCY_PENALTY, float(settings.frequency_penalty)
        )
    if settings.presence_penalty is not None:
        span.set_attribute(
            _ATTR_REQUEST_PRESENCE_PENALTY, float(settings.presence_penalty)
        )


# ---------------------------------------------------------------------------
# Agent span
# ---------------------------------------------------------------------------


class AgentSpan:
    """Handle returned by ``agent_span`` — call ``record_*`` to attach post-hoc data."""

    def __init__(self, span: Any) -> None:
        self._span = span

    def record_completion(
        self,
        *,
        iterations: int | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        stop_reason: str | None = None,
    ) -> None:
        span = self._span
        if span is None:
            return
        if iterations is not None:
            span.set_attribute("koala.agent.iterations", int(iterations))
        if input_tokens is not None:
            span.set_attribute(_ATTR_USAGE_INPUT_TOKENS, int(input_tokens))
        if output_tokens is not None:
            span.set_attribute(_ATTR_USAGE_OUTPUT_TOKENS, int(output_tokens))
        if stop_reason:
            span.set_attribute("koala.agent.stop_reason", stop_reason)

    def record_error(self, exc: BaseException) -> None:
        span = self._span
        if span is None:
            return
        span.set_status(Status(StatusCode.ERROR, str(exc)))
        span.record_exception(exc)


@contextmanager
def agent_span(
    *,
    name: str,
    description: str | None = None,
    conversation_id: str | None = None,
) -> Iterator[AgentSpan]:
    """Open a span for a whole agent run (across the tool-calling loop)."""
    tracer = _get_tracer()
    if tracer is None:
        yield AgentSpan(span=None)
        return

    span_name = f"invoke_agent {name}"
    with tracer.start_as_current_span(span_name) as span:
        span.set_attribute(_ATTR_OPERATION_NAME, "invoke_agent")
        span.set_attribute(_ATTR_AGENT_NAME, name)
        if description:
            span.set_attribute(_ATTR_AGENT_DESCRIPTION, description)
        if conversation_id:
            span.set_attribute(_ATTR_CONVERSATION_ID, conversation_id)
        handle = AgentSpan(span=span)
        try:
            yield handle
        except BaseException as e:  # noqa: BLE001
            handle.record_error(e)
            raise


# ---------------------------------------------------------------------------
# Tool span
# ---------------------------------------------------------------------------


class ToolSpan:
    """Handle returned by ``tool_span`` — call ``record_*`` after execution."""

    def __init__(self, span: Any) -> None:
        self._span = span

    def record_result(self, result: Any, *, is_error: bool = False) -> None:
        span = self._span
        if span is None:
            return
        # Trim to a reasonable size — full tool output is often huge.
        text = result if isinstance(result, str) else _safe_json_dumps(result)
        span.set_attribute(_ATTR_TOOL_CALL_RESULT, _trim(text, 2048))
        if is_error:
            span.set_status(Status(StatusCode.ERROR, "tool returned is_error=True"))

    def record_error(self, exc: BaseException) -> None:
        span = self._span
        if span is None:
            return
        span.set_status(Status(StatusCode.ERROR, str(exc)))
        span.record_exception(exc)


@contextmanager
def tool_span(
    *,
    name: str,
    call_id: str | None = None,
    arguments: dict[str, Any] | None = None,
    description: str | None = None,
) -> Iterator[ToolSpan]:
    """Open a span for one tool execution.

    Arguments are JSON-serialised onto the span (trimmed to 2 KB). Tool
    return values are recorded via ``ToolSpan.record_result``.
    """
    tracer = _get_tracer()
    if tracer is None:
        yield ToolSpan(span=None)
        return

    span_name = f"execute_tool {name}"
    with tracer.start_as_current_span(span_name) as span:
        span.set_attribute(_ATTR_OPERATION_NAME, "execute_tool")
        span.set_attribute(_ATTR_TOOL_NAME, name)
        if call_id:
            span.set_attribute(_ATTR_TOOL_CALL_ID, call_id)
        if description:
            span.set_attribute(_ATTR_TOOL_DESCRIPTION, description)
        if arguments is not None:
            span.set_attribute(
                _ATTR_TOOL_CALL_ARGUMENTS,
                _trim(_safe_json_dumps(arguments), 2048),
            )
        handle = ToolSpan(span=span)
        try:
            yield handle
        except BaseException as e:  # noqa: BLE001
            handle.record_error(e)
            raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)


def _trim(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"
