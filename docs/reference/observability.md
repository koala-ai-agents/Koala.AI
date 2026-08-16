# `koala.observability`

OpenTelemetry emitter with GenAI semantic conventions. See the
[Observability guide](../guide/observability.md).

Requires the `[otel]` extra for real span export; degrades to no-ops otherwise.

## Public entry points

::: koala.observability.otel.is_available
::: koala.observability.otel.model_span
::: koala.observability.otel.agent_span
::: koala.observability.otel.tool_span

## Handle types

Returned by the three context managers to attach post-hoc data (response
tokens, finish reason, tool result, errors).

::: koala.observability.otel.ModelSpan
::: koala.observability.otel.AgentSpan
::: koala.observability.otel.ToolSpan
