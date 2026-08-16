# OpenTelemetry

Koala emits OpenTelemetry spans with the [GenAI semantic conventions][sc]
for every LLM call, agent run, and tool execution. Any OTLP-speaking
backend (Jaeger, Tempo, Datadog, Honeycomb, W&B Weave, Langfuse,
Traceloop) can display the traces without custom mapping.

[sc]: https://github.com/open-telemetry/semantic-conventions/tree/main/docs/gen-ai

OpenTelemetry is a soft-optional dependency — install the `[otel]` extra
to get real spans:

```bash
uv pip install -e ".[otel]"
```

Without the extra, every `model_span` / `agent_span` / `tool_span` call
turns into a cheap no-op. Your code stays unchanged; the traces just
don't ship.

## Set up an exporter

Koala doesn't install a TracerProvider for you — that's the application's
job. Simplest setup, printing to stdout:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
```

For an OTLP backend, swap the exporter:

```python
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://collector:4317"))
)
```

Now every Koala model call, agent run, and tool execution emits a real
span with `gen_ai.*` attributes.

## What Koala instruments

Three span types, wired into three hot paths:

### `chat <model>` — one LLM request

Emitted by:

- **`Model.astream(...)`** — direct `Model` usage.
- **`Agent.astream(...)`** — per iteration, as a child of the enclosing
  `invoke_agent` span.

Attributes:

| Attribute | Value |
|---|---|
| `gen_ai.operation.name` | `"chat"` |
| `gen_ai.system` | provider slug (`"openai"`, `"groq"`, ...) |
| `gen_ai.provider.name` | same as `system` — GenAI-semconv-current name |
| `gen_ai.request.model` | model name |
| `gen_ai.request.stream` | `true` |
| `gen_ai.request.temperature` | from ChatSettings, if set |
| `gen_ai.request.max_tokens` | from ChatSettings, if set |
| `gen_ai.request.top_p` | from ChatSettings, if set |
| `gen_ai.request.stop_sequences` | from ChatSettings, if set |
| `gen_ai.request.seed` | from ChatSettings, if set |
| `gen_ai.request.frequency_penalty` | from ChatSettings, if set |
| `gen_ai.request.presence_penalty` | from ChatSettings, if set |
| `gen_ai.response.model` | model name (echoed) |
| `gen_ai.response.finish_reasons` | `["stop"]` or `["tool_calls"]` |
| `gen_ai.usage.input_tokens` | from `UsageEvent` |
| `gen_ai.usage.output_tokens` | from `UsageEvent` |

On exception: span status set to ERROR and the exception is recorded.

### `invoke_agent <name>` — one agent run

Emitted by `Agent.astream`. Wraps the entire tool-calling loop.

Attributes:

| Attribute | Value |
|---|---|
| `gen_ai.operation.name` | `"invoke_agent"` |
| `gen_ai.agent.name` | `agent.name` |
| `gen_ai.agent.description` | `agent.instructions`, if set |
| `gen_ai.conversation.id` | `ctx.session_id` |
| `gen_ai.usage.input_tokens` | cumulative across iterations |
| `gen_ai.usage.output_tokens` | cumulative across iterations |
| `koala.agent.iterations` | number of model calls made |
| `koala.agent.stop_reason` | `"final_output"`, `"max_iterations"`, `"error"`, or `"cancelled"` |

Each `chat` span and `execute_tool` span in the loop is a child of this
one, so traces show the full agent execution tree.

### `execute_tool <name>` — one tool call

Emitted by `Agent.astream` around each `tool.run(...)`. Not emitted for
tools called directly (outside an agent).

Attributes:

| Attribute | Value |
|---|---|
| `gen_ai.operation.name` | `"execute_tool"` |
| `gen_ai.tool.name` | tool name |
| `gen_ai.tool.call.id` | model-provided call id |
| `gen_ai.tool.description` | tool description, if available |
| `gen_ai.tool.call.arguments` | JSON of arguments (trimmed to 2 KB) |
| `gen_ai.tool.call.result` | tool return value (trimmed to 2 KB) |

On tool error the span status is set to ERROR and the exception is
recorded.

## Instrumenting your own code

The three helpers are context managers with a small handle for post-hoc
recording:

```python
from koala.observability import model_span, agent_span, tool_span

with model_span(
    system="mycompany",
    model="my-model",
    settings=settings,   # ChatSettings — optional
) as span:
    reply = await client.chat(...)
    span.record_response(
        input_tokens=reply.usage.input_tokens,
        output_tokens=reply.usage.output_tokens,
        finish_reason="stop",
    )

with agent_span(name="planner", description="planning specialist",
                conversation_id="conv-1") as span:
    result = run_the_agent()
    span.record_completion(
        iterations=result.iterations,
        input_tokens=result.usage.input_tokens,
        output_tokens=result.usage.output_tokens,
        stop_reason=result.stop_reason,
    )

with tool_span(name="fetch", call_id="c1", arguments={"url": "..."}) as span:
    out = fetch(...)
    span.record_result(out, is_error=False)
```

Every handle exposes `record_error(exc)` for explicit error tagging. If
an exception propagates out of the `with` block, the handle records it
automatically.

## Checking availability

```python
from koala.observability import is_available

if is_available():
    print("OTel API is installed; spans will be emitted.")
else:
    print("OTel not installed — spans degrade to no-ops.")
```

## Non-OpenAI wire attribute mapping

`UniversalProvider` is the only shipped provider today, so `gen_ai.system`
matches the registry slug. If you wire a custom `BaseProvider` for an
Anthropic-native or Gemini-native wire, pass the slug the framework
should record via `Model(provider="anthropic", ...)` — the span picks it up
automatically.

## Reference

- `koala.observability.is_available` — checks if OTel API is importable.
- `koala.observability.model_span(...) -> ModelSpan`.
- `koala.observability.agent_span(...) -> AgentSpan`.
- `koala.observability.tool_span(...) -> ToolSpan`.

See the [API reference for `koala.observability`](../reference/observability.md).
