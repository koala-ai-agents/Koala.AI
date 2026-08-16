# Runnable + events

The single most important idea in Koala: **the event stream is the ground
truth**. Model outputs, tool calls, HITL prompts, usage snapshots, and
errors all flow through one typed union that every layer speaks.

## The `Runnable` Protocol

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class Runnable(Protocol[InputT, OutputT]):
    def astream(
        self, ctx: RunContext, input: InputT, /
    ) -> AsyncIterator[Event]:
        ...
```

That's it. One method. Any class with a method named `astream` matching
this signature *is* a `Runnable`. No inheritance, no registration, no
metaclass. `runtime_checkable` means `isinstance(x, Runnable)` works
for structural dispatch.

## The contract

Every `Runnable.astream` MUST:

1. Emit **`Start`** first.
2. Emit **`Done`** last.
3. If a final value is produced, emit **`Output(value=...)`** before `Done`.
4. If a fatal **`Error`** is emitted, `Done` must follow immediately and
   nothing else.

Everything in between is up to the implementation. A `Model` streams
`ModelDelta` chunks, a `ModelMessage` on completion, and `UsageEvent`s.
A `Tool` typically just emits `Output` + `Done` (or `Error` + `Done`).
An `Agent` interleaves the two.

## The `Event` union

```python
Event = Union[
    Start,            # kind="start"       run_id, name, input
    ModelDelta,       # kind="model_delta" text (visible content)
    ThinkingDelta,    # kind="thinking_delta" text (reasoning tokens)
    ModelMessage,     # kind="model_message" complete Message
    ToolCall,         # kind="tool_call"   ToolCallBlock
    ToolResult,       # kind="tool_result" ToolResultBlock
    AwaitingApproval, # kind="awaiting_approval" call, request_id, reason
    UsageEvent,       # kind="usage"       cumulative Usage snapshot
    Output,           # kind="output"      final value
    Error,            # kind="error"       error, fatal, exc_type
    Done,             # kind="done"        run_id
]
```

Every event is a frozen dataclass with `slots=True`, so they are cheap
and safe to pass around. The `kind` field is a `Literal[...]` string so
type checkers can narrow via `event.kind` too — pick whichever style
suits.

### `ModelDelta` vs `ThinkingDelta`

Reasoning models (DeepSeek-R1, Qwen QwQ, OpenAI o-series with reasoning
output) stream *two* logically separate content channels: their chain-of-
thought reasoning and their user-visible answer. Koala separates them:

- **`ModelDelta`** — visible content the model wants shown to the user.
- **`ThinkingDelta`** — reasoning tokens. Non-reasoning models never emit
  this event, so consumers can subscribe unconditionally.

## Consuming a Runnable

The idiomatic pattern:

```python
async for event in runnable.astream(ctx, input):
    match event:
        case ModelDelta(text=t):
            print(t, end="", flush=True)
        case ToolCall(call=c):
            log.info("Calling %s", c.name)
        case Output(value=v):
            result = v
        case Done():
            break
```

Or use the free consumer helpers when you don't care about intermediate
events:

```python
from koala.core import ainvoke, invoke, acollect

# Reduce to just the final Output value
result = await ainvoke(agent, ctx, "hello")

# Same, sync (wraps asyncio.run)
result = invoke(agent, ctx, "hello")

# Get the full event history
events = await acollect(agent, ctx, "hello")
```

## `RunContext` — the run-scoped bag

Every `astream` call takes a `RunContext` as its first argument. It's a
generic dataclass holding:

- **`deps`** — user-provided dependencies (a DB pool, a client, whatever).
  Tools that take a `ctx: RunContext` parameter reach into `deps` here.
- **`usage`** — a `Usage` accumulator. Models add to this as they see
  usage from the provider; the Agent surfaces the total on `RunResult`.
- **`session_id`** — for memory / checkpoint keying. Auto-generated if
  not set.
- **`cancel`** — a `CancelToken`. Set `cancel.cancel()` from anywhere;
  the Agent's tool loop observes this between iterations and exits
  cleanly with `stop_reason="cancelled"`.
- **`metadata`** — a plain `dict[str, Any]` for arbitrary tags. Child
  contexts get a copy.
- **`approval_resolver`** — set by `AgentSession` when a session is open.
  Turns approval-chain `"ask"` outcomes into real HITL round-trips.

`ctx.child()` forks a context sharing `session_id`, `cancel`, and
`usage` but with its own `metadata` copy — useful for sub-runs and
handoffs.

## The `Channel` Protocol

For long-running conversations, `Runnable` is one-shot: input goes in,
events come out, done. `Channel[I, O]` closes the loop:

```python
@runtime_checkable
class Channel(Protocol[InputT, OutputT]):
    async def send(self, input: InputT) -> None: ...
    def events(self) -> AsyncIterator[Event]: ...
    async def close(self) -> None: ...
```

`AgentSession` implements this: you can `send()` new user input at any
time while continuing to consume `events()`. That's what makes real HITL
possible — see [Sessions](../guide/sessions.md).

## Why this matters

Once every layer speaks the same `Event` stream, you get several things
for free:

- **Uniform composition**: A Flow step can be an Agent, a Model, or a
  Tool, because they all emit the same events. `LocalExecutor` doesn't
  care which.
- **Uniform streaming**: `show(agent, ...)`, `show(model, ...)`,
  `show(session.events())` — one renderer, one contract.
- **Uniform observability**: `OpenTelemetry` spans wrap the same
  event-producing methods regardless of layer.
- **Uniform testing**: You can `acollect()` the events from any layer
  and assert on them. That's exactly what the internal test suite does.

## Exceptions become events

Fatal errors don't propagate out of an `astream` iterator — they're
serialized into `Error(fatal=True)` followed by `Done`. That means
consumers don't need `try/except` around the iterator to detect failure;
they just watch the event stream.

`Model.chat` is the one exception: it's a convenience non-streaming API
that returns a `Message` directly. Use `Model.stream(...)` or
`Model.astream(ctx, ...)` if you want events.

See the [API reference for `koala.core`](../reference/core.md).
