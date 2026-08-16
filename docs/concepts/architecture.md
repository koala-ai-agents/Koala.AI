# Architecture

Koala is a layered stack. Each layer has one job and depends only on
layers below it. The whole design collapses to three ideas:

1. **`Runnable` is a Protocol, not a base class.** Anything with an
   `astream(ctx, input) -> AsyncIterator[Event]` method is a `Runnable`.
   Models, Tools, Agents, and Flows all satisfy it.
2. **The event stream is the ground truth.** Not callbacks, not return
   values. Every layer emits and consumes the same `Event` union.
3. **Bidirectional channels close the loop.** `AgentSession` implements
   `Channel[str, Event]` — you can `send()` new input mid-run, and events
   flow out continuously.

## The stack

```text
┌────────────────────────────────────────────────────────────────────┐
│  Application code                                                  │
├────────────────────────────────────────────────────────────────────┤
│  L8  koala.harness         AgentSession + Checkpointer             │
│      koala.ui              show / ashow renderer                   │
│  L7  koala.orchestration   flow, LocalExecutor, Airflow            │
│  L6  koala.agents          Agent, BaseAgent, AgentTool             │
│  L5  koala.behaviors       Persona, ToolPack, ApprovalPolicy       │
│  L4  koala.memory          InMemoryMemory, SQLiteMemory            │
│  L3  koala.tools           @tool, BaseTool, approval, MCP          │
│  L2  koala.models          Model, UniversalProvider, keys          │
│  L1  koala.core            Message, Event, Runnable, RunContext    │
├────────────────────────────────────────────────────────────────────┤
│      koala.observability   OpenTelemetry GenAI semconv             │
└────────────────────────────────────────────────────────────────────┘
```

The observability module is cross-cutting — layers L2, L3, and L6 emit
spans through it, but nothing depends on it at runtime (all OTel calls
degrade to no-ops when the SDK isn't installed).

Executors at L7 are pluggable and don't preserve identical semantics.
`LocalExecutor` runs Runnables in-process and consumes their full event
stream directly. `AirflowExecutor` compiles a Flow to a thin generated
DAG file plus a JSON spec, so anything that doesn't fit inside an Airflow
task boundary (dependency injection, the L1 event stream, HITL approvals,
OTel context) has to be bridged explicitly. Those bridges are documented
per-executor rather than in this diagram — see
[Airflow deployment](../guide/airflow.md) for the executor-specific
surface and its knobs.

## Why layered

Every layer above L1 depends only on L1 primitives plus the layer directly
below. That constraint keeps the abstractions honest:

- **L2** wraps a provider — it produces `Event`s from an HTTP round-trip.
- **L3** wraps a callable — it produces `Event`s from a Python function.
- **L4** persists messages — it uses only `Message` from L1.
- **L5** composes L2/L3 into an agent spec — no runtime code.
- **L6** ties L2 + L3 + L4 + L5 into the tool-calling loop.
- **L7** composes any `Runnable` (L2, L3, L6, or user-defined) into a DAG.
- **L8** wraps L6 in a bidirectional channel.

You can use each layer in isolation:

```python
# L2 alone — just talk to a model
model = Model("groq/llama-3.3-70b-versatile")
msg = await model.chat([Message.user("Hi")])

# L3 alone — just run a tool
@tool
def add(a: int, b: int) -> int:
    """Add."""
    return a + b
result = await add.run(RunContext(deps=None), {"a": 1, "b": 2})

# L6 combines them
agent = Agent(model, tools=[add])
```

## No hidden state

- `Model` is stateless — every call is independent.
- `Tool` is stateless — the function you decorate stays yours.
- `Agent` carries an instructions string, a tool list, and settings.
  No mutable state between runs.
- `AgentSession` carries a queue of inputs, a queue of events, and a
  pending-approval map. All state is explicit and (optionally) persisted
  via a `Checkpointer`.

If you want to break at any of these boundaries and inject a mock, you
can — because every seam is a Protocol.

## Async native

Every path is `async` first. Sync methods (`Agent.run`, `Model.chat`,
`show`, `invoke`) are thin `asyncio.run()` wrappers around the async
version, so they can't be called from inside a running event loop.

## Zero surprise dependencies

Base runtime dependencies: **`httpx`** and **`pydantic`**. Everything else
is behind an extra (`[mcp]`, `[otel]`, `[dev]`, `[docs]`). Install more
only when you need those subsystems.

See [The layer stack](layers.md) for what's in each layer, and
[Runnable + events](runnable-and-events.md) for the L1 contract in detail.
