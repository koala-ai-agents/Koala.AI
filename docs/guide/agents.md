# Agents

`Agent` is the standard tool-calling loop. It composes a `Model` + tools +
instructions + optional constraints (approval rules, output type, handoffs,
memory, behaviors) and runs the classical loop:

1. Send messages + tool schemas to the model.
2. If the model returns tool calls, execute each and append the results.
3. Loop until the model returns a plain answer or `max_iterations` hits.
4. Optionally validate the final text against a Pydantic `output_type`.

## Constructing an agent

```python
from koala import Agent, tool


@tool
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


agent = Agent(
    model="groq/llama-3.3-70b-versatile",   # str or Model
    instructions="You are a precise assistant.",
    tools=[add],
    max_iterations=20,      # cap on tool-call loops
    name="calculator",      # public name; shows up in Start events
)
```

Every constructor argument is keyword-only after `model`.

## The three entry points

### Sync — `agent.run(...)`

```python
result = agent.run("What is 7 plus 5?")
```

Wraps `asyncio.run()`. Must not be called from inside a running event loop.

### Async — `await agent.arun(...)`

```python
result = await agent.arun(
    "hello",
    deps={"pool": db_pool},        # stored on RunContext.deps
    session_id="conv-42",          # memory / checkpoint key
)
```

### Streaming — `agent.astream(ctx, input)`

Yields events as they happen. See [Streaming](streaming.md) for `show()` /
`ashow()` which wrap this for you.

```python
from koala.core import RunContext, ModelDelta, ToolCall, Output, Done

ctx = RunContext(deps=None)
async for event in agent.astream(ctx, "hi"):
    match event:
        case ModelDelta(text=t):
            print(t, end="", flush=True)
        case ToolCall(call=c):
            print(f"\n[calling {c.name}]")
        case Output(value=v):
            final = v
        case Done():
            break
```

## `RunResult`

`run()` and `arun()` return a `RunResult` dataclass:

```python
@dataclass
class RunResult:
    output: Any                 # str, or Pydantic instance if output_type set
    messages: list[Message]     # full conversation, ready to feed back in
    usage: Usage                # cumulative tokens across the run
    iterations: int             # number of model calls
    stop_reason: StopReason     # "final_output" | "max_iterations" | "error" | "cancelled"
    error: str | None
    metadata: dict[str, Any]
```

`stop_reason` values:

- `"final_output"` — happy path. Model returned an answer without tool calls.
- `"max_iterations"` — hit the loop cap.
- `"error"` — model or tool raised something fatal.
- `"cancelled"` — `ctx.cancel.cancel()` was called mid-run.

## Model resolution

The `model` argument accepts either a `Model` instance or a shorthand string:

```python
# String — resolves via the provider registry
Agent(model="openai/gpt-4o-mini")

# Model instance — for when you need custom settings or headers
m = Model("openai/gpt-4o-mini", temperature=0.2, max_tokens=512)
Agent(model=m)
```

## Instructions

The `instructions` arg becomes the system prompt. If a Behavior appends
more, they're joined with `\n\n`. When `output_type` is set on a model
without native structured-output support, Koala appends a JSON-schema
hint to the system prompt automatically.

## Tool loop internals

Per iteration, `Agent.astream`:

1. Emits `Start` (first iteration only).
2. Opens an OpenTelemetry `invoke_agent` span (see [Observability](observability.md)).
3. For each iteration, opens a `chat <model>` child span and calls
   `model.provider.stream_chat(...)`.
4. Forwards every model event (deltas, thinking, message, usage).
5. If the returned assistant message has no tool calls: parses / validates
   final output, emits `Output` + `Done`, records `stop_reason="final_output"`.
6. Otherwise, for each `ToolCall`:
   - Runs the [approval chain](approval-hitl.md). On `"deny"`, appends a
     denial `ToolResult`.
   - On `"allow"`, opens an `execute_tool <name>` child span, calls
     `tool.run(ctx, arguments)`, appends the `ToolResult`.
   - `ToolValidationError` and `ToolExecutionError` are caught and appended
     as error `ToolResult`s so the model can retry.
7. Loops.

Cancellation is checked at the top of every iteration — set
`ctx.cancel.cancel()` from anywhere and the loop exits after the current
model call finishes.

## Memory

Pass a `BaseMemory` instance (typically `SQLiteMemory("koala_history.db")`)
and Koala:

- Loads prior turns for `session_id` before each `arun`.
- Prepends them to the effective input.
- Persists the new turn (user input + assistant messages + tool results)
  on `stop_reason="final_output"`.

```python
from koala.memory import SQLiteMemory

agent = Agent(
    "openai/gpt-4o-mini",
    memory=SQLiteMemory("history.db"),
)
r1 = agent.run("My name is Sam.", session_id="user-1")
r2 = agent.run("What's my name?", session_id="user-1")   # "Sam"
```

See the [Memory guide](memory.md) for backends and semantics.

## Behaviors

`behaviors=[...]` composes reusable configuration slices:

```python
from koala.behaviors import Persona, ToolPack, ApprovalPolicy
from koala.tools import DenyList

agent = Agent(
    "openai/gpt-4o-mini",
    behaviors=[
        Persona("You are Sam, a helpful support agent."),
        ToolPack(lookup_order, refund, name="support-tools"),
        ApprovalPolicy(DenyList({"refund"}), name="safe-mode"),
    ],
)
```

See the [Behaviors guide](behaviors.md).

## Handoffs

`handoffs=[...]` exposes other agents as `transfer_to_<name>` tools:

```python
math = Agent("...", name="math")
writer = Agent("...", name="writer")

coordinator = Agent(
    "...",
    handoffs=[math, writer],
    instructions="Route to the right specialist.",
)
```

See [Handoffs](handoffs.md).

## Sessions

`agent.session(...)` opens a bidirectional `AgentSession` that implements
the `Channel` protocol — the right primitive for interactive UIs and HITL.

```python
async with agent.session(session_id="conv-1") as s:
    await s.send("hello")
    async for event in s.events():
        ...
```

See [Sessions](sessions.md).

## Sub-classing

`BaseAgent` is a one-method ABC — subclass it only if you want to replace
the entire tool-calling loop with your own graph, RL policy, or DSL:

```python
from koala.agents import BaseAgent
from koala.core import Start, Output, Done

class EchoAgent(BaseAgent):
    name = "echo"

    async def astream(self, ctx, input):
        yield Start(run_id=ctx.session_id, name="echo", input=input)
        yield Output(value=str(input))
        yield Done(run_id=ctx.session_id)
```

Because `BaseAgent` satisfies `Runnable`, your subclass plugs into every
`Flow`, `AgentSession`, `show`, and `ainvoke` call unchanged.

See the [API reference for `koala.agents`](../reference/agents.md).
