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
    max_iterations=20,          # cap on tool-call loops
    name="calculator",          # public name; shows up in Start events
    parallel_tools=True,        # execute multiple tool calls in parallel (default: True)
    max_tool_concurrency=10,    # maximum concurrent tool executions (default: 10)
    max_output_retries=2,       # reflection loop retries on validation failure (default: 0)
    context_policy=None,        # optional ContextPolicy for token pruning
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
3. Applies `context_policy.prune(messages)` (if configured) to ensure history fits within token budgets without breaking atomic tool turns.
4. For each iteration, opens a `chat <model>` child span and calls
   `model.provider.stream_chat(...)`.
5. Forwards every model event (deltas, thinking, message, usage).
6. If the returned assistant message has no tool calls: parses and validates
   final output against `output_type`.
   - If validation fails and `max_output_retries > 0`, appends the validation error message and loops so the model can self-correct!
   - On success, emits `Output` + `Done`, records `stop_reason="final_output"`.
7. Otherwise, evaluates tool approvals:
   - For `"deny"`, builds a denied `ToolResult`.
   - For `"allow"`, executes tools:
     - When `parallel_tools=True` (default) and multiple tools are called, runs them concurrently using `asyncio.gather` bounded by `max_tool_concurrency`.
     - When sequential, runs them in order.
   - Preserves exact call order when emitting `ToolResult` events and updating conversation history.
   - Surfaces `ModelRetry` exceptions directly to the model as retry requests.
8. Loops.

Cancellation is checked at the top of every iteration — set
`ctx.cancel.cancel()` from anywhere and the loop exits after the current
model call finishes.

## Parallel tool execution

When a model requests multiple tool invocations in a single turn (e.g. searching 3 sources or fetching multiple user records), running them sequentially incurs unnecessary latency.

`Agent` executes multiple approved tool calls concurrently by default:

```python
agent = Agent(
    "openai/gpt-4o-mini",
    tools=[fetch_user, fetch_orders, fetch_recommendations],
    parallel_tools=True,        # Enabled by default
    max_tool_concurrency=10,    # Semaphore limit to protect downstream services
)
```

- **Safety & Bounded Concurrency**: An `asyncio.Semaphore(max_tool_concurrency)` prevents rate-limit explosions on downstream APIs.
- **Deterministic Order**: Results are matched back to their exact original `tool_call_id` and emitted in the exact order requested by the model.
- **Threadpool Offloading**: Synchronous `@tool` functions are automatically offloaded to worker threads via `asyncio.to_thread`.

## Structured output reflection loop

If `output_type` is specified and the model emits a response that fails Pydantic schema validation, Koala can trigger an automated reflection loop:

```python
agent = Agent(
    "openai/gpt-4o-mini",
    output_type=UserProfile,
    max_output_retries=3,   # Allow up to 3 self-correction iterations
)
```

When validation fails, the agent generates an actionable error prompt:
```text
Your response did not match the required schema. Validation errors:
- email: value is not a valid email address
Please correct the errors and return the valid JSON object strictly matching the schema.
```
The model receives this feedback on the subsequent turn and self-corrects. See the [Structured output guide](structured-output.md) and [Retries & resilience guide](resilience.md).

## Context management & token pruning

To keep agent conversations within model context limits across extended runs, attach a `ContextPolicy`:

```python
from koala.agents import ContextPolicy

agent = Agent(
    "openai/gpt-4o-mini",
    context_policy=ContextPolicy(
        max_tokens=8000,
        keep_last_turns=4,
        max_tool_output_tokens=500,
    ),
)
```

`ContextPolicy` groups assistant calls and tool outputs into unbreakable **atomic turns**, preventing orphaned tool call errors. See the [Context management & pruning guide](context-policy.md).

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
