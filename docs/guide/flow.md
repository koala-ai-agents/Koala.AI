# Flow orchestration

A `Flow` is a directed acyclic graph of steps. Each step's action can be
an `Agent`, `BaseTool`, `Model`, a plain Python callable, or anything
satisfying the L1 `Runnable` protocol. Steps run concurrently when their
dependencies are satisfied.

Two executors ship: `LocalExecutor` (in-process, async) and `AirflowExecutor`
(generates a real Airflow DAG — see [Airflow guide](airflow.md)).

## Building a Flow

```python
from koala import Agent, tool
from koala.orchestration import flow, LocalExecutor


@tool
def word_count(text: str) -> int:
    """Count whitespace-separated tokens."""
    return len(text.split())


researcher = Agent("groq/llama-3.3-70b-versatile", name="researcher")
writer = Agent("groq/llama-3.3-70b-versatile", name="writer")

pipeline = (
    flow("research-and-write")
    .step("research", researcher, input="$input.topic")
    .step("tweet", writer, input="$result.research")
    .step("length", word_count, text="$result.tweet")
    .edge("research", "tweet")
    .edge("tweet", "length")
    .build()
)

results = LocalExecutor().run(pipeline, input={"topic": "koalas"})
print(results["tweet"], results["length"])
```

## The fluent builder

```python
flow(id, version=None)
    .step(id, action, *, timeout=None, retries=0, **kwargs)
    .edge(from_id, to_id)
    .build()
```

- **`step()`** — add a node. Any extra kwargs become the step's `args`
  dict, passed to the action at execution time. Duplicate step ids raise
  `FlowError` immediately.
- **`edge()`** — add a `from -> to` dependency. Endpoints must be
  registered steps; self-loops raise. Cycles are detected on `build()`.
- **`build()`** — validate and freeze the graph. Returns a `Flow`.

## Step action types

`LocalExecutor` dispatches on the action's runtime type:

| Action | How it runs |
|---|---|
| `str` | Looked up in `LocalExecutor(registry=...)`; called with resolved args. |
| `BaseAgent` (Agent) | `await action.arun(prompt, ctx=ctx.child())` — pulls the prompt from args (`prompt`, `input`, or the single arg). |
| `BaseTool` (`@tool` result) | `await action.run(ctx, args)`. |
| `Model` | `await action.chat(messages)` — messages come from `args["messages"]` or built from `args["prompt"]`. |
| Custom `Runnable` | `await ainvoke(action, ctx, args)`. |
| Async function | `await action(**args)`. |
| Sync function | `await asyncio.to_thread(lambda: action(**args))`. |

Concrete types are checked before the generic `Runnable`/`callable`
fallbacks because they also structurally satisfy those.

## Reference substitution

Step args can contain string references that get substituted at run time:

- **`"$input.<key>"`** — pull from the top-level `input` dict passed to
  `executor.run(flow, input={...})`.
- **`"$result.<step_id>"`** — the full result of a prior step.
- **`"$result.<step_id>.<field>"`** — dot-drill into dict keys or Pydantic
  attributes. Works with structured outputs from Agents.

```python
pipeline = (
    flow("triage")
    .step("classify", classifier, text="$input.message")
    .step("route", router,
          category="$result.classify.category",   # dot-drill
          priority="$result.classify.priority")
    .edge("classify", "route")
    .build()
)
```

## Concurrent execution

Independent branches run concurrently by default. Steps with the same
dependency-satisfied point get scheduled together via `asyncio.wait`:

```python
pipeline = (
    flow("parallel-lookups")
    .step("weather", get_weather, city="$input.city")
    .step("news",    get_news,    topic="$input.topic")
    .step("stocks",  get_stocks,  ticker="$input.ticker")
    .step("summarize", summarizer,
          w="$result.weather", n="$result.news", s="$result.stocks")
    .edge("weather", "summarize")
    .edge("news",    "summarize")
    .edge("stocks",  "summarize")
    .build()
)
```

`weather`, `news`, and `stocks` fire in parallel; `summarize` waits for
all three.

## Executor internals

`LocalExecutor.arun`:

1. Builds the step map, in-edges, and out-edges.
2. Detects cycles (raises `FlowError`).
3. Schedules all root steps (in-degree 0) as tasks.
4. Loops: `asyncio.wait(FIRST_COMPLETED)`, decrement dependents, schedule
   any step whose in-degree hit 0.
5. On any step failure: cancel every running task, gather, then raise
   `StepExecutionError(step_id, original)`.

Method signatures:

```python
LocalExecutor.run(flow, *, deps=None, input=None) -> dict[str, Any]        # sync wrapper
LocalExecutor.arun(flow, *, deps=None, input=None, ctx=None) -> dict[str, Any]
```

Sync `.run()` uses `asyncio.run()`, so it can't be called from inside a
running event loop.

## Per-step timeout and retries

The `Step` dataclass carries `timeout` and `retries` fields. When you
deploy the flow via `AirflowExecutor`, they become Airflow's
`execution_timeout` and `retries` on the generated task.

## Errors

```python
from koala.orchestration import FlowError, StepExecutionError

try:
    results = LocalExecutor().run(pipeline)
except StepExecutionError as e:
    print(f"Step {e.step_id!r} failed: {e.__cause__!r}")
except FlowError as e:
    # cycle, missing edge endpoint, no roots, arg-ref lookup failure, etc.
    ...
```

## Introspection

```python
flow = pipeline
flow.id            # "research-and-write"
flow.steps         # list[Step]
flow.edges         # list[tuple[str, str]]
flow.version       # str, defaults to "0.1.0"

for step in flow.steps:
    print(step.id, type(step.action).__name__, step.args)
```

## Reference

- `koala.orchestration.flow` — builder entry point.
- `koala.orchestration.Flow`, `Step`, `FlowBuilder`, `StepAction`.
- `koala.orchestration.LocalExecutor`, `AirflowExecutor`.
- `koala.orchestration.FlowError`, `StepExecutionError`.

See the [API reference for `koala.orchestration`](../reference/orchestration.md).
