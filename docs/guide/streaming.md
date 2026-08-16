# Streaming (`show` / `ashow`)

`show` and `ashow` render any Koala thing to stdout — as simple as `print()`.
They auto-detect what you hand them and do the right thing.

## The two entry points

- **`show(target, input=None, *, deps=None, end="\n")`** — sync. Wraps
  `asyncio.run()`. Must NOT be called from inside a running event loop.
- **`ashow(target, input=None, *, deps=None, end="\n")`** — async. Same
  contract; call from `async def`.

Both are re-exported at the top level:

```python
from koala import show, ashow
```

They're also available as methods on `Agent`, `Model`, and `AgentSession`:

```python
agent.show("hello")
await agent.ashow("hello")

model.show("hello")
await model.ashow("hello")

await session.ashow()   # drain the session's event stream
```

## What can be shown

| Target | What `show` does |
|---|---|
| `Agent` / `BaseAgent` | Stream deltas, tool calls, results, errors. Returns the final output. |
| `Model` | Stream a completion; `input` is a prompt string or `list[Message]`. Returns concatenated text. |
| `BaseTool` | Run once with `input` as the arguments dict; print + return the result. |
| Async iterator of events | Consume until `Done`. Used by `session.ashow()`. |
| Custom `Runnable` | Call `astream(ctx, input)`; print events. |
| `RunResult` | Print `.output`; return the `RunResult`. |
| `Message` | Print `.text`. |
| Plain callable | Call `target(input)` (or `target()` if input is None); print; return. |
| Anything else | `print(target)` fallback. |

## Output rendering

The state-machine renderer prefixes reasoning tokens with `[reason]`,
tool calls with `[tool]`, tool errors with `!`, approval prompts with
`[approval needed]`, and general errors with `[error]`:

```
Let me think about this…
[reason] The user is asking about arithmetic. I should call the add tool.
[tool] add({"a": 7, "b": 5})
  -> 12
The sum of 7 and 5 is 12.
```

The `[reason]` marker appears **once per reasoning block**, not on every
token — the renderer tracks the current mode (`text` / `thinking` / block)
and only inserts markers on transitions. Non-reasoning models never emit
`ThinkingDelta`, so the marker never appears for them.

## Examples

### Agent

```python
from koala import Agent, tool

@tool
def add(a: int, b: int) -> int:
    """Add."""
    return a + b

agent = Agent("groq/llama-3.3-70b-versatile", tools=[add])
agent.show("What is 7 + 5?")
```

### Model directly

```python
from koala import Model

m = Model("groq/llama-3.3-70b-versatile")
m.show("Write a haiku about koalas.")
```

Or with pre-built messages:

```python
from koala.core import Message

m.show([Message.system("Be terse."), Message.user("Hi.")])
```

### Tool

```python
@tool
def multiply(a: int, b: int) -> int:
    """Multiply."""
    return a * b

show(multiply, {"a": 4, "b": 5})
# 20
```

### RunResult

```python
r = agent.run("hi")
show(r)         # prints r.output
```

### Session

```python
async with agent.session() as s:
    await s.send("hi")
    await s.ashow()   # drains events until Done
```

## Non-streaming models

If the provider doesn't stream (or the model didn't emit `ModelDelta`s
during a run — e.g. it returned the whole message in one shot), `show`
falls back to printing the assembled text once when it lands. You never
get silent output.

## Error handling

Errors are printed inline, never raised. A stream that fails looks like:

```
Working on it…
[error] BadRequestError: prompt too long
```

The function returns `None` after an error.

## Custom rendering

For a bespoke UI (Textual TUI, web frontend, tail-following file),
skip `show` and consume `astream` / `events()` yourself. The event
vocabulary is stable and typed — pattern-match with `isinstance` or
dispatch on `event.kind`.

## Reference

See `koala.ui.show`, `koala.ui.ashow`, and `koala.ui.show._render_events`
in [`koala.ui` API reference](../reference/ui.md).
