# Sessions

`AgentSession` is Koala's bidirectional, streaming, HITL-capable channel.
It wraps an `Agent` so you can:

- Push new user input at any time via `send()`.
- Consume events continuously via `events()`.
- Reply to approval prompts via `reply_approval()`.
- Cancel the current turn via `cancel()`.
- Persist state via a `Checkpointer` and resume across process restarts.

It implements L1's `Channel[str | list[Message], Event]` protocol.

## Basic

```python
from koala import Agent
from koala.core import Done, ModelDelta


agent = Agent("openai/gpt-4o-mini")

async def main():
    async with agent.session(session_id="chat-42") as s:
        await s.send("hello")
        async for event in s.events():
            match event:
                case ModelDelta(text=t):
                    print(t, end="", flush=True)
                case Done():
                    break

        await s.send("what did I just say?")
        async for event in s.events():
            match event:
                case ModelDelta(text=t):
                    print(t, end="", flush=True)
                case Done():
                    break
```

## The session lifecycle

Inside `async with agent.session(...) as s:`:

1. A background worker task starts. It reads from `s._input_queue` and
   runs `agent.astream(ctx, effective_input)` for each pushed input.
2. Every event the agent emits is forwarded onto `s._event_queue`, which
   is what `s.events()` reads from.
3. HITL approvals hit `s._resolve_approval` — the session creates a
   `Future`, awaits it, and returns whatever the outside answered.
4. Between turns, the session refreshes `ctx.cancel` so a fresh token
   is in play for the next turn.
5. On `__aexit__` (or explicit `s.close()`), the worker is cancelled,
   pending approval futures are drained with a safe `"deny"`, and the
   event stream ends.

## `agent.session(...)` — all args

```python
agent.session(
    session_id: str | None = None,
    deps: Any = None,
    approval_timeout: float = 300.0,
    checkpointer: Checkpointer | None = None,
)
```

- `session_id` — auto-generated UUID4 if omitted.
- `deps` — stored on `RunContext.deps`; reachable from tools that take a
  `ctx: RunContext` parameter.
- `approval_timeout` — seconds to wait for a `reply_approval` before the
  resolver raises `ResolverTimeoutError`.
- `checkpointer` — optional durable state store. See [Checkpointing](checkpointing.md).

## Reading events

`s.events()` is an async iterator. It yields every event from every turn
until the session closes (`None` sentinel on the internal queue).
Terminal-turn markers: watch for `Done()` to know the current turn
ended, and the next `send()` starts a fresh one.

```python
async for event in s.events():
    if isinstance(event, Done):
        # end of THIS turn — loop can continue for the next send
        break
```

## Sending input

Non-blocking. Input is queued; the worker picks it up when the current
turn finishes.

```python
await s.send("first")             # runs turn 1
await s.send("second")            # queued; runs turn 2 after turn 1's Done
await s.send([Message.user("x")]) # list[Message] is also accepted
```

Sending after `close()` raises `SessionClosedError`.

## Interrupting a turn

`s.cancel()` sets the shared cancel token. The agent observes it at the
top of its next iteration and emits `Error(error="Run cancelled",
fatal=True) + Done`. The worker then continues to the next queued input.

```python
async with agent.session() as s:
    await s.send("a very long task")
    # ... some time later ...
    s.cancel()      # this turn stops at the next loop iteration
    await s.send("try something else")     # this one runs from a fresh cancel token
```

## HITL — approvals

```python
from koala.core import AwaitingApproval, Done
from koala.tools import RequireApprovalFor

agent = Agent(
    "...",
    tools=[dangerous_tool],
    approval_rules=[RequireApprovalFor(names=frozenset({"dangerous_tool"}))],
)

async with agent.session(approval_timeout=60.0) as s:
    await s.send("do the dangerous thing")
    async for e in s.events():
        match e:
            case AwaitingApproval(call=c, request_id=r):
                await s.reply_approval(r, "allow")   # or "deny"
            case Done():
                break
```

`s.pending_approvals` gives you the current live list — useful for
building a UI that shows outstanding requests:

```python
for p in s.pending_approvals:
    print(f"{p.tool_call.name}({p.tool_call.arguments}) — {p.reason}")
```

See the [Approval + HITL guide](approval-hitl.md) for the full rule chain.

## Access to `RunContext`

`s.context` exposes the shared `RunContext` for the session — handy if
you need to inspect / mutate `deps`, `usage`, or `metadata` from outside
the agent:

```python
async with agent.session(deps={"pool": pool}) as s:
    ctx = s.context
    ctx.metadata["trace_id"] = "abc123"
    await s.send("hi")
```

## Introspection

```python
s.session_id      # str
s.closed          # bool
s.context         # RunContext
s.checkpointer    # Checkpointer | None
s.pending_approvals   # list[PendingApproval]
```

## Streaming to stdout

The session has an `ashow()` convenience — same as
`await koala.ashow(session.events())` but discoverable as a method:

```python
async with agent.session() as s:
    await s.send("hi")
    await s.ashow()   # prints deltas + tool calls + errors, returns final output
```

See the [Streaming guide](streaming.md).

## Durability

Wire a `Checkpointer` to survive process restarts:

```python
from koala.harness import SQLiteCheckpointer

cp = SQLiteCheckpointer("checkpoints.db")

async with agent.session(session_id="chat-42", checkpointer=cp) as s:
    await s.send("hello")
    # ... crash ...

# Later, in a new process:
resumed = await AgentSession.resume(agent, "chat-42", cp)
# resumed.pending_approvals reflects the last saved state
async with resumed as s:
    await s.send("we were talking about...")
```

See [Checkpointing](checkpointing.md).

## Reference

- `koala.harness.AgentSession` — main class.
- `koala.harness.SessionError`, `SessionClosedError`, `ResolverTimeoutError`.

See the [API reference for `koala.harness`](../reference/harness.md).
