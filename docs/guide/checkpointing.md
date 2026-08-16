# Checkpointing

A `Checkpointer` snapshots the state of an `AgentSession` so it can
survive process restarts. Koala persists two things per session:

1. The full conversation history (`messages`).
2. Any pending HITL approvals waiting for a human reply.

## Scope

This is **turn-boundary** checkpointing, not mid-turn. Koala saves after
every terminal event (`Done`). If a process dies during a running turn,
the previous successful turn's messages plus any pending approvals from
that turn are preserved; the interrupted turn itself is not resumable.

That's enough for the common case: a user asks something, an agent
starts working, hits an approval prompt, the process restarts, another
process picks up the session and can still see the pending approvals to
render in the UI.

## Two backends

### `InMemoryCheckpointer`

Dict-backed. Not durable across processes. Useful for tests, ephemeral
services, or as a template for custom backends.

```python
from koala.harness import InMemoryCheckpointer

cp = InMemoryCheckpointer()
```

### `SQLiteCheckpointer`

Stdlib `sqlite3` in `asyncio.to_thread`. Single row per session — the
whole checkpoint (messages + pending approvals + metadata) is JSON-encoded
into one BLOB column.

```python
from koala.harness import SQLiteCheckpointer

cp = SQLiteCheckpointer("checkpoints.db")
# ":memory:" for a per-process in-memory DB (useful in tests)
```

Schema:

```sql
CREATE TABLE IF NOT EXISTS checkpoints (
    session_id TEXT PRIMARY KEY,
    payload    TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
```

`created_at` is preserved across saves; `updated_at` refreshes each write.

## Wiring into a session

```python
from koala import Agent
from koala.harness import SQLiteCheckpointer


agent = Agent("openai/gpt-4o-mini")
cp = SQLiteCheckpointer("checkpoints.db")

async with agent.session(session_id="conv-42", checkpointer=cp) as s:
    await s.send("hello")
    async for e in s.events():
        from koala.core import Done
        if isinstance(e, Done):
            break
```

Behavior:

- On every `Done` event (terminal for a turn), the session runs
  `_save_checkpoint()`:
  - If `agent.memory` is set, its messages become the checkpoint's
    `messages`.
  - `session.pending_approvals` become the checkpoint's `pending_approvals`.
  - The record is atomically written via `checkpointer.save`.

The checkpoint reflects the observable state at the moment the turn
finished — mid-turn state (partial tool executions, in-flight approvals
that haven't yet emitted `AwaitingApproval`) is not captured.

## Resuming

```python
from koala.harness import AgentSession

resumed = await AgentSession.resume(
    agent,
    session_id="conv-42",
    checkpointer=cp,
)
```

`resume` is an async classmethod that:

1. Loads the checkpoint (`KeyError` on miss).
2. Constructs a new `AgentSession` bound to the same session id and
   checkpointer.
3. Repopulates `session.pending_approvals` from the checkpoint.
4. Returns the session — **not yet entered**. Use `async with resumed as s:`.

Conversation history is read from `agent.memory` on the next turn, same
as any other session. So the recommended pattern is: use the same
`SQLiteMemory` file (or shared memory backend) for both the original
and resumed sessions.

## Full resume example

```python
from koala import Agent
from koala.core import AwaitingApproval, Done
from koala.harness import AgentSession, SQLiteCheckpointer
from koala.memory import SQLiteMemory
from koala.tools import RequireApprovalFor


agent = Agent(
    "openai/gpt-4o-mini",
    tools=[dangerous_tool],
    approval_rules=[RequireApprovalFor(names=frozenset({"dangerous_tool"}))],
    memory=SQLiteMemory("history.db"),
)
cp = SQLiteCheckpointer("checkpoints.db")


# --- process A ---
async def start_session():
    async with agent.session(session_id="conv-1", checkpointer=cp) as s:
        await s.send("do the dangerous thing")
        async for e in s.events():
            if isinstance(e, AwaitingApproval):
                # process crashes here — the checkpoint captured this pending approval
                return
            if isinstance(e, Done):
                break


# --- process B (later) ---
async def resume_session():
    resumed = await AgentSession.resume(agent, "conv-1", cp)
    # Show the pending approvals to a human
    for p in resumed.pending_approvals:
        print(f"pending: {p.tool_call.name}({p.tool_call.arguments}) — {p.reason}")
    # ...user responds, and now we can continue the conversation
    async with resumed as s:
        await s.send("Actually, forget it — do something safe.")
        async for e in s.events():
            if isinstance(e, Done):
                break
```

## Checkpoint contents

```python
@dataclass
class Checkpoint:
    session_id: str
    messages: list[Message]                    # full conversation
    pending_approvals: list[PendingApproval]   # HITL waiting for reply
    metadata: dict[str, Any]                   # user-defined passthrough
    created_at: float                          # first save timestamp
    updated_at: float                          # last save timestamp


@dataclass(frozen=True)
class PendingApproval:
    request_id: str
    tool_call: ToolCallBlock
    reason: str = ""
    created_at: float
```

The serialization reuses `SQLiteMemory`'s content-block round-trip
helpers, so every message variant (Text, Thinking, Image, ToolCall,
ToolResult) preserves losslessly.

## Managing checkpoints

```python
# Explicit save from outside a session
await cp.save(Checkpoint(session_id="s-1", ...))

# Load
c = await cp.load("s-1")   # None if unknown

# Delete
await cp.delete("s-1")     # idempotent

# List all
ids = await cp.sessions()

# Release the SQLite connection
await cp.close()
```

## Custom backends — implement the Protocol

```python
from koala.harness import Checkpoint, Checkpointer


class RedisCheckpointer:
    """Runtime-checkable Protocol satisfaction — no inheritance needed."""

    def __init__(self, redis_client):
        self._r = redis_client

    async def save(self, checkpoint: Checkpoint) -> None:
        ...

    async def load(self, session_id: str) -> Checkpoint | None:
        ...

    async def delete(self, session_id: str) -> None:
        ...

    async def sessions(self) -> list[str]:
        ...

    async def close(self) -> None:
        ...


assert isinstance(RedisCheckpointer(client), Checkpointer)  # True
```

## Comparison with memory

- **Memory** (`BaseMemory`) — messages only. Every completed turn.
  Backs conversation continuity across `arun` / `send` calls.
- **Checkpointer** (`Checkpointer`) — messages plus pending approvals
  plus metadata plus timestamps. One record per session. Backs process-
  restart resume.

Both can be used at the same time. Memory is where the agent's tool loop
loads history from; the checkpointer is a separate durability layer
for `AgentSession` state.

## Reference

- `koala.harness.Checkpoint` — the record.
- `koala.harness.PendingApproval` — held-approval snapshot.
- `koala.harness.Checkpointer` — Protocol.
- `koala.harness.InMemoryCheckpointer` — dict-backed.
- `koala.harness.SQLiteCheckpointer` — durable.

See the [API reference for `koala.harness`](../reference/harness.md).
