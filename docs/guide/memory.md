# Memory

Conversation-history persistence, keyed by session id. Not a vector store,
not RAG — just "the messages from previous turns so the agent has context."

## The `BaseMemory` contract

```python
class BaseMemory(ABC):
    async def append(self, session_id: str, messages: list[Message]) -> None: ...
    async def get(self, session_id: str) -> list[Message]: ...
    async def clear(self, session_id: str) -> None: ...
    async def sessions(self) -> list[str]: ...
```

## Built-in backends

### `InMemoryMemory`

Dict-backed. Fine for tests, single-process scripts, and Jupyter.

```python
from koala.memory import InMemoryMemory

memory = InMemoryMemory()
```

### `SQLiteMemory`

Stdlib `sqlite3` wrapped in `asyncio.to_thread`. Durable across restarts.
All content-block variants (Text, Thinking, Image, ToolCall, ToolResult)
round-trip losslessly through JSON.

```python
from koala.memory import SQLiteMemory

memory = SQLiteMemory("koala_history.db")
# ":memory:" for an in-process SQLite DB (useful in tests)
```

Schema:

```sql
CREATE TABLE IF NOT EXISTS messages (
    session_id TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    payload    TEXT NOT NULL,
    PRIMARY KEY (session_id, seq)
);
```

## Wiring into an agent

```python
from koala import Agent
from koala.memory import SQLiteMemory

agent = Agent(
    "openai/gpt-4o-mini",
    memory=SQLiteMemory("history.db"),
)

r1 = agent.run("My name is Sam.", session_id="user-42")
r2 = agent.run("What's my name?", session_id="user-42")
# r2.output → "Your name is Sam."
```

What happens per `arun`:

1. Load prior history: `messages = await memory.get(session_id)`.
2. Prepend to the effective input passed to `astream`.
3. Run to completion.
4. **Only on `stop_reason="final_output"`**: append the new turn
   (user input + assistant messages + tool results) via `memory.append`.

Errored or cancelled turns are NOT persisted, so a partial turn doesn't
pollute the history.

## Wiring into a session

```python
agent = Agent("openai/gpt-4o-mini", memory=SQLiteMemory("h.db"))

async with agent.session(session_id="user-42") as s:
    await s.send("hello")
    async for e in s.events():
        if isinstance(e, Done):
            break
```

Same load/append semantics as `arun`, but per-turn — every `send()` runs
one turn against the freshly-loaded prior history.

## Session id conventions

Any string works. Common patterns:

- `"user-{user_id}"` — one continuous conversation per user.
- `"user-{user_id}:{room}"` — per-room / per-thread within a user.
- `uuid.uuid4()` — one-shot conversation, generated at request time.

Auto-generated ids from `RunContext(session_id=...)` default to a fresh
UUID4.

## Manual manipulation

```python
# Inspect what's stored
history = await memory.get("user-42")

# Nuke a session
await memory.clear("user-42")

# List every session id in the store
ids = await memory.sessions()

# Prepend a synthetic system message
from koala.core import Message
await memory.append("user-42", [Message.system("You are extra polite today.")])
```

## Custom backends

Implement `BaseMemory`. That's it. Postgres, Redis, DynamoDB, whatever —
the framework only needs the four async methods above.

```python
from koala.memory import BaseMemory
from koala.core import Message


class RedisMemory(BaseMemory):
    def __init__(self, redis_client):
        self._r = redis_client

    async def append(self, session_id, messages):
        ...

    async def get(self, session_id):
        ...

    async def clear(self, session_id):
        ...

    async def sessions(self):
        ...
```

## Scope

Memory stores completed-turn messages only. Long sessions grow
unbounded — do your own windowing / summarization before calling
`memory.append`, or subclass `SQLiteMemory` and override `get`.

For durable session state (pending HITL approvals, mid-turn context)
see [Checkpointing](checkpointing.md).

## Reference

See [`koala.memory` API reference](../reference/memory.md).
