# Approval + HITL

Koala's approval chain lets you gate tool calls on rules — deny some,
allow some, force human review for others. Combined with `AgentSession`,
it becomes real human-in-the-loop with typed events flowing both directions.

## The chain

```python
from koala.tools import (
    DenyList,
    AllowList,
    AlwaysAsk,
    AlwaysAllow,
    RequireApprovalFor,
)

rules = [
    DenyList(frozenset({"drop_table", "rm_rf_home"})),      # never
    AllowList(frozenset({"read_file"})),                    # always OK
    RequireApprovalFor(prefixes=frozenset({"send_"})),      # human review
]

agent = Agent("openai/gpt-4o-mini", tools=[...], approval_rules=rules)
```

Rules are evaluated in order. The **first non-None decision wins**.
Everything the chain doesn't decide on defaults to `"ask"` (safe default).

## Decisions

Every rule returns one of:

- `"allow"` — proceed to execute the tool.
- `"deny"` — skip the tool; return an error `ToolResult` to the model.
- `"ask"` — needs human review. See below.
- `None` — defer; let the next rule decide.

## Built-in rules

| Rule | What it does |
|---|---|
| `DenyList(names)` | Denies calls whose `name` is in `names`; defers otherwise. |
| `AllowList(names)` | Allows calls in `names`; defers otherwise. |
| `AlwaysAsk()` | Terminal — every call needs human approval. |
| `AlwaysAllow()` | Terminal — every call auto-approved. Useful for CI/demos. |
| `RequireApprovalFor(names, prefixes)` | Forces `"ask"` for names in `names` or with any prefix in `prefixes`. |

Recommended production default:

```python
rules = [
    DenyList(frozenset({"drop_table"})),           # hard no
    RequireApprovalFor(prefixes=frozenset({"delete_", "send_", "publish_"})),
    # anything else defaults to "ask" too
]
```

If you want silent auto-approve for anything not deny-listed, add
`AlwaysAllow()` at the end:

```python
rules = [
    DenyList(frozenset({"drop_table"})),
    RequireApprovalFor(prefixes=frozenset({"delete_"})),
    AlwaysAllow(),        # rest are OK
]
```

## What `"deny"` looks like

The Agent's loop:

1. Emits `AwaitingApproval(call=..., request_id=call.id, reason=...)` event.
2. Skips execution.
3. Appends a `ToolResult(content="Tool call denied by ...", is_error=True)`
   to the message history so the model can retry with a different tool.

The model sees the denial and typically tries a different approach or
explains why it can't complete the task.

## What `"ask"` looks like — no session

Without an `AgentSession`, there's no channel to prompt through, so
`"ask"` **degrades to `"deny"`** with a reason:

```
Tool call denied: requires human approval which is not available in
this run context
```

If you need HITL, use `agent.session(...)` — that wires the resolver.

## What `"ask"` looks like — with a session

Full round-trip:

```python
from koala import Agent
from koala.core import AwaitingApproval, Done, ModelDelta
from koala.tools import RequireApprovalFor


@tool
def send_email(to: str, subject: str, body: str) -> str:
    """Send an email."""
    return "sent"


agent = Agent(
    "openai/gpt-4o-mini",
    tools=[send_email],
    approval_rules=[RequireApprovalFor(prefixes=frozenset({"send_"}))],
)


async def main():
    async with agent.session(approval_timeout=60.0) as s:
        await s.send("Send Sam an update about the meeting.")
        async for event in s.events():
            match event:
                case ModelDelta(text=t):
                    print(t, end="", flush=True)
                case AwaitingApproval(call=c, request_id=r, reason=why):
                    print(f"\n\nApprove {c.name}? {c.arguments}")
                    resp = input("[y/N] ").strip().lower()
                    await s.reply_approval(r, "allow" if resp == "y" else "deny")
                case Done():
                    break
```

## `approval_timeout` and `ResolverTimeoutError`

`Agent.session(approval_timeout=300.0)` sets how long the session waits for
a `reply_approval(...)` call before giving up.

**On timeout, the resolver raises `ResolverTimeoutError`** — it doesn't
silently return "deny". The Agent's tool loop catches it and appends a
`ToolResult` with an informative denial reason:

```
Tool call denied: approval resolver raised ResolverTimeoutError:
approval resolver for request 'req-1' timed out after 60.0s
```

This means an unattended session never silently allows a tool run — the
default is deny, and the reason is on the record.

## Custom rules

Any class with a `check(ctx, call, /)` method returning
`ApprovalDecision | None` is a valid `ApprovalRule`:

```python
from dataclasses import dataclass
from koala.core import ApprovalDecision, RunContext
from koala.core.messages import ToolCallBlock


@dataclass(frozen=True)
class BusinessHoursOnly:
    """Deny sends outside 9-17 UTC."""

    def check(self, ctx: RunContext, call: ToolCallBlock, /) -> ApprovalDecision | None:
        if not call.name.startswith("send_"):
            return None
        import datetime
        hour = datetime.datetime.utcnow().hour
        if 9 <= hour < 17:
            return None    # defer to next rule
        return "deny"
```

## What sees the approval

The agent's `RunContext.approval_resolver` is set by `AgentSession` to
its `_resolve_approval` coroutine. It:

1. Creates an `asyncio.Future` keyed by `call.id`.
2. Awaits with `asyncio.wait_for(fut, timeout=approval_timeout)`.
3. Returns the decision the caller pushed via `reply_approval(request_id, decision)`.
4. On timeout, raises `ResolverTimeoutError`.

`session.pending_approvals` exposes the live list of `PendingApproval`
snapshots so a UI can render "N pending approvals" without draining the
event stream.

## Approval + checkpointing

The `Checkpointer` persists `pending_approvals` on every terminal event.
If your process dies while an approval is outstanding, a resumed session
sees the pending entries via `resume().pending_approvals` — you can then
render them in the UI and let the user reply before the next `send()`.

See [Checkpointing](checkpointing.md).

## Reference

- `koala.core.approval` — `ApprovalRule`, `ApprovalDecision`, `ApprovalResult`.
- `koala.tools.approval` — built-in rules + `evaluate_approval_chain`.
- `koala.harness.errors.ResolverTimeoutError` — raised on approval timeout.

See the [API reference for `koala.tools`](../reference/tools.md) and
[`koala.harness`](../reference/harness.md).
