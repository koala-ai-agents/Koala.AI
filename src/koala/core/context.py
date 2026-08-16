"""Per-run execution context.

`RunContext` is the explicit alternative to implicit config threading. It
carries user-provided dependencies, cumulative usage, session identity,
cooperative cancellation, HITL resolvers, and free-form metadata through
every level of a run. Every Runnable's `astream(ctx, input)` takes a
`RunContext` as the first arg so no piece of state has to travel through
hidden globals.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from .types import Usage

if TYPE_CHECKING:
    from .approval import ApprovalDecision
    from .messages import ToolCallBlock

DepsT = TypeVar("DepsT")

# Type alias for the approval resolver wired in by the harness layer (L8).
# Uses `Callable[..., Awaitable[...]]` at runtime (via
# `collections.abc.Callable`) so we don't pull in `messages`/`approval` here;
# type checkers see the precise shape via the TYPE_CHECKING imports above.
ApprovalResolver = Callable[["ToolCallBlock", str], Awaitable["ApprovalDecision"]]
"""Called when an approval rule chain returns 'ask'. Must return
'allow' or 'deny'."""


class CancelToken:
    """Cooperative cancellation flag.

    This is a higher-level "please stop" signal than `asyncio.CancelledError`:
    setting it doesn't abort in-flight I/O, it just flips a flag that
    long-running callers should poll via `.cancelled` or `.check()` at safe
    points in their loop.
    """

    __slots__ = ("_event",)

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def cancel(self) -> None:
        """Set the cancel flag. Idempotent."""
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        """Await until cancelled. Useful for `asyncio.wait([..., cancel.wait()])`."""
        await self._event.wait()

    def check(self) -> None:
        """Raise `asyncio.CancelledError` if the flag is set. No-op otherwise."""
        if self._event.is_set():
            raise asyncio.CancelledError("Run cancelled by CancelToken")


@dataclass
class RunContext(Generic[DepsT]):
    """Explicit per-run context passed to every Runnable.

    Args:
        deps: User-provided dependencies. This is the escape hatch for wiring
            db pools, http clients, secrets, etc. into tool functions.
        usage: Cumulative usage counters. Callees accumulate into this.
        session_id: Stable id for the whole run/session; propagates to children.
        cancel: Cooperative cancellation token, shared with children.
        metadata: Free-form dict for tracing tags, tenant ids, feature flags.
        approval_resolver: Async callable that produces an ``ApprovalDecision``
            for a tool call whose approval chain resolved to ``"ask"``. When
            ``None`` (the default), agents auto-deny on ``"ask"`` because
            there's no channel to prompt through. Set by ``AgentSession``.
    """

    deps: DepsT
    usage: Usage = field(default_factory=Usage)
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    cancel: CancelToken = field(default_factory=CancelToken)
    metadata: dict[str, Any] = field(default_factory=dict)
    approval_resolver: ApprovalResolver | None = None

    def child(self, deps: Any = None) -> "RunContext[Any]":
        """Fork a child context sharing session_id, cancel token, and usage.

        The child gets its own metadata copy so per-step tags don't leak
        back into the parent. Passing `deps` replaces the child's deps;
        omitting it inherits the parent's. The approval resolver is
        inherited so nested runs (e.g. handoffs) still trigger HITL.
        """
        return RunContext(
            deps=self.deps if deps is None else deps,
            usage=self.usage,
            session_id=self.session_id,
            cancel=self.cancel,
            metadata=dict(self.metadata),
            approval_resolver=self.approval_resolver,
        )
