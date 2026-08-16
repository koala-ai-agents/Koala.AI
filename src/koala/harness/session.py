"""AgentSession — the L1 Channel implementation for HITL, streaming, multi-turn agents.

An ``AgentSession`` wraps an ``Agent`` to give you a bidirectional conversation
you can push into (``send``), consume from (``events``), and reply to
interactively (``reply_approval``). It's the concrete answer to the L1
``Channel[str, Event]`` protocol.

Usage::

    async with agent.session(session_id="user-42") as s:
        await s.send("Refactor the payment module.")
        async for event in s.events():
            match event:
                case ModelDelta(text=t):
                    print(t, end="", flush=True)
                case AwaitingApproval(call=c, request_id=r):
                    ok = input(f"\\nApprove {c.name}? y/n ")
                    await s.reply_approval(r, "allow" if ok == "y" else "deny")
                case Done():
                    break

Design notes
------------
* Turns are serialised. Each ``send()`` enqueues input; the background worker
  drains the queue and runs the agent for one turn at a time.
* Prior conversation history is loaded from the Agent's memory (if any) and
  persisted on success. The Session honours the same session_id semantics as
  ``Agent.arun``.
* Approvals are wired via a ``RunContext`` resolver, so the Agent's normal
  loop stays unchanged from the outside. On timeout the resolver raises
  ``ResolverTimeoutError``; the Agent surfaces that as a "deny" for the
  offending tool call and continues.
* ``close()`` (and the ``async with`` exit) cancels the worker, drains any
  pending resolver futures with safe defaults, and signals end-of-stream.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from ..core.approval import ApprovalDecision
from ..core.context import RunContext
from ..core.events import (
    AwaitingApproval,
    Done,
    Error,
    Event,
    ModelMessage,
    Output,
    ToolResult,
)
from ..core.messages import Message, ToolCallBlock
from .checkpoint import Checkpoint, Checkpointer, PendingApproval
from .errors import ResolverTimeoutError, SessionClosedError

if TYPE_CHECKING:
    from ..agents.agent import Agent


class AgentSession:
    """Bidirectional, streaming, HITL-capable session wrapping an Agent.

    Satisfies the L1 ``Channel[str | list[Message], Event]`` protocol.
    """

    def __init__(
        self,
        agent: "Agent",
        *,
        session_id: str | None = None,
        deps: Any = None,
        approval_timeout: float = 300.0,
        checkpointer: Checkpointer | None = None,
    ) -> None:
        self._agent = agent
        self._session_id: str = session_id or str(uuid.uuid4())
        self._deps = deps
        self._approval_timeout = approval_timeout
        self._checkpointer = checkpointer

        self._input_queue: asyncio.Queue[str | list[Message]] = asyncio.Queue()
        # None on the event queue signals end-of-stream to consumers.
        self._event_queue: asyncio.Queue[Event | None] = asyncio.Queue()

        # Pending approval requests keyed by request_id.
        self._approval_futures: dict[str, asyncio.Future[ApprovalDecision]] = {}
        # PendingApproval snapshots for durable checkpointing. Populated when
        # the worker forwards an ``AwaitingApproval`` event; cleared on
        # ``reply_approval``. Exposed on ``session.pending_approvals``.
        self._pending_approvals: dict[str, PendingApproval] = {}

        self._worker_task: asyncio.Task[None] | None = None
        self._closed = False

        # Shared RunContext for every turn on this session. The approval
        # resolver wires HITL back to the outside world; cancel is shared so
        # the caller can interrupt from the outside.
        self._ctx: RunContext[Any] = RunContext(
            deps=deps,
            session_id=self._session_id,
            approval_resolver=self._resolve_approval,
        )

    # ------------------------------------------------------------------
    # Public introspection
    # ------------------------------------------------------------------

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def context(self) -> RunContext[Any]:
        """The shared RunContext used for every turn on this session."""
        return self._ctx

    @property
    def checkpointer(self) -> Checkpointer | None:
        """The durable checkpointer, if one is configured."""
        return self._checkpointer

    @property
    def pending_approvals(self) -> list[PendingApproval]:
        """HITL approval requests that have not yet been answered.

        Populated live as the worker emits ``AwaitingApproval`` events, and
        cleared as ``reply_approval`` calls arrive. Also restored when a
        session is resumed via :meth:`resume` from a checkpoint that had
        outstanding approvals.
        """
        return list(self._pending_approvals.values())

    # ------------------------------------------------------------------
    # Channel protocol
    # ------------------------------------------------------------------

    async def send(self, input: str | list[Message]) -> None:
        """Push new user input into the session.

        Non-blocking: input is queued and the worker picks it up when the
        current turn finishes. To interrupt a running turn first, call
        ``cancel()``.
        """
        if self._closed:
            raise SessionClosedError(
                "Cannot send to a closed session."
            )
        await self._input_queue.put(input)

    async def events(self) -> AsyncIterator[Event]:
        """Consume events as they arrive.

        Yields until the session is closed. Consumers typically dispatch on
        ``event.kind`` (or ``isinstance``) and use ``Done`` to detect end
        of a single turn.
        """
        while True:
            evt = await self._event_queue.get()
            if evt is None:
                return
            yield evt

    async def close(self) -> None:
        """Shut down the session cleanly.

        Cancels the background worker, unblocks any pending approval
        resolvers with a safe "deny" default, and signals end-of-stream.
        Idempotent.
        """
        if self._closed:
            return
        self._closed = True

        # Signal end-of-stream to consumers.
        await self._event_queue.put(None)

        # Cancel the worker.
        if self._worker_task is not None and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

        # Drain pending approval futures with a safe "deny" so callers
        # awaiting them don't hang forever.
        for fut in list(self._approval_futures.values()):
            if not fut.done():
                fut.set_result("deny")
        self._approval_futures.clear()

    # ------------------------------------------------------------------
    # HITL reply APIs
    # ------------------------------------------------------------------

    async def reply_approval(
        self, request_id: str, decision: ApprovalDecision
    ) -> None:
        """Respond to an ``AwaitingApproval`` event.

        ``decision`` should be ``"allow"`` or ``"deny"``. Idempotent — a
        second reply for the same request is silently dropped.
        """
        self._pending_approvals.pop(request_id, None)
        fut = self._approval_futures.pop(request_id, None)
        if fut is not None and not fut.done():
            fut.set_result(decision)

    def cancel(self) -> None:
        """Cooperatively cancel the currently running turn.

        The agent's next loop iteration observes ``ctx.cancel.cancelled`` and
        exits with a ``cancelled`` stop reason. The worker then picks up the
        next queued input, so the session stays usable.
        """
        self._ctx.cancel.cancel()

    # ------------------------------------------------------------------
    # UI convenience — same as `await koala.ashow(session.events())` but
    # discoverable as a method on the session.
    # ------------------------------------------------------------------

    async def ashow(self, *, end: str = "\n") -> Any:
        """Consume events from this session and print them until Done.

        Meant to be called after ``send()`` for the "print the current turn"
        pattern. Returns whatever value was carried on the final ``Output``
        event, or None.
        """
        from ..ui.show import _render_events

        return await _render_events(self.events(), end=end)

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "AgentSession":
        await self._start()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal: background worker
    # ------------------------------------------------------------------

    async def _start(self) -> None:
        """Kick off the background worker task. Idempotent."""
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(
                self._worker(), name=f"AgentSession-worker[{self._session_id}]"
            )

    async def _worker(self) -> None:
        """Drain the input queue, run the agent for each turn, forward events."""
        agent = self._agent

        try:
            while not self._closed:
                # Wait for the next user input.
                new_input = await self._input_queue.get()

                # Load prior history from the agent's memory (if configured).
                prior_history: list[Message] = []
                if agent.memory is not None:
                    prior_history = await agent.memory.get(self._session_id)

                new_turn_input: list[Message] = (
                    [Message.user(new_input)]
                    if isinstance(new_input, str)
                    else list(new_input)
                )
                effective_input = prior_history + new_turn_input

                # NOTE: we do NOT reset the cancel token here — a cancel
                # requested before this turn started is honored by astream on
                # its first iteration. The token is refreshed after the turn
                # completes so subsequent turns start fresh.

                # Run the agent, forwarding every event and tracking produced
                # messages for memory persistence.
                turn_produced: list[Message] = []
                success = False
                saw_done = False
                try:
                    async for event in agent.astream(self._ctx, effective_input):
                        await self._event_queue.put(event)
                        if isinstance(event, ModelMessage):
                            turn_produced.append(event.message)
                        elif isinstance(event, ToolResult):
                            turn_produced.append(
                                Message.tool(
                                    tool_call_id=event.result.tool_call_id,
                                    content=event.result.content,
                                )
                            )
                        elif isinstance(event, AwaitingApproval):
                            # Track for durable checkpointing + external
                            # inspection via `session.pending_approvals`.
                            self._pending_approvals[event.request_id] = (
                                PendingApproval(
                                    request_id=event.request_id,
                                    tool_call=event.call,
                                    reason=event.reason,
                                )
                            )
                        elif isinstance(event, Output):
                            success = True
                        elif isinstance(event, Done):
                            saw_done = True
                except Exception as e:  # noqa: BLE001
                    await self._event_queue.put(
                        Error(
                            error=str(e),
                            exc_type=type(e).__name__,
                            fatal=True,
                        )
                    )
                    success = False

                # Persist new turn to memory on success.
                if success and agent.memory is not None:
                    await agent.memory.append(
                        self._session_id, new_turn_input + turn_produced
                    )

                # Persist a durable checkpoint after every terminal event
                # (both success and failure — the caller may want to inspect
                # a failed session's state). Only saves when a checkpointer
                # is configured.
                if self._checkpointer is not None and saw_done:
                    await self._save_checkpoint()

                # Refresh the cancel token for the NEXT turn.
                self._ctx.cancel = type(self._ctx.cancel)()
        except asyncio.CancelledError:
            # Worker cancellation is expected during close(); re-raise so
            # asyncio can clean up.
            raise

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    async def _save_checkpoint(self) -> None:
        """Snapshot current session state to the configured checkpointer.

        Captures the full conversation history from ``agent.memory`` (if
        set) plus the current pending-approval map. Callers can inspect
        the resulting :class:`Checkpoint` via :meth:`Checkpointer.load`.
        """
        assert self._checkpointer is not None
        messages: list[Message] = []
        if self._agent.memory is not None:
            messages = await self._agent.memory.get(self._session_id)

        checkpoint = Checkpoint(
            session_id=self._session_id,
            messages=messages,
            pending_approvals=list(self._pending_approvals.values()),
        )
        await self._checkpointer.save(checkpoint)

    @classmethod
    async def resume(
        cls,
        agent: "Agent",
        session_id: str,
        checkpointer: Checkpointer,
        *,
        deps: Any = None,
        approval_timeout: float = 300.0,
    ) -> "AgentSession":
        """Rebuild an :class:`AgentSession` from a stored checkpoint.

        The returned session is fully populated but NOT started — enter it
        as an async context manager (or call ``send()``) to kick off the
        worker. Any pending approvals from the checkpoint are visible via
        :attr:`pending_approvals`, and callers can respond with
        ``reply_approval`` before or during the next turn.

        Args:
            agent: The Agent to attach. If the checkpointer preserved
                conversation history in ``agent.memory`` (e.g. shared
                :class:`SQLiteMemory`), no explicit re-injection is needed.
            session_id: The session to load.
            checkpointer: Backend where the checkpoint lives.
            deps: Passed through to the new session's :class:`RunContext`.
            approval_timeout: Passed through.

        Raises:
            KeyError: If the session id is not in the checkpointer.
        """
        checkpoint = await checkpointer.load(session_id)
        if checkpoint is None:
            raise KeyError(f"No checkpoint for session_id={session_id!r}")

        session = cls(
            agent=agent,
            session_id=session_id,
            deps=deps,
            approval_timeout=approval_timeout,
            checkpointer=checkpointer,
        )
        session._pending_approvals = {
            p.request_id: p for p in checkpoint.pending_approvals
        }
        return session

    # ------------------------------------------------------------------
    # Internal: approval resolver (wired into RunContext)
    # ------------------------------------------------------------------

    async def _resolve_approval(
        self, call: ToolCallBlock, reason: str
    ) -> ApprovalDecision:
        """Create a pending Future for this approval and await the reply.

        Raises ``ResolverTimeoutError`` if no ``reply_approval(...)`` arrives
        within ``approval_timeout``. The Agent's tool loop catches this as
        a resolver exception and turns it into a "deny" for that call with
        the timeout as the deny reason, so a slow reviewer never silently
        allows a tool run.
        """
        request_id = call.id
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[ApprovalDecision] = loop.create_future()
        self._approval_futures[request_id] = fut
        try:
            return await asyncio.wait_for(fut, timeout=self._approval_timeout)
        except asyncio.TimeoutError as exc:
            self._approval_futures.pop(request_id, None)
            raise ResolverTimeoutError(
                kind="approval",
                request_id=request_id,
                timeout=self._approval_timeout,
            ) from exc
