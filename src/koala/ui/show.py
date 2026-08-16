"""``show`` / ``ashow`` — print-simple output for any Koala component.

The idea: replace the async-for boilerplate you'd otherwise write with a
single ``show(thing, input)`` call. It auto-detects whether the target
streams and does the right thing either way.

    show(agent, "hi")                    # streams live to stdout
    show(model, "hi")                    # ditto (str is wrapped as user msg)
    show(model, [Message.user("hi")])    # or pass messages directly
    show(tool, {"a": 1, "b": 2})         # runs the tool + prints result
    show(some_run_result)                # prints .output
    show(some_message)                   # prints .text
    show("plain string")                 # falls back to print()

Or from already-async code::

    await ashow(agent, "hi")
    await ashow(session.events())        # drain an async iterator

Or as a method on components::

    agent.show("hi")
    model.show("hi")
    await session.ashow()

Everything is stdout-only and best-effort. If a target doesn't stream, the
final value is printed once when it's ready. If a stream errors, the error
is printed inline — nothing is raised.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import sys
from collections.abc import AsyncIterator
from typing import Any

from ..core.context import RunContext
from ..core.events import (
    AwaitingApproval,
    Done,
    Error,
    Event,
    ModelDelta,
    ModelMessage,
    Output,
    ThinkingDelta,
    ToolCall,
    ToolResult,
)
from ..core.messages import Message
from ..core.runnable import Runnable

# ---------------------------------------------------------------------------
# Public entrypoints
# ---------------------------------------------------------------------------


def show(
    target: Any,
    input: Any = None,
    *,
    deps: Any = None,
    end: str = "\n",
) -> Any:
    """Run ``target`` and print output as it arrives. Simple as ``print()``.

    Dispatch by target type:
        - ``Agent`` / ``BaseAgent`` — stream deltas, tool calls, and results
          live. Returns the final output value.
        - ``Model`` — stream a completion; ``input`` may be a string (wrapped
          as user message) or a ``list[Message]``. Returns concatenated text.
        - ``BaseTool`` — run once with the ``input`` dict as arguments,
          print + return the result.
        - Async iterator of Events — consume and print until Done.
        - Other ``Runnable`` — call ``astream(ctx, input)`` and print events.
        - ``RunResult`` — print ``.output``, return the RunResult.
        - ``Message`` — print ``.text``.
        - Plain callable — call it (with ``input`` if provided), print, return.
        - Anything else — plain ``print(target)`` fallback.

    Args:
        target: What to invoke.
        input: The input to pass, if applicable.
        deps: Value stored on ``RunContext.deps`` for tools that need it.
        end: Trailing string, like ``print()``'s ``end``. Defaults to ``\\n``.

    Returns:
        Whatever the target produced (or ``target`` itself for value inputs).

    Note:
        Cannot be called from within a running event loop — use ``ashow``
        there instead.
    """
    return asyncio.run(ashow(target, input, deps=deps, end=end))


async def ashow(
    target: Any,
    input: Any = None,
    *,
    deps: Any = None,
    end: str = "\n",
) -> Any:
    """Async version of :func:`show`. Same semantics; call from ``async def``."""
    # Local imports keep this module import-cheap and avoid cycles.
    from ..agents.agent import BaseAgent
    from ..agents.result import RunResult
    from ..models.model import Model
    from ..tools.base import BaseTool

    # 1. Async iterator of events (e.g. session.events(), agent.astream(...))
    if _is_async_iter(target):
        return await _render_events(target, end=end)

    # 2. Concrete koala types — check BEFORE the generic Runnable Protocol
    # because Agent/Tool/Model structurally satisfy Runnable too.
    if isinstance(target, BaseAgent):
        return await _show_agent(target, input, deps=deps, end=end)

    if isinstance(target, Model):
        return await _show_model(target, input, end=end)

    if isinstance(target, BaseTool):
        return await _show_tool(target, input, deps=deps, end=end)

    # 3. Already-completed values.
    if isinstance(target, RunResult):
        _write(str(target.output))
        _write(end)
        return target

    if isinstance(target, Message):
        _write(target.text)
        _write(end)
        return target

    # 4. Generic Runnable (custom user-defined types).
    if isinstance(target, Runnable):
        ctx = RunContext(deps=deps)
        try:
            events = target.astream(ctx, input)
            return await _render_events(events, end=end)
        except Exception as e:  # noqa: BLE001 — inline the error, don't raise
            _write(f"[error] {e}")
            _write(end)
            return None

    # 5. Plain callable.
    if callable(target):
        try:
            result = target(input) if input is not None else target()
            if inspect.iscoroutine(result):
                result = await result
        except Exception as e:  # noqa: BLE001
            _write(f"[error] {e}")
            _write(end)
            return None
        _write(str(result))
        _write(end)
        return result

    # 6. Anything else — plain print.
    _write(str(target))
    _write(end)
    return target


# ---------------------------------------------------------------------------
# Type-specific renderers
# ---------------------------------------------------------------------------


async def _show_agent(agent: Any, input: Any, *, deps: Any, end: str) -> Any:
    if input is None:
        raise TypeError(
            "show(agent, ...) needs an input (string or list of Messages)."
        )
    ctx = RunContext(deps=deps)
    return await _render_events(agent.astream(ctx, input), end=end)


async def _show_model(model: Any, input: Any, *, end: str) -> Any:
    if input is None:
        raise TypeError(
            "show(model, ...) needs an input (prompt string or list of Messages)."
        )
    if isinstance(input, str):
        messages: list[Message] = [Message.user(input)]
    elif isinstance(input, list) and all(isinstance(m, Message) for m in input):
        messages = list(input)
    else:
        raise TypeError(
            "show(model, ...) input must be a string or list[Message], got "
            f"{type(input).__name__}."
        )
    return await _render_events(model.stream(messages), end=end)


async def _show_tool(tool: Any, input: Any, *, deps: Any, end: str) -> Any:
    args: dict[str, Any]
    if input is None:
        args = {}
    elif isinstance(input, dict):
        args = input
    else:
        raise TypeError(
            "show(tool, ...) input must be a dict of arguments, got "
            f"{type(input).__name__}."
        )
    ctx = RunContext(deps=deps)
    try:
        result = await tool.run(ctx, args)
    except Exception as e:  # noqa: BLE001
        _write(f"[tool error] {e}")
        _write(end)
        return None
    _write(_short(result, limit=10_000))
    _write(end)
    return result


async def _render_events(
    events: AsyncIterator[Event],
    *,
    end: str = "\n",
    stop_on_done: bool = True,
) -> Any:
    """Consume an event stream and render each event with a small state machine.

    The state machine tracks whether the current output is "text" (visible
    assistant content) or "thinking" (reasoning tokens). On transitions:

        - Entering thinking mode: prefix ``[reason] ``.
        - Leaving thinking mode: insert a newline separator so subsequent
          text starts on its own line.

    That way the ``[reason]`` marker appears once per reasoning block, not on
    every streamed token. Non-reasoning models never emit ``ThinkingDelta``,
    so the marker never shows up for them.

    Returns the final ``Output`` value if one was emitted, else ``None``.
    Stops at the first ``Done`` event when ``stop_on_done`` is True — needed
    for open-ended sources like ``AgentSession.events()``, harmless for
    finite iterators like ``agent.astream(...)``.
    """
    output: Any = None
    saw_delta = False
    saw_output = False
    delta_text: list[str] = []
    mode: str | None = None  # "text" | "thinking" | None (block-level output)

    def switch(new: str | None) -> None:
        nonlocal mode
        if mode == new:
            return
        # Close the previous streaming block cleanly with a newline.
        if mode in ("text", "thinking"):
            _write("\n")
        # Open the new block with its marker.
        if new == "thinking":
            _write("[reason] ")
        mode = new

    try:
        async for event in events:
            if isinstance(event, ModelDelta):
                switch("text")
                _write(event.text)
                delta_text.append(event.text)
                saw_delta = True
            elif isinstance(event, ThinkingDelta):
                switch("thinking")
                _write(event.text)
            elif isinstance(event, ToolCall):
                switch(None)
                _write(
                    f"[tool] {event.call.name}"
                    f"({_short_args(event.call.arguments)})\n"
                )
            elif isinstance(event, ToolResult):
                switch(None)
                marker = "!" if event.result.is_error else "->"
                _write(f"  {marker} {_short(event.result.content)}\n")
            elif isinstance(event, AwaitingApproval):
                switch(None)
                _write(
                    f"[approval needed] {event.call.name} "
                    f"(request_id={event.request_id})\n"
                )
            elif isinstance(event, Error):
                switch(None)
                _write(f"[error] {event.error}\n")
            elif isinstance(event, ModelMessage):
                # For streams that don't emit ModelDelta (non-streaming
                # provider), print the assembled text once. If we already
                # streamed deltas this is redundant — skip.
                if not saw_delta:
                    text = event.message.text
                    if text:
                        switch("text")
                        _write(text)
                        delta_text.append(text)
                        saw_delta = True
            elif isinstance(event, Output):
                output = event.value
                saw_output = True
            elif isinstance(event, Done) and stop_on_done:
                break
            # UsageEvent, Start — silent by design.
    except Exception as e:  # noqa: BLE001
        switch(None)
        _write(f"[error] {e}\n")

    # Close any open streaming block before the final newline.
    switch(None)
    # Non-streaming source with an Output but no deltas: print value once.
    if saw_output and not saw_delta and output is not None:
        _write(str(output))
    _write(end)
    # Return semantics: Output value if one was emitted (Agent path);
    # otherwise the concatenated delta text (Model.stream path); else None.
    if saw_output:
        return output
    if delta_text:
        return "".join(delta_text)
    return None


# ---------------------------------------------------------------------------
# Legacy single-event renderer (kept for potential external callers, not used
# by _render_events which is now a state machine).
# ---------------------------------------------------------------------------


def _render_event(event: Event) -> None:
    """Render one event to stdout. Deprecated internally; ``_render_events``
    now handles state transitions itself for cleaner output. Kept for
    external callers who want per-event rendering.
    """
    if isinstance(event, ModelDelta):
        _write(event.text)
    elif isinstance(event, ThinkingDelta):
        _write(f"[reason] {event.text}")
    elif isinstance(event, ToolCall):
        _write(
            f"\n[tool] {event.call.name}({_short_args(event.call.arguments)})\n"
        )
    elif isinstance(event, ToolResult):
        marker = "!" if event.result.is_error else "->"
        _write(f"  {marker} {_short(event.result.content)}\n")
    elif isinstance(event, AwaitingApproval):
        _write(
            f"\n[approval needed] {event.call.name} "
            f"(request_id={event.request_id})\n"
        )
    elif isinstance(event, Error):
        _write(f"\n[error] {event.error}\n")
    # Start, Done, ModelMessage, UsageEvent — silent


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _write(text: str) -> None:
    """Buffered write to stdout with a flush. Sink is easy to redirect."""
    sys.stdout.write(text)
    sys.stdout.flush()


def _is_async_iter(obj: Any) -> bool:
    """True for async iterators (has __aiter__). Excludes Agent/Model/Tool
    instances which are classes that happen to have astream methods."""
    return (
        hasattr(obj, "__aiter__")
        and not hasattr(obj, "astream")   # exclude our Runnable classes
    )


def _short(value: Any, limit: int = 120) -> str:
    text = value if isinstance(value, str) else str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _short_args(args: dict[str, Any]) -> str:
    if not args:
        return ""
    try:
        rendered = json.dumps(args, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        rendered = str(args)
    return _short(rendered, 80)
