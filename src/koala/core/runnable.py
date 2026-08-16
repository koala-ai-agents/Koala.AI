"""Runnable and Channel protocols, plus small consumer helpers.

`Runnable[I, O]` is a Protocol: anything that has an `astream(ctx, input)`
method producing an `AsyncIterator[Event]` satisfies it. No inheritance,
no fat base class. `runtime_checkable` means `isinstance(x, Runnable)` works
for structural dispatch.

`Channel[I, O]` is the bidirectional counterpart used by the harness:
new input can be pushed in mid-run via `send()`, and events flow out
continuously via `events()`. This is the primitive that enables
"conversation as channel, not function call" — interrupt, queue, and steer.

`ainvoke`, `invoke`, and `acollect` are free functions that consume a
Runnable's stream and reduce it, so `Runnable` itself stays minimal.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Protocol, TypeVar, runtime_checkable

from .context import RunContext
from .events import Done, Error, Event, Output

InputT = TypeVar("InputT", contravariant=True)
OutputT = TypeVar("OutputT", covariant=True)


@runtime_checkable
class Runnable(Protocol[InputT, OutputT]):
    """A one-shot runnable that emits an ordered event stream.

    Contract:
        - Must yield `Start` first and `Done` last.
        - If a final value is produced, yield `Output(value=...)` before `Done`.
        - Fatal `Error` must be followed by `Done` and no further events.
    """

    def astream(
        self, ctx: RunContext, input: InputT, /
    ) -> AsyncIterator[Event]:
        ...


@runtime_checkable
class Channel(Protocol[InputT, OutputT]):
    """A bidirectional, long-running conversation.

    Unlike Runnable, a Channel keeps producing events as new input is pushed
    in via `send()`. Callers consume `events()` and can interleave sends.
    """

    async def send(self, input: InputT) -> None:
        """Push new input into the running channel."""
        ...

    def events(self) -> AsyncIterator[Event]:
        """Consume the ongoing event stream."""
        ...

    async def close(self) -> None:
        """Gracefully stop the channel."""
        ...


# ---------------------------------------------------------------------------
# Consumer helpers — free functions, not methods, to keep Runnable minimal.
# ---------------------------------------------------------------------------


async def acollect(
    runnable: Runnable[InputT, OutputT],
    ctx: RunContext,
    input: InputT,
) -> list[Event]:
    """Drain a runnable's stream into a list.

    Useful in tests and for callers that want the full event history rather
    than just the final output.
    """
    events: list[Event] = []
    async for event in runnable.astream(ctx, input):
        events.append(event)
    return events


async def ainvoke(
    runnable: Runnable[InputT, OutputT],
    ctx: RunContext,
    input: InputT,
) -> OutputT:
    """Run to completion and return the final `Output.value`.

    Raises:
        RuntimeError: If no `Output` event fires before `Done`.
        RuntimeError: If a fatal `Error` event fires. The error message is
            propagated on the exception.
    """
    output: OutputT | None = None
    output_seen = False
    async for event in runnable.astream(ctx, input):
        if isinstance(event, Output):
            output = event.value  # type: ignore[assignment]
            output_seen = True
        elif isinstance(event, Error) and event.fatal:
            raise RuntimeError(f"Runnable errored: {event.error}")
        elif isinstance(event, Done):
            break
    if not output_seen:
        raise RuntimeError("Runnable finished without emitting Output")
    return output  # type: ignore[return-value]


def invoke(
    runnable: Runnable[InputT, OutputT],
    ctx: RunContext,
    input: InputT,
) -> OutputT:
    """Synchronous wrapper around `ainvoke`.

    Uses `asyncio.run`, so it must NOT be called from within a running event
    loop. From async code, call `ainvoke` directly.
    """
    return asyncio.run(ainvoke(runnable, ctx, input))
