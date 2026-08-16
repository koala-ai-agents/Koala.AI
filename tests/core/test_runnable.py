"""Tests for koala.core.runnable — Protocol conformance and consumer helpers."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from koala.core import (
    Done,
    Error,
    Event,
    Output,
    RunContext,
    Runnable,
    Start,
    acollect,
    ainvoke,
    invoke,
)


class Echo:
    """Minimal Runnable that echoes its input back as the Output value."""

    async def astream(
        self, ctx: RunContext, input: object, /
    ) -> AsyncIterator[Event]:
        yield Start(run_id="1", name="Echo", input=input)
        yield Output(value=input)
        yield Done(run_id="1")


class NoOutput:
    async def astream(
        self, ctx: RunContext, input: object, /
    ) -> AsyncIterator[Event]:
        yield Start(run_id="1", name="NoOutput")
        yield Done(run_id="1")


class FailingRunnable:
    async def astream(
        self, ctx: RunContext, input: object, /
    ) -> AsyncIterator[Event]:
        yield Start(run_id="1", name="Failing")
        yield Error(error="something broke", fatal=True)
        yield Done(run_id="1")


def test_echo_satisfies_runnable_protocol_structurally():
    assert isinstance(Echo(), Runnable)


def test_arbitrary_object_does_not_satisfy_runnable():
    assert not isinstance(object(), Runnable)


@pytest.mark.asyncio
async def test_ainvoke_returns_final_output_value():
    result = await ainvoke(Echo(), RunContext(deps=None), "hello")
    assert result == "hello"


@pytest.mark.asyncio
async def test_acollect_returns_the_full_ordered_stream():
    events = await acollect(Echo(), RunContext(deps=None), "hi")
    assert [e.kind for e in events] == ["start", "output", "done"]


def test_invoke_synchronous_wrapper():
    assert invoke(Echo(), RunContext(deps=None), "sync") == "sync"


@pytest.mark.asyncio
async def test_ainvoke_raises_when_no_output_event():
    with pytest.raises(RuntimeError, match="without emitting Output"):
        await ainvoke(NoOutput(), RunContext(deps=None), "x")


@pytest.mark.asyncio
async def test_ainvoke_raises_on_fatal_error_event():
    with pytest.raises(RuntimeError, match="something broke"):
        await ainvoke(FailingRunnable(), RunContext(deps=None), "x")


@pytest.mark.asyncio
async def test_ainvoke_stops_reading_after_done():
    events_produced: list[str] = []

    class TrailingEvents:
        async def astream(
            self, ctx: RunContext, input: object, /
        ) -> AsyncIterator[Event]:
            events_produced.append("start")
            yield Start(run_id="1", name="Trailing")
            events_produced.append("output")
            yield Output(value="ok")
            events_produced.append("done")
            yield Done(run_id="1")
            events_produced.append("trailing")  # should not be reached
            yield Output(value="ignored")

    result = await ainvoke(TrailingEvents(), RunContext(deps=None), None)
    assert result == "ok"
    assert "trailing" not in events_produced
