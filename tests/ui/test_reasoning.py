"""Tests for ThinkingDelta rendering and the no-duplicate-ToolCall guarantee.

Answers two concrete concerns raised while running a real agent:
    1. When a reasoning model streams ``reasoning_content``, is it visually
       distinguishable from regular assistant text (marked ``[reason]``)?
    2. When an agent runs a tool, does ``[tool] name(args)`` print exactly
       once — not twice like the earlier duplicated-emit bug?
    3. Non-reasoning models must still work: no ``ThinkingDelta`` should ever
       be emitted or displayed for them.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))
from conftest import (  # noqa: E402
    assistant_text,
    assistant_tool_call,
    make_scripted_model,
)

from koala import Agent, ashow, show, tool  # noqa: E402
from koala.core import (  # noqa: E402
    Done,
    Event,
    RunContext,
    Start,
    ThinkingDelta,
    ToolCall,
    acollect,
)

# ---------------------------------------------------------------------------
# Reasoning-model rendering: [reason] marker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_thinking_delta_renders_with_reason_marker(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def stream() -> AsyncIterator[Event]:
        yield Start(run_id="1", name="fake")
        # A reasoning-model style stream: reasoning tokens first, then
        # visible answer.
        yield ThinkingDelta(text="let me ")
        yield ThinkingDelta(text="think ")
        yield ThinkingDelta(text="carefully.")
        yield Done(run_id="1")

    await ashow(stream())
    out = capsys.readouterr().out
    # [reason] marker appears exactly once for the whole thinking block.
    assert out.count("[reason]") == 1
    # And the reasoning text is there
    assert "let me think carefully." in out


@pytest.mark.asyncio
async def test_reason_marker_only_once_per_transition(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Streaming many small thinking tokens must not print [reason] per token."""

    async def stream() -> AsyncIterator[Event]:
        yield Start(run_id="1", name="fake")
        for tok in ("plan", "n", "ing"):
            yield ThinkingDelta(text=tok)
        yield Done(run_id="1")

    await ashow(stream())
    out = capsys.readouterr().out
    assert out.count("[reason]") == 1
    assert "planning" in out


@pytest.mark.asyncio
async def test_thinking_then_text_puts_reason_first(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from koala.core import ModelDelta

    async def stream() -> AsyncIterator[Event]:
        yield Start(run_id="1", name="fake")
        yield ThinkingDelta(text="i should sum them")
        yield ModelDelta(text="The answer is 12.")
        yield Done(run_id="1")

    await ashow(stream())
    out = capsys.readouterr().out
    # Thinking block precedes the visible text; the two are separated.
    assert out.index("[reason]") < out.index("The answer is 12.")
    assert "i should sum them" in out


# ---------------------------------------------------------------------------
# Non-reasoning models: no [reason] marker ever appears
# ---------------------------------------------------------------------------


def test_no_reason_marker_for_normal_agent_run(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A model that emits only ModelDelta events must not trigger [reason]."""
    model, _ = make_scripted_model([assistant_text("hi from a plain model")])
    agent = Agent(model)

    show(agent, "hello")
    out = capsys.readouterr().out
    assert "[reason]" not in out
    assert "hi from a plain model" in out


def test_no_reason_marker_for_agent_with_tool(
    capsys: pytest.CaptureFixture[str],
) -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 7, "b": 5}, call_id="c1"),
            assistant_text("The sum is twelve."),
        ]
    )
    agent = Agent(model, tools=[add])

    show(agent, "seven plus five")
    out = capsys.readouterr().out
    assert "[reason]" not in out


# ---------------------------------------------------------------------------
# Duplicate ToolCall regression guard — this was the "[tool] appears twice" bug
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_call_emitted_exactly_once_by_agent() -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 7, "b": 5}, call_id="c1"),
            assistant_text("done"),
        ]
    )
    agent = Agent(model, tools=[add])

    events = await acollect(agent, RunContext(deps=None), "7 + 5")
    tool_call_events = [e for e in events if isinstance(e, ToolCall)]
    # Exactly one ToolCall event for exactly one tool call the model made.
    assert len(tool_call_events) == 1
    assert tool_call_events[0].call.name == "add"
    assert tool_call_events[0].call.arguments == {"a": 7, "b": 5}


def test_show_agent_prints_tool_line_exactly_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 7, "b": 5}, call_id="c1"),
            assistant_text("done."),
        ]
    )
    agent = Agent(model, tools=[add])

    show(agent, "7 + 5")
    out = capsys.readouterr().out
    # Exactly one "[tool] add" line — the earlier bug printed it twice.
    assert out.count("[tool] add") == 1


@pytest.mark.asyncio
async def test_parallel_tool_calls_each_emitted_once() -> None:
    """Two distinct tool calls in one assistant message = two ToolCall events."""
    from koala.core import Message, TextBlock, ToolCallBlock

    @tool
    def a(x: int) -> int:
        """A."""
        return x

    @tool
    def b(x: int) -> int:
        """B."""
        return x

    parallel = Message(
        role="assistant",
        content=[
            TextBlock(text=""),
            ToolCallBlock(id="c1", name="a", arguments={"x": 1}),
            ToolCallBlock(id="c2", name="b", arguments={"x": 2}),
        ],
    )
    from tests.agents.conftest import assistant_text as text_msg

    model, _ = make_scripted_model([parallel, text_msg("both done")])
    agent = Agent(model, tools=[a, b])

    events = await acollect(agent, RunContext(deps=None), "run both")
    tool_calls = [e for e in events if isinstance(e, ToolCall)]
    assert len(tool_calls) == 2
    assert {tc.call.name for tc in tool_calls} == {"a", "b"}


# ---------------------------------------------------------------------------
# The specific pattern from the bug report
# ---------------------------------------------------------------------------


def test_your_exact_scenario_now_produces_clean_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Regression test for the exact k.py scenario the user reported.

    Non-reasoning model, one tool call, one final answer. Output should NOT
    contain a repeated ``[tool]`` line, and no ``[reason]`` marker."""

    @tool
    def addition(a: int, b: int) -> int:
        """addition"""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call(
                "addition", {"a": 7, "b": 5}, call_id="c1"
            ),
            assistant_text("The sum of seven and five is twelve."),
        ]
    )
    agent = Agent(
        model,
        instructions=(
            "You are a specialized addition agent. "
            "Use the addition tool to answer."
        ),
        tools=[addition],
    )
    show(agent, "seven plus five")
    out = capsys.readouterr().out

    assert out.count("[tool] addition") == 1
    assert "[reason]" not in out
    assert "-> 12" in out
    assert "The sum of seven and five is twelve." in out
