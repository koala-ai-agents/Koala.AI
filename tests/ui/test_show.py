"""Tests for koala.ui.show — the print-simple output helper.

Verifies:
    * Every target type dispatch path (Agent, Model, Tool, RunResult,
      Message, Runnable, plain value, async iterator).
    * Streaming vs non-streaming behavior — a ScriptedProvider that emits
      deltas gets streamed; a static Message target is printed once.
    * Return-value semantics for each dispatch.
    * The `.show` / `.ashow` methods on Agent / Model / Session.
    * Never raises on normal-error paths — errors get printed inline.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

# Reuse the scripted provider from the agents test suite.
_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))
from conftest import (  # noqa: E402
    assistant_text,
    assistant_tool_call,
    make_scripted_model,
)

from koala import Agent, ashow, show, tool  # noqa: E402
from koala.agents.result import RunResult  # noqa: E402
from koala.core import (  # noqa: E402
    Done,
    Event,
    Message,
    Output,
    RunContext,
    Start,
    TextBlock,
    Usage,
)

# ---------------------------------------------------------------------------
# Simple values — RunResult, Message, plain value
# ---------------------------------------------------------------------------


def test_show_plain_string(capsys: pytest.CaptureFixture[str]) -> None:
    ret = show("hello world")
    captured = capsys.readouterr()
    assert captured.out == "hello world\n"
    assert ret == "hello world"


def test_show_int(capsys: pytest.CaptureFixture[str]) -> None:
    ret = show(42)
    assert capsys.readouterr().out == "42\n"
    assert ret == 42


def test_show_message(capsys: pytest.CaptureFixture[str]) -> None:
    m = Message.assistant("koalas eat eucalyptus")
    ret = show(m)
    assert capsys.readouterr().out == "koalas eat eucalyptus\n"
    assert ret is m


def test_show_run_result(capsys: pytest.CaptureFixture[str]) -> None:
    r = RunResult(
        output="done",
        messages=[],
        usage=Usage(),
        iterations=1,
        stop_reason="final_output",
    )
    ret = show(r)
    assert capsys.readouterr().out == "done\n"
    assert ret is r


def test_show_custom_end_suffix(capsys: pytest.CaptureFixture[str]) -> None:
    show("x", end="!")
    assert capsys.readouterr().out == "x!"


# ---------------------------------------------------------------------------
# Agent — streaming deltas, tool calls, and final output
# ---------------------------------------------------------------------------


def test_show_agent_streams_model_deltas(
    capsys: pytest.CaptureFixture[str],
) -> None:
    model, _ = make_scripted_model([assistant_text("hi from agent")])
    agent = Agent(model, name="test")

    ret = show(agent, "hello")
    out = capsys.readouterr().out

    assert "hi from agent" in out
    assert ret == "hi from agent"


def test_show_agent_renders_tool_call_and_result(
    capsys: pytest.CaptureFixture[str],
) -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 2, "b": 3}, call_id="c1"),
            assistant_text("Result: 5"),
        ]
    )
    agent = Agent(model, tools=[add])

    show(agent, "add 2 and 3")
    out = capsys.readouterr().out

    assert "[tool] add" in out
    assert "->" in out  # tool result marker
    assert "5" in out  # tool returned 5
    assert "Result: 5" in out  # final assistant text


def test_show_agent_no_input_raises() -> None:
    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model)
    with pytest.raises(TypeError, match="input"):
        show(agent)


def test_show_agent_via_method(capsys: pytest.CaptureFixture[str]) -> None:
    model, _ = make_scripted_model([assistant_text("via method")])
    agent = Agent(model)
    ret = agent.show("hello")
    assert "via method" in capsys.readouterr().out
    assert ret == "via method"


# ---------------------------------------------------------------------------
# Model — streams deltas or falls back to final message
# ---------------------------------------------------------------------------


def test_show_model_with_string_input(
    capsys: pytest.CaptureFixture[str],
) -> None:
    model, provider = make_scripted_model([assistant_text("model reply")])
    ret = show(model, "prompt me")
    assert "model reply" in capsys.readouterr().out
    assert ret == "model reply"
    # The str input was wrapped as a user message.
    sent = provider.calls[0]["messages"]
    assert any(m.role == "user" and m.text == "prompt me" for m in sent)


def test_show_model_with_messages_list(
    capsys: pytest.CaptureFixture[str],
) -> None:
    model, provider = make_scripted_model([assistant_text("ok")])
    msgs = [Message.system("be terse"), Message.user("hi")]
    show(model, msgs)
    assert "ok" in capsys.readouterr().out
    # Ensure messages were forwarded verbatim.
    sent = provider.calls[0]["messages"]
    assert [m.role for m in sent] == ["system", "user"]


def test_show_model_bad_input_type_raises() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    with pytest.raises(TypeError, match="string or list"):
        show(model, 123)  # type: ignore[arg-type]


def test_show_model_via_method(capsys: pytest.CaptureFixture[str]) -> None:
    model, _ = make_scripted_model([assistant_text("via method")])
    ret = model.show("hi")
    assert "via method" in capsys.readouterr().out
    assert ret == "via method"


# ---------------------------------------------------------------------------
# BaseTool
# ---------------------------------------------------------------------------


def test_show_tool_runs_and_prints_result(
    capsys: pytest.CaptureFixture[str],
) -> None:
    @tool
    def multiply(a: int, b: int) -> int:
        """Multiply."""
        return a * b

    ret = show(multiply, {"a": 3, "b": 4})
    assert capsys.readouterr().out.strip() == "12"
    assert ret == 12


def test_show_tool_with_empty_dict_when_no_args_needed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    @tool
    def hello() -> str:
        """Say hi."""
        return "hi"

    ret = show(hello, {})
    assert capsys.readouterr().out.strip() == "hi"
    assert ret == "hi"


def test_show_tool_non_dict_input_raises() -> None:
    @tool
    def foo(a: int) -> int:
        """Foo."""
        return a

    with pytest.raises(TypeError, match="dict of arguments"):
        show(foo, "not a dict")


def test_show_tool_error_printed_inline_not_raised(
    capsys: pytest.CaptureFixture[str],
) -> None:
    @tool
    def bad() -> int:
        """Bad tool."""
        raise RuntimeError("boom")

    ret = show(bad, {})
    out = capsys.readouterr().out
    assert "[tool error]" in out
    assert "boom" in out
    assert ret is None


# ---------------------------------------------------------------------------
# Async iterator input — session.events() style
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ashow_async_iterator(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_stream() -> AsyncIterator[Event]:
        yield Start(run_id="1", name="test")
        yield Output(value="from stream")
        yield Done(run_id="1")

    result = await ashow(fake_stream())
    out = capsys.readouterr().out
    # Non-streaming source (no ModelDelta), so the final Output prints once.
    assert "from stream" in out
    assert result == "from stream"


@pytest.mark.asyncio
async def test_ashow_agent_astream_from_async() -> None:
    """Passing an already-started agent.astream(...) generator works."""
    model, _ = make_scripted_model([assistant_text("streamed")])
    agent = Agent(model)
    ctx: RunContext[None] = RunContext(deps=None)
    events = agent.astream(ctx, "hi")
    result = await ashow(events)
    # The final Output carries the assistant Message
    assert result is not None


# ---------------------------------------------------------------------------
# Callable fallback
# ---------------------------------------------------------------------------


def test_show_callable_sync(capsys: pytest.CaptureFixture[str]) -> None:
    def greet(name: str) -> str:
        return f"hello {name}"

    ret = show(greet, "world")
    assert capsys.readouterr().out == "hello world\n"
    assert ret == "hello world"


@pytest.mark.asyncio
async def test_ashow_callable_async(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def greet(name: str) -> str:
        return f"hi {name}"

    ret = await ashow(greet, "there")
    assert capsys.readouterr().out == "hi there\n"
    assert ret == "hi there"


def test_show_callable_error_printed_inline(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def bad(x: int) -> int:
        raise ValueError("x")

    ret = show(bad, 1)
    out = capsys.readouterr().out
    assert "[error]" in out
    assert ret is None


# ---------------------------------------------------------------------------
# Generic Runnable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ashow_custom_runnable(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class Echo:
        """A generic Runnable (not Agent/Tool/Model)."""

        async def astream(
            self, ctx: RunContext, input: object, /
        ) -> AsyncIterator[Event]:
            yield Start(run_id="1", name="Echo")
            yield Output(value=f"echoed: {input}")
            yield Done(run_id="1")

    ret = await ashow(Echo(), "hello")
    out = capsys.readouterr().out
    assert "echoed: hello" in out
    assert ret == "echoed: hello"


# ---------------------------------------------------------------------------
# Session.ashow — drains events until Done
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_ashow_prints_current_turn(
    capsys: pytest.CaptureFixture[str],
) -> None:
    model, _ = make_scripted_model([assistant_text("via session")])
    agent = Agent(model)

    async with agent.session() as s:
        await s.send("hi")
        result = await s.ashow()

    out = capsys.readouterr().out
    assert "via session" in out
    assert result is not None


@pytest.mark.asyncio
async def test_session_ashow_multi_turn(
    capsys: pytest.CaptureFixture[str],
) -> None:
    model, _ = make_scripted_model(
        [assistant_text("first turn"), assistant_text("second turn")]
    )
    agent = Agent(model)

    async with agent.session() as s:
        await s.send("first")
        await s.ashow()
        await s.send("second")
        await s.ashow()

    out = capsys.readouterr().out
    assert "first turn" in out
    assert "second turn" in out


# ---------------------------------------------------------------------------
# Non-streaming Message content is printed once (fallback path)
# ---------------------------------------------------------------------------


def test_show_message_with_thinking_only_prints_text(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from koala.core import ThinkingBlock

    m = Message(
        role="assistant",
        content=[
            ThinkingBlock(text="hidden reasoning"),
            TextBlock(text="visible answer"),
        ],
    )
    show(m)
    out = capsys.readouterr().out
    # .text concats only TextBlock content — thinking is hidden by default
    assert out == "visible answer\n"


# ---------------------------------------------------------------------------
# Return-value semantics summary
# ---------------------------------------------------------------------------


def test_show_returns_the_target_for_plain_values() -> None:
    obj = {"a": 1}
    assert show(obj) is obj


def test_show_returns_none_for_error_paths(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def bad() -> None:
        raise RuntimeError("nope")

    assert show(bad) is None
    capsys.readouterr()  # flush
