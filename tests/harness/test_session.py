"""Tests for koala.harness.AgentSession — the L1 Channel over an Agent.

Exercises:
    - basic send/events lifecycle
    - HITL approval round-trip (rule says ask, human replies allow/deny)
    - HITL user-input round-trip via resolver
    - conversation memory persistence across turns within one session
    - cancellation mid-turn
    - close() safety (idempotent, drains pending resolvers)
    - approval timeout defaults to deny
    - handoff scenarios still work under a Session
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Reuse the scripted provider from the agents test suite.
_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))
from conftest import (  # noqa: E402
    ScriptedProvider,
    assistant_text,
    assistant_tool_call,
    make_scripted_model,
)

from koala import Agent, AgentSession, tool  # noqa: E402
from koala.core import (  # noqa: E402
    AwaitingApproval,
    Done,
    Event,
    ModelDelta,
    Output,
    ToolCall,
    ToolResult,
)
from koala.harness import SessionClosedError  # noqa: E402
from koala.memory import InMemoryMemory  # noqa: E402
from koala.tools import RequireApprovalFor  # noqa: E402

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


async def _collect_until_done(session: AgentSession) -> list[Event]:
    """Consume events until the next Done, then stop reading."""
    events: list[Event] = []
    async for e in session.events():
        events.append(e)
        if isinstance(e, Done):
            break
    return events


# ---------------------------------------------------------------------------
# Basic lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_open_send_receive_close() -> None:
    model, _ = make_scripted_model([assistant_text("hi there")])
    agent = Agent(model)

    async with agent.session() as s:
        assert not s.closed
        await s.send("hello")
        events = await _collect_until_done(s)

    outputs = [e for e in events if isinstance(e, Output)]
    assert outputs and outputs[0].value == "hi there"
    assert s.closed  # closed by context manager exit


@pytest.mark.asyncio
async def test_send_after_close_raises() -> None:
    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model)

    async with agent.session() as s:
        pass  # exits and closes

    with pytest.raises(SessionClosedError):
        await s.send("late")


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model)

    s = agent.session()
    await s.close()
    await s.close()  # must not raise


@pytest.mark.asyncio
async def test_session_id_default_is_generated() -> None:
    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model)

    async with agent.session() as s:
        assert s.session_id
        assert len(s.session_id) > 4


@pytest.mark.asyncio
async def test_session_id_explicit_is_honored() -> None:
    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model)

    async with agent.session(session_id="my-session") as s:
        assert s.session_id == "my-session"


# ---------------------------------------------------------------------------
# HITL approval — rule says ask, session provides the reply
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_approval_allow_lets_tool_run() -> None:
    executed: list[int] = []

    @tool
    def pay(amount: int) -> str:
        """Charge the customer."""
        executed.append(amount)
        return "paid"

    model, _ = make_scripted_model(
        [
            assistant_tool_call("pay", {"amount": 42}, call_id="req-1"),
            assistant_text("Done — paid $42."),
        ]
    )
    rules = [RequireApprovalFor(names=frozenset({"pay"}))]
    agent = Agent(model, tools=[pay], approval_rules=rules)

    async with agent.session(approval_timeout=5.0) as s:
        await s.send("run it")

        # Drain events; when we see the pending approval, reply "allow".
        async for e in s.events():
            if isinstance(e, AwaitingApproval):
                assert e.call.name == "pay"
                await s.reply_approval(e.request_id, "allow")
            elif isinstance(e, Done):
                break

    assert executed == [42]


@pytest.mark.asyncio
async def test_approval_deny_prevents_tool_execution() -> None:
    executed: list[int] = []

    @tool
    def pay(amount: int) -> str:
        """Charge the customer."""
        executed.append(amount)
        return "paid"

    model, _ = make_scripted_model(
        [
            assistant_tool_call("pay", {"amount": 99}, call_id="req-1"),
            assistant_text("Understood — not charging."),
        ]
    )
    rules = [RequireApprovalFor(names=frozenset({"pay"}))]
    agent = Agent(model, tools=[pay], approval_rules=rules)

    async with agent.session(approval_timeout=5.0) as s:
        await s.send("try")
        async for e in s.events():
            if isinstance(e, AwaitingApproval):
                await s.reply_approval(e.request_id, "deny")
            elif isinstance(e, Done):
                break

    assert executed == []


@pytest.mark.asyncio
async def test_approval_timeout_defaults_to_deny() -> None:
    executed: list[int] = []

    @tool
    def pay(amount: int) -> str:
        """Charge."""
        executed.append(amount)
        return "paid"

    model, _ = make_scripted_model(
        [
            assistant_tool_call("pay", {"amount": 1}, call_id="req-1"),
            assistant_text("no reply -> denied"),
        ]
    )
    rules = [RequireApprovalFor(names=frozenset({"pay"}))]
    agent = Agent(model, tools=[pay], approval_rules=rules)

    async with agent.session(approval_timeout=0.05) as s:
        await s.send("try")
        # Do NOT reply — timeout kicks in and defaults to deny.
        async for e in s.events():
            if isinstance(e, Done):
                break

    assert executed == []


@pytest.mark.asyncio
async def test_approval_timeout_surfaces_resolver_timeout_error_in_deny_reason() -> None:
    """The resolver must raise ResolverTimeoutError on timeout so the deny
    reason on the ToolResult is informative — not a silent generic deny.
    """
    from koala.core import ToolResult as ToolResultEvent

    @tool
    def pay(amount: int) -> str:
        """Charge."""
        return "paid"

    model, _ = make_scripted_model(
        [
            assistant_tool_call("pay", {"amount": 1}, call_id="req-1"),
            assistant_text("done"),
        ]
    )
    rules = [RequireApprovalFor(names=frozenset({"pay"}))]
    agent = Agent(model, tools=[pay], approval_rules=rules)

    tool_result_contents: list[str] = []
    async with agent.session(approval_timeout=0.02) as s:
        await s.send("try")
        async for e in s.events():
            if isinstance(e, ToolResultEvent):
                tool_result_contents.append(e.result.content)
            elif isinstance(e, Done):
                break

    assert tool_result_contents, "Expected at least one ToolResult event"
    denied = tool_result_contents[0]
    assert "denied" in denied.lower()
    assert "ResolverTimeoutError" in denied


@pytest.mark.asyncio
async def test_reply_approval_for_unknown_id_is_noop() -> None:
    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model)

    async with agent.session() as s:
        # Must not raise
        await s.reply_approval("nonexistent", "allow")
        await s.send("hi")
        async for e in s.events():
            if isinstance(e, Done):
                break


# ---------------------------------------------------------------------------
# Approval event still fires even when there's a resolver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_awaiting_approval_event_still_emitted() -> None:
    @tool
    def pay(amount: int) -> str:
        """Charge."""
        return "paid"

    model, _ = make_scripted_model(
        [
            assistant_tool_call("pay", {"amount": 1}, call_id="req-1"),
            assistant_text("done"),
        ]
    )
    rules = [RequireApprovalFor(names=frozenset({"pay"}))]
    agent = Agent(model, tools=[pay], approval_rules=rules)

    async with agent.session(approval_timeout=5.0) as s:
        await s.send("go")
        events: list[Event] = []
        async for e in s.events():
            events.append(e)
            if isinstance(e, AwaitingApproval):
                await s.reply_approval(e.request_id, "allow")
            if isinstance(e, Done):
                break

    assert any(isinstance(e, AwaitingApproval) for e in events)


# ---------------------------------------------------------------------------
# Memory persists across turns within a single session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_turns_share_memory_in_one_session() -> None:
    mem = InMemoryMemory()

    model, provider = make_scripted_model(
        [
            assistant_text("Nice to meet you, Alice."),
            assistant_text("Your name is Alice."),
        ]
    )
    agent = Agent(model, memory=mem)

    async with agent.session(session_id="conv-1") as s:
        # Turn 1
        await s.send("My name is Alice.")
        async for e in s.events():
            if isinstance(e, Done):
                break

        # Turn 2 — same session, memory should include turn 1
        await s.send("What's my name?")
        async for e in s.events():
            if isinstance(e, Done):
                break

    # The 2nd model call must have seen turn 1 in its messages.
    sent = provider.calls[1]["messages"]
    joined = " ".join(m.text for m in sent if m.text)
    assert "Alice" in joined
    assert "Nice to meet you" in joined


@pytest.mark.asyncio
async def test_session_persists_to_memory_on_success() -> None:
    mem = InMemoryMemory()
    model, _ = make_scripted_model([assistant_text("saved")])
    agent = Agent(model, memory=mem)

    async with agent.session(session_id="s1") as s:
        await s.send("remember me")
        async for e in s.events():
            if isinstance(e, Done):
                break

    stored = await mem.get("s1")
    assert [m.role for m in stored] == ["user", "assistant"]
    assert stored[0].text == "remember me"
    assert stored[1].text == "saved"


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_short_circuits_current_turn() -> None:
    @tool
    def spin(x: int) -> int:
        """Spin."""
        return x + 1

    # Model would loop forever.
    model, _ = make_scripted_model(
        [
            assistant_tool_call("spin", {"x": i}, call_id=f"c{i}")
            for i in range(20)
        ]
    )
    agent = Agent(model, tools=[spin], max_iterations=100)

    async with agent.session() as s:
        await s.send("go")
        # Cancel immediately — first loop iteration will see it.
        s.cancel()

        events: list[Event] = []
        async for e in s.events():
            events.append(e)
            if isinstance(e, Done):
                break

    # Should have terminated quickly with a cancelled Error before spinning.
    from koala.core import Error

    error_events = [e for e in events if isinstance(e, Error)]
    assert error_events
    assert "cancel" in error_events[0].error.lower()


# ---------------------------------------------------------------------------
# Streaming — ModelDelta events reach the consumer in order
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_model_deltas_stream_to_consumer() -> None:
    model, _ = make_scripted_model([assistant_text("hello world")])
    agent = Agent(model)

    async with agent.session() as s:
        await s.send("hi")
        deltas: list[str] = []
        async for e in s.events():
            if isinstance(e, ModelDelta):
                deltas.append(e.text)
            if isinstance(e, Done):
                break

    # ScriptedProvider emits the whole text as a single delta
    assert deltas == ["hello world"]


# ---------------------------------------------------------------------------
# Tool call events surface via the session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_call_and_result_events_visible() -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 1, "b": 2}, call_id="c1"),
            assistant_text("3"),
        ]
    )
    agent = Agent(model, tools=[add])

    async with agent.session() as s:
        await s.send("what is 1+2")
        events: list[Event] = []
        async for e in s.events():
            events.append(e)
            if isinstance(e, Done):
                break

    assert any(isinstance(e, ToolCall) for e in events)
    assert any(isinstance(e, ToolResult) for e in events)


# ---------------------------------------------------------------------------
# Handoff still works inside a Session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handoff_inside_session() -> None:
    specialist_model, _ = make_scripted_model([assistant_text("specialist reply")])
    specialist = Agent(specialist_model, name="specialist")

    triage_model, triage_provider = make_scripted_model(
        [
            assistant_tool_call(
                "transfer_to_specialist",
                {"input": "please handle"},
                call_id="c1",
            ),
            assistant_text("Result from specialist: specialist reply"),
        ]
    )
    triage = Agent(triage_model, handoffs=[specialist])

    async with triage.session() as s:
        await s.send("help me")
        outputs: list[str] = []
        async for e in s.events():
            if isinstance(e, Output):
                outputs.append(e.value)
            if isinstance(e, Done):
                break

    assert outputs
    assert "specialist reply" in outputs[0]


# ---------------------------------------------------------------------------
# Session context is accessible for inspection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_context_exposed_via_session_context_property() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(model)

    async with agent.session(deps={"pool": "db"}) as s:
        ctx = s.context
        assert ctx.deps == {"pool": "db"}
        assert ctx.session_id == s.session_id
        # Approval resolver is wired
        assert ctx.approval_resolver is not None


# ---------------------------------------------------------------------------
# Multi-turn with tool call in each turn
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multi_turn_tool_calls_share_session() -> None:
    @tool
    def upper(text: str) -> str:
        """Upper case."""
        return text.upper()

    model, _ = make_scripted_model(
        [
            # Turn 1
            assistant_tool_call("upper", {"text": "hi"}, call_id="c1"),
            assistant_text("Result: HI"),
            # Turn 2
            assistant_tool_call("upper", {"text": "bye"}, call_id="c2"),
            assistant_text("Result: BYE"),
        ]
    )
    agent = Agent(model, tools=[upper], memory=InMemoryMemory())

    async with agent.session(session_id="s1") as s:
        outputs: list[str] = []

        await s.send("first")
        async for e in s.events():
            if isinstance(e, Output):
                outputs.append(e.value)
            if isinstance(e, Done):
                break

        await s.send("second")
        async for e in s.events():
            if isinstance(e, Output):
                outputs.append(e.value)
            if isinstance(e, Done):
                break

    assert outputs == ["Result: HI", "Result: BYE"]


# ---------------------------------------------------------------------------
# ScriptedProvider sanity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_uses_scripted_provider_correctly() -> None:
    model, provider = make_scripted_model([assistant_text("ok")])
    assert isinstance(provider, ScriptedProvider)
    agent = Agent(model)

    async with agent.session() as s:
        await s.send("hi")
        async for e in s.events():
            if isinstance(e, Done):
                break

    assert len(provider.calls) == 1
