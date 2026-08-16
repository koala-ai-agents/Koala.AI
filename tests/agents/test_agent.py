"""Tests for koala.agents.Agent — the tool-calling loop, structured output, and handoffs."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from koala import Agent, tool
from koala.core import (
    Capability,
    Done,
    ModelMessage,
    Output,
    Runnable,
    Start,
    ToolCall,
    ToolResult,
)
from koala.tools import DenyList, RequireApprovalFor

from .conftest import (
    ScriptedProvider,
    assistant_text,
    assistant_tool_call,
    make_scripted_model,
)

# ---------------------------------------------------------------------------
# Construction / validation
# ---------------------------------------------------------------------------


def test_agent_from_model_instance() -> None:
    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model, name="test")
    assert agent.model is model
    assert agent.name == "test"
    assert agent.max_iterations == 20


def test_agent_rejects_bad_model_type() -> None:
    with pytest.raises(TypeError, match="Model instance"):
        Agent(model=42)  # type: ignore[arg-type]


def test_agent_duplicate_tool_name_raises() -> None:
    @tool
    def add(a: int) -> int:
        """Add."""
        return a

    @tool(name="add")
    def add2(x: int) -> int:
        """Add 2."""
        return x

    model, _ = make_scripted_model([assistant_text("ok")])
    with pytest.raises(ValueError, match="Duplicate tool"):
        Agent(model, tools=[add, add2])


def test_agent_output_type_must_be_basemodel() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    with pytest.raises(TypeError, match="BaseModel"):
        Agent(model, output_type=int)  # type: ignore[arg-type]


def test_agent_max_iterations_must_be_positive() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    with pytest.raises(ValueError, match=">= 1"):
        Agent(model, max_iterations=0)


def test_agent_repr() -> None:
    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model, name="test")
    assert "test" in repr(agent)
    assert "tools=0" in repr(agent)


# ---------------------------------------------------------------------------
# Plain text runs (no tools)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_simple_text_run() -> None:
    model, provider = make_scripted_model([assistant_text("koalas eat eucalyptus.")])
    agent = Agent(model, name="test")
    result = await agent.arun("What do koalas eat?")

    assert result.output == "koalas eat eucalyptus."
    assert result.stop_reason == "final_output"
    assert result.iterations == 1
    assert result.error is None
    # Message order: user, assistant
    assert [m.role for m in result.messages] == ["user", "assistant"]
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_instructions_become_system_message() -> None:
    model, provider = make_scripted_model([assistant_text("ok")])
    agent = Agent(model, instructions="Be concise.")
    await agent.arun("hello")

    sent_messages = provider.calls[0]["messages"]
    assert sent_messages[0].role == "system"
    assert sent_messages[0].text == "Be concise."


@pytest.mark.asyncio
async def test_message_list_input_preserved() -> None:
    model, provider = make_scripted_model([assistant_text("ok")])
    agent = Agent(model)
    from koala.core import Message

    history = [
        Message.user("first"),
        Message.assistant("earlier reply"),
        Message.user("second"),
    ]
    await agent.arun(history)

    sent = provider.calls[0]["messages"]
    # No instructions, no prepended system message
    assert [m.role for m in sent] == ["user", "assistant", "user"]


@pytest.mark.asyncio
async def test_instructions_prepend_to_existing_system_message() -> None:
    model, provider = make_scripted_model([assistant_text("ok")])
    agent = Agent(model, instructions="Be brief.")

    from koala.core import Message

    await agent.arun([Message.system("Existing system."), Message.user("hi")])
    sent = provider.calls[0]["messages"]
    assert sent[0].role == "system"
    assert "Be brief." in sent[0].text
    assert "Existing system." in sent[0].text


# ---------------------------------------------------------------------------
# Sync run wrapper
# ---------------------------------------------------------------------------


def test_sync_run_convenience() -> None:
    model, _ = make_scripted_model([assistant_text("sync ok")])
    agent = Agent(model)
    result = agent.run("hi")
    assert result.output == "sync ok"


# ---------------------------------------------------------------------------
# Tool loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_call_round_trip() -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, provider = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 2, "b": 3}, call_id="c1"),
            assistant_text("The answer is 5."),
        ]
    )
    agent = Agent(model, tools=[add])
    result = await agent.arun("what is 2 + 3?")

    assert result.output == "The answer is 5."
    assert result.iterations == 2  # 1 tool call round + 1 final text
    assert result.stop_reason == "final_output"
    assert len(provider.calls) == 2
    # Second model call must include the tool result in messages
    second_call_msgs = provider.calls[1]["messages"]
    tool_msgs = [m for m in second_call_msgs if m.role == "tool"]
    assert len(tool_msgs) == 1


@pytest.mark.asyncio
async def test_multi_step_tool_calls() -> None:
    @tool
    def step_one(x: int) -> int:
        """First step."""
        return x + 1

    @tool
    def step_two(y: int) -> int:
        """Second step."""
        return y * 2

    model, provider = make_scripted_model(
        [
            assistant_tool_call("step_one", {"x": 5}, call_id="c1"),
            assistant_tool_call("step_two", {"y": 6}, call_id="c2"),
            assistant_text("Final answer: 12"),
        ]
    )
    agent = Agent(model, tools=[step_one, step_two])
    result = await agent.arun("compute")

    assert result.output == "Final answer: 12"
    assert result.iterations == 3
    assert len(provider.calls) == 3


@pytest.mark.asyncio
async def test_tool_not_found_feeds_error_back() -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, provider = make_scripted_model(
        [
            assistant_tool_call("subtract", {"a": 1, "b": 2}, call_id="c1"),
            assistant_text("I recovered."),
        ]
    )
    agent = Agent(model, tools=[add])
    result = await agent.arun("hi")

    assert result.output == "I recovered."
    # Verify the not-found error made it into the messages fed back to the model
    second_call = provider.calls[1]["messages"]
    tool_msg = next(m for m in second_call if m.role == "tool")
    assert "not found" in tool_msg.text.lower() or "not found" in str(
        tool_msg.content[0]
    )


@pytest.mark.asyncio
async def test_tool_execution_error_is_captured() -> None:
    @tool
    def bad(a: int) -> int:
        """Bad tool."""
        raise ValueError("boom")

    model, provider = make_scripted_model(
        [
            assistant_tool_call("bad", {"a": 1}, call_id="c1"),
            assistant_text("I saw the error."),
        ]
    )
    agent = Agent(model, tools=[bad])
    result = await agent.arun("hi")

    assert result.output == "I saw the error."
    # Tool result message should mention the error
    tool_msg_content = next(
        m for m in provider.calls[1]["messages"] if m.role == "tool"
    )
    body = str(tool_msg_content.content[0])
    assert "ValueError" in body or "boom" in body


@pytest.mark.asyncio
async def test_tool_argument_validation_error_captured() -> None:
    @tool
    def foo(a: int) -> int:
        """Foo."""
        return a

    model, provider = make_scripted_model(
        [
            # Missing required arg `a`
            assistant_tool_call("foo", {}, call_id="c1"),
            assistant_text("recovered."),
        ]
    )
    agent = Agent(model, tools=[foo])
    result = await agent.arun("hi")

    assert result.output == "recovered."
    tool_msg = next(m for m in provider.calls[1]["messages"] if m.role == "tool")
    assert "validation" in str(tool_msg.content[0]).lower()


# ---------------------------------------------------------------------------
# Max iterations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_max_iterations_enforced() -> None:
    @tool
    def spin(n: int) -> int:
        """Never stop."""
        return n + 1

    # Model keeps calling the tool forever.
    model, _ = make_scripted_model(
        [assistant_tool_call("spin", {"n": i}, call_id=f"c{i}") for i in range(10)]
    )
    agent = Agent(model, tools=[spin], max_iterations=3)
    result = await agent.arun("run")

    assert result.stop_reason == "max_iterations"
    assert result.output is None
    assert "Max iterations" in (result.error or "")


# ---------------------------------------------------------------------------
# Approval
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_approval_deny_short_circuits_tool() -> None:
    call_log: list[int] = []

    @tool
    def dangerous(x: int) -> int:
        """Dangerous."""
        call_log.append(x)
        return x

    model, _ = make_scripted_model(
        [
            assistant_tool_call("dangerous", {"x": 1}, call_id="c1"),
            assistant_text("blocked as expected"),
        ]
    )
    rules = [DenyList(names=frozenset({"dangerous"}))]
    agent = Agent(model, tools=[dangerous], approval_rules=rules)
    result = await agent.arun("try it")

    assert result.output == "blocked as expected"
    # Tool must NOT have run
    assert call_log == []


@pytest.mark.asyncio
async def test_approval_ask_emits_awaiting_event_and_auto_denies() -> None:
    from koala.core import AwaitingApproval

    call_log: list[int] = []

    @tool
    def payment(amount: int) -> str:
        """Charge."""
        call_log.append(amount)
        return "charged"

    model, _ = make_scripted_model(
        [
            assistant_tool_call("payment", {"amount": 100}, call_id="c1"),
            assistant_text("I could not charge."),
        ]
    )
    rules = [RequireApprovalFor(names=frozenset({"payment"}))]
    agent = Agent(model, tools=[payment], approval_rules=rules)

    from koala.core import RunContext

    events = []
    async for e in agent.astream(RunContext(deps=None), "pay"):
        events.append(e)

    assert any(isinstance(e, AwaitingApproval) for e in events)
    assert call_log == []  # payment must not have been executed


# ---------------------------------------------------------------------------
# Structured output
# ---------------------------------------------------------------------------


class Report(BaseModel):
    title: str
    score: int


@pytest.mark.asyncio
async def test_structured_output_native_pathway() -> None:
    # Model advertises native structured output.
    model, provider = make_scripted_model(
        [assistant_text('{"title": "hello", "score": 7}')],
        capabilities=frozenset({Capability.STRUCTURED_OUTPUT}),
    )
    agent = Agent(model, output_type=Report)
    result = await agent.arun("give me a report")

    assert isinstance(result.output, Report)
    assert result.output.title == "hello"
    assert result.output.score == 7
    # response_format was set for the native path
    assert provider.calls[0]["response_format"] is not None
    assert provider.calls[0]["response_format"]["type"] == "json_schema"


@pytest.mark.asyncio
async def test_structured_output_prompt_fallback_pathway() -> None:
    # Model does NOT advertise structured output — prompt-engineered fallback.
    model, provider = make_scripted_model(
        [assistant_text('{"title": "prompted", "score": 3}')],
        capabilities=frozenset(),
    )
    agent = Agent(model, output_type=Report)
    result = await agent.arun("give a report")

    assert isinstance(result.output, Report)
    assert result.output.title == "prompted"
    # Native response_format must not be sent
    assert provider.calls[0]["response_format"] is None
    # System message should include schema hint
    system_msg = provider.calls[0]["messages"][0]
    assert system_msg.role == "system"
    assert "JSON" in system_msg.text


@pytest.mark.asyncio
async def test_structured_output_strips_markdown_fences() -> None:
    model, _ = make_scripted_model(
        [assistant_text('```json\n{"title": "x", "score": 1}\n```')],
        capabilities=frozenset(),
    )
    agent = Agent(model, output_type=Report)
    result = await agent.arun("go")
    assert isinstance(result.output, Report)
    assert result.output.title == "x"


@pytest.mark.asyncio
async def test_structured_output_parse_error() -> None:
    model, _ = make_scripted_model(
        [assistant_text("this is not JSON at all.")],
        capabilities=frozenset(),
    )
    agent = Agent(model, output_type=Report)
    result = await agent.arun("go")
    assert result.stop_reason == "error"
    assert result.error is not None
    assert "parse" in result.error.lower() or "json" in result.error.lower()


# ---------------------------------------------------------------------------
# Handoff / Agent-as-Tool
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_can_be_used_as_a_tool() -> None:
    # The specialist just replies with plain text.
    specialist_model, _ = make_scripted_model([assistant_text("specialist answer")])
    specialist = Agent(specialist_model, name="specialist")
    as_tool = specialist.as_tool()

    assert as_tool.name == "transfer_to_specialist"
    assert "input" in as_tool.schema["properties"]

    from koala.core import RunContext

    result = await as_tool.run(RunContext(deps=None), {"input": "ping"})
    assert result == "specialist answer"


@pytest.mark.asyncio
async def test_handoff_end_to_end() -> None:
    # Specialist agent that answers directly.
    specialist_model, _ = make_scripted_model([assistant_text("specialist speaks")])
    specialist = Agent(specialist_model, name="specialist")

    # Triage agent that hands off to the specialist.
    triage_model, triage_provider = make_scripted_model(
        [
            assistant_tool_call(
                "transfer_to_specialist",
                {"input": "please handle"},
                call_id="c1",
            ),
            assistant_text("Specialist said: specialist speaks"),
        ]
    )
    triage = Agent(triage_model, handoffs=[specialist], name="triage")

    result = await triage.arun("please help")
    assert "specialist speaks" in result.output
    # transfer_to_specialist should appear in the tools payload
    tools_payload = triage_provider.calls[0]["tools"]
    tool_names = {t["function"]["name"] for t in tools_payload}
    assert "transfer_to_specialist" in tool_names


# ---------------------------------------------------------------------------
# L1 Runnable conformance
# ---------------------------------------------------------------------------


def test_agent_satisfies_runnable_protocol() -> None:
    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model)
    assert isinstance(agent, Runnable)


@pytest.mark.asyncio
async def test_astream_event_sequence_for_simple_run() -> None:
    from koala.core import RunContext, acollect

    model, _ = make_scripted_model([assistant_text("hello")])
    agent = Agent(model, name="unit")
    events = await acollect(agent, RunContext(deps=None), "hi")

    kinds = [e.kind for e in events]
    # Sequence: start, [model events], output, done
    assert kinds[0] == "start"
    assert kinds[-1] == "done"
    assert "output" in kinds
    assert isinstance(events[0], Start)
    assert isinstance(events[-1], Done)
    output_events = [e for e in events if isinstance(e, Output)]
    assert len(output_events) == 1
    assert output_events[0].value == "hello"


@pytest.mark.asyncio
async def test_astream_event_sequence_with_tool_call() -> None:
    from koala.core import RunContext, acollect

    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 1, "b": 2}, call_id="c1"),
            assistant_text("done"),
        ]
    )
    agent = Agent(model, tools=[add])
    events = await acollect(agent, RunContext(deps=None), "compute")

    # Should contain ToolCall and ToolResult events
    assert any(isinstance(e, ToolCall) for e in events)
    assert any(isinstance(e, ToolResult) for e in events)
    # And two ModelMessage events (one per turn)
    assert sum(1 for e in events if isinstance(e, ModelMessage)) == 2


@pytest.mark.asyncio
async def test_usage_accumulates_in_run_context() -> None:
    from koala.core import RunContext

    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 1, "b": 2}, call_id="c1"),
            assistant_text("done"),
        ]
    )
    agent = Agent(model, tools=[add])
    ctx: RunContext[None] = RunContext(deps=None)
    await agent.arun("compute", ctx=ctx)

    # Two model calls, each contributes usage.
    assert ctx.usage.requests == 2
    assert ctx.usage.input_tokens == 10  # 5 per call
    assert ctx.usage.output_tokens == 10


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancellation_stops_the_loop() -> None:
    from koala.core import RunContext

    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    # Model that would keep calling the tool
    model, _ = make_scripted_model(
        [assistant_tool_call("add", {"a": 1, "b": 1}, call_id=f"c{i}") for i in range(10)]
    )
    agent = Agent(model, tools=[add], max_iterations=100)
    ctx: RunContext[None] = RunContext(deps=None)
    ctx.cancel.cancel()  # cancel before starting
    result = await agent.arun("go", ctx=ctx)

    assert result.stop_reason == "cancelled"


# ---------------------------------------------------------------------------
# ScriptedProvider sanity check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scripted_provider_records_calls_and_forwards_tools() -> None:
    model, provider = make_scripted_model([assistant_text("ok")])
    agent = Agent(model)
    await agent.arun("hi")
    assert isinstance(provider, ScriptedProvider)
    assert len(provider.calls) == 1
    assert provider.calls[0]["model_name"] == "test-model"
