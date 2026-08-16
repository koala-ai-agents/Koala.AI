"""Tests for Agent + Memory integration.

Uses the ScriptedProvider fixture from ``tests/agents/conftest.py`` to drive
deterministic agent runs while verifying that:
    - prior history from a session is loaded and prepended to the model call
    - new turns are persisted to memory on success
    - runs without memory don't interact with any store
    - session_id kwarg routes correctly and defaults sensibly
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make the agents conftest importable so we can reuse its scripted provider
_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))
from conftest import (  # noqa: E402
    assistant_text,
    assistant_tool_call,
    make_scripted_model,
)

from koala import Agent, tool  # noqa: E402
from koala.memory import InMemoryMemory, SQLiteMemory  # noqa: E402

# ---------------------------------------------------------------------------
# Basic memory loop — two turns in the same session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_second_turn_sees_prior_conversation() -> None:
    mem = InMemoryMemory()

    # Turn 1: user says name, assistant acknowledges
    model_1, provider_1 = make_scripted_model([assistant_text("Nice to meet you, Alice.")])
    agent_1 = Agent(model_1, memory=mem, name="a")
    result_1 = await agent_1.arun("My name is Alice.", session_id="conv-1")
    assert result_1.output == "Nice to meet you, Alice."

    # Turn 2: NEW agent instance sharing the same memory + session_id
    model_2, provider_2 = make_scripted_model([assistant_text("Your name is Alice.")])
    agent_2 = Agent(model_2, memory=mem, name="a")
    result_2 = await agent_2.arun("What's my name?", session_id="conv-1")

    # The second model call must have seen turn 1 in its messages
    sent = provider_2.calls[0]["messages"]
    joined = " ".join(m.text for m in sent if m.text)
    assert "Alice" in joined  # prior user message was replayed to the model
    assert "Nice to meet you" in joined  # prior assistant reply too
    assert result_2.output == "Your name is Alice."


@pytest.mark.asyncio
async def test_persisted_history_grows_with_each_turn() -> None:
    mem = InMemoryMemory()

    model, _ = make_scripted_model(
        [
            assistant_text("reply 1"),
            assistant_text("reply 2"),
            assistant_text("reply 3"),
        ]
    )
    agent = Agent(model, memory=mem, name="a")

    await agent.arun("first", session_id="s1")
    assert len(await mem.get("s1")) == 2  # user + assistant

    await agent.arun("second", session_id="s1")
    assert len(await mem.get("s1")) == 4

    await agent.arun("third", session_id="s1")
    stored = await mem.get("s1")
    assert len(stored) == 6
    # Order check
    assert [m.text for m in stored] == [
        "first",
        "reply 1",
        "second",
        "reply 2",
        "third",
        "reply 3",
    ]


@pytest.mark.asyncio
async def test_isolated_sessions_do_not_leak() -> None:
    mem = InMemoryMemory()
    model, _ = make_scripted_model(
        [assistant_text("in a"), assistant_text("in b")]
    )
    agent = Agent(model, memory=mem)

    await agent.arun("hello", session_id="A")
    await agent.arun("hello", session_id="B")

    a = await mem.get("A")
    b = await mem.get("B")
    assert [m.text for m in a] == ["hello", "in a"]
    assert [m.text for m in b] == ["hello", "in b"]


# ---------------------------------------------------------------------------
# Tool-calling round-trip is persisted intact
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_call_round_trip_is_persisted() -> None:
    mem = InMemoryMemory()

    @tool
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 2, "b": 3}, call_id="c1"),
            assistant_text("The answer is 5."),
        ]
    )
    agent = Agent(model, tools=[add], memory=mem)
    result = await agent.arun("what's 2 + 3?", session_id="s1")
    assert result.output == "The answer is 5."

    stored = await mem.get("s1")
    roles = [m.role for m in stored]
    # Persisted turn = [user, assistant(tool_call), tool_result, assistant(final)]
    assert roles == ["user", "assistant", "tool", "assistant"]


# ---------------------------------------------------------------------------
# Failure semantics: memory is NOT polluted when a run errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_run_does_not_persist() -> None:
    mem = InMemoryMemory()

    @tool
    def spin(x: int) -> int:
        """Never stop."""
        return x + 1

    # Model keeps looping tool calls -> hits max_iterations
    model, _ = make_scripted_model(
        [
            assistant_tool_call("spin", {"x": i}, call_id=f"c{i}")
            for i in range(10)
        ]
    )
    agent = Agent(model, tools=[spin], memory=mem, max_iterations=2)
    result = await agent.arun("run", session_id="s1")

    assert result.stop_reason == "max_iterations"
    # Memory should stay empty for a failed run
    assert await mem.get("s1") == []


# ---------------------------------------------------------------------------
# No-memory mode: agent works with no store, produces no side effects
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_without_memory_still_runs() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(model)  # no memory
    result = await agent.arun("hi", session_id="s1")
    assert result.output == "ok"
    # session_id is stashed in metadata for observability
    assert result.metadata["session_id"] == "s1"


# ---------------------------------------------------------------------------
# session_id defaults + result metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_id_stashed_in_result_metadata() -> None:
    mem = InMemoryMemory()
    model, _ = make_scripted_model([assistant_text("done")])
    agent = Agent(model, memory=mem)
    result = await agent.arun("hi", session_id="explicit-id")
    assert result.metadata["session_id"] == "explicit-id"


@pytest.mark.asyncio
async def test_no_session_id_generates_one() -> None:
    model, _ = make_scripted_model([assistant_text("done")])
    agent = Agent(model)
    result = await agent.arun("hi")
    # Auto-generated uuid — not empty
    assert result.metadata["session_id"]
    assert len(result.metadata["session_id"]) > 4


# ---------------------------------------------------------------------------
# SQLite backend actually persists across Agent instances
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sqlite_memory_persists_across_agent_instances(
    tmp_path: Path,
) -> None:
    db = str(tmp_path / "conv.db")

    # First agent writes a turn
    mem1 = SQLiteMemory(db)
    model1, _ = make_scripted_model([assistant_text("stored")])
    agent1 = Agent(model1, memory=mem1)
    await agent1.arun("remember me", session_id="s1")
    await mem1.close()

    # Second agent opens the same DB and can read the history
    mem2 = SQLiteMemory(db)
    model2, provider2 = make_scripted_model([assistant_text("I remember you.")])
    agent2 = Agent(model2, memory=mem2)
    result = await agent2.arun("do you remember me?", session_id="s1")

    # The prior turn's messages were fed to the second model call
    sent = provider2.calls[0]["messages"]
    joined = " ".join(m.text for m in sent if m.text)
    assert "remember me" in joined
    assert "stored" in joined
    assert result.output == "I remember you."
    await mem2.close()
