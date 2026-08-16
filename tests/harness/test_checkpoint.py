"""Tests for koala.harness.checkpoint — Checkpointer backends + resume flow."""

from __future__ import annotations

import asyncio

import pytest
from tests.agents.conftest import (
    assistant_text,
    assistant_tool_call,
    make_scripted_model,
)

from koala import Agent, tool
from koala.core.messages import Message, ToolCallBlock
from koala.harness.checkpoint import (
    Checkpoint,
    InMemoryCheckpointer,
    PendingApproval,
    SQLiteCheckpointer,
    _checkpoint_from_json,
    _checkpoint_to_json,
)
from koala.harness.session import AgentSession
from koala.memory import InMemoryMemory
from koala.tools.approval import RequireApprovalFor

# ---------------------------------------------------------------------------
# Serialization round-trip
# ---------------------------------------------------------------------------


def test_checkpoint_json_round_trip_preserves_messages_and_approvals() -> None:
    original = Checkpoint(
        session_id="s-1",
        messages=[
            Message.user("hi"),
            Message.assistant("hello there"),
        ],
        pending_approvals=[
            PendingApproval(
                request_id="req-1",
                tool_call=ToolCallBlock(
                    id="c1", name="pay", arguments={"amount": 42}
                ),
                reason="requires human review",
            )
        ],
        metadata={"user_id": "u-42"},
    )
    payload = _checkpoint_to_json(original)
    restored = _checkpoint_from_json(payload)

    assert restored.session_id == "s-1"
    assert len(restored.messages) == 2
    assert restored.messages[0].text == "hi"
    assert restored.messages[1].text == "hello there"
    assert len(restored.pending_approvals) == 1
    p = restored.pending_approvals[0]
    assert p.request_id == "req-1"
    assert p.tool_call.name == "pay"
    assert p.tool_call.arguments == {"amount": 42}
    assert p.reason == "requires human review"
    assert restored.metadata == {"user_id": "u-42"}


# ---------------------------------------------------------------------------
# InMemoryCheckpointer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_memory_save_and_load_round_trip() -> None:
    cp = InMemoryCheckpointer()
    checkpoint = Checkpoint(
        session_id="s-1", messages=[Message.user("hi")]
    )
    await cp.save(checkpoint)

    loaded = await cp.load("s-1")
    assert loaded is not None
    assert loaded.session_id == "s-1"
    assert loaded.messages[0].text == "hi"


@pytest.mark.asyncio
async def test_in_memory_load_unknown_returns_none() -> None:
    cp = InMemoryCheckpointer()
    assert await cp.load("nope") is None


@pytest.mark.asyncio
async def test_in_memory_delete_is_idempotent() -> None:
    cp = InMemoryCheckpointer()
    await cp.save(Checkpoint(session_id="s-1"))
    await cp.delete("s-1")
    await cp.delete("s-1")  # second call must not raise
    assert await cp.load("s-1") is None


@pytest.mark.asyncio
async def test_in_memory_sessions_returns_sorted() -> None:
    cp = InMemoryCheckpointer()
    await cp.save(Checkpoint(session_id="c"))
    await cp.save(Checkpoint(session_id="a"))
    await cp.save(Checkpoint(session_id="b"))
    assert await cp.sessions() == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_in_memory_preserves_created_at_across_saves() -> None:
    cp = InMemoryCheckpointer()
    c1 = Checkpoint(session_id="s-1", messages=[Message.user("a")])
    await cp.save(c1)
    first_created = c1.created_at

    # Small sleep so timestamps differ
    await asyncio.sleep(0.01)
    c2 = Checkpoint(session_id="s-1", messages=[Message.user("b")])
    await cp.save(c2)

    loaded = await cp.load("s-1")
    assert loaded is not None
    assert loaded.created_at == first_created
    assert loaded.updated_at >= loaded.created_at
    assert loaded.messages[0].text == "b"  # second save overwrites


# ---------------------------------------------------------------------------
# SQLiteCheckpointer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sqlite_save_load_persists_across_new_instances(tmp_path) -> None:
    path = str(tmp_path / "koala_ck.db")
    cp1 = SQLiteCheckpointer(path)
    await cp1.save(
        Checkpoint(
            session_id="s-1",
            messages=[Message.user("hello"), Message.assistant("hi!")],
            pending_approvals=[
                PendingApproval(
                    request_id="r1",
                    tool_call=ToolCallBlock(id="c1", name="pay"),
                    reason="",
                )
            ],
        )
    )
    await cp1.close()

    # Reopen — different Python object, same file.
    cp2 = SQLiteCheckpointer(path)
    loaded = await cp2.load("s-1")
    assert loaded is not None
    assert loaded.session_id == "s-1"
    assert len(loaded.messages) == 2
    assert loaded.messages[1].text == "hi!"
    assert loaded.pending_approvals[0].tool_call.name == "pay"
    await cp2.close()


@pytest.mark.asyncio
async def test_sqlite_delete_removes_row(tmp_path) -> None:
    cp = SQLiteCheckpointer(str(tmp_path / "ck.db"))
    await cp.save(Checkpoint(session_id="x"))
    await cp.delete("x")
    assert await cp.load("x") is None
    assert await cp.sessions() == []
    await cp.close()


@pytest.mark.asyncio
async def test_sqlite_in_memory_path_works() -> None:
    cp = SQLiteCheckpointer(":memory:")
    await cp.save(Checkpoint(session_id="s"))
    loaded = await cp.load("s")
    assert loaded is not None
    await cp.close()


# ---------------------------------------------------------------------------
# AgentSession integration — checkpoint after turn
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_saves_checkpoint_after_successful_turn() -> None:
    cp = InMemoryCheckpointer()
    memory = InMemoryMemory()
    model, _ = make_scripted_model([assistant_text("hi there")])
    agent = Agent(model, memory=memory)

    async with agent.session(
        session_id="conv-1", checkpointer=cp
    ) as s:
        await s.send("hello")
        async for ev in s.events():
            from koala.core import Done

            if isinstance(ev, Done):
                break

    loaded = await cp.load("conv-1")
    assert loaded is not None
    assert loaded.session_id == "conv-1"
    # Memory captured user + assistant messages; checkpoint mirrors memory.
    assert len(loaded.messages) >= 2
    assert any(m.role == "user" and m.text == "hello" for m in loaded.messages)
    assert any(
        m.role == "assistant" and m.text == "hi there" for m in loaded.messages
    )


@pytest.mark.asyncio
async def test_session_checkpoint_captures_pending_approvals() -> None:
    """When a turn ends with a still-pending approval, the checkpoint keeps it."""
    cp = InMemoryCheckpointer()

    @tool
    def pay(amount: int) -> str:
        """Charge."""
        return "paid"

    model, _ = make_scripted_model(
        [
            assistant_tool_call("pay", {"amount": 99}, call_id="req-1"),
            assistant_text("done"),
        ]
    )
    rules = [RequireApprovalFor(names=frozenset({"pay"}))]
    agent = Agent(model, tools=[pay], approval_rules=rules)

    # Very short timeout so the approval times out and the turn ends with a
    # denied tool call. The AwaitingApproval event was still emitted, so
    # session._pending_approvals gets populated during that turn.
    async with agent.session(
        session_id="conv-2",
        checkpointer=cp,
        approval_timeout=0.02,
    ) as s:
        await s.send("try")
        async for ev in s.events():
            from koala.core import Done

            if isinstance(ev, Done):
                break

    loaded = await cp.load("conv-2")
    assert loaded is not None
    # At least one AwaitingApproval was recorded during the turn.
    assert len(loaded.pending_approvals) >= 1
    p = loaded.pending_approvals[0]
    assert p.tool_call.name == "pay"
    assert p.tool_call.arguments == {"amount": 99}


# ---------------------------------------------------------------------------
# resume()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_restores_pending_approvals() -> None:
    cp = InMemoryCheckpointer()

    # Manually seed a checkpoint with a pending approval — simulate a prior
    # process that died mid-approval.
    await cp.save(
        Checkpoint(
            session_id="s-42",
            messages=[Message.user("charge me")],
            pending_approvals=[
                PendingApproval(
                    request_id="prior-req",
                    tool_call=ToolCallBlock(
                        id="c-prior", name="pay", arguments={"amount": 12}
                    ),
                    reason="held over",
                )
            ],
        )
    )

    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(model)

    resumed = await AgentSession.resume(agent, "s-42", cp)
    assert resumed.session_id == "s-42"
    assert len(resumed.pending_approvals) == 1
    assert resumed.pending_approvals[0].tool_call.name == "pay"


@pytest.mark.asyncio
async def test_resume_missing_session_raises_key_error() -> None:
    cp = InMemoryCheckpointer()
    model, _ = make_scripted_model([assistant_text("noop")])
    agent = Agent(model)
    with pytest.raises(KeyError):
        await AgentSession.resume(agent, "never-saved", cp)


@pytest.mark.asyncio
async def test_reply_approval_clears_pending_map() -> None:
    """reply_approval must remove the entry from session.pending_approvals."""

    @tool
    def pay(amount: int) -> str:
        """Charge."""
        return "paid"

    model, _ = make_scripted_model(
        [
            assistant_tool_call("pay", {"amount": 1}, call_id="pa-1"),
            assistant_text("done"),
        ]
    )
    rules = [RequireApprovalFor(names=frozenset({"pay"}))]
    agent = Agent(model, tools=[pay], approval_rules=rules)

    from koala.core import AwaitingApproval, Done

    async with agent.session(approval_timeout=5.0) as s:
        await s.send("try")
        async for e in s.events():
            if isinstance(e, AwaitingApproval):
                # Before reply, pending map has an entry.
                assert any(
                    p.request_id == e.request_id for p in s.pending_approvals
                )
                await s.reply_approval(e.request_id, "allow")
                # After reply, entry is gone.
                assert not any(
                    p.request_id == e.request_id for p in s.pending_approvals
                )
            elif isinstance(e, Done):
                break


@pytest.mark.asyncio
async def test_session_without_checkpointer_still_works() -> None:
    """The checkpointer is optional — everything must work when omitted."""
    from koala.core import Done

    model, _ = make_scripted_model([assistant_text("hi")])
    agent = Agent(model)

    async with agent.session() as s:
        assert s.checkpointer is None
        await s.send("hey")
        async for e in s.events():
            if isinstance(e, Done):
                break
