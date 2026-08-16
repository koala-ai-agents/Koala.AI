"""Tests for koala.memory backends — InMemoryMemory and SQLiteMemory.

The same test suite runs against both backends via parametrization so the
contract stays honest.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pytest

from koala.core import (
    ImageBlock,
    Message,
    TextBlock,
    ThinkingBlock,
    ToolCallBlock,
    ToolResultBlock,
)
from koala.memory import BaseMemory, InMemoryMemory, SQLiteMemory

# ---------------------------------------------------------------------------
# Backend fixtures — every test runs on both InMemory + SQLite (in-memory DB).
# ---------------------------------------------------------------------------


BackendFactory = Callable[[], BaseMemory]


def _in_memory_factory() -> BaseMemory:
    return InMemoryMemory()


def _sqlite_factory() -> BaseMemory:
    return SQLiteMemory(":memory:")


BACKENDS: list[tuple[str, BackendFactory]] = [
    ("in_memory", _in_memory_factory),
    ("sqlite", _sqlite_factory),
]


@pytest.fixture(
    params=BACKENDS, ids=[name for name, _ in BACKENDS]
)
def backend(request) -> BaseMemory:  # type: ignore[no-untyped-def]
    _, factory = request.param
    return factory()


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_session_returns_empty_list(backend: BaseMemory) -> None:
    assert await backend.get("nobody") == []


@pytest.mark.asyncio
async def test_append_then_get_roundtrips(backend: BaseMemory) -> None:
    msgs = [Message.user("hi"), Message.assistant("hello")]
    await backend.append("s1", msgs)
    result = await backend.get("s1")
    assert [m.role for m in result] == ["user", "assistant"]
    assert result[0].text == "hi"
    assert result[1].text == "hello"


@pytest.mark.asyncio
async def test_append_preserves_order(backend: BaseMemory) -> None:
    await backend.append("s1", [Message.user("one")])
    await backend.append("s1", [Message.assistant("two")])
    await backend.append("s1", [Message.user("three")])
    result = await backend.get("s1")
    assert [m.text for m in result] == ["one", "two", "three"]


@pytest.mark.asyncio
async def test_get_limit_returns_last_n(backend: BaseMemory) -> None:
    for i in range(5):
        await backend.append("s1", [Message.user(f"msg{i}")])
    tail = await backend.get("s1", limit=2)
    assert [m.text for m in tail] == ["msg3", "msg4"]


@pytest.mark.asyncio
async def test_get_limit_zero_returns_empty(backend: BaseMemory) -> None:
    await backend.append("s1", [Message.user("a"), Message.user("b")])
    assert await backend.get("s1", limit=0) == []


@pytest.mark.asyncio
async def test_get_limit_larger_than_history(backend: BaseMemory) -> None:
    await backend.append("s1", [Message.user("only")])
    assert len(await backend.get("s1", limit=99)) == 1


@pytest.mark.asyncio
async def test_sessions_are_isolated(backend: BaseMemory) -> None:
    await backend.append("a", [Message.user("in a")])
    await backend.append("b", [Message.user("in b")])
    assert (await backend.get("a"))[0].text == "in a"
    assert (await backend.get("b"))[0].text == "in b"


@pytest.mark.asyncio
async def test_clear_removes_only_the_target_session(
    backend: BaseMemory,
) -> None:
    await backend.append("keep", [Message.user("stays")])
    await backend.append("kill", [Message.user("goes")])
    await backend.clear("kill")
    assert await backend.get("kill") == []
    assert (await backend.get("keep"))[0].text == "stays"


@pytest.mark.asyncio
async def test_clear_unknown_session_is_noop(backend: BaseMemory) -> None:
    # Must not raise
    await backend.clear("never-created")


@pytest.mark.asyncio
async def test_sessions_lists_known_ids_sorted(backend: BaseMemory) -> None:
    await backend.append("charlie", [Message.user("c")])
    await backend.append("alpha", [Message.user("a")])
    await backend.append("bravo", [Message.user("b")])
    assert await backend.sessions() == ["alpha", "bravo", "charlie"]


@pytest.mark.asyncio
async def test_append_empty_list_is_noop(backend: BaseMemory) -> None:
    await backend.append("s1", [])
    assert await backend.get("s1") == []


@pytest.mark.asyncio
async def test_returned_list_is_not_shared_reference(
    backend: BaseMemory,
) -> None:
    """Mutating the returned list must not corrupt the internal store."""
    await backend.append("s1", [Message.user("initial")])
    history = await backend.get("s1")
    history.append(Message.user("SHOULD_NOT_STICK"))
    fresh = await backend.get("s1")
    assert [m.text for m in fresh] == ["initial"]


# ---------------------------------------------------------------------------
# Content-block preservation — every koala block variant must round-trip.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_content_block_variants_roundtrip(
    backend: BaseMemory,
) -> None:
    rich = Message(
        role="assistant",
        content=[
            TextBlock(text="visible"),
            ThinkingBlock(text="reasoning"),
            ImageBlock(source="http://x/y.png", media_type="image/jpeg"),
            ToolCallBlock(id="c1", name="add", arguments={"a": 1, "b": 2}),
        ],
    )
    tool_reply = Message(
        role="tool",
        content=[
            ToolResultBlock(
                tool_call_id="c1", content={"result": 3}, is_error=False
            )
        ],
    )
    await backend.append("s1", [rich, tool_reply])
    got = await backend.get("s1")
    assert len(got) == 2

    r = got[0]
    assert r.role == "assistant"
    assert isinstance(r.content[0], TextBlock)
    assert r.content[0].text == "visible"
    assert isinstance(r.content[1], ThinkingBlock)
    assert r.content[1].text == "reasoning"
    assert isinstance(r.content[2], ImageBlock)
    assert r.content[2].source == "http://x/y.png"
    assert r.content[2].media_type == "image/jpeg"
    assert isinstance(r.content[3], ToolCallBlock)
    assert r.content[3].id == "c1"
    assert r.content[3].arguments == {"a": 1, "b": 2}

    t = got[1]
    assert isinstance(t.content[0], ToolResultBlock)
    assert t.content[0].tool_call_id == "c1"
    assert t.content[0].content == {"result": 3}
    assert t.content[0].is_error is False


@pytest.mark.asyncio
async def test_message_name_field_roundtrips(backend: BaseMemory) -> None:
    msg = Message(
        role="user", content=[TextBlock(text="hi")], name="alice"
    )
    await backend.append("s1", [msg])
    got = await backend.get("s1")
    assert got[0].name == "alice"


# ---------------------------------------------------------------------------
# SQLite-specific: persistence across instances (same file)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sqlite_persists_across_instances(tmp_path: Path) -> None:
    db_path = str(tmp_path / "conv.db")

    m1 = SQLiteMemory(db_path)
    await m1.append("s1", [Message.user("first"), Message.assistant("hi")])
    await m1.close()

    # New instance pointing at the same file
    m2 = SQLiteMemory(db_path)
    got = await m2.get("s1")
    assert [x.text for x in got] == ["first", "hi"]
    await m2.close()


@pytest.mark.asyncio
async def test_sqlite_creates_parent_directory(tmp_path: Path) -> None:
    db_path = str(tmp_path / "nested" / "dirs" / "conv.db")
    m = SQLiteMemory(db_path)
    try:
        await m.append("s1", [Message.user("hi")])
        assert (tmp_path / "nested" / "dirs" / "conv.db").exists()
    finally:
        await m.close()
