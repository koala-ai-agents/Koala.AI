"""Tests for koala.core.context."""

from __future__ import annotations

import asyncio

import pytest

from koala.core import CancelToken, RunContext


def test_run_context_default_deps_none():
    ctx: RunContext[None] = RunContext(deps=None)
    assert ctx.deps is None
    assert ctx.usage.total_tokens == 0
    assert ctx.session_id
    assert not ctx.cancel.cancelled
    assert ctx.metadata == {}


def test_run_context_carries_arbitrary_deps():
    class DB:
        pass

    db = DB()
    ctx = RunContext(deps=db)
    assert ctx.deps is db


def test_cancel_token_starts_uncancelled():
    tok = CancelToken()
    assert not tok.cancelled


def test_cancel_token_cancel_is_idempotent():
    tok = CancelToken()
    tok.cancel()
    tok.cancel()
    assert tok.cancelled


def test_cancel_check_raises_after_cancel():
    tok = CancelToken()
    tok.check()  # no-op when not cancelled
    tok.cancel()
    with pytest.raises(asyncio.CancelledError):
        tok.check()


@pytest.mark.asyncio
async def test_cancel_token_wait_resolves_on_cancel():
    tok = CancelToken()

    async def canceller() -> None:
        await asyncio.sleep(0.01)
        tok.cancel()

    task = asyncio.create_task(canceller())
    await tok.wait()
    assert tok.cancelled
    await task


def test_child_context_shares_usage_session_and_cancel():
    ctx: RunContext[str] = RunContext(deps="parent")
    ctx.usage.input_tokens = 42

    child = ctx.child(deps="child")

    assert child.deps == "child"
    assert child.usage is ctx.usage
    assert child.session_id == ctx.session_id
    assert child.cancel is ctx.cancel


def test_child_context_metadata_is_copied_not_shared():
    ctx = RunContext(deps=None, metadata={"tenant": "a"})
    child = ctx.child()
    child.metadata["tenant"] = "b"
    assert ctx.metadata["tenant"] == "a"


def test_child_inherits_parent_deps_when_not_overridden():
    ctx = RunContext(deps="parent-deps")
    child = ctx.child()
    assert child.deps == "parent-deps"


def test_cancellation_propagates_to_children():
    ctx = RunContext(deps=None)
    child = ctx.child()
    ctx.cancel.cancel()
    assert child.cancel.cancelled
