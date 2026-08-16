"""Tests for koala.core.events."""

from __future__ import annotations

import dataclasses

import pytest

from koala.core import (
    AwaitingApproval,
    Done,
    Error,
    Message,
    ModelDelta,
    ModelMessage,
    Output,
    Start,
    ToolCall,
    ToolCallBlock,
    ToolResult,
    ToolResultBlock,
    Usage,
    UsageEvent,
)


def test_every_event_has_a_unique_kind_discriminator():
    events = [
        Start(run_id="r", name="test"),
        ModelDelta(text="hi"),
        ModelMessage(message=Message.assistant("hi")),
        ToolCall(call=ToolCallBlock(id="1", name="f")),
        ToolResult(result=ToolResultBlock(tool_call_id="1", content="ok")),
        AwaitingApproval(
            call=ToolCallBlock(id="1", name="rm"), request_id="a1"
        ),
        UsageEvent(usage=Usage(input_tokens=10)),
        Output(value=42),
        Error(error="boom"),
        Done(run_id="r"),
    ]
    kinds = [e.kind for e in events]
    assert len(kinds) == len(set(kinds))


def test_error_defaults_to_fatal():
    assert Error(error="x").fatal is True


def test_events_are_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        Output(value=1).value = 2  # type: ignore[misc]


def test_awaiting_approval_reason_default_empty():
    e = AwaitingApproval(
        call=ToolCallBlock(id="1", name="rm"), request_id="a1"
    )
    assert e.reason == ""
