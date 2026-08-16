"""Tests for koala.core.messages."""

from __future__ import annotations

import dataclasses

import pytest

from koala.core import (
    ImageBlock,
    Message,
    TextBlock,
    ThinkingBlock,
    ToolCallBlock,
    ToolResultBlock,
)


def test_user_helper_wraps_text_in_text_block():
    m = Message.user("hello")
    assert m.role == "user"
    assert len(m.content) == 1
    assert isinstance(m.content[0], TextBlock)
    assert m.content[0].text == "hello"


def test_system_and_assistant_helpers():
    assert Message.system("be nice").role == "system"
    assert Message.assistant("hi").role == "assistant"


def test_tool_helper_builds_tool_result_message():
    m = Message.tool("call_abc", "sunny")
    assert m.role == "tool"
    assert isinstance(m.content[0], ToolResultBlock)
    assert m.content[0].tool_call_id == "call_abc"
    assert m.content[0].content == "sunny"


def test_text_property_concatenates_only_text_blocks():
    m = Message(
        role="assistant",
        content=[
            TextBlock("part 1 "),
            ThinkingBlock("hidden reasoning"),
            TextBlock("part 2"),
        ],
    )
    assert m.text == "part 1 part 2"


def test_tool_calls_property_filters_correctly():
    call = ToolCallBlock(id="c1", name="add", arguments={"a": 1})
    m = Message(role="assistant", content=[TextBlock("let me compute"), call])
    assert m.tool_calls == [call]


def test_message_is_frozen():
    m = Message.user("x")
    with pytest.raises(dataclasses.FrozenInstanceError):
        m.role = "assistant"  # type: ignore[misc]


def test_all_blocks_have_distinct_kind_discriminators():
    kinds = {
        TextBlock(text="").kind,
        ThinkingBlock(text="").kind,
        ImageBlock(source="x").kind,
        ToolCallBlock(id="", name="").kind,
        ToolResultBlock(tool_call_id="", content="").kind,
    }
    assert len(kinds) == 5


def test_image_block_defaults_to_png():
    assert ImageBlock(source="u").media_type == "image/png"


def test_tool_call_arguments_default_to_empty_dict():
    tc = ToolCallBlock(id="c", name="f")
    assert tc.arguments == {}
