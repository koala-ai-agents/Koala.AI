from __future__ import annotations

import asyncio
import time

import pytest

from koala.agents.agent import Agent
from koala.core.errors import ModelRetry
from koala.core.messages import Message, TextBlock, ToolCallBlock
from koala.models.base import BaseProvider
from koala.models.model import Model
from koala.tools.function_tool import tool


class ScriptedProvider(BaseProvider):
    def __init__(self, turns: list[Message]) -> None:
        self.slug = "scripted"
        self.base_url = "http://scripted.test"
        self.capabilities = frozenset()
        self._turns = list(turns)
        self._index = 0

    async def chat(self, *args, **kwargs):
        msg = self._turns[self._index]
        self._index += 1
        from koala.core.types import Usage
        return msg, Usage()

    async def stream_chat(self, *args, **kwargs):
        from koala.core.events import ModelMessage, ToolCall
        msg = self._turns[self._index]
        self._index += 1
        for tc in msg.tool_calls:
            yield ToolCall(call=tc)
        yield ModelMessage(message=msg)

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_parallel_tool_execution_concurrency():
    execution_order = []

    @tool
    async def slow_tool_1() -> str:
        """Slow tool 1."""
        await asyncio.sleep(0.15)
        execution_order.append("tool_1")
        return "res1"

    @tool
    async def slow_tool_2() -> str:
        """Slow tool 2."""
        await asyncio.sleep(0.15)
        execution_order.append("tool_2")
        return "res2"

    turn1 = Message(
        role="assistant",
        content=[
            ToolCallBlock(id="call_1", name="slow_tool_1", arguments={}),
            ToolCallBlock(id="call_2", name="slow_tool_2", arguments={}),
        ],
    )
    turn2 = Message(role="assistant", content=[TextBlock(text="done")])

    provider = ScriptedProvider([turn1, turn2])
    model = Model("custom/test", provider_instance=provider, base_url="http://test")
    agent = Agent(model, tools=[slow_tool_1, slow_tool_2], parallel_tools=True)

    start = time.perf_counter()
    result = await agent.arun("start")
    elapsed = time.perf_counter() - start

    assert result.output == "done"
    # Concurrent: ~0.15s, not sequential ~0.30s
    assert elapsed < 0.28
    assert len(execution_order) == 2

    # Verify message sequence order matches call_1, call_2
    tool_msgs = [m for m in result.messages if m.role == "tool"]
    assert len(tool_msgs) == 2
    assert tool_msgs[0].content[0].tool_call_id == "call_1"
    assert tool_msgs[1].content[0].tool_call_id == "call_2"


@pytest.mark.asyncio
async def test_parallel_tool_error_isolation():
    @tool
    async def failing_tool() -> str:
        """Fails."""
        raise RuntimeError("boom")

    @tool
    async def ok_tool() -> str:
        """Succeeds."""
        return "ok"

    turn1 = Message(
        role="assistant",
        content=[
            ToolCallBlock(id="call_fail", name="failing_tool", arguments={}),
            ToolCallBlock(id="call_ok", name="ok_tool", arguments={}),
        ],
    )
    turn2 = Message(role="assistant", content=[TextBlock(text="recovered")])

    provider = ScriptedProvider([turn1, turn2])
    model = Model("custom/test", provider_instance=provider, base_url="http://test")
    agent = Agent(model, tools=[failing_tool, ok_tool], parallel_tools=True)

    result = await agent.arun("run")
    assert result.output == "recovered"
    tool_msgs = [m for m in result.messages if m.role == "tool"]
    assert len(tool_msgs) == 2
    assert tool_msgs[0].content[0].is_error is True
    assert "boom" in str(tool_msgs[0].content[0].content)
    assert tool_msgs[1].content[0].is_error is False
    assert tool_msgs[1].content[0].content == "ok"


@pytest.mark.asyncio
async def test_tool_model_retry_exception():
    @tool
    def validate_code(code: str) -> str:
        """Validate code."""
        if not code.startswith("KOALA-"):
            raise ModelRetry("Code must begin with 'KOALA-' prefix")
        return "valid"

    turn1 = Message(
        role="assistant",
        content=[
            ToolCallBlock(id="call_val", name="validate_code", arguments={"code": "123"}),
        ],
    )
    turn2 = Message(role="assistant", content=[TextBlock(text="fixed")])

    provider = ScriptedProvider([turn1, turn2])
    model = Model("custom/test", provider_instance=provider, base_url="http://test")
    agent = Agent(model, tools=[validate_code])

    result = await agent.arun("test")
    assert result.output == "fixed"
    tool_msgs = [m for m in result.messages if m.role == "tool"]
    assert len(tool_msgs) == 1
    assert tool_msgs[0].content[0].is_error is True
    assert "Tool requested retry: Code must begin with 'KOALA-' prefix" in str(tool_msgs[0].content[0].content)
