from __future__ import annotations

import pytest

from koala.agents.agent import Agent
from koala.agents.context_policy import ContextPolicy, estimate_tokens
from koala.core.messages import Message, TextBlock, ToolCallBlock, ToolResultBlock
from koala.models.base import BaseProvider
from koala.models.model import Model


def test_estimate_tokens():
    assert estimate_tokens("") == 0
    assert estimate_tokens("hi") == 1
    assert estimate_tokens("a" * 40) == 10


def test_context_policy_preserves_system_instructions():
    policy = ContextPolicy(max_turns=1, preserve_system=True)
    messages = [
        Message.system("System instructions"),
        Message.user("Turn 1 question"),
        Message.assistant("Turn 1 answer"),
        Message.user("Turn 2 question"),
        Message.assistant("Turn 2 answer"),
    ]
    pruned = policy.prune(messages)
    assert len(pruned) == 3
    assert pruned[0].role == "system"
    assert pruned[0].text == "System instructions"
    assert pruned[1].text == "Turn 2 question"
    assert pruned[2].text == "Turn 2 answer"


def test_context_policy_atomic_tool_call_pairing():
    # If a turn has an assistant calling a tool and a tool result, they must stay together!
    policy = ContextPolicy(max_turns=1)
    messages = [
        Message.system("System prompt"),
        Message.user("Turn 1"),
        Message(
            role="assistant",
            content=[ToolCallBlock(id="call_1", name="search", arguments={})],
        ),
        Message(
            role="tool",
            content=[ToolResultBlock(tool_call_id="call_1", content="search results")],
        ),
        Message.assistant("Turn 1 final answer"),
        Message.user("Turn 2"),
        Message(
            role="assistant",
            content=[ToolCallBlock(id="call_2", name="calc", arguments={})],
        ),
        Message(
            role="tool",
            content=[ToolResultBlock(tool_call_id="call_2", content="calc result")],
        ),
    ]
    pruned = policy.prune(messages)

    # Should keep Turn 2 atomic unit (assistant with tool call + tool result)
    assert pruned[0].role == "system"
    # Last atomic unit has call_2 and its tool result intact
    tool_msgs = [m for m in pruned if m.role == "tool"]
    assert len(tool_msgs) == 1
    assert tool_msgs[0].content[0].tool_call_id == "call_2"
    assistant_calls = [m for m in pruned if m.role == "assistant" and m.tool_calls]
    assert len(assistant_calls) == 1
    assert assistant_calls[0].tool_calls[0].id == "call_2"


def test_tool_output_truncation():
    policy = ContextPolicy(max_tool_output_chars=100)
    huge_text = "x" * 500
    messages = [
        Message(
            role="tool",
            content=[ToolResultBlock(tool_call_id="c1", content=huge_text)],
        )
    ]
    pruned = policy.prune(messages)
    content = pruned[0].content[0].content
    assert len(content) < 500
    assert "Output truncated" in content
    assert "400 characters omitted" in content


class ScriptedProvider(BaseProvider):
    def __init__(self, turns: list[Message]) -> None:
        self.slug = "scripted"
        self.base_url = "http://scripted.test"
        self.capabilities = frozenset()
        self._turns = list(turns)
        self._index = 0
        self.received_messages: list[list[Message]] = []

    async def chat(self, *args, **kwargs):
        msg = self._turns[self._index]
        self._index += 1
        from koala.core.types import Usage
        return msg, Usage()

    async def stream_chat(self, model_name, messages, *args, **kwargs):
        self.received_messages.append(list(messages))
        from koala.core.events import ModelMessage
        msg = self._turns[self._index]
        self._index += 1
        yield ModelMessage(message=msg)

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_agent_with_context_policy_prunes_prompt():
    turn1 = Message(role="assistant", content=[TextBlock(text="answer 1")])
    provider = ScriptedProvider([turn1])
    model = Model("custom/test", provider_instance=provider, base_url="http://test")

    policy = ContextPolicy(max_turns=1)
    agent = Agent(model, context_policy=policy)

    # Send 3 prior messages
    history = [
        Message.user("User old question"),
        Message.assistant("Assistant old answer"),
        Message.user("User latest question"),
    ]
    result = await agent.arun(history)
    assert result.output == "answer 1"

    # Verify that the provider received pruned messages (only latest user question, not old turn)
    sent = provider.received_messages[0]
    user_sent = [m for m in sent if m.role == "user"]
    assert len(user_sent) == 1
    assert user_sent[0].text == "User latest question"
