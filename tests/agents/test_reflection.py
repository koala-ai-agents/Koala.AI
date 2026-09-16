from __future__ import annotations

import pytest
from pydantic import BaseModel, Field

from koala.agents.agent import Agent
from koala.core.messages import Message, TextBlock
from koala.models.base import BaseProvider
from koala.models.model import Model


class UserProfile(BaseModel):
    name: str
    age: int = Field(gt=0)


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
        from koala.core.events import ModelMessage
        msg = self._turns[self._index]
        self._index += 1
        yield ModelMessage(message=msg)

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_structured_output_reflection_self_correction():
    # Turn 1: invalid JSON (bad age <= 0)
    turn1 = Message(role="assistant", content=[TextBlock(text='{"name": "Alice", "age": -5}')])
    # Turn 2: corrected JSON
    turn2 = Message(role="assistant", content=[TextBlock(text='{"name": "Alice", "age": 28}')])

    provider = ScriptedProvider([turn1, turn2])
    model = Model("custom/test", provider_instance=provider, base_url="http://test")
    agent = Agent(model, output_type=UserProfile, max_output_retries=2)

    result = await agent.arun("Get user profile")
    assert isinstance(result.output, UserProfile)
    assert result.output.name == "Alice"
    assert result.output.age == 28

    # Verify that a reflection feedback message was sent to the model
    user_msgs = [m for m in result.messages if m.role == "user"]
    assert len(user_msgs) == 2
    assert "Your response did not match the required schema" in user_msgs[1].text
    assert "age" in user_msgs[1].text


@pytest.mark.asyncio
async def test_structured_output_reflection_exhausts_retries():
    # 3 turns of bad JSON
    turn1 = Message(role="assistant", content=[TextBlock(text='invalid json 1')])
    turn2 = Message(role="assistant", content=[TextBlock(text='invalid json 2')])

    provider = ScriptedProvider([turn1, turn2])
    model = Model("custom/test", provider_instance=provider, base_url="http://test")
    agent = Agent(model, output_type=UserProfile, max_output_retries=1)

    result = await agent.arun("Get user profile")
    assert result.stop_reason == "error"
    assert "could not parse output" in (result.error or "")
