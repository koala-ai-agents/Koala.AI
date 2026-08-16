"""OpenAI live-provider smoke test.

Runs a small Agent + structured output against a real OpenAI endpoint.
Skipped unless ``OPENAI_API_KEY`` or ``LLM_API_KEY`` is set.
"""

from __future__ import annotations

import os

import pytest
from pydantic import BaseModel

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")),
        reason="no OPENAI_API_KEY / LLM_API_KEY set",
    ),
]


def test_openai_agent_hello_world() -> None:
    from koala import Agent

    agent = Agent(
        "openai/gpt-4o-mini",
        instructions="Reply with a single short sentence.",
    )
    result = agent.run("Say hi.")
    assert result.stop_reason == "final_output"
    assert isinstance(result.output, str)
    assert result.usage.output_tokens > 0


def test_openai_structured_output() -> None:
    from koala import Agent

    class Coord(BaseModel):
        city: str
        country: str

    agent = Agent(
        "openai/gpt-4o-mini",
        instructions="Return the city + country only.",
        output_type=Coord,
    )
    result = agent.run("Where is the Eiffel Tower located?")
    assert isinstance(result.output, Coord)
    assert result.output.country.lower().startswith("fra")
