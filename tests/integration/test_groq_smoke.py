"""Groq live-provider smoke test.

Runs a small Agent + tool loop against a real Groq endpoint. Skipped unless
a Groq or universal ``LLM_API_KEY`` is set. Uses a cheap Llama model.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.getenv("GROQ_API_KEY") or os.getenv("LLM_API_KEY")),
        reason="no GROQ_API_KEY / LLM_API_KEY set",
    ),
]


def test_groq_agent_hello_world() -> None:
    from koala import Agent

    agent = Agent(
        "groq/llama-3.3-70b-versatile",
        instructions="Reply with a single short sentence.",
    )
    result = agent.run("Say hi in exactly three words.")
    assert result.stop_reason == "final_output"
    assert isinstance(result.output, str)
    assert len(result.output) > 0
    # Non-zero usage confirms this hit a real endpoint.
    assert result.usage.input_tokens > 0
    assert result.usage.output_tokens > 0


def test_groq_agent_with_tool_call() -> None:
    from koala import Agent, tool

    @tool
    def add(a: int, b: int) -> int:
        """Return a + b."""
        return a + b

    agent = Agent(
        "groq/llama-3.3-70b-versatile",
        instructions="Use tools for arithmetic. Answer briefly.",
        tools=[add],
    )
    result = agent.run("What is 7 plus 5? Use the add tool.")
    assert result.stop_reason == "final_output"
    # We expect the model to have taken at least one tool-call round-trip.
    assert result.iterations >= 2
    assert "12" in str(result.output)
