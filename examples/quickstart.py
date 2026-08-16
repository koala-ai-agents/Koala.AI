"""Quickstart — one agent, one tool, live-streamed to your terminal.

Requires an ``LLM_API_KEY`` env var (or a provider-specific key like
``GROQ_API_KEY``). Change the model ref below to any provider slug from
``koala.models.BUILTIN_PROVIDERS`` — openai, groq, deepseek, xai, together,
fireworks, openrouter, ollama, lmstudio, custom.

Run::

    uv run examples/quickstart.py
"""

from __future__ import annotations

from koala import Agent, tool


@tool
def add(a: int, b: int) -> int:
    """Add two integers and return the sum."""
    return a + b


def main() -> None:
    agent = Agent(
        "groq/llama-3.3-70b-versatile",
        instructions="You are a precise assistant. Use tools when helpful.",
        tools=[add],
    )
    # `.show(...)` streams deltas + tool calls live to stdout and returns the
    # final output value.
    agent.show("What is 7 plus 5?")


if __name__ == "__main__":
    main()
