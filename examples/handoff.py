"""Handoff — one agent delegates to another via the ``AgentTool`` wrapper.

Pattern: a coordinator agent decides which specialist to call. Specialists
are exposed as tools via the ``handoffs=`` kwarg, which internally wraps each
agent as a ``transfer_to_<name>`` tool.

Run::

    uv run examples/handoff.py
"""

from __future__ import annotations

from koala import Agent


def main() -> None:
    math_agent = Agent(
        "groq/llama-3.3-70b-versatile",
        name="math",
        instructions=(
            "You are a math specialist. Answer numerical questions concisely."
        ),
    )

    writer_agent = Agent(
        "groq/llama-3.3-70b-versatile",
        name="writer",
        instructions=(
            "You are a copywriter. Turn dry facts into a short, engaging line."
        ),
    )

    coordinator = Agent(
        "groq/llama-3.3-70b-versatile",
        name="coordinator",
        instructions=(
            "Route the user's request to the right specialist. "
            "Use `transfer_to_math` for calculations, `transfer_to_writer` "
            "for phrasing. Combine their outputs into the final answer."
        ),
        handoffs=[math_agent, writer_agent],
    )

    coordinator.show(
        "Compute 12 squared, then give me a punchy one-liner about the result."
    )


if __name__ == "__main__":
    main()
