"""Flow — DAG orchestration where each step is an Agent, a Tool, or a callable.

The ``Flow`` API accepts any ``Runnable`` (Agent, Tool, Model) or a plain
Python callable as a step action. Values in ``args=`` can reference the
top-level input via ``"$input.<key>"`` and prior step results via
``"$result.<step_id>"``.

Run::

    uv run examples/flow.py
"""

from __future__ import annotations

from koala import Agent, tool
from koala.orchestration import LocalExecutor, flow


@tool
def word_count(text: str) -> int:
    """Return the number of whitespace-separated tokens in ``text``."""
    return len(text.split())


def main() -> None:
    researcher = Agent(
        "groq/llama-3.3-70b-versatile",
        name="researcher",
        instructions="Write two crisp sentences of factual context on the topic.",
    )
    writer = Agent(
        "groq/llama-3.3-70b-versatile",
        name="writer",
        instructions="Turn the research into a single tweet-length line.",
    )

    pipeline = (
        flow("research-and-write")
        .step("research", researcher, input="$input.topic")
        .step("tweet", writer, input="$result.research")
        .step("length", word_count, text="$result.tweet")
        .edge("research", "tweet")
        .edge("tweet", "length")
        .build()
    )

    results = LocalExecutor().run(pipeline, input={"topic": "koalas"})
    print("--- tweet ---")
    print(results["tweet"])
    print(f"\n[word count: {results['length']}]")


if __name__ == "__main__":
    main()
