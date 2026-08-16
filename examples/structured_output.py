"""Structured output — validate the final answer against a Pydantic model.

Set ``output_type=`` to a ``BaseModel`` subclass. Koala uses the provider's
native ``response_format`` when the model supports it, and falls back to a
prompt-engineered JSON-schema hint otherwise. Either way, ``result.output``
is a validated instance of the model.

Run::

    uv run examples/structured_output.py
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from koala import Agent


class MovieRec(BaseModel):
    title: str = Field(description="The movie title.")
    year: int = Field(description="Release year.")
    genre: str = Field(description="Primary genre.")
    reason: str = Field(description="Why the user would like it.")


def main() -> None:
    agent = Agent(
        "groq/llama-3.3-70b-versatile",
        instructions="Recommend one movie based on the user's taste.",
        output_type=MovieRec,
    )
    result = agent.run("I like slow-burn sci-fi.")
    rec: MovieRec = result.output  # already validated
    print(f"{rec.title} ({rec.year}) — {rec.genre}")
    print(rec.reason)


if __name__ == "__main__":
    main()
