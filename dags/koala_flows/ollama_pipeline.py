"""Two-step Koala Flow that runs an Ollama-backed agent inside Airflow.

    summarize  ->  count

* ``summarize`` — an Ollama-backed ``Agent`` that produces a two-sentence
  brief for ``$input.topic``.
* ``count`` — a plain ``@tool`` that counts words in
  ``$result.summarize``.

Run this file directly to deploy + trigger + wait::

    uv run dags/koala_flows/ollama_pipeline.py
    uv run dags/koala_flows/ollama_pipeline.py "quantum computing"

Under the hood, ``Flow.deploy_to_airflow`` writes two artifacts:

* ``dags/koala_summarize.py`` — a ~30-line thin DAG file that imports
  :func:`koala.orchestration.airflow_runtime.run_step` and calls it from
  every ``PythonOperator``. No Agent construction at DAG-parse time.
* ``dags/koala_specs/summarize.json`` — the flow shape: step ids, action
  refs, args, edges, tags. Read by the DAG file each parse cycle.

When the Koala package version on your Airflow workers is bumped, the
runtime picks up the new behaviour; deployed DAG files rarely need
regeneration.

Environment
-----------

* ``OLLAMA_BASE_URL`` — where to reach Ollama. Falls back to
  ``http://localhost:11434/v1``. The shipped ``docker-compose.yaml`` sets
  ``http://host.docker.internal:11434/v1`` on every Airflow container.
* ``KOALA_OLLAMA_MODEL`` — model tag. Defaults to ``qwen2.5:7b``.
"""

from __future__ import annotations

import os
import sys

from koala import Agent, Model, tool
from koala.orchestration import flow

# ---------------------------------------------------------------------------
# Agent + tool. Module-level so both host and workers resolve the same
# import path via ``koala_flows.ollama_pipeline:summarizer``.
# ---------------------------------------------------------------------------

summarizer = Agent(
    Model(
        f"ollama/{os.getenv('KOALA_OLLAMA_MODEL', 'qwen2.5:7b')}",
        temperature=0.2,
    ),
    name="summarizer",
    instructions=(
        "You are a concise research assistant. Given a topic, produce "
        "exactly two sentences of factual context. No preamble, no lists."
    ),
)


@tool
def word_count(text: str) -> int:
    """Return the number of whitespace-separated tokens in ``text``.

    Args:
        text: The passage to measure.
    """
    return len(text.split())


# ---------------------------------------------------------------------------
# Flow. Builder-only work — no HTTP, no LLM calls at import time.
# ---------------------------------------------------------------------------

pipeline = (
    flow("summarize")
    .step("summarize", summarizer, input="$input.topic")
    .step("count",     word_count, text="$result.summarize")
    .edge("summarize", "count")
    .build()
)


# ---------------------------------------------------------------------------
# Deploy + trigger when run directly.
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    topic = sys.argv[1] if len(sys.argv) > 1 else "koalas"

    # Local iteration — runs in this Python process. Fast feedback loop, no
    # Docker, no scheduler waits, no auth. Use this while you're building
    # and debugging your flow.
    results = pipeline.run(input={"topic": topic})

    # When the flow is stable and you want production scheduling / retries
    # / distributed execution, swap the line above for:
    #
    #     results = pipeline.deploy_to_airflow(
    #         input={"topic": topic},
    #         timeout=600.0,
    #     )
    #
    # Same Flow definition, same Agents, same tools. Only the executor
    # changes.

    print(f"\n=== topic: {topic!r} ===")
    print("\n[summarize]")
    print(results["summarize"])
    print(f"\n[count] {results['count']} words\n")
