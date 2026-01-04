"""Reference Agent B: Human-in-the-loop review workflow.

Pipeline:
- summarizer(text) -> summary
- assign_reviewer(summary) -> reviewer
- approve_step(summary, reviewer) -> approved summary (simulated human step)

Demonstrates a simple serial workflow using Kola DAG.
"""

from __future__ import annotations

from typing import Any, Dict

from koala.flow import LocalExecutor, dag
from koala.tools import default_registry, tool


@tool("summarizer")
def summarizer(text: str) -> str:
    # trivial summarizer: take first 10 words
    return " ".join(text.split()[:10])


@tool("assign_reviewer")
def assign_reviewer(summary: str) -> Dict[str, Any]:
    # simple round-robin stub
    return {"reviewer": "alice", "summary": summary}


@tool("approve_step")
def approve_step(payload: Dict[str, Any], approved: bool = True) -> Dict[str, Any]:
    # simulate human approval
    return {
        "approved": approved,
        "by": payload["reviewer"],
        "summary": payload["summary"],
    }


def build_flow(text: str):
    return (
        dag("agent-b")
        .step("sum", "summarizer", text=text)
        .step("assign", "assign_reviewer", summary="$result.sum")
        .step("approve", "approve_step", payload="$result.assign", approved=True)
        # Explicit dependencies
        .edge("sum", "assign")
        .edge("assign", "approve")
        .build()
    )


def main():
    flow = build_flow(
        "This is a long document that we want to summarize for human review."
    )
    ex = LocalExecutor()
    registry = {name: meta.func for name, meta in default_registry._tools.items()}
    res = ex.run_dagflow(flow, registry)
    print("Agent B result:", res)


if __name__ == "__main__":
    main()
