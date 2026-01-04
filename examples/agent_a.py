"""Reference Agent A: Data ingestion -> enrichment -> output.

Pipeline:
- downloader(url) -> bytes/text
- parser(content) -> records
- re_rank(records) -> sorted records
- output(records) -> prints or stores

Uses the Kola DAG flow to wire these steps.
"""

from __future__ import annotations

from typing import Any, Dict, List, Union

from koala.flow import LocalExecutor, dag
from koala.tools import default_registry, tool


@tool("downloader")
def downloader(url: str) -> str:
    # simple stub: return a fake page
    return (
        f"<html><body><h1>Example for {url}</h1><p>alpha beta gamma</p></body></html>"
    )


@tool("parser")
def parser(html: str) -> List[Dict[str, Any]]:
    # naive parser: extract words
    words = [w.strip("<>/ ") for w in html.split() if w.isalpha()]
    return [{"word": w, "score": len(w)} for w in words]


@tool("re_rank")
def re_rank(
    items: List[Union[Dict[str, Any], str]], top_k: int = 5
) -> List[Dict[str, Any]]:
    # Normalize inputs: allow a list of strings or dicts with score
    normalized: List[Dict[str, Any]] = []
    for it in items:
        if isinstance(it, dict) and "score" in it:
            normalized.append(it)
        elif isinstance(it, str):
            normalized.append({"word": it, "score": len(it)})
        else:
            # skip unknown types
            continue
    return sorted(normalized, key=lambda x: x["score"], reverse=True)[:top_k]


@tool("output_records")
def output_records(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    # For demo, just return a summary
    return {"count": len(items), "top": items}


def build_flow(url: str):
    return (
        dag("agent-a")
        .step("download", "downloader", url=url)
        .step("parse", "parser", html="$result.download")
        .step("rank", "re_rank", items="$result.parse", top_k=5)
        .step("out", "output_records", items="$result.rank")
        # Explicit dependencies to ensure correct execution order
        .edge("download", "parse")
        .edge("parse", "rank")
        .edge("rank", "out")
        .build()
    )


def main():
    flow = build_flow("https://example.com")
    ex = LocalExecutor()
    registry = {name: meta.func for name, meta in default_registry._tools.items()}
    res = ex.run_dagflow(flow, registry)
    print("Agent A result:", res)


if __name__ == "__main__":
    main()
