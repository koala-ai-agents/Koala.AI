from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from koala.flow import LocalExecutor
from koala.tools import default_registry


def _load_example_module(name: str, rel_path: str):
    path = Path(rel_path)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_agent_a_flow_runs():
    agent_a = _load_example_module("agent_a", "examples/agent_a.py")
    build_flow = agent_a.build_flow

    flow = build_flow("https://example.com")
    ex = LocalExecutor()
    registry = {name: meta.func for name, meta in default_registry._tools.items()}
    res = ex.run_dagflow(flow, registry)
    assert isinstance(res, dict)
    # Expect final step key to be present
    assert "out" in res
    out = res["out"]
    assert isinstance(out, dict)
    assert "count" in out and "top" in out
    assert isinstance(out["top"], list)


def test_agent_b_flow_runs():
    agent_b = _load_example_module("agent_b", "examples/agent_b.py")
    build_flow = agent_b.build_flow

    flow = build_flow("This is a document to summarize for testing purposes.")
    ex = LocalExecutor()
    registry = {name: meta.func for name, meta in default_registry._tools.items()}
    res = ex.run_dagflow(flow, registry)
    assert isinstance(res, dict)
    assert "approve" in res
    approved = res["approve"]
    assert isinstance(approved, dict)
    assert approved.get("approved") is True
    assert "by" in approved and "summary" in approved
