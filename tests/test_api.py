import pytest


def test_run_flow_endpoint():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from koala.api import app
    from koala.tools import tool

    # register a simple tool
    @tool("add", input_schema={"a": "int", "b": "int"})
    def add(a: int, b: int):
        return a + b

    client = TestClient(app)
    payload = {
        "flow": {
            "id": "t1",
            "steps": [{"id": "s1", "action": "add", "args": {"a": 1, "b": 2}}],
            "edges": [],
        }
    }
    r = client.post("/run-flow", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["results"]["s1"] == 3
