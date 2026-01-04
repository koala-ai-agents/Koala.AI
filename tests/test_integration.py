from __future__ import annotations

from koala.flow import LocalExecutor, dag
from koala.orchestrator import Orchestrator
from koala.tools import tool


@tool("add_ints_integration")
def add_ints_integration(x: int, y: int) -> int:
    return x + y


def test_end_to_end_local_run():
    # Build a tiny flow and run via orchestrator
    flow = dag("integration").step("s1", "add_ints_integration", x=1, y=2).build()
    # Build a minimal registry explicitly to avoid collisions with other tests
    registry = {"add_ints_integration": add_ints_integration}
    ex = LocalExecutor()
    orch = Orchestrator()
    run_id = orch.submit_flow(flow, ex, registry)

    # Poll status and fetch result synchronously (simple join)
    # Since orchestrator runs in thread pool, wait a bit by checking status
    # Wait until orchestrator reports completion
    import time

    for _ in range(200):
        status = orch.get_status(run_id)
        if status == "done":
            break
        time.sleep(0.01)
    assert orch.get_status(run_id) == "done"

    result = orch.get_result(run_id)
    assert result is not None
    assert result.get("s1") == 3
