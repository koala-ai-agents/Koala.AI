from koala.flow import LocalExecutor, dag
from koala.orchestrator import Orchestrator


def test_orchestrator_submits_and_returns_result():
    f = (
        dag("orchflow")
        .step("a", "noop", x=1)
        .step("b", "noop", x=2)
        .edge("a", "b")
        .build()
    )
    reg = {"noop": lambda **kwargs: kwargs.get("x")}
    ex = LocalExecutor(max_workers=2)
    orch = Orchestrator(max_workers=2)
    run_id = orch.submit_flow(f, ex, reg)

    # wait for completion
    import time

    for _ in range(20):
        st = orch.get_status(run_id)
        if st == "done":
            break
        time.sleep(0.05)

    assert orch.get_status(run_id) == "done"
    res = orch.get_result(run_id)
    assert res and res["a"] == 1 and res["b"] == 2
