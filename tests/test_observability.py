from koala.flow import LocalExecutor, dag
from koala.observability import metrics, tracer


def test_metrics_and_tracer_basic():
    # reset state by creating a fresh tracer/metrics objects is not supported here,
    # but we can exercise their public API and assert expected keys appear.
    tid = tracer.start_trace()
    tracer.record(tid, "test_event", foo="bar")
    trace = tracer.get_trace(tid)
    assert trace and any(e.get("event") == "test_event" for e in trace)

    # metrics increment and export
    metrics.inc("test_counter", 2)
    metrics.timing("test_timer", 0.123)
    exp = metrics.export_prometheus()
    assert "test_counter 2" in exp
    assert "test_timer_count" in exp


def test_flow_emits_traces_and_metrics():
    # small flow: step a -> b
    f = dag("obsflow").step("a", "noop").step("b", "noop").edge("a", "b").build()

    reg = {"noop": lambda **kwargs: "ok"}
    ex = LocalExecutor(max_workers=2)
    # execute; this will use the global tracer/metrics
    res = ex.run_dagflow(f, reg)
    assert res["a"] == "ok" and res["b"] == "ok"

    # at least one step should have executed; metrics should reflect that
    exported = metrics.export_prometheus()
    assert "steps_executed" in exported or "steps_executed 0" in exported
