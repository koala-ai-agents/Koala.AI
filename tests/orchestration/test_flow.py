"""Tests for koala.orchestration.flow — Flow, Step, FlowBuilder, toposort."""

from __future__ import annotations

import pytest

from koala.orchestration import Flow, FlowBuilder, FlowError, Step, flow


def _noop() -> None:
    return None


# ---------------------------------------------------------------------------
# FlowBuilder basics
# ---------------------------------------------------------------------------


def test_builder_produces_empty_flow() -> None:
    f = flow("empty").build()
    assert isinstance(f, Flow)
    assert f.id == "empty"
    assert f.steps == []
    assert f.edges == []


def test_step_adds_to_flow() -> None:
    f = flow("x").step("a", _noop, foo=1).step("b", _noop).build()
    assert [s.id for s in f.steps] == ["a", "b"]
    assert f.steps[0].args == {"foo": 1}
    assert f.steps[1].args == {}


def test_duplicate_step_id_raises() -> None:
    b = flow("x").step("a", _noop)
    with pytest.raises(FlowError, match="Duplicate step id"):
        b.step("a", _noop)


def test_edge_requires_known_endpoints() -> None:
    b = flow("x").step("a", _noop)
    with pytest.raises(FlowError, match="unknown step"):
        b.edge("a", "z")
    with pytest.raises(FlowError, match="unknown step"):
        b.edge("z", "a")


def test_self_loop_edge_rejected() -> None:
    b = flow("x").step("a", _noop)
    with pytest.raises(FlowError, match="Self-loop"):
        b.edge("a", "a")


def test_step_kwargs_become_args() -> None:
    f = flow("x").step("a", _noop, alpha=1, beta="two").build()
    assert f.steps[0].args == {"alpha": 1, "beta": "two"}


def test_timeout_and_retries_stored() -> None:
    f = flow("x").step("a", _noop, timeout=1.5, retries=3, arg="v").build()
    step = f.steps[0]
    assert step.timeout == 1.5
    assert step.retries == 3
    assert step.args == {"arg": "v"}


# ---------------------------------------------------------------------------
# Toposort / cycle detection
# ---------------------------------------------------------------------------


def test_linear_flow_builds_ok() -> None:
    f = (
        flow("x")
        .step("a", _noop)
        .step("b", _noop)
        .step("c", _noop)
        .edge("a", "b")
        .edge("b", "c")
        .build()
    )
    assert len(f.steps) == 3
    assert len(f.edges) == 2


def test_diamond_flow_builds_ok() -> None:
    (
        flow("diamond")
        .step("root", _noop)
        .step("left", _noop)
        .step("right", _noop)
        .step("join", _noop)
        .edge("root", "left")
        .edge("root", "right")
        .edge("left", "join")
        .edge("right", "join")
        .build()
    )


def test_cycle_detected_at_build_time() -> None:
    b = (
        flow("cyclic")
        .step("a", _noop)
        .step("b", _noop)
        .step("c", _noop)
        .edge("a", "b")
        .edge("b", "c")
        .edge("c", "a")
    )
    with pytest.raises(FlowError, match="cycle"):
        b.build()


def test_direct_step_construction() -> None:
    s = Step(id="raw", action=_noop, args={"k": "v"})
    assert s.id == "raw"
    assert s.args == {"k": "v"}
    assert s.timeout is None
    assert s.retries == 0


def test_direct_flow_construction() -> None:
    f = Flow(id="raw", steps=[Step(id="a", action=_noop)], edges=[])
    assert f.id == "raw"
    assert len(f.steps) == 1


def test_flow_builder_type() -> None:
    b = flow("x")
    assert isinstance(b, FlowBuilder)


# ---------------------------------------------------------------------------
# Flow convenience runners — flow.run() / flow.deploy_to_airflow()
# ---------------------------------------------------------------------------


def _add(a: int, b: int) -> int:
    return a + b


def test_flow_run_uses_local_executor_and_returns_results() -> None:
    """Flow.run() is a shortcut for LocalExecutor().run(self, ...)."""
    f = flow("shortcut").step("sum", _add, a=3, b=4).build()
    results = f.run()
    assert results == {"sum": 7}


def test_flow_run_forwards_input_and_deps() -> None:
    seen = {}

    def use_deps(a: int, b: int) -> int:
        # deps aren't kwargs — this just proves args flow through.
        seen["called_with"] = (a, b)
        return a * b

    f = flow("io").step("mul", use_deps, a="$input.x", b="$input.y").build()
    results = f.run(input={"x": 5, "y": 6})
    assert results == {"mul": 30}
    assert seen["called_with"] == (5, 6)


def test_flow_deploy_to_airflow_wraps_executor_run(monkeypatch) -> None:
    """deploy_to_airflow instantiates AirflowExecutor and forwards to .run()."""
    from koala.orchestration import airflow as airflow_module

    captured: dict = {}

    class _FakeExecutor:
        def __init__(self, **kwargs):
            captured["ctor"] = kwargs

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def run(self, flow_, *, input=None, conf=None):
            captured["run_flow"] = flow_
            captured["run_input"] = input
            captured["run_conf"] = conf
            return {"stub_step": "stub_result"}

    monkeypatch.setattr(airflow_module, "AirflowExecutor", _FakeExecutor)

    f = flow("wired").step("s", _add, a=1, b=2).build()
    result = f.deploy_to_airflow(
        input={"topic": "koalas"},
        airflow_url="http://x",
        dag_tags=["custom"],
    )

    assert result == {"stub_step": "stub_result"}
    assert captured["ctor"] == {
        "airflow_url": "http://x",
        "dag_tags": ["custom"],
    }
    assert captured["run_flow"] is f
    assert captured["run_input"] == {"topic": "koalas"}
    assert captured["run_conf"] is None
