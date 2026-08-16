"""Regression tests for the worker-side additions to airflow_runtime.

Covers everything that was bolted on after the initial dispatch surface:
spec-version gating, ``deps_factory`` injection into ``RunContext``,
``event_sink`` capturing the L1 event stream, and OTel traceparent
propagation into task spans.

No live Airflow. ``run_step`` accepts a plain ``**context`` dict so we
build one manually.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))
from conftest import assistant_text, make_scripted_model  # noqa: E402

from koala import Agent, tool  # noqa: E402
from koala.core.context import RunContext  # noqa: E402
from koala.orchestration.airflow_runtime import (  # noqa: E402
    _check_spec_version,
    _extract_traceparent,
    _load_deps_factory,
    _load_event_sink,
    load_spec,
    run_step,
)


# ---------------------------------------------------------------------------
# Module-level actions the runtime imports via module:attribute.
# ---------------------------------------------------------------------------


# A record of what the deps_factory and event_sink saw. Populated by
# ``rt_deps_factory`` and ``rt_event_sink`` below; each test resets it.
CAPTURED: dict[str, Any] = {"deps_calls": 0, "events": [], "tool_ctx_deps": None}


def rt_deps_factory() -> dict[str, str]:
    """A deps_factory referenced by tests via 'this_module:rt_deps_factory'."""
    CAPTURED["deps_calls"] += 1
    return {"tenant": "acme", "http": "mock-http-client"}


def rt_event_sink(step_id: str, event: Any) -> None:
    """An event_sink referenced by tests via 'this_module:rt_event_sink'."""
    CAPTURED["events"].append((step_id, type(event).__name__))


@tool
def rt_recording_tool(x: int, ctx: RunContext) -> int:
    """A tool that captures the RunContext.deps it was given. Used to
    prove deps_factory reaches BaseTool dispatch.
    """
    CAPTURED["tool_ctx_deps"] = ctx.deps
    return x * 2


# Per-test agents live under module globals whose names encode the test
# — scripted models are consume-once, so sharing an Agent across tests
# leaks state (a second run raises ``ScriptedProvider exhausted``).
_rt_events_agent_model, _ = make_scripted_model([assistant_text("agent-final")])
rt_events_agent = Agent(_rt_events_agent_model, name="rt_events_agent")

_rt_brokensink_agent_model, _ = make_scripted_model(
    [assistant_text("agent-final")]
)
rt_brokensink_agent = Agent(
    _rt_brokensink_agent_model, name="rt_brokensink_agent"
)


# ---------------------------------------------------------------------------
# Airflow context shims
# ---------------------------------------------------------------------------


class _DagRun:
    def __init__(self, conf: dict[str, Any] | None = None) -> None:
        self.conf = conf or {}


class _TI:
    def __init__(self, xcoms: dict[str, Any] | None = None) -> None:
        self._x = xcoms or {}

    def xcom_pull(self, task_ids: str) -> Any:
        return self._x.get(task_ids)


def _ctx(
    conf: dict[str, Any] | None = None,
    xcoms: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {"dag_run": _DagRun(conf), "ti": _TI(xcoms)}


def _write_spec(
    tmp_path: Path,
    *,
    steps: list[dict[str, Any]],
    version: int = 1,
    deps_factory: str | None = None,
    event_sink: str | None = None,
) -> str:
    spec: dict[str, Any] = {
        "koala_spec_version": version,
        "flow_id": "t",
        "flow_version": "0.1.0",
        "dag_id": "koala_t",
        "input_key": "input",
        "tags": ["koala"],
        "default_args": {},
        "steps": steps,
        "edges": [],
    }
    if deps_factory is not None:
        spec["deps_factory"] = deps_factory
    if event_sink is not None:
        spec["event_sink"] = event_sink
    p = tmp_path / "t.json"
    p.write_text(json.dumps(spec))
    load_spec.cache_clear()
    return str(p)


@pytest.fixture(autouse=True)
def _reset_captured_and_caches() -> None:
    """Each test starts with clean CAPTURED + fresh factory/sink caches."""
    CAPTURED["deps_calls"] = 0
    CAPTURED["events"] = []
    CAPTURED["tool_ctx_deps"] = None
    _load_deps_factory.cache_clear()
    _load_event_sink.cache_clear()


# ===========================================================================
# Spec version gate
# ===========================================================================


class TestSpecVersionGate:
    def test_current_version_accepted(self) -> None:
        _check_spec_version({"koala_spec_version": 1})  # does not raise

    def test_missing_version_defaults_to_v1_and_passes(self) -> None:
        """Absence must not fail — pre-versioning specs default to v1."""
        _check_spec_version({})  # does not raise

    def test_newer_version_fails_loudly(self) -> None:
        with pytest.raises(RuntimeError, match="newer than this koala runtime"):
            _check_spec_version({"koala_spec_version": 9999})

    def test_non_int_version_fails(self) -> None:
        with pytest.raises(RuntimeError):
            _check_spec_version({"koala_spec_version": "one"})

    def test_run_step_refuses_newer_spec(self, tmp_path: Path) -> None:
        spec_path = _write_spec(
            tmp_path,
            version=99,
            steps=[
                {
                    "id": "s",
                    "action_ref": f"{__name__}:rt_recording_tool",
                    "args": {"x": 1},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        with pytest.raises(RuntimeError, match="newer than this koala runtime"):
            run_step(spec_path=spec_path, step_id="s", **_ctx())


# ===========================================================================
# deps_factory reaches every action type
# ===========================================================================


class TestDepsFactoryInjection:
    def test_deps_factory_lookup_rejects_non_callable(self) -> None:
        # Point at something that IS importable but not callable.
        with pytest.raises(TypeError, match="non-callable"):
            _load_deps_factory(f"{__name__}:CAPTURED")

    def test_deps_reach_tool_run_context(self, tmp_path: Path) -> None:
        """The whole point of deps_factory is to feed ``RunContext.deps``
        for tools. Verify the tool sees the exact object the factory
        returned.
        """
        spec_path = _write_spec(
            tmp_path,
            deps_factory=f"{__name__}:rt_deps_factory",
            steps=[
                {
                    "id": "s",
                    "action_ref": f"{__name__}:rt_recording_tool",
                    "args": {"x": 3},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        result = run_step(spec_path=spec_path, step_id="s", **_ctx())

        assert result == 6
        assert CAPTURED["deps_calls"] == 1
        assert CAPTURED["tool_ctx_deps"] == {
            "tenant": "acme",
            "http": "mock-http-client",
        }

    def test_deps_factory_called_once_per_task_but_cached_across_tasks(
        self, tmp_path: Path
    ) -> None:
        """The factory itself is cached per-worker (LRU by ref string).
        Two tasks in the same worker should hit the same factory object,
        but each task calls it once — so ``deps_calls`` grows by 1 per
        task, and there is no re-import per call.
        """
        spec_path = _write_spec(
            tmp_path,
            deps_factory=f"{__name__}:rt_deps_factory",
            steps=[
                {
                    "id": "s",
                    "action_ref": f"{__name__}:rt_recording_tool",
                    "args": {"x": 1},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        run_step(spec_path=spec_path, step_id="s", **_ctx())
        run_step(spec_path=spec_path, step_id="s", **_ctx())
        assert CAPTURED["deps_calls"] == 2

    def test_no_deps_factory_means_deps_is_none(self, tmp_path: Path) -> None:
        spec_path = _write_spec(
            tmp_path,
            steps=[
                {
                    "id": "s",
                    "action_ref": f"{__name__}:rt_recording_tool",
                    "args": {"x": 5},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        run_step(spec_path=spec_path, step_id="s", **_ctx())
        assert CAPTURED["tool_ctx_deps"] is None


# ===========================================================================
# event_sink captures the L1 event stream for Agent steps
# ===========================================================================


class TestEventSinkCapture:
    def test_agent_events_reach_sink(self, tmp_path: Path) -> None:
        """The old runtime called ``agent.arun`` and discarded events.
        The new runtime drives ``astream`` and forwards every Event.
        """
        spec_path = _write_spec(
            tmp_path,
            event_sink=f"{__name__}:rt_event_sink",
            steps=[
                {
                    "id": "chat",
                    "action_ref": f"{__name__}:rt_events_agent",
                    "args": {"prompt": "hi"},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        result = run_step(spec_path=spec_path, step_id="chat", **_ctx())

        assert result == "agent-final"
        # Every Koala Agent run emits Start + ... + Output + Done. The
        # sink should have seen at least those three.
        kinds = {t for _, t in CAPTURED["events"]}
        assert "Start" in kinds
        assert "Output" in kinds
        assert "Done" in kinds
        # And step_id must be forwarded on every call.
        assert {step for step, _ in CAPTURED["events"]} == {"chat"}

    def test_broken_sink_does_not_fail_the_step(self, tmp_path: Path) -> None:
        """A bug in the sink must not fail an otherwise-successful task.
        Airflow already has enough real failures; sinks that raise are
        logged and swallowed.
        """
        spec_path = _write_spec(
            tmp_path,
            event_sink=f"{__name__}:_raise_sink",
            steps=[
                {
                    "id": "chat",
                    "action_ref": f"{__name__}:rt_brokensink_agent",
                    "args": {"prompt": "hi"},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        # Should still return the agent's output despite the sink raising.
        result = run_step(spec_path=spec_path, step_id="chat", **_ctx())
        assert result == "agent-final"


def _raise_sink(step_id: str, event: Any) -> None:
    """Sink referenced by ``test_broken_sink_does_not_fail_the_step``."""
    raise RuntimeError(f"sink boom for {step_id}")


# ===========================================================================
# OTel traceparent extraction
# ===========================================================================


class TestTraceparentExtraction:
    def test_from_conf_koala_key(self) -> None:
        tp = "00-11111111111111111111111111111111-2222222222222222-01"
        ctx = _ctx(conf={"koala_traceparent": tp})
        assert _extract_traceparent(ctx) == tp

    def test_from_conf_generic_key_fallback(self) -> None:
        tp = "00-33333333333333333333333333333333-4444444444444444-01"
        ctx = _ctx(conf={"traceparent": tp})
        assert _extract_traceparent(ctx) == tp

    def test_koala_key_wins_over_generic(self) -> None:
        koala = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01"
        generic = "00-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-2222222222222222-01"
        ctx = _ctx(
            conf={"koala_traceparent": koala, "traceparent": generic}
        )
        assert _extract_traceparent(ctx) == koala

    def test_empty_string_treated_as_absent(self) -> None:
        ctx = _ctx(conf={"koala_traceparent": ""})
        assert _extract_traceparent(ctx) is None

    def test_env_var_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        tp = "00-99999999999999999999999999999999-8888888888888888-01"
        monkeypatch.setenv("OTEL_TRACEPARENT", tp)
        assert _extract_traceparent(_ctx()) == tp

    def test_returns_none_when_nothing_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("OTEL_TRACEPARENT", raising=False)
        assert _extract_traceparent(_ctx()) is None
