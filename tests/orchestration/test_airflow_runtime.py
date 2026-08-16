"""Tests for koala.orchestration.airflow_runtime — the worker-side helpers
that ship inside every generated Koala Airflow DAG.

No live Airflow needed. `run_step` accepts a plain ``**context`` dict, so
we build one manually with the ``dag_run`` and ``ti`` fields the resolver
uses.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

# Reuse the scripted-provider harness.
_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))
from conftest import assistant_text, make_scripted_model  # noqa: E402

from koala import Agent, tool  # noqa: E402
from koala.orchestration.airflow_runtime import (  # noqa: E402  # noqa: E402
    _dot_drill,
    _extract_prompt,
    _import_action,
    _resolve_args,
    _resolve_ref,
    load_spec,
    run_step,
)

# ---------------------------------------------------------------------------
# Module-level fixtures actions must find via importlib.
# ---------------------------------------------------------------------------


def multiply_nums(a: int, b: int) -> int:
    """Module-level plain callable — the runtime imports this via
    airflow_runtime._import_ref (aliased as _import_action for
    backwards-compatible imports).
    """
    return a * b


async def async_greet(name: str) -> str:
    """Module-level async callable — should be awaited inside _dispatch."""
    return f"hello, {name}"


@tool
def echo(text: str) -> str:
    """Echo — a FunctionTool the runtime dispatches via its BaseTool path."""
    return f"echo: {text}"


_model, _ = make_scripted_model([assistant_text("agent-said-hi")])
scripted_agent = Agent(_model, name="scripted_agent")


# ---------------------------------------------------------------------------
# Test doubles for the Airflow context
# ---------------------------------------------------------------------------


class _FakeDagRun:
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf


class _FakeTaskInstance:
    def __init__(self, xcoms: dict[str, Any]) -> None:
        self._xcoms = xcoms

    def xcom_pull(self, task_ids: str) -> Any:
        return self._xcoms.get(task_ids)


def _ctx(
    conf: dict[str, Any] | None = None,
    xcoms: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "dag_run": _FakeDagRun(conf or {}),
        "ti": _FakeTaskInstance(xcoms or {}),
    }


# ===========================================================================
# _dot_drill + argument resolution
# ===========================================================================


class TestArgResolution:
    def test_dot_drill_dict(self) -> None:
        assert _dot_drill({"a": {"b": {"c": 1}}}, "a.b.c") == 1

    def test_dot_drill_object_attrs(self) -> None:
        class X:
            pass

        x = X()
        x.y = X()  # type: ignore[attr-defined]
        x.y.z = 42  # type: ignore[attr-defined]
        assert _dot_drill(x, "y.z") == 42

    def test_input_reference_resolves_from_dag_run_conf(self) -> None:
        ctx = _ctx(conf={"input": {"topic": "koalas", "count": 3}})
        assert (
            _resolve_ref("$input.topic", ctx, "input") == "koalas"
        )
        assert _resolve_ref("$input.count", ctx, "input") == 3

    def test_input_reference_with_nested_key(self) -> None:
        ctx = _ctx(conf={"input": {"user": {"id": "u42"}}})
        assert (
            _resolve_ref("$input.user.id", ctx, "input") == "u42"
        )

    def test_input_reference_honours_custom_input_key(self) -> None:
        ctx = _ctx(conf={"payload": {"topic": "birds"}})
        assert (
            _resolve_ref("$input.topic", ctx, "payload") == "birds"
        )

    def test_result_reference_resolves_via_xcom_pull(self) -> None:
        ctx = _ctx(xcoms={"earlier_step": "the-result"})
        assert (
            _resolve_ref("$result.earlier_step", ctx, "input")
            == "the-result"
        )

    def test_result_reference_with_dotted_field(self) -> None:
        ctx = _ctx(xcoms={"earlier": {"nested": {"leaf": 99}}})
        assert (
            _resolve_ref(
                "$result.earlier.nested.leaf", ctx, "input"
            )
            == 99
        )

    def test_result_reference_raises_without_ti(self) -> None:
        ctx = {"dag_run": _FakeDagRun({})}
        with pytest.raises(RuntimeError, match="task_instance"):
            _resolve_ref("$result.x", ctx, "input")

    def test_plain_string_passes_through(self) -> None:
        ctx = _ctx()
        assert (
            _resolve_ref("just a string", ctx, "input")
            == "just a string"
        )

    def test_non_strings_pass_through(self) -> None:
        ctx = _ctx()
        assert _resolve_ref(42, ctx, "input") == 42
        assert _resolve_ref([1, 2, 3], ctx, "input") == [1, 2, 3]

    def test_resolve_args_full_dict(self) -> None:
        ctx = _ctx(
            conf={"input": {"topic": "koalas"}},
            xcoms={"prior": {"count": 3}},
        )
        raw = {
            "text": "$input.topic",
            "n": "$result.prior.count",
            "literal": "unchanged",
        }
        assert _resolve_args(raw, ctx, "input") == {
            "text": "koalas",
            "n": 3,
            "literal": "unchanged",
        }


# ===========================================================================
# _import_action
# ===========================================================================


class TestImportAction:
    def test_imports_module_level_callable(self) -> None:
        ref = f"{__name__}:multiply_nums"
        obj = _import_action(ref)
        assert obj is multiply_nums

    def test_imports_module_level_tool(self) -> None:
        ref = f"{__name__}:echo"
        obj = _import_action(ref)
        assert obj is echo

    def test_imports_module_level_agent(self) -> None:
        ref = f"{__name__}:scripted_agent"
        obj = _import_action(ref)
        assert obj is scripted_agent

    def test_missing_separator_raises(self) -> None:
        with pytest.raises(ValueError, match="module:attribute"):
            _import_action("no_separator")

    def test_missing_attribute_raises(self) -> None:
        with pytest.raises(AttributeError):
            _import_action(f"{__name__}:nonexistent_symbol_xyz")


# ===========================================================================
# _extract_prompt
# ===========================================================================


class TestExtractPrompt:
    def test_prefers_prompt_key(self) -> None:
        assert _extract_prompt({"prompt": "hi", "input": "bye"}) == "hi"

    def test_falls_back_to_input_then_text(self) -> None:
        assert _extract_prompt({"input": "hi"}) == "hi"
        assert _extract_prompt({"text": "hi"}) == "hi"

    def test_single_arg_used_when_no_conventional_key(self) -> None:
        assert _extract_prompt({"topic": "koalas"}) == "koalas"

    def test_ambiguous_args_raise(self) -> None:
        with pytest.raises(ValueError, match="needs a 'prompt'"):
            _extract_prompt({"a": 1, "b": 2})

    def test_stringifies_non_strings(self) -> None:
        assert _extract_prompt({"prompt": 42}) == "42"


# ===========================================================================
# run_step — end-to-end with fake context
# ===========================================================================


class TestRunStep:
    def _write_spec(
        self, tmp_path: Path, steps: list[dict[str, Any]]
    ) -> str:
        spec = {
            "koala_spec_version": 1,
            "flow_id": "t",
            "flow_version": "0.1.0",
            "dag_id": "koala_t",
            "input_key": "input",
            "tags": ["koala"],
            "default_args": {},
            "steps": steps,
            "edges": [],
        }
        p = tmp_path / "t.json"
        p.write_text(json.dumps(spec))
        load_spec.cache_clear()
        return str(p)

    def test_plain_callable_step(self, tmp_path: Path) -> None:
        spec_path = self._write_spec(
            tmp_path,
            [
                {
                    "id": "mul",
                    "action_ref": f"{__name__}:multiply_nums",
                    "args": {"a": 4, "b": 5},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        result = run_step(
            spec_path=spec_path, step_id="mul", **_ctx()
        )
        assert result == 20

    def test_callable_with_input_reference(
        self, tmp_path: Path
    ) -> None:
        spec_path = self._write_spec(
            tmp_path,
            [
                {
                    "id": "mul",
                    "action_ref": f"{__name__}:multiply_nums",
                    "args": {"a": "$input.left", "b": "$input.right"},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        result = run_step(
            spec_path=spec_path,
            step_id="mul",
            **_ctx(conf={"input": {"left": 7, "right": 6}}),
        )
        assert result == 42

    def test_callable_with_result_reference(
        self, tmp_path: Path
    ) -> None:
        spec_path = self._write_spec(
            tmp_path,
            [
                {
                    "id": "mul",
                    "action_ref": f"{__name__}:multiply_nums",
                    "args": {"a": "$result.prior", "b": 2},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        result = run_step(
            spec_path=spec_path,
            step_id="mul",
            **_ctx(xcoms={"prior": 5}),
        )
        assert result == 10

    def test_async_callable_is_awaited(self, tmp_path: Path) -> None:
        spec_path = self._write_spec(
            tmp_path,
            [
                {
                    "id": "greet",
                    "action_ref": f"{__name__}:async_greet",
                    "args": {"name": "Sam"},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        result = run_step(
            spec_path=spec_path, step_id="greet", **_ctx()
        )
        assert result == "hello, Sam"

    def test_function_tool_step(self, tmp_path: Path) -> None:
        spec_path = self._write_spec(
            tmp_path,
            [
                {
                    "id": "e",
                    "action_ref": f"{__name__}:echo",
                    "args": {"text": "hi"},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        result = run_step(spec_path=spec_path, step_id="e", **_ctx())
        assert result == "echo: hi"

    def test_agent_step_returns_output(self, tmp_path: Path) -> None:
        spec_path = self._write_spec(
            tmp_path,
            [
                {
                    "id": "agent",
                    "action_ref": f"{__name__}:scripted_agent",
                    "args": {"prompt": "say hi"},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        result = run_step(
            spec_path=spec_path, step_id="agent", **_ctx()
        )
        assert result == "agent-said-hi"

    def test_missing_step_id_raises(self, tmp_path: Path) -> None:
        spec_path = self._write_spec(
            tmp_path,
            [
                {
                    "id": "mul",
                    "action_ref": f"{__name__}:multiply_nums",
                    "args": {"a": 1, "b": 1},
                    "timeout": None,
                    "retries": 0,
                }
            ],
        )
        with pytest.raises(KeyError, match="ghost"):
            run_step(spec_path=spec_path, step_id="ghost", **_ctx())

    def test_unknown_action_type_raises(self, tmp_path: Path) -> None:
        """Something that isn't Agent, Tool, Model, or callable."""
        not_callable = object()
        sys.modules[__name__].__dict__["_not_callable"] = not_callable
        try:
            spec_path = self._write_spec(
                tmp_path,
                [
                    {
                        "id": "s",
                        "action_ref": f"{__name__}:_not_callable",
                        "args": {},
                        "timeout": None,
                        "retries": 0,
                    }
                ],
            )
            with pytest.raises(TypeError, match="not executable"):
                run_step(
                    spec_path=spec_path, step_id="s", **_ctx()
                )
        finally:
            sys.modules[__name__].__dict__.pop("_not_callable", None)


# ===========================================================================
# load_spec — caching behaviour
# ===========================================================================


class TestLoadSpec:
    def test_load_spec_returns_json_dict(
        self, tmp_path: Path
    ) -> None:
        p = tmp_path / "x.json"
        p.write_text('{"hello": "world"}')
        load_spec.cache_clear()
        assert load_spec(str(p)) == {"hello": "world"}

    def test_load_spec_caches_by_path(self, tmp_path: Path) -> None:
        p = tmp_path / "y.json"
        p.write_text('{"v": 1}')
        load_spec.cache_clear()
        first = load_spec(str(p))
        # Mutate the file — the cache should still return the OLD value
        # because @lru_cache doesn't invalidate on disk changes.
        p.write_text('{"v": 2}')
        second = load_spec(str(p))
        assert first is second
        assert second == {"v": 1}
        # Clear cache and it picks up the new file.
        load_spec.cache_clear()
        assert load_spec(str(p)) == {"v": 2}
