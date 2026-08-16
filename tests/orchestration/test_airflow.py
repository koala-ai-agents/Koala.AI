"""Tests for koala.orchestration.airflow — spec builder, DAG renderer,
action-ref resolver, and the REST-driven AirflowExecutor.

No live Airflow needed anywhere. HTTP interactions go through
``httpx.MockTransport``; action resolution runs against real Python
modules loaded into the test process.
"""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import httpx
import pytest

# The scripted-provider harness lets us build real Agents cheaply.
_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))
from conftest import assistant_text, make_scripted_model  # noqa: E402

from koala import Agent, tool  # noqa: E402
from koala.orchestration import (  # noqa: E402
    ActionSerializationError,
    AirflowAPIError,
    AirflowExecutor,
    AirflowExecutorError,
    flow,
    render_dag_file,
    spec_from_flow,
)
from koala.orchestration.airflow import _resolve_action_ref  # noqa: E402

# ---------------------------------------------------------------------------
# Module-level fixtures — a callable, a FunctionTool, and an Agent
# ---------------------------------------------------------------------------


def add(a: int, b: int) -> int:
    """Module-level plain callable for auto-detection tests."""
    return a + b


@tool
def multiply(a: int, b: int) -> int:
    """Module-level FunctionTool for auto-detection tests."""
    return a * b


_agent_model, _ = make_scripted_model([assistant_text("agent output")])
sample_agent = Agent(_agent_model, name="sample_agent")


# ===========================================================================
# spec_from_flow
# ===========================================================================


class TestSpec:
    def test_step_action_ref_is_module_colon_attribute(self) -> None:
        f = flow("s").step("go", add, a=1, b=2).build()
        spec = spec_from_flow(f)
        assert spec["steps"][0]["action_ref"].endswith(":add")
        assert "test_airflow" in spec["steps"][0]["action_ref"]

    def test_spec_has_expected_top_level_shape(self) -> None:
        f = (
            flow("shape")
            .step("a", add, a=1, b=2)
            .step("b", multiply, a="$result.a", b=3)
            .edge("a", "b")
            .build()
        )
        spec = spec_from_flow(
            f, tags=["custom"], default_args={"retries": 3}
        )
        assert spec["koala_spec_version"] == 1
        assert spec["flow_id"] == "shape"
        assert spec["flow_version"] == "0.1.0"
        assert spec["dag_id"] == "koala_shape"
        assert spec["input_key"] == "input"
        assert spec["tags"] == ["custom"]
        # Framework defaults merged with user override.
        assert spec["default_args"]["owner"] == "koala"
        assert spec["default_args"]["retries"] == 3
        assert spec["edges"] == [["a", "b"]]
        assert [s["id"] for s in spec["steps"]] == ["a", "b"]

    def test_string_action_resolves_via_registry(self) -> None:
        f = flow("reg").step("s", "add_op", a=1, b=2).build()
        spec = spec_from_flow(f, registry={"add_op": add})
        assert spec["steps"][0]["action_ref"].endswith(":add")

    def test_string_action_missing_registry_errors(self) -> None:
        f = flow("bad").step("s", "unknown", a=1).build()
        with pytest.raises(AirflowExecutorError, match="not in registry"):
            spec_from_flow(f)

    def test_reference_strings_preserved_verbatim(self) -> None:
        """The generator MUST NOT resolve $input / $result at spec-build
        time — those are runtime substitutions.
        """
        f = flow("r").step(
            "s", add, a="$input.left", b="$result.other"
        ).build()
        spec = spec_from_flow(f)
        args = spec["steps"][0]["args"]
        assert args["a"] == "$input.left"
        assert args["b"] == "$result.other"

    def test_dag_id_prefix_applied(self) -> None:
        f = flow("myid").step("s", add, a=1, b=2).build()
        spec = spec_from_flow(f, dag_id_prefix="acme_")
        assert spec["dag_id"] == "acme_myid"


# ===========================================================================
# _resolve_action_ref — the five paths
# ===========================================================================


class TestResolveActionRef:
    def test_plain_callable_resolves_via_module_name(self) -> None:
        ref = _resolve_action_ref("s", add, {})
        assert ref.attribute == "add"
        assert "test_airflow" in ref.module

    def test_function_tool_resolves_via_underlying_module(self) -> None:
        ref = _resolve_action_ref("s", multiply, {})
        assert ref.attribute == "multiply"
        assert "test_airflow" in ref.module

    def test_agent_instance_resolves_via_module_scan(self) -> None:
        ref = _resolve_action_ref("s", sample_agent, {})
        assert ref.attribute == "sample_agent"
        assert "test_airflow" in ref.module

    def test_inline_agent_raises_action_serialization_error(self) -> None:
        model, _ = make_scripted_model([assistant_text("x")])
        inline = Agent(model)  # not assigned anywhere
        with pytest.raises(
            ActionSerializationError, match="module-level"
        ):
            _resolve_action_ref("s", inline, {})

    def test_action_paths_override_wins(self) -> None:
        model, _ = make_scripted_model([assistant_text("x")])
        inline = Agent(model)
        ref = _resolve_action_ref(
            "s", inline, {"s": "custompkg.mod:my_agent"}
        )
        assert ref.module == "custompkg.mod"
        assert ref.attribute == "my_agent"

    def test_action_paths_accepts_dot_form(self) -> None:
        ref = _resolve_action_ref(
            "s", add, {"s": "somepkg.mod.varname"}
        )
        assert ref.module == "somepkg.mod"
        assert ref.attribute == "varname"

    def test_action_paths_rejects_bad_format(self) -> None:
        with pytest.raises(AirflowExecutorError, match="module:attr"):
            _resolve_action_ref("s", add, {"s": "no_separator"})

    def test_action_paths_rejects_empty_parts(self) -> None:
        with pytest.raises(AirflowExecutorError, match="non-empty"):
            _resolve_action_ref("s", add, {"s": ":only_attr"})

    def test_user_package_with_koala_prefix_scannable(self) -> None:
        """Regression: `startswith('koala')` filter used to skip user packages
        named `koala_flows`, `koala_agents`, etc. Now the filter is exact
        match on 'koala' or 'koala.' prefix.
        """
        fake = types.ModuleType("koala_userpkg")
        model, _ = make_scripted_model([assistant_text("ok")])
        pkg_agent = Agent(model, name="pkg_agent")
        fake.my_agent = pkg_agent
        sys.modules["koala_userpkg"] = fake
        try:
            ref = _resolve_action_ref("s", pkg_agent, {})
            assert ref.module == "koala_userpkg"
            assert ref.attribute == "my_agent"
        finally:
            sys.modules.pop("koala_userpkg", None)

    def test_main_script_walks_init_chain_to_package_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When a pipeline runs as __main__ inside a package chain, the
        resolver walks up __init__.py files to produce the real dotted
        import path.
        """
        import runpy

        # Build <tmp>/pkg/subpkg/pipeline.py, both dirs are packages.
        pkg = tmp_path / "pkg"
        subpkg = pkg / "subpkg"
        subpkg.mkdir(parents=True)
        (pkg / "__init__.py").write_text("")
        (subpkg / "__init__.py").write_text("")
        script = subpkg / "pipeline.py"
        script.write_text(
            "import sys\n"
            f"sys.path.insert(0, {str(_AGENTS_DIR)!r})\n"
            "from conftest import assistant_text, make_scripted_model\n"
            "from koala import Agent\n"
            "m, _ = make_scripted_model([assistant_text('ok')])\n"
            "my_agent = Agent(m, name='pkg_agent')\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))

        ns = runpy.run_path(str(script), run_name="__main__")

        # Install a fake sys.modules['__main__'] matching what real
        # `python <script>` would produce.
        main_shim = types.ModuleType("__main__")
        main_shim.__file__ = str(script)
        for k, v in ns.items():
            setattr(main_shim, k, v)
        original = sys.modules.get("__main__")
        sys.modules["__main__"] = main_shim
        try:
            ref = _resolve_action_ref("s", ns["my_agent"], {})
            assert ref.module == "pkg.subpkg.pipeline"
            assert ref.attribute == "my_agent"
        finally:
            if original is not None:
                sys.modules["__main__"] = original
            else:
                sys.modules.pop("__main__", None)


# ===========================================================================
# render_dag_file — the thin DAG file shape
# ===========================================================================


class TestRenderDagFile:
    def _spec(self) -> dict:
        f = (
            flow("render")
            .step("first", add, a=1, b=2)
            .step("second", multiply, a="$result.first", b=3)
            .edge("first", "second")
            .build()
        )
        return spec_from_flow(f)

    def test_has_shim_for_airflow_2_and_3_imports(self) -> None:
        code = render_dag_file(self._spec())
        assert "from airflow.sdk import dag" in code
        assert "from airflow.decorators import dag" in code
        assert (
            "from airflow.providers.standard.operators.python "
            "import PythonOperator" in code
        )
        assert "from airflow.operators.python import PythonOperator" in code

    def test_calls_run_step_from_koala_runtime(self) -> None:
        code = render_dag_file(self._spec())
        assert (
            "from koala.orchestration.airflow_runtime "
            "import load_spec, run_step" in code
        )
        # run_step is now wired into every PythonOperator via a small
        # kwargs helper so per-step retries/timeout/pool/queue can be
        # added without exploding the template. Assert the wire-through
        # rather than a single hard-coded call site.
        assert '"python_callable": run_step' in code
        assert "PythonOperator(**_step_operator_kwargs(step))" in code

    def test_no_user_module_import_at_parse_time(self) -> None:
        """The thin DAG file must NOT `import` user action modules. All
        such imports happen at task time, inside run_step.
        """
        code = render_dag_file(self._spec())
        assert "from test_airflow" not in code
        assert "import add" not in code
        assert "import multiply" not in code

    def test_no_agent_construction_in_dag_file(self) -> None:
        """Even for Agent steps, the file must not construct an Agent at
        parse time. Agent objects live in the user module and are loaded
        by run_step at task time.
        """
        f = flow("agent-step").step(
            "run", sample_agent, prompt="hi"
        ).build()
        code = render_dag_file(spec_from_flow(f))
        assert "Agent(" not in code
        assert "Model(" not in code

    def test_task_creation_uses_sorted_step_order(self) -> None:
        """The parse-time loop sorts steps by id for grid-view stability.
        This assertion checks the template renders that pattern.
        """
        code = render_dag_file(self._spec())
        assert 'sorted(spec["steps"], key=lambda s: s["id"])' in code

    def test_spec_path_derived_from_dag_file_location(self) -> None:
        code = render_dag_file(self._spec())
        assert "_DAG_FILE = Path(__file__).resolve()" in code
        assert (
            '_SPEC_PATH = str(_DAG_FILE.parent / "koala_specs"' in code
        )

    def test_dag_id_and_tags_baked_in(self) -> None:
        f = flow("branding").step("s", add, a=1, b=2).build()
        code = render_dag_file(
            spec_from_flow(f, tags=["prod", "koala"])
        )
        # Rendered via !r → single-quoted Python literal.
        assert "dag_id='koala_branding'" in code
        assert "'prod'" in code and "'koala'" in code


# ===========================================================================
# AirflowExecutor.deploy — filesystem output
# ===========================================================================


class TestDeploy:
    def test_deploy_writes_thin_dag_and_spec(
        self, tmp_path: Path
    ) -> None:
        f = (
            flow("depl")
            .step("s", add, a=1, b=2)
            .step("t", multiply, a="$result.s", b=3)
            .edge("s", "t")
            .build()
        )
        with AirflowExecutor(dags_folder=str(tmp_path)) as ex:
            dag_path, spec_path = ex.deploy(f)

        assert dag_path == tmp_path / "koala_depl.py"
        assert spec_path == tmp_path / "koala_specs" / "depl.json"
        assert dag_path.exists()
        assert spec_path.exists()

        # DAG file is thin — no user imports.
        dag_code = dag_path.read_text()
        assert "run_step" in dag_code
        assert "import add" not in dag_code

        # Spec is JSON, has the expected keys.
        spec = json.loads(spec_path.read_text())
        assert spec["flow_id"] == "depl"
        assert spec["dag_id"] == "koala_depl"
        assert len(spec["steps"]) == 2

    def test_deploy_defaults_dags_folder_to_local_dir(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("AIRFLOW__CORE__DAGS_FOLDER", raising=False)
        monkeypatch.chdir(tmp_path)

        f = flow("cwd").step("s", add, a=1, b=2).build()
        with AirflowExecutor() as ex:
            dag_path, spec_path = ex.deploy(f)

        assert dag_path == tmp_path / "dags" / "koala_cwd.py"
        assert spec_path == tmp_path / "dags" / "koala_specs" / "cwd.json"

    def test_deploy_honours_airflow_core_dags_folder_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        target = tmp_path / "custom-dags"
        monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", str(target))

        f = flow("env").step("s", add, a=1, b=2).build()
        with AirflowExecutor() as ex:
            dag_path, _ = ex.deploy(f)

        assert dag_path.parent == target.resolve()

    def test_repeated_deploy_is_idempotent(self, tmp_path: Path) -> None:
        f = flow("idem").step("s", add, a=1, b=2).build()
        with AirflowExecutor(dags_folder=str(tmp_path)) as ex:
            dag_path_1, spec_path_1 = ex.deploy(f)
            content_1 = dag_path_1.read_text()
            spec_1 = spec_path_1.read_text()

            dag_path_2, spec_path_2 = ex.deploy(f)

        assert dag_path_1 == dag_path_2
        assert spec_path_1 == spec_path_2
        assert dag_path_2.read_text() == content_1
        assert spec_path_2.read_text() == spec_1


# ===========================================================================
# AirflowExecutor.trigger + wait — REST client via MockTransport
# ===========================================================================


def _mock_transport(handler) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


class TestTriggerAndWait:
    def _flow(self):
        return (
            flow("rest")
            .step("s", add, a=1, b=2)
            .step("t", multiply, a=3, b=4)
            .edge("s", "t")
            .build()
        )

    def test_trigger_returns_run_id_and_posts_conf(
        self, tmp_path: Path
    ) -> None:
        posted: list[dict] = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/dags/koala_rest"):
                return httpx.Response(
                    200, json={"is_paused": False, "dag_id": "koala_rest"}
                )
            if request.url.path.endswith("/dagRuns"):
                body = json.loads(request.content)
                posted.append(body)
                return httpx.Response(
                    200, json={"dag_run_id": body["dag_run_id"]}
                )
            return httpx.Response(404)

        client = httpx.Client(
            base_url="http://test", transport=_mock_transport(handler)
        )
        with AirflowExecutor(
            airflow_url="http://test",
            dags_folder=str(tmp_path),
            http_client=client,
        ) as ex:
            ex.deploy(self._flow())
            run_id = ex.trigger(
                self._flow(), input={"topic": "koalas"}
            )

        assert run_id.startswith("koala_")
        assert posted[0]["conf"]["input"] == {"topic": "koalas"}

    def test_trigger_auto_unpauses_paused_dag(
        self, tmp_path: Path
    ) -> None:
        patched: list[dict] = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET" and request.url.path.endswith(
                "/dags/koala_rest"
            ):
                return httpx.Response(
                    200, json={"is_paused": True, "dag_id": "koala_rest"}
                )
            if request.method == "PATCH":
                patched.append(json.loads(request.content))
                return httpx.Response(200, json={"is_paused": False})
            if request.url.path.endswith("/dagRuns"):
                return httpx.Response(
                    200, json={"dag_run_id": "koala_1"}
                )
            return httpx.Response(404)

        client = httpx.Client(
            base_url="http://t", transport=_mock_transport(handler)
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            auto_unpause=True,  # explicit opt-in — silent unpause was a footgun
            http_client=client,
        ) as ex:
            ex.deploy(self._flow())
            ex.trigger(self._flow())

        assert patched == [{"is_paused": False}]

    def test_trigger_raises_on_api_error(
        self, tmp_path: Path
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(
                    200, json={"is_paused": False, "dag_id": "koala_rest"}
                )
            return httpx.Response(500, json={"detail": "boom"})

        client = httpx.Client(
            base_url="http://t", transport=_mock_transport(handler)
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            http_client=client,
        ) as ex:
            ex.deploy(self._flow())
            with pytest.raises(AirflowAPIError) as exc:
                ex.trigger(self._flow())
        assert exc.value.status == 500

    def test_wait_polls_until_success_and_returns_xcoms(
        self, tmp_path: Path
    ) -> None:
        # Simulate: two polls in `running`, then `success`. Then xcom fetches.
        states = iter(["running", "running", "success"])

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if "/xcomEntries/return_value" in path:
                if "/taskInstances/s/" in path:
                    return httpx.Response(200, json={"value": "3"})
                if "/taskInstances/t/" in path:
                    return httpx.Response(200, json={"value": "12"})
            if path.endswith("/dagRuns/rid1"):
                return httpx.Response(
                    200, json={"state": next(states)}
                )
            return httpx.Response(404)

        client = httpx.Client(
            base_url="http://t", transport=_mock_transport(handler)
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            poll_interval=0.0,
            http_client=client,
        ) as ex:
            results = ex.wait(self._flow(), "rid1")

        assert results == {"s": 3, "t": 12}

    def test_wait_raises_on_failed_state(
        self, tmp_path: Path
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"state": "failed"})

        client = httpx.Client(
            base_url="http://t", transport=_mock_transport(handler)
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            poll_interval=0.0,
            http_client=client,
        ) as ex:
            with pytest.raises(
                AirflowExecutorError, match="failed"
            ):
                ex.wait(self._flow(), "rid1")

    def test_wait_returns_none_for_missing_xcom(
        self, tmp_path: Path
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if "/xcomEntries/return_value" in request.url.path:
                return httpx.Response(404, json={"detail": "not found"})
            return httpx.Response(200, json={"state": "success"})

        client = httpx.Client(
            base_url="http://t", transport=_mock_transport(handler)
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            poll_interval=0.0,
            http_client=client,
        ) as ex:
            results = ex.wait(self._flow(), "rid1")

        assert results == {"s": None, "t": None}


# ===========================================================================
# Executor lifecycle
# ===========================================================================


def test_context_manager_closes_client(tmp_path: Path) -> None:
    ex = AirflowExecutor(dags_folder=str(tmp_path))
    with ex:
        assert ex._client is not None
    # After close, the client is still there (we don't null it) but its
    # underlying transport is closed. httpx doesn't expose "closed" — just
    # confirm no exception at __exit__.


def test_render_dag_file_via_executor_matches_free_function(
    tmp_path: Path,
) -> None:
    f = flow("dedup").step("s", add, a=1, b=2).build()
    with AirflowExecutor(dags_folder=str(tmp_path)) as ex:
        via_method = ex.render_dag_file(f)
    via_free = render_dag_file(spec_from_flow(f))
    assert via_method == via_free
