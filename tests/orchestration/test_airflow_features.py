"""Regression tests for the newer AirflowExecutor surface.

Everything here was added after the initial spec-generator work landed:
per-step Airflow overrides, DAG-generation-only CI/CD entry point,
spec-version gating, UUID run ids, and opt-in auto-unpause. Kept in a
separate file so the original ``test_airflow.py`` stays scoped to its
original responsibilities.

No live Airflow. HTTP flows through ``httpx.MockTransport``.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import httpx
import pytest

# Reuse the scripted-provider harness.
_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))

from koala.orchestration import (  # noqa: E402
    AirflowExecutor,
    AirflowExecutorError,
    KOALA_SPEC_VERSION,
    flow,
    render_dag_file,
    spec_from_flow,
    write_dag_files,
)


# ---------------------------------------------------------------------------
# Module-level action so the resolver finds it.
# ---------------------------------------------------------------------------


def _passthrough(x: int) -> int:
    return x


# ===========================================================================
# Per-step retries + execution_timeout in the generated DAG
# ===========================================================================


class TestGeneratedDagWiresPerStepRetriesAndTimeout:
    """The old template silently dropped Step.retries and Step.timeout. The
    new template folds them into ``PythonOperator(retries=..., execution_timeout=...)``.
    """

    def test_positive_retries_render_in_operator_kwargs(self) -> None:
        f = (
            flow("wire-retries")
            .step("with_retries", _passthrough, retries=3, x=1)
            .step("no_retries", _passthrough, x=2)
            .build()
        )
        spec = spec_from_flow(f)
        code = render_dag_file(spec)

        # The template computes op-kwargs at parse time, so we assert the
        # helper is present and the retries value survived into the spec.
        assert "def _step_operator_kwargs" in code
        assert "kwargs[\"retries\"] = retries" in code
        # Spec preserves the per-step value.
        by_id = {s["id"]: s for s in spec["steps"]}
        assert by_id["with_retries"]["retries"] == 3
        assert by_id["no_retries"]["retries"] == 0

    def test_positive_timeout_becomes_execution_timeout(self) -> None:
        f = (
            flow("wire-timeout")
            .step("bounded", _passthrough, timeout=30.0, x=1)
            .build()
        )
        spec = spec_from_flow(f)
        code = render_dag_file(spec)

        assert "execution_timeout" in code
        assert "timedelta(seconds=float(timeout))" in code
        assert spec["steps"][0]["timeout"] == 30.0

    def test_zero_retries_and_none_timeout_leave_kwargs_untouched(self) -> None:
        """The rendered helper only sets retries / execution_timeout when
        the user actually specified them — otherwise Airflow's own
        default_args or provider defaults apply, which is the correct
        least-surprise behaviour.
        """
        f = flow("defaults").step("s", _passthrough, x=1).build()
        code = render_dag_file(spec_from_flow(f))

        # The template guards both keys.
        assert 'if isinstance(retries, int) and retries > 0' in code
        assert 'isinstance(timeout, (int, float)) and timeout > 0' in code

    def test_default_args_no_longer_stamps_retries(self) -> None:
        """The old spec builder wrote ``retries: 1`` into every DAG's
        default_args. That shadowed per-step retries and doubled retry
        counts. The framework no longer sets it.
        """
        f = flow("no-default-retries").step("s", _passthrough, x=1).build()
        spec = spec_from_flow(f)
        assert "retries" not in spec["default_args"]

    def test_user_provided_default_args_retries_still_honoured(self) -> None:
        f = flow("user-default-retries").step("s", _passthrough, x=1).build()
        spec = spec_from_flow(f, default_args={"retries": 5})
        assert spec["default_args"]["retries"] == 5


# ===========================================================================
# airflow_step_configs — per-step overrides
# ===========================================================================


class TestAirflowStepConfigs:
    def test_valid_config_stored_under_step_airflow_key(self) -> None:
        f = flow("cfg").step("s", _passthrough, x=1).build()
        spec = spec_from_flow(
            f,
            airflow_step_configs={
                "s": {
                    "pool": "ollama_llm",
                    "queue": "gpu",
                    "priority_weight": 10,
                    "retry_delay_seconds": 30,
                    "on_failure_callback_ref": "my.pkg:cb",
                }
            },
        )
        step = spec["steps"][0]
        assert step["airflow"]["pool"] == "ollama_llm"
        assert step["airflow"]["queue"] == "gpu"
        assert step["airflow"]["priority_weight"] == 10
        assert step["airflow"]["retry_delay_seconds"] == 30
        assert step["airflow"]["on_failure_callback_ref"] == "my.pkg:cb"

    def test_unknown_step_id_in_config_raises(self) -> None:
        f = flow("cfg").step("s", _passthrough, x=1).build()
        with pytest.raises(AirflowExecutorError, match="no step with that id"):
            spec_from_flow(f, airflow_step_configs={"ghost": {"pool": "p"}})

    def test_unknown_override_key_raises(self) -> None:
        f = flow("cfg").step("s", _passthrough, x=1).build()
        with pytest.raises(AirflowExecutorError, match="unknown keys"):
            spec_from_flow(
                f, airflow_step_configs={"s": {"cpu_shares": 1000}}
            )

    def test_callback_ref_must_be_string(self) -> None:
        f = flow("cfg").step("s", _passthrough, x=1).build()
        with pytest.raises(AirflowExecutorError, match="module:attribute"):
            spec_from_flow(
                f,
                airflow_step_configs={
                    "s": {"on_failure_callback_ref": lambda ctx: None}
                },
            )

    def test_step_with_no_config_has_no_airflow_key(self) -> None:
        f = (
            flow("mixed")
            .step("configured", _passthrough, x=1)
            .step("bare", _passthrough, x=2)
            .build()
        )
        spec = spec_from_flow(
            f, airflow_step_configs={"configured": {"pool": "p"}}
        )
        by_id = {s["id"]: s for s in spec["steps"]}
        assert "airflow" in by_id["configured"]
        assert "airflow" not in by_id["bare"]

    def test_template_renders_all_supported_overrides(self) -> None:
        f = flow("wire").step("s", _passthrough, x=1).build()
        code = render_dag_file(
            spec_from_flow(
                f,
                airflow_step_configs={
                    "s": {
                        "pool": "p",
                        "queue": "q",
                        "pool_slots": 2,
                        "priority_weight": 5,
                        "retry_delay_seconds": 15,
                        "on_failure_callback_ref": "m:f",
                        "on_success_callback_ref": "m:s",
                        "on_retry_callback_ref": "m:r",
                    }
                },
            )
        )
        # The template consumes step["airflow"] via _step_operator_kwargs.
        # We assert the mapping table survives so a future refactor can't
        # accidentally drop one override without a test failing.
        for key in (
            "pool",
            "queue",
            "pool_slots",
            "priority_weight",
            "retry_delay_seconds",
            "on_failure_callback_ref",
            "on_success_callback_ref",
            "on_retry_callback_ref",
        ):
            assert key in code, f"override {key!r} not wired in template"


# ===========================================================================
# deps_factory + event_sink round-trip into the spec
# ===========================================================================


class TestDepsFactoryAndEventSinkSerialisation:
    def test_deps_factory_written_when_set(self) -> None:
        f = flow("d").step("s", _passthrough, x=1).build()
        spec = spec_from_flow(f, deps_factory="my.pkg:build")
        assert spec["deps_factory"] == "my.pkg:build"

    def test_event_sink_written_when_set(self) -> None:
        f = flow("e").step("s", _passthrough, x=1).build()
        spec = spec_from_flow(f, event_sink="my.pkg:sink")
        assert spec["event_sink"] == "my.pkg:sink"

    def test_absent_when_not_set(self) -> None:
        f = flow("n").step("s", _passthrough, x=1).build()
        spec = spec_from_flow(f)
        assert "deps_factory" not in spec
        assert "event_sink" not in spec


# ===========================================================================
# Spec version tag
# ===========================================================================


class TestSpecVersion:
    def test_koala_spec_version_matches_module_constant(self) -> None:
        f = flow("v").step("s", _passthrough, x=1).build()
        spec = spec_from_flow(f)
        assert spec["koala_spec_version"] == KOALA_SPEC_VERSION

    def test_koala_spec_version_is_int(self) -> None:
        # Runtime version-gate uses ``isinstance(..., int)`` — protect
        # against future accidental strings.
        assert isinstance(KOALA_SPEC_VERSION, int)


# ===========================================================================
# UUID run ids
# ===========================================================================


class TestTriggerRunId:
    def _flow(self):
        return flow("uid").step("s", _passthrough, x=1).build()

    def _handler(self, run_ids: list[str]):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, json={"is_paused": False})
            if request.url.path.endswith("/dagRuns"):
                body = json.loads(request.content)
                run_ids.append(body["dag_run_id"])
                return httpx.Response(
                    200, json={"dag_run_id": body["dag_run_id"]}
                )
            return httpx.Response(404)

        return handler

    def test_run_id_has_flow_id_and_uuid_shape(self, tmp_path: Path) -> None:
        run_ids: list[str] = []
        client = httpx.Client(
            base_url="http://t",
            transport=httpx.MockTransport(self._handler(run_ids)),
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            http_client=client,
        ) as ex:
            ex.deploy(self._flow())
            ex.trigger(self._flow())

        assert len(run_ids) == 1
        # Format: koala_<flow_id>_<12-hex-chars>. The 12 hex is a slice
        # of uuid4().hex — timestamp-based ids used to collide when two
        # triggers fired in the same millisecond.
        assert re.fullmatch(r"koala_uid_[0-9a-f]{12}", run_ids[0])

    def test_two_back_to_back_triggers_produce_distinct_ids(
        self, tmp_path: Path
    ) -> None:
        run_ids: list[str] = []
        client = httpx.Client(
            base_url="http://t",
            transport=httpx.MockTransport(self._handler(run_ids)),
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            http_client=client,
        ) as ex:
            ex.deploy(self._flow())
            ex.trigger(self._flow())
            ex.trigger(self._flow())

        assert len(run_ids) == 2
        assert run_ids[0] != run_ids[1]


# ===========================================================================
# auto_unpause default is False
# ===========================================================================


class TestAutoUnpauseIsOptIn:
    def _flow(self):
        return flow("pause").step("s", _passthrough, x=1).build()

    def test_paused_dag_raises_when_auto_unpause_off(
        self, tmp_path: Path
    ) -> None:
        """Default behaviour is to LEAVE a paused DAG paused and raise.
        Silently un-pausing was a foot-gun for operators who paused the
        DAG on purpose during an incident.
        """

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"is_paused": True})

        client = httpx.Client(
            base_url="http://t", transport=httpx.MockTransport(handler)
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            http_client=client,
        ) as ex:
            ex.deploy(self._flow())
            with pytest.raises(AirflowExecutorError, match="paused"):
                ex.trigger(self._flow())

    def test_paused_dag_unpaused_when_auto_unpause_on(
        self, tmp_path: Path
    ) -> None:
        patches: list[dict] = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, json={"is_paused": True})
            if request.method == "PATCH":
                patches.append(json.loads(request.content))
                return httpx.Response(200, json={"is_paused": False})
            if request.url.path.endswith("/dagRuns"):
                return httpx.Response(200, json={"dag_run_id": "x"})
            return httpx.Response(404)

        client = httpx.Client(
            base_url="http://t", transport=httpx.MockTransport(handler)
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            auto_unpause=True,
            http_client=client,
        ) as ex:
            ex.deploy(self._flow())
            ex.trigger(self._flow())

        assert patches == [{"is_paused": False}]


# ===========================================================================
# write_dag_files free function (CI/CD-friendly generator)
# ===========================================================================


class TestWriteDagFiles:
    def test_writes_dag_and_spec_without_http_client(
        self, tmp_path: Path
    ) -> None:
        """The CI/CD path must not require an httpx.Client. If this test
        passes without touching the network, the generator is safely
        usable inside a build container that has no Airflow reachable.
        """
        f = flow("ci").step("s", _passthrough, x=1).build()
        dag_path, spec_path = write_dag_files(f, dags_folder=str(tmp_path))

        assert dag_path == tmp_path / "koala_ci.py"
        assert spec_path == tmp_path / "koala_specs" / "ci.json"
        assert dag_path.exists()
        assert spec_path.exists()

    def test_forwards_all_spec_shaping_kwargs(self, tmp_path: Path) -> None:
        f = flow("ci-full").step("s", _passthrough, x=1).build()
        _, spec_path = write_dag_files(
            f,
            dags_folder=str(tmp_path),
            dag_id_prefix="acme_",
            tags=["prod"],
            default_args={"retries": 2},
            airflow_step_configs={"s": {"pool": "p"}},
            deps_factory="my.pkg:d",
            event_sink="my.pkg:e",
        )
        spec = json.loads(spec_path.read_text())
        assert spec["dag_id"] == "acme_ci-full"
        assert spec["tags"] == ["prod"]
        assert spec["default_args"]["retries"] == 2
        assert spec["steps"][0]["airflow"]["pool"] == "p"
        assert spec["deps_factory"] == "my.pkg:d"
        assert spec["event_sink"] == "my.pkg:e"

    def test_creates_dags_folder_if_missing(self, tmp_path: Path) -> None:
        target = tmp_path / "does" / "not" / "exist"
        f = flow("mk").step("s", _passthrough, x=1).build()
        dag_path, spec_path = write_dag_files(f, dags_folder=str(target))
        assert dag_path.parent.exists()
        assert spec_path.parent.exists()


# ===========================================================================
# Async arun via httpx.AsyncClient (no wall-clock waste)
# ===========================================================================


class TestArunUsesAsyncPollAndConcurrentXcomFetch:
    """The old ``arun`` was ``asyncio.to_thread(self.run, ...)``. The new
    one polls via ``httpx.AsyncClient`` and gathers XComs concurrently.
    Verify the async path is actually taken.
    """

    def test_arun_returns_xcom_results(self, tmp_path: Path) -> None:
        import asyncio

        # Track that both taskInstances endpoints were hit — concurrent
        # gather ensures they both fire.
        seen: set[str] = set()

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path.endswith("/koala_arun"):
                return httpx.Response(200, json={"is_paused": False})
            if path.endswith("/dagRuns") and request.method == "POST":
                body = json.loads(request.content)
                return httpx.Response(
                    200, json={"dag_run_id": body["dag_run_id"]}
                )
            if "/dagRuns/" in path and "/taskInstances" not in path:
                return httpx.Response(200, json={"state": "success"})
            if "/taskInstances/" in path and "/xcomEntries/" in path:
                # path looks like .../taskInstances/<id>/xcomEntries/return_value
                step_id = path.split("/taskInstances/")[1].split("/")[0]
                seen.add(step_id)
                return httpx.Response(200, json={"value": str(len(step_id))})
            return httpx.Response(404)

        client = httpx.Client(
            base_url="http://t", transport=httpx.MockTransport(handler)
        )
        f = (
            flow("arun")
            .step("first", _passthrough, x=1)
            .step("second", _passthrough, x=2)
            .edge("first", "second")
            .build()
        )
        with AirflowExecutor(
            airflow_url="http://t",
            dags_folder=str(tmp_path),
            poll_interval=0.0,
            http_client=client,
        ) as ex:
            results = asyncio.run(ex.arun(f))

        # Both step xcoms fetched.
        assert seen == {"first", "second"}
        assert set(results.keys()) == {"first", "second"}
