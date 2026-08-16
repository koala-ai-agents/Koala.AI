"""Deploy Koala Flows to Apache Airflow.

Design follows the pattern used by Metaflow (`airflow create`), Kedro
(`kedro airflow create`), and Airflow's own [Dynamic DAG Generation]
guidance: emit a **thin per-flow ``.py`` file** plus a **JSON spec**, and
put all task-time logic behind
:func:`koala.orchestration.airflow_runtime.run_step`.

Two artifacts per Flow::

    dags/koala_<flow_id>.py              # ~30-line thin DAG file
    dags/koala_specs/<flow_id>.json      # steps + edges + tags + action refs

The thin DAG file only:
    * imports ``run_step`` + ``load_spec`` from the framework runtime
    * declares the DAG shell (id, schedule, tags)
    * loops the spec, creates one ``PythonOperator`` per step
    * wires edges

Everything else — argument resolution, action loading, type dispatch —
lives in :mod:`airflow_runtime`. When Airflow bumps its API surface, we
ship a new koala; deployed DAG files rarely need regeneration.

[Dynamic DAG Generation]:
    https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from .errors import FlowError
from .flow import Flow

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class AirflowExecutorError(FlowError):
    """Base for Airflow deployment errors."""


class AirflowAPIError(AirflowExecutorError):
    """Non-2xx response from Airflow's REST API."""

    def __init__(self, status: int, body: Any) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Airflow API {status}: {body}")


class ActionSerializationError(AirflowExecutorError):
    """An action can't be resolved to a module:attribute reference."""


# ---------------------------------------------------------------------------
# Action-ref resolver
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _ActionRef:
    """Where an action lives in importable Python."""

    module: str
    attribute: str

    def as_ref(self) -> str:
        """Return the ``module:attribute`` reference string."""
        return f"{self.module}:{self.attribute}"


def _resolve_module_name(mod_name: str) -> str | None:
    """Return an importable module name.

    For everything except ``__main__`` this is a passthrough. For
    ``__main__`` we walk the ``__init__.py`` chain from the running script
    up to the first non-package parent, producing the real dotted import
    path so Airflow workers can resolve it via their ``dags/`` PYTHONPATH
    entry.

    Returns ``None`` if ``__main__`` has no ``__file__`` (REPL) or if the
    script isn't inside a package (no adjacent ``__init__.py``).
    """
    if mod_name != "__main__":
        return mod_name
    main = sys.modules.get("__main__")
    if main is None:
        return None
    file = getattr(main, "__file__", None)
    if not file:
        return None
    script = Path(file).resolve()
    parts: list[str] = [script.stem]
    parent = script.parent
    while (parent / "__init__.py").exists():
        parts.append(parent.name)
        parent = parent.parent
    if len(parts) < 2:
        # No package chain — the script isn't importable by workers.
        return None
    parts.reverse()
    return ".".join(parts)


def _find_in_namespace(namespace: Any, obj: Any) -> str | None:
    """Return the first module-level variable name whose value ``is`` ``obj``."""
    try:
        items = vars(namespace)
    except TypeError:
        return None
    for name, value in items.items():
        if name.startswith("_"):
            continue
        if value is obj:
            return name
    return None


def _find_in_module(mod_name: str, obj: Any) -> str | None:
    """Import a module and scan it for a variable pointing at ``obj``."""
    try:
        module = importlib.import_module(mod_name)
    except ImportError:
        return None
    return _find_in_namespace(module, obj)


def _try_get_attr(mod_name: str, attr: str) -> Any:
    """Return ``getattr(import_module(mod_name), attr, None)``."""
    try:
        module = importlib.import_module(mod_name)
    except ImportError:
        return None
    return getattr(module, attr, None)


def _scan_all_modules(obj: Any) -> _ActionRef | None:
    """Walk every loaded module looking for a variable pointing at ``obj``.

    Order:
        1. ``__main__`` first (special-cased — walked via ``_resolve_module_name``).
        2. User modules (anything not part of the koala framework).

    Skips ``koala.*`` framework modules and stdlib-ish private modules.
    """
    for mod_name, module in list(sys.modules.items()):
        if module is None:
            continue

        # __main__ needs the __init__.py walk before we can name it. Check
        # it FIRST — before the underscore filter below, which would skip
        # `__main__` too.
        if mod_name == "__main__":
            resolved = _resolve_module_name(mod_name)
            if not resolved:
                continue
            found = _find_in_namespace(module, obj)
            if found is not None:
                return _ActionRef(module=resolved, attribute=found)
            continue

        # Skip the framework itself but keep user packages that share a
        # "koala_..." prefix (e.g. koala_flows).
        if mod_name == "koala" or mod_name.startswith("koala."):
            continue
        if mod_name.startswith("_"):
            continue

        found = _find_in_namespace(module, obj)
        if found is not None:
            return _ActionRef(module=mod_name, attribute=found)
    return None


def _resolve_action_ref(
    step_id: str,
    action: Any,
    action_paths: dict[str, str],
) -> _ActionRef:
    """Find the ``module:attribute`` pair for ``action``.

    Resolution order:
        1. ``action_paths[step_id]`` override.
        2. If ``action`` is a plain callable with ``__module__`` +
           ``__qualname__``, verify the module actually exposes it and
           return that.
        3. If ``action`` is a ``FunctionTool``, check the underlying
           function's origin module (special-casing ``__main__``).
        4. If ``action`` is any other instance (Agent, Model, ...),
           check the class's origin module.
        5. Scan every loaded module.
        6. Raise :class:`ActionSerializationError` with an actionable
           message.
    """
    # 1. Explicit override — highest priority.
    if step_id in action_paths:
        return _parse_ref_override(step_id, action_paths[step_id])

    # 2. Plain callable — trust __module__ + __qualname__ if the import
    # yields the same object back.
    if callable(action) and hasattr(action, "__qualname__"):
        module_name = _resolve_module_name(
            getattr(action, "__module__", "") or ""
        )
        if module_name:
            attr = action.__qualname__.split(".")[0]
            candidate = _try_get_attr(module_name, attr)
            if candidate is action:
                return _ActionRef(module=module_name, attribute=attr)

    # 3. FunctionTool — check the underlying function's module.
    from ..tools.function_tool import FunctionTool

    if isinstance(action, FunctionTool):
        original = action.func.__module__
        module_name = _resolve_module_name(original)
        if module_name:
            # For __main__ scripts, the identity lives in
            # sys.modules['__main__'] — a fresh import would produce new
            # objects that aren't `is` the action we're serialising.
            if original == "__main__":
                main = sys.modules.get("__main__")
                if main is not None:
                    found = _find_in_namespace(main, action)
                    if found is not None:
                        return _ActionRef(
                            module=module_name, attribute=found
                        )
            else:
                found = _find_in_module(module_name, action)
                if found is not None:
                    return _ActionRef(
                        module=module_name, attribute=found
                    )

    # 4. Generic instance (Agent, Model, custom Runnable) — check the
    # class's origin module.
    class_module = _resolve_module_name(type(action).__module__)
    if class_module:
        found = _find_in_module(class_module, action)
        if found is not None:
            return _ActionRef(module=class_module, attribute=found)

    # 5. Scan every loaded module.
    ref = _scan_all_modules(action)
    if ref is not None:
        return ref

    # 6. Give up with a message the user can act on.
    raise ActionSerializationError(
        f"Step {step_id!r}: can't find a module-level name for "
        f"{type(action).__name__} instance {action!r}. Either assign it to "
        f"a module-level variable in an importable package, or pass "
        f"action_paths={{{step_id!r}: 'module:attribute'}} on "
        "AirflowExecutor."
    )


def _parse_ref_override(step_id: str, ref: str) -> _ActionRef:
    """Accept either ``module:attr`` or ``module.attr`` in action_paths."""
    if ":" in ref:
        module, attr = ref.split(":", 1)
    elif "." in ref:
        module, attr = ref.rsplit(".", 1)
    else:
        raise AirflowExecutorError(
            f"action_paths[{step_id!r}]={ref!r}: must be 'module:attr' "
            "(or the legacy 'module.attr' form)."
        )
    if not module or not attr:
        raise AirflowExecutorError(
            f"action_paths[{step_id!r}]={ref!r}: module and attribute "
            "must both be non-empty."
        )
    return _ActionRef(module=module.strip(), attribute=attr.strip())


# ---------------------------------------------------------------------------
# Spec builder
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Spec version
# ---------------------------------------------------------------------------

#: Bumped whenever the spec schema gains a required field or an existing
#: field changes shape. ``run_step`` refuses to execute a spec whose
#: ``koala_spec_version`` is greater than the version the runtime supports —
#: the failure surface is a clear error, not silent misbehaviour when an
#: older Airflow worker sees a newer DAG file.
KOALA_SPEC_VERSION = 1


#: Whitelist of Airflow keys we forward from ``airflow_step_configs`` into
#: the generated DAG. Anything outside this set is rejected at spec-build
#: time — keeps the surface small and prevents silent typos.
_AIRFLOW_STEP_OVERRIDES: frozenset[str] = frozenset(
    {
        "pool",
        "pool_slots",
        "queue",
        "priority_weight",
        "retry_delay_seconds",
        "on_failure_callback_ref",
        "on_success_callback_ref",
        "on_retry_callback_ref",
    }
)


def _validate_step_overrides(
    step_id: str, overrides: dict[str, Any]
) -> dict[str, Any]:
    """Return the overrides dict unchanged after checking keys + types."""
    unknown = set(overrides) - _AIRFLOW_STEP_OVERRIDES
    if unknown:
        raise AirflowExecutorError(
            f"airflow_step_configs[{step_id!r}]: unknown keys "
            f"{sorted(unknown)}. Allowed: {sorted(_AIRFLOW_STEP_OVERRIDES)}"
        )
    # Callback refs must be strings — we serialise them into the DAG file
    # as module:attribute imports, not live callables.
    for cb_key in (
        "on_failure_callback_ref",
        "on_success_callback_ref",
        "on_retry_callback_ref",
    ):
        if cb_key in overrides and not isinstance(overrides[cb_key], str):
            raise AirflowExecutorError(
                f"airflow_step_configs[{step_id!r}][{cb_key!r}] must be a "
                "'module:attribute' string; live callables can't be JSON "
                "serialised into a spec."
            )
    return dict(overrides)


def spec_from_flow(
    flow: Flow,
    *,
    dag_id_prefix: str = "koala_",
    input_key: str = "input",
    tags: list[str] | None = None,
    default_args: dict[str, Any] | None = None,
    action_paths: dict[str, str] | None = None,
    registry: dict[str, Any] | None = None,
    airflow_step_configs: dict[str, dict[str, Any]] | None = None,
    deps_factory: str | None = None,
    event_sink: str | None = None,
) -> dict[str, Any]:
    """Turn a Flow into a JSON-serialisable spec dict.

    Every field is a plain string, number, list, or dict. Action objects
    become ``"module:attribute"`` references — nothing Pythonic survives
    JSON round-trip.

    Args:
        flow: The Flow to serialise.
        dag_id_prefix: Prepended to ``flow.id`` for the Airflow ``dag_id``.
        input_key: DAG-conf key holding the ``$input`` bag.
        tags: Airflow DAG tags.
        default_args: Extra values merged into the DAG's ``default_args``.
            The framework defaults ``owner="koala"`` and
            ``depends_on_past=False``. **Framework does NOT set a global
            ``retries``** — per-step retries live on each ``Step`` and are
            honoured task-by-task. If you want a DAG-wide default, pass
            ``default_args={"retries": N}``.
        action_paths: Explicit ``{step_id: "module:attribute"}`` overrides.
        registry: String-to-callable map for legacy string actions.
        airflow_step_configs: Airflow-only per-step overrides —
            ``{step_id: {pool, queue, priority_weight, retry_delay_seconds,
            on_failure_callback_ref, ...}}``. These are stored under
            ``step["airflow"]`` in the spec and consumed by the generated
            DAG file. **Never touch the shared ``Step`` dataclass** — they
            only apply to the Airflow executor.
        deps_factory: ``"module:callable"`` string used by the runtime to
            build ``deps`` for Agent/Tool ``RunContext``. See
            :mod:`koala.orchestration.airflow_runtime`.
        event_sink: ``"module:callable"`` string used by the runtime to
            forward Koala Events emitted during a step. See
            :mod:`koala.orchestration.airflow_runtime`.
    """
    resolved_paths = dict(action_paths or {})
    resolved_registry = dict(registry or {})
    resolved_step_configs = dict(airflow_step_configs or {})

    # Reject overrides that reference unknown step ids so users get a
    # loud error rather than silently-ignored config.
    known_step_ids = {s.id for s in flow.steps}
    for cfg_step_id in resolved_step_configs:
        if cfg_step_id not in known_step_ids:
            raise AirflowExecutorError(
                f"airflow_step_configs[{cfg_step_id!r}]: no step with "
                f"that id in flow {flow.id!r}. Steps: {sorted(known_step_ids)}"
            )

    step_specs: list[dict[str, Any]] = []
    for step in flow.steps:
        action = step.action
        # String action: resolve via registry, then serialise the callable.
        if isinstance(action, str):
            if action not in resolved_registry:
                raise AirflowExecutorError(
                    f"Step {step.id!r}: string action {action!r} "
                    f"not in registry. Available: "
                    f"{sorted(resolved_registry) or '(none)'}"
                )
            action = resolved_registry[action]

        ref = _resolve_action_ref(step.id, action, resolved_paths)

        step_spec: dict[str, Any] = {
            "id": step.id,
            "action_ref": ref.as_ref(),
            "args": dict(step.args),
            "timeout": step.timeout,
            "retries": step.retries,
        }
        if step.id in resolved_step_configs:
            step_spec["airflow"] = _validate_step_overrides(
                step.id, resolved_step_configs[step.id]
            )
        step_specs.append(step_spec)

    # Framework defaults for default_args. Notably absent: ``retries`` — LLM
    # calls are not idempotent, so we let per-step ``retries`` win instead
    # of stamping a DAG-wide value that would double every step's retry
    # count.
    merged_default_args: dict[str, Any] = {
        "owner": "koala",
        "depends_on_past": False,
    }
    merged_default_args.update(default_args or {})

    spec: dict[str, Any] = {
        "koala_spec_version": KOALA_SPEC_VERSION,
        "flow_id": flow.id,
        "flow_version": flow.version,
        "dag_id": f"{dag_id_prefix}{flow.id}",
        "input_key": input_key,
        "tags": list(tags or ["koala"]),
        "default_args": merged_default_args,
        "steps": step_specs,
        "edges": [list(e) for e in flow.edges],
    }
    if deps_factory is not None:
        spec["deps_factory"] = deps_factory
    if event_sink is not None:
        spec["event_sink"] = event_sink
    return spec


# ---------------------------------------------------------------------------
# Thin DAG file template
# ---------------------------------------------------------------------------

_DAG_FILE_TEMPLATE = '''\
"""GENERATED by koala. Flow id: {flow_id!s}. Flow version: {flow_version!s}.

DO NOT EDIT — regenerate via ``Flow.deploy_to_airflow()`` or
``AirflowExecutor.deploy()``.

Task-time logic (argument resolution, action loading, dispatch) lives in
:func:`koala.orchestration.airflow_runtime.run_step` — bumping the koala
package version picks up runtime improvements without needing to
regenerate this file.

Per-step Airflow overrides (pool, queue, priority_weight, retry_delay,
callback refs) are read from ``step["airflow"]`` in the spec at parse
time — see :func:`koala.orchestration.airflow.spec_from_flow` for the
schema. Callback refs are late-imported here so a broken callback module
never breaks DAG parsing.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# Airflow 3.x prefers `airflow.sdk`; 2.x uses `airflow.decorators`.
try:
    from airflow.sdk import dag  # Airflow 3
except ImportError:  # pragma: no cover
    from airflow.decorators import dag  # Airflow 2

# PythonOperator moved to the standard provider in Airflow 3.
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    from airflow.operators.python import PythonOperator

from koala.orchestration.airflow_runtime import load_spec, run_step

_DAG_FILE = Path(__file__).resolve()
_SPEC_PATH = str(_DAG_FILE.parent / "koala_specs" / "{flow_id!s}.json")


def _import_callback(ref: str) -> Any:
    """Late-import a ``module:attribute`` callback ref. Never fails at
    parse time — a bad ref just falls back to ``None`` so the DAG still
    imports and the operator runs with no callback.
    """
    if not isinstance(ref, str) or ":" not in ref:
        return None
    module_name, attr_name = ref.split(":", 1)
    try:
        return getattr(importlib.import_module(module_name), attr_name, None)
    except ImportError:
        return None


def _step_operator_kwargs(step: dict[str, Any]) -> dict[str, Any]:
    """Fold the step's spec + optional Airflow overrides into
    PythonOperator kwargs. Absent fields are left unset (Airflow uses its
    own default_args or built-in defaults).
    """
    kwargs: dict[str, Any] = {{
        "task_id": step["id"],
        "python_callable": run_step,
        "op_kwargs": {{
            "spec_path": _SPEC_PATH,
            "step_id": step["id"],
        }},
    }}
    retries = step.get("retries")
    if isinstance(retries, int) and retries > 0:
        kwargs["retries"] = retries
    timeout = step.get("timeout")
    if isinstance(timeout, (int, float)) and timeout > 0:
        kwargs["execution_timeout"] = timedelta(seconds=float(timeout))

    overrides = step.get("airflow") or {{}}
    for key in ("pool", "queue"):
        if key in overrides:
            kwargs[key] = overrides[key]
    if "pool_slots" in overrides:
        kwargs["pool_slots"] = overrides["pool_slots"]
    if "priority_weight" in overrides:
        kwargs["priority_weight"] = overrides["priority_weight"]
    if "retry_delay_seconds" in overrides:
        kwargs["retry_delay"] = timedelta(
            seconds=float(overrides["retry_delay_seconds"])
        )
    for cb_key, op_key in (
        ("on_failure_callback_ref", "on_failure_callback"),
        ("on_success_callback_ref", "on_success_callback"),
        ("on_retry_callback_ref", "on_retry_callback"),
    ):
        if cb_key in overrides:
            cb = _import_callback(overrides[cb_key])
            if cb is not None:
                kwargs[op_key] = cb
    return kwargs


@dag(
    dag_id={dag_id!r},
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags={tags!r},
    default_args={default_args!r},
    description="Generated by koala for flow {flow_id!r}.",
)
def _koala_flow() -> None:
    """DAG factory. Called once at parse time."""
    spec = load_spec(_SPEC_PATH)
    tasks: dict[str, PythonOperator] = {{}}
    # Deterministic ordering so the grid view is stable across parses.
    for step in sorted(spec["steps"], key=lambda s: s["id"]):
        tasks[step["id"]] = PythonOperator(**_step_operator_kwargs(step))
    for from_id, to_id in spec["edges"]:
        tasks[from_id] >> tasks[to_id]


_koala_flow()
'''


def render_dag_file(spec: dict[str, Any]) -> str:
    """Return the Python source for the thin per-flow DAG file.

    Pure — writes nothing, imports nothing framework-heavy. Given the same
    spec, produces the same file every time.
    """
    return _DAG_FILE_TEMPLATE.format(
        flow_id=spec["flow_id"],
        flow_version=spec["flow_version"],
        dag_id=spec["dag_id"],
        tags=spec["tags"],
        default_args=spec["default_args"],
    )


# ---------------------------------------------------------------------------
# CI/CD-friendly generator — writes DAG files without touching an
# Airflow REST endpoint.
# ---------------------------------------------------------------------------


def write_dag_files(
    flow: Flow,
    *,
    dags_folder: str | os.PathLike[str],
    dag_id_prefix: str = "koala_",
    input_key: str = "input",
    tags: list[str] | None = None,
    default_args: dict[str, Any] | None = None,
    action_paths: dict[str, str] | None = None,
    registry: dict[str, Any] | None = None,
    airflow_step_configs: dict[str, dict[str, Any]] | None = None,
    deps_factory: str | None = None,
    event_sink: str | None = None,
) -> tuple[Path, Path]:
    """Render + write a Flow's DAG file and JSON spec. No network I/O.

    Use this from CI/CD pipelines when you want to check the generated
    artifacts into git or into a built image, rather than mutating the
    scheduler's ``dags/`` folder from application code at runtime. It
    accepts every kwarg that :class:`AirflowExecutor` uses to shape the
    spec, so a generate step and a deploy step can share the same
    settings via a factory function or config file.

    Returns ``(dag_path, spec_path)`` — both absolute.
    """
    spec = spec_from_flow(
        flow,
        dag_id_prefix=dag_id_prefix,
        input_key=input_key,
        tags=tags,
        default_args=default_args,
        action_paths=action_paths,
        registry=registry,
        airflow_step_configs=airflow_step_configs,
        deps_factory=deps_factory,
        event_sink=event_sink,
    )
    dags = Path(dags_folder).resolve()
    specs = dags / "koala_specs"
    dags.mkdir(parents=True, exist_ok=True)
    specs.mkdir(parents=True, exist_ok=True)

    dag_path = dags / f"{dag_id_prefix}{flow.id}.py"
    spec_path = specs / f"{flow.id}.json"

    dag_path.write_text(render_dag_file(spec), encoding="utf-8")
    spec_path.write_text(
        json.dumps(spec, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return dag_path, spec_path


# ---------------------------------------------------------------------------
# AirflowExecutor
# ---------------------------------------------------------------------------


class AirflowExecutor:
    """Deploy Koala Flows to Airflow.

    Emits a thin per-flow ``.py`` file + a JSON spec, then uses Airflow's
    REST API to trigger + monitor the run. Framework versions
    independently of deployed flows — the thin DAG file imports
    :mod:`koala.orchestration.airflow_runtime`, so upgrading koala on your
    Airflow workers picks up runtime improvements without regenerating
    every DAG file.

    Usage::

        with AirflowExecutor(airflow_url="http://localhost:8080") as ex:
            results = ex.run(pipeline, input={"topic": "koalas"})

    Or via the one-liner on Flow::

        pipeline.deploy_to_airflow(input={"topic": "koalas"})

    Args:
        airflow_url: Base URL of the Airflow webserver.
        auth: ``(username, password)`` tuple. Default ``("admin", "admin")``.
        dags_folder: Where to write the ``.py`` and ``koala_specs/*.json``.
            Defaults to ``AIRFLOW__CORE__DAGS_FOLDER`` env var, or
            ``./dags`` relative to CWD.
        poll_interval: Seconds between DAG-run status polls.
        timeout: Max seconds to wait for a run. ``None`` disables.
        api_prefix: URL prefix for the Airflow REST API. ``"api/v1"``
            (default) works with Airflow 2.x and Airflow 3.x +
            FabAuthManager. Set ``"api/v2"`` for pure Airflow 3.x.
        default_args: Extra values merged into every generated DAG's
            ``default_args``. **Framework does NOT set a global
            ``retries``** — set per step via ``.step(..., retries=N)`` or
            add ``default_args={"retries": N}`` here.
        tags: Airflow DAG tags. Default ``["koala"]``.
        dag_id_prefix: Prepended to ``flow.id`` for the Airflow ``dag_id``.
        input_key: DAG-conf key holding the flow's ``$input`` bag.
        registry: Same shape as ``LocalExecutor.registry`` — resolves
            string step actions to callables at spec-generation time.
        action_paths: Explicit ``{step_id: "module:attribute"}`` overrides.
        airflow_step_configs: Per-step Airflow overrides —
            ``{step_id: {"pool", "queue", "priority_weight",
            "retry_delay_seconds", "on_failure_callback_ref", ...}}``.
            Airflow-only; never leaks into ``LocalExecutor``.
        deps_factory: ``"module:callable"`` string used by the runtime to
            build ``deps`` for ``RunContext``. The callable is imported
            once per worker and cached. Signature:
            ``deps_factory() -> Any``.
        event_sink: ``"module:callable"`` string used by the runtime to
            forward Koala Events during a step. Signature:
            ``event_sink(step_id: str, event: Event) -> None``.
        auto_unpause: When ``True``, ``trigger`` PATCHes the DAG to
            ``is_paused=False`` before firing. Default ``False`` — a
            paused DAG is often paused deliberately by an operator, and
            silent un-pause is a foot-gun in production.
        http_client: Optional pre-configured ``httpx.Client`` (useful for
            tests via ``MockTransport``).
    """

    def __init__(
        self,
        *,
        airflow_url: str = "http://localhost:8080",
        auth: tuple[str, str] | None = None,
        dags_folder: str | None = None,
        poll_interval: float = 2.0,
        timeout: float | None = 300.0,
        api_prefix: str = "api/v2",
        default_args: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        dag_id_prefix: str = "koala_",
        input_key: str = "input",
        registry: dict[str, Any] | None = None,
        action_paths: dict[str, str] | None = None,
        airflow_step_configs: dict[str, dict[str, Any]] | None = None,
        deps_factory: str | None = None,
        event_sink: str | None = None,
        auto_unpause: bool = False,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.airflow_url = airflow_url.rstrip("/")
        self.auth = auth or ("admin", "admin")
        if dags_folder is None:
            dags_folder = (
                os.environ.get("AIRFLOW__CORE__DAGS_FOLDER") or "./dags"
            )
        self.dags_folder = str(Path(dags_folder).resolve())
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.api_prefix = api_prefix.strip("/")
        self.default_args = dict(default_args or {})
        self.tags = list(tags or ["koala"])
        self.dag_id_prefix = dag_id_prefix
        self.input_key = input_key
        self.registry = dict(registry or {})
        self.action_paths = dict(action_paths or {})
        self.airflow_step_configs = dict(airflow_step_configs or {})
        self.deps_factory = deps_factory
        self.event_sink = event_sink
        self.auto_unpause = auto_unpause

        # Auth strategy depends on the API version:
        # * ``api/v2`` (Airflow 3, default) uses JWT — POST /auth/token with
        #   the credentials as JSON, then send ``Authorization: Bearer <jwt>``
        #   on every request.
        # * ``api/v1`` (Airflow 2 or legacy compat) uses HTTP Basic.
        if http_client is not None:
            self._client = http_client
        elif "v1" in self.api_prefix and "v2" not in self.api_prefix:
            self._client = httpx.Client(
                base_url=self.airflow_url,
                auth=httpx.BasicAuth(*self.auth),
                timeout=httpx.Timeout(30.0, connect=10.0),
            )
        else:
            self._client = httpx.Client(
                base_url=self.airflow_url,
                auth=_JwtAuth(
                    base_url=self.airflow_url,
                    username=self.auth[0],
                    password=self.auth[1],
                ),
                timeout=httpx.Timeout(30.0, connect=10.0),
            )

    # ------------------------------------------------------------------
    # Context management
    # ------------------------------------------------------------------

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "AirflowExecutor":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_spec(self, flow: Flow) -> dict[str, Any]:
        """Return the JSON-safe spec for a Flow. Pure — no I/O."""
        return spec_from_flow(
            flow,
            dag_id_prefix=self.dag_id_prefix,
            input_key=self.input_key,
            tags=self.tags,
            default_args=self.default_args,
            action_paths=self.action_paths,
            registry=self.registry,
            airflow_step_configs=self.airflow_step_configs,
            deps_factory=self.deps_factory,
            event_sink=self.event_sink,
        )

    def render_dag_file(self, flow: Flow) -> str:
        """Return the thin DAG-file Python source. Pure — no I/O."""
        return render_dag_file(self.build_spec(flow))

    def deploy(self, flow: Flow) -> tuple[Path, Path]:
        """Write the thin DAG file + JSON spec.

        Returns ``(dag_path, spec_path)`` — both absolute paths.
        """
        spec = self.build_spec(flow)
        dags = Path(self.dags_folder)
        specs = dags / "koala_specs"
        dags.mkdir(parents=True, exist_ok=True)
        specs.mkdir(parents=True, exist_ok=True)

        dag_path = dags / f"{self.dag_id_prefix}{flow.id}.py"
        spec_path = specs / f"{flow.id}.json"

        dag_path.write_text(render_dag_file(spec), encoding="utf-8")
        spec_path.write_text(
            json.dumps(spec, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        return dag_path, spec_path

    def trigger(
        self,
        flow: Flow,
        *,
        input: dict[str, Any] | None = None,
        conf: dict[str, Any] | None = None,
    ) -> str:
        """Trigger a DAG run. Returns the run id.

        Blocks until the scheduler reports the DAG parsed + auto-unpauses
        it before firing the trigger.
        """
        dag_id = f"{self.dag_id_prefix}{flow.id}"
        self._wait_for_dag_parsed(dag_id)

        payload: dict[str, Any] = dict(conf or {})
        if input is not None:
            existing = payload.get(self.input_key)
            if isinstance(existing, dict):
                existing.update(input)
            else:
                payload[self.input_key] = dict(input)

        # Best-effort OTel traceparent bridge — inject the current span's
        # traceparent into dag_run.conf so ``run_step`` can attach worker
        # spans as children. No-op when OTel isn't installed on the
        # trigger side.
        if "koala_traceparent" not in payload:
            tp = _current_traceparent()
            if tp is not None:
                payload["koala_traceparent"] = tp

        # UUID-based run id — a monotonic timestamp was previously used but
        # collided when two triggers fired within the same millisecond
        # (parallel test runs, retry loops). The flow.id prefix keeps
        # names greppable in the Airflow UI.
        run_id = f"koala_{flow.id}_{uuid.uuid4().hex[:12]}"
        resp = self._client.post(
            f"/{self.api_prefix}/dags/{dag_id}/dagRuns",
            json={
                "dag_run_id": run_id,
                "conf": payload,
                "logical_date": _now_iso_utc(),
            },
        )
        if resp.status_code >= 400:
            raise AirflowAPIError(resp.status_code, _body(resp))
        return run_id

    def wait(self, flow: Flow, run_id: str) -> dict[str, Any]:
        """Poll a DAG run to completion. Returns ``{step_id: xcom_value}``.

        Raises :class:`AirflowExecutorError` on timeout or terminal
        failure.
        """
        dag_id = f"{self.dag_id_prefix}{flow.id}"
        deadline = time.time() + (
            self.timeout if self.timeout is not None else float("inf")
        )

        state: str | None = None
        while True:
            resp = self._client.get(
                f"/{self.api_prefix}/dags/{dag_id}/dagRuns/{run_id}"
            )
            if resp.status_code >= 400:
                raise AirflowAPIError(resp.status_code, _body(resp))
            state = resp.json().get("state")
            if state in ("success", "failed"):
                break
            if time.time() > deadline:
                raise AirflowExecutorError(
                    f"DAG run {run_id!r} timed out in state "
                    f"{state!r} after {self.timeout}s"
                )
            time.sleep(self.poll_interval)

        if state != "success":
            raise AirflowExecutorError(
                f"DAG run {run_id!r} finished with state {state!r}. "
                "Check the Airflow UI for task logs."
            )

        return self._fetch_xcoms(dag_id, run_id, flow)

    def run(
        self,
        flow: Flow,
        *,
        input: dict[str, Any] | None = None,
        conf: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """``deploy`` + ``trigger`` + ``wait`` in one call."""
        self.deploy(flow)
        run_id = self.trigger(flow, input=input, conf=conf)
        return self.wait(flow, run_id)

    async def arun(
        self,
        flow: Flow,
        *,
        input: dict[str, Any] | None = None,
        conf: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Async version of :meth:`run`.

        The wait-poll loop runs on a real ``httpx.AsyncClient`` so multiple
        ``arun`` calls in the same event loop don't serialise on 2-second
        sleeps. ``deploy`` (filesystem I/O) and ``trigger`` (one POST)
        stay on the sync client — offloaded to a worker thread — because
        the async savings there are negligible and duplicating the sync
        logic would be worse than the small ``asyncio.to_thread`` cost.
        """
        import asyncio

        # Deploy + trigger via the sync path, offloaded so the loop keeps
        # spinning during Airflow's dag-parse wait.
        await asyncio.to_thread(self.deploy, flow)
        run_id = await asyncio.to_thread(
            self.trigger, flow, input=input, conf=conf
        )
        return await self._await(flow, run_id)

    async def _await(self, flow: Flow, run_id: str) -> dict[str, Any]:
        """Async twin of :meth:`wait` — uses a real ``httpx.AsyncClient``.

        The async client borrows the same auth strategy as the sync client
        via ``_build_async_auth``. Constructed per-call so it inherits
        whatever the sync ``self._client`` was configured with (mock
        transport for tests, JWT for prod, basic for legacy).
        """
        import asyncio

        dag_id = f"{self.dag_id_prefix}{flow.id}"
        deadline = time.time() + (
            self.timeout if self.timeout is not None else float("inf")
        )
        state: str | None = None

        async with self._async_client_like_sync() as client:
            while True:
                resp = await client.get(
                    f"/{self.api_prefix}/dags/{dag_id}/dagRuns/{run_id}"
                )
                if resp.status_code >= 400:
                    raise AirflowAPIError(resp.status_code, _body(resp))
                state = resp.json().get("state")
                if state in ("success", "failed"):
                    break
                if time.time() > deadline:
                    raise AirflowExecutorError(
                        f"DAG run {run_id!r} timed out in state "
                        f"{state!r} after {self.timeout}s"
                    )
                await asyncio.sleep(self.poll_interval)

            if state != "success":
                raise AirflowExecutorError(
                    f"DAG run {run_id!r} finished with state {state!r}. "
                    "Check the Airflow UI for task logs."
                )

            return await self._afetch_xcoms(client, dag_id, run_id, flow)

    def _async_client_like_sync(self) -> httpx.AsyncClient:
        """Build an ``AsyncClient`` that mirrors the sync client's config.

        We can't share the sync transport across event loops, so we build
        a fresh async client with the same base URL and a compatible auth
        strategy. Tests that inject a MockTransport can construct their
        own async pipeline; production paths get the same JWT/Basic
        strategy as the sync client.
        """
        # If the caller pre-wired a custom sync transport (tests), give
        # them the same handler on the async side too.
        transport = getattr(self._client, "_transport", None)
        if isinstance(transport, httpx.MockTransport):
            async_transport = httpx.MockTransport(transport.handler)  # type: ignore[attr-defined]
            return httpx.AsyncClient(
                base_url=self.airflow_url,
                transport=async_transport,
                timeout=httpx.Timeout(30.0, connect=10.0),
            )

        # Production: mirror the sync auth choice.
        if "v1" in self.api_prefix and "v2" not in self.api_prefix:
            return httpx.AsyncClient(
                base_url=self.airflow_url,
                auth=httpx.BasicAuth(*self.auth),
                timeout=httpx.Timeout(30.0, connect=10.0),
            )
        return httpx.AsyncClient(
            base_url=self.airflow_url,
            auth=_JwtAuth(
                base_url=self.airflow_url,
                username=self.auth[0],
                password=self.auth[1],
            ),
            timeout=httpx.Timeout(30.0, connect=10.0),
        )

    async def _afetch_xcoms(
        self,
        client: httpx.AsyncClient,
        dag_id: str,
        run_id: str,
        flow: Flow,
    ) -> dict[str, Any]:
        """Async twin of :meth:`_fetch_xcoms` — fetches every step's
        ``return_value`` XCom concurrently.
        """
        import asyncio

        async def fetch_one(step_id: str) -> tuple[str, Any]:
            resp = await client.get(
                f"/{self.api_prefix}/dags/{dag_id}"
                f"/dagRuns/{run_id}"
                f"/taskInstances/{step_id}"
                f"/xcomEntries/return_value",
            )
            if resp.status_code == 404:
                return step_id, None
            if resp.status_code >= 400:
                raise AirflowAPIError(resp.status_code, _body(resp))
            body = resp.json()
            value = body.get("value")
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    pass
            return step_id, value

        pairs = await asyncio.gather(
            *(fetch_one(s.id) for s in flow.steps)
        )
        return dict(pairs)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wait_for_dag_parsed(
        self, dag_id: str, timeout: float = 45.0
    ) -> None:
        """Wait for the scheduler to parse a newly-written DAG file.

        Airflow's dag-processor scans ``dags_folder`` every
        ``min_file_process_interval`` (default 30 s). Freshly written
        files typically appear within 5–30 seconds.

        If the DAG is paused and ``auto_unpause=True`` was passed to the
        constructor, PATCH it un-paused before returning. Otherwise leave
        the pause state alone — silently overriding an operator's pause
        is a foot-gun in production.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = self._client.get(f"/{self.api_prefix}/dags/{dag_id}")
            if resp.status_code == 200:
                body = resp.json()
                if body.get("is_paused") and self.auto_unpause:
                    self._client.patch(
                        f"/{self.api_prefix}/dags/{dag_id}",
                        json={"is_paused": False},
                    )
                elif body.get("is_paused"):
                    # The caller has to know so they can un-pause + retry.
                    raise AirflowExecutorError(
                        f"DAG {dag_id!r} is paused. Un-pause it in the "
                        "Airflow UI, or construct AirflowExecutor with "
                        "auto_unpause=True."
                    )
                return
            time.sleep(1.0)
        raise AirflowExecutorError(
            f"DAG {dag_id!r} didn't appear in Airflow within {timeout}s. "
            "Check the scheduler / dag-processor logs for import errors."
        )

    def _fetch_xcoms(
        self, dag_id: str, run_id: str, flow: Flow
    ) -> dict[str, Any]:
        """Pull each step's ``return_value`` XCom."""
        results: dict[str, Any] = {}
        for step in flow.steps:
            resp = self._client.get(
                f"/{self.api_prefix}/dags/{dag_id}"
                f"/dagRuns/{run_id}"
                f"/taskInstances/{step.id}"
                f"/xcomEntries/return_value",
            )
            if resp.status_code == 200:
                body = resp.json()
                value = body.get("value")
                # Airflow returns XCom values as JSON-encoded strings by
                # default — attempt a decode, fall back to raw.
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (json.JSONDecodeError, ValueError):
                        pass
                results[step.id] = value
            elif resp.status_code == 404:
                # Some tasks skip return_value (e.g. no-op steps). Record
                # None so callers can tell they ran but didn't emit XCom.
                results[step.id] = None
            else:
                raise AirflowAPIError(resp.status_code, _body(resp))
        return results


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


class _JwtAuth(httpx.Auth):
    """Airflow 3.x FabAuthManager JWT auth.

    Airflow 3 dropped basic auth for its ``/api/v2/*`` REST surface. The
    replacement flow is:

        1. POST ``{"username", "password"}`` (JSON) to ``/auth/token``.
        2. Include ``Authorization: Bearer <jwt>`` on every subsequent
           request.
        3. If a request comes back 401, the token has expired — refresh
           and retry once.

    Tokens are lazily fetched: no network call happens until the first
    real API request.

    This class implements both ``auth_flow`` (sync path) and
    ``async_auth_flow`` (async path). The async path uses an
    ``httpx.AsyncClient`` for the token fetch so it does NOT block the
    event loop. The base ``httpx.Auth.async_auth_flow`` implementation
    would otherwise run the sync ``auth_flow`` on the calling thread,
    which combined with a blocking ``httpx.Client`` inside would stall
    the loop for the full token-fetch duration.
    """

    # We need to inspect response status codes to detect expiry.
    requires_response_body = False

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._token: str | None = None

    # -- sync path ------------------------------------------------------

    def auth_flow(self, request: httpx.Request):
        if self._token is None:
            self._fetch_token_sync()
        request.headers["Authorization"] = f"Bearer {self._token}"
        response = yield request

        # On 401, refresh once and retry.
        if response.status_code == 401:
            self._fetch_token_sync()
            request.headers["Authorization"] = f"Bearer {self._token}"
            yield request

    def _fetch_token_sync(self) -> None:
        # Bare client — must not recurse through this auth handler.
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                f"{self._base_url}/auth/token",
                json={
                    "username": self._username,
                    "password": self._password,
                },
            )
        self._absorb_token(resp)

    # -- async path -----------------------------------------------------

    async def async_auth_flow(self, request: httpx.Request):
        if self._token is None:
            await self._fetch_token_async()
        request.headers["Authorization"] = f"Bearer {self._token}"
        response = yield request

        if response.status_code == 401:
            await self._fetch_token_async()
            request.headers["Authorization"] = f"Bearer {self._token}"
            yield request

    async def _fetch_token_async(self) -> None:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{self._base_url}/auth/token",
                json={
                    "username": self._username,
                    "password": self._password,
                },
            )
        self._absorb_token(resp)

    # -- shared ---------------------------------------------------------

    def _absorb_token(self, resp: httpx.Response) -> None:
        if resp.status_code >= 400:
            raise AirflowAPIError(resp.status_code, _body(resp))
        payload = resp.json()
        token = payload.get("access_token")
        if not isinstance(token, str):
            raise AirflowExecutorError(
                f"/auth/token response missing 'access_token': {payload!r}"
            )
        self._token = token


def _body(resp: httpx.Response) -> Any:
    """Best-effort JSON body extraction for error messages."""
    try:
        return resp.json()
    except (json.JSONDecodeError, ValueError):
        return resp.text


def _now_iso_utc() -> str:
    """Return an ISO-8601 UTC timestamp Airflow accepts as ``logical_date``."""
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _current_traceparent() -> str | None:
    """Return the W3C traceparent for the active OTel span, or ``None``.

    Silently returns ``None`` when OpenTelemetry isn't installed or when
    no span is currently active — the trigger side must not fail just
    because tracing isn't wired up.
    """
    try:
        from opentelemetry.propagate import inject
    except ImportError:
        return None
    carrier: dict[str, str] = {}
    try:
        inject(carrier)
    except Exception:  # pragma: no cover — defensive
        return None
    return carrier.get("traceparent") or None


__all__ = [
    "KOALA_SPEC_VERSION",
    "AirflowExecutor",
    "AirflowExecutorError",
    "AirflowAPIError",
    "ActionSerializationError",
    "spec_from_flow",
    "render_dag_file",
    "write_dag_files",
]
