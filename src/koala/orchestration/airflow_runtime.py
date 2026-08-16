"""Task-time helpers used by generated Koala Airflow DAG files.

Everything here runs INSIDE Airflow — either during DAG parsing
(``load_spec``) or at task-execution time (``run_step``). The import
surface is deliberately tight so the Airflow dag-processor's per-file
parse stays fast; nothing here touches an LLM, opens an HTTP connection,
or constructs an ``Agent`` at module scope.

Public API used by generated DAG files:
    - :func:`load_spec` — cheap, cached JSON read of a flow spec.
    - :func:`run_step` — the sole task-time entry point. One call per
      Airflow task instance.

Design notes
------------

* **Bring-your-own-dispatch.** ``run_step`` resolves the action reference
  from the spec into a live Python object at task time (never at parse
  time), so a broken action never breaks DAG parsing.
* **Deterministic argument resolution.** ``$input.<key>`` reads from the
  DAG run's ``conf[input_key]`` (default ``conf["input"]``);
  ``$result.<step_id>[.<dotted.field>]`` pulls from Airflow XCom.
* **Type dispatch mirrors ``LocalExecutor``.** ``BaseAgent`` gets driven
  through ``astream`` so its Event stream survives the Airflow boundary;
  ``BaseTool`` gets ``run(ctx, args)``; ``Model`` gets
  ``chat([Message.user(prompt)])``; plain callables get invoked directly.
* **Spec version.** ``run_step`` refuses to execute a spec whose
  ``koala_spec_version`` is greater than the runtime's supported version
  so a newer DAG file hitting an older worker fails loudly, not
  silently.
* **DI + events + tracing are Airflow-only** — they're plumbed via
  ``deps_factory``, ``event_sink``, and ``traceparent`` (dag_run conf).
  The core Runnable protocol is unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import logging
import os
import re
from collections.abc import Iterator
from functools import lru_cache
from typing import Any

log = logging.getLogger("koala.airflow")

# The version this runtime speaks. Must match the value written by
# ``spec_from_flow``. Kept in sync via a one-line import to avoid a
# cyclic dependency: ``airflow.py`` imports this module.
_SUPPORTED_SPEC_VERSION = 1

# Env var that carries a W3C traceparent from the trigger process into
# the Airflow worker via dag_run.conf. Falls back to any Airflow-set
# traceparent that the OTel integration may have injected.
_TRACEPARENT_KEYS: tuple[str, ...] = (
    "koala_traceparent",
    "traceparent",
)

# ---------------------------------------------------------------------------
# Spec loading + version gating
# ---------------------------------------------------------------------------


@lru_cache(maxsize=256)
def load_spec(spec_path: str) -> dict[str, Any]:
    """Read a flow spec JSON, cached per-path.

    Called both at DAG-parse time (once per parse cycle) and at
    task-execution time. The lru_cache means repeated parses re-use the
    same dict without hitting disk. Cache invalidation happens naturally
    when the worker restarts; if you're mutating specs and don't see the
    change, restart your Airflow workers.

    Args:
        spec_path: Absolute path to the ``.json`` spec file. Generated DAG
            files pass an absolute path derived from ``__file__``.
    """
    with open(spec_path, encoding="utf-8") as f:
        return json.load(f)


def _check_spec_version(spec: dict[str, Any]) -> None:
    """Fail loud when a spec written by a newer koala hits an older runtime.

    Older specs (version <= supported) are always accepted — we grow the
    schema in backwards-compatible ways.
    """
    version = spec.get("koala_spec_version", 1)
    if not isinstance(version, int) or version > _SUPPORTED_SPEC_VERSION:
        raise RuntimeError(
            f"Spec version {version!r} is newer than this koala runtime "
            f"supports (version {_SUPPORTED_SPEC_VERSION}). Upgrade the "
            "koala package on your Airflow workers."
        )


# ---------------------------------------------------------------------------
# Argument reference resolution
# ---------------------------------------------------------------------------

_REF_INPUT = re.compile(r"^\$input\.(.+)$")
_REF_RESULT = re.compile(r"^\$result\.([^.]+)(?:\.(.+))?$")


def _dot_drill(value: Any, path: str) -> Any:
    """Walk ``a.b.c`` into a dict or object graph."""
    for part in path.split("."):
        if isinstance(value, dict):
            value = value[part]
        else:
            value = getattr(value, part)
    return value


def _run_conf(context: dict[str, Any]) -> dict[str, Any]:
    """Extract the ``dag_run.conf`` dict from an Airflow task context."""
    dag_run = context.get("dag_run")
    if dag_run is None:
        return {}
    return getattr(dag_run, "conf", None) or {}


def _resolve_ref(
    value: Any, context: dict[str, Any], input_key: str
) -> Any:
    """Resolve a single reference string, or pass non-strings through."""
    if not isinstance(value, str):
        return value

    m = _REF_INPUT.match(value)
    if m:
        conf = _run_conf(context)
        bag = conf.get(input_key, {})
        return _dot_drill(bag, m.group(1))

    m = _REF_RESULT.match(value)
    if m:
        step_id, remainder = m.group(1), m.group(2)
        ti = context.get("ti") or context.get("task_instance")
        if ti is None:
            raise RuntimeError(
                "cannot resolve $result reference: task_instance not in context"
            )
        pulled = ti.xcom_pull(task_ids=step_id)
        if remainder is None:
            return pulled
        return _dot_drill(pulled, remainder)

    return value


def _resolve_args(
    raw_args: dict[str, Any], context: dict[str, Any], input_key: str
) -> dict[str, Any]:
    """Substitute reference strings in a step's args dict."""
    return {
        k: _resolve_ref(v, context, input_key) for k, v in raw_args.items()
    }


# ---------------------------------------------------------------------------
# module:attribute import (used by action_ref, deps_factory, event_sink)
# ---------------------------------------------------------------------------


def _import_ref(ref: str) -> Any:
    """Load a ``module:attribute`` reference into a live Python object.

    Raises:
        ValueError: If ``ref`` doesn't contain ``:`` .
        ImportError: If the module can't be imported.
        AttributeError: If the module doesn't have the attribute.
    """
    if ":" not in ref:
        raise ValueError(
            f"invalid ref {ref!r}: expected 'module:attribute'"
        )
    module_name, attr_name = ref.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


# Preserved for backwards compatibility with tests that import
# ``_import_action`` directly. New code should call ``_import_ref``.
_import_action = _import_ref


@lru_cache(maxsize=32)
def _load_deps_factory(ref: str) -> Any:
    """Cache the deps_factory callable per-worker.

    LRU by the ``module:attribute`` string. The factory itself is called
    once per task instance in :func:`_dispatch`.
    """
    factory = _import_ref(ref)
    if not callable(factory):
        raise TypeError(
            f"deps_factory {ref!r} resolved to a non-callable "
            f"{type(factory).__name__}"
        )
    return factory


@lru_cache(maxsize=32)
def _load_event_sink(ref: str) -> Any:
    """Cache the event sink callable per-worker.

    The sink is called with ``(step_id, event)`` for every Koala Event a
    Runnable emits during a step. Exceptions from the sink are logged and
    swallowed so a broken sink can't fail an otherwise-successful task.
    """
    sink = _import_ref(ref)
    if not callable(sink):
        raise TypeError(
            f"event_sink {ref!r} resolved to a non-callable "
            f"{type(sink).__name__}"
        )
    return sink


# ---------------------------------------------------------------------------
# OTel traceparent bridging
# ---------------------------------------------------------------------------


def _extract_traceparent(context: dict[str, Any]) -> str | None:
    """Best-effort extraction of a W3C traceparent from the task context.

    Order:
        1. ``dag_run.conf["koala_traceparent"]`` (set by
           ``AirflowExecutor.trigger``).
        2. ``dag_run.conf["traceparent"]`` (fallback for external triggers).
        3. ``OTEL_TRACEPARENT`` env var.

    Returns ``None`` if none of the above are present.
    """
    conf = _run_conf(context)
    for key in _TRACEPARENT_KEYS:
        tp = conf.get(key)
        if isinstance(tp, str) and tp:
            return tp
    return os.environ.get("OTEL_TRACEPARENT") or None


@contextlib.contextmanager
def _optional_step_span(
    step_id: str, flow_id: str, traceparent: str | None
) -> Iterator[None]:
    """Enter an OTel span for the step if the SDK is available.

    Silently no-ops when OTel isn't installed on the worker — Airflow
    installations without OTel are common and should not fail.

    When ``traceparent`` is a W3C string, the extracted parent context is
    passed directly to ``start_as_current_span``. That's the documented
    way to root a new span in a remote parent; the older
    ``context.attach`` + ``start_as_current_span`` pair works but adds a
    manual detach step and an easy leak.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.propagate import extract
    except ImportError:
        yield
        return

    parent_ctx = None
    if traceparent:
        try:
            parent_ctx = extract({"traceparent": traceparent})
        except Exception:  # pragma: no cover — defensive
            parent_ctx = None

    tracer = trace.get_tracer("koala.airflow")
    with tracer.start_as_current_span(
        f"koala.step.{step_id}", context=parent_ctx
    ) as span:
        with contextlib.suppress(Exception):
            span.set_attribute("koala.flow_id", flow_id)
            span.set_attribute("koala.step_id", step_id)
        yield


# ---------------------------------------------------------------------------
# Type-based dispatch
# ---------------------------------------------------------------------------


def _extract_prompt(args: dict[str, Any]) -> str:
    """Best-effort prompt extraction for Agent/Model steps."""
    for key in ("prompt", "input", "text", "message", "question"):
        if key in args:
            return str(args[key])
    if len(args) == 1:
        return str(next(iter(args.values())))
    raise ValueError(
        f"Agent/Model step needs a 'prompt'/'input'/'text' arg; "
        f"got {list(args)}"
    )


def _emit_event(step_id: str, event: Any, sink: Any) -> None:
    """Log + forward one Koala Event. Never raises."""
    kind = getattr(event, "kind", type(event).__name__)
    log.info("koala.event step=%s kind=%s", step_id, kind)
    if sink is None:
        return
    try:
        sink(step_id, event)
    except Exception:  # noqa: BLE001 — event sinks must not fail the step
        log.exception(
            "event_sink raised for step=%s kind=%s; continuing", step_id, kind
        )


async def _drive_agent(
    agent: Any, prompt: str, deps: Any, step_id: str, sink: Any
) -> Any:
    """Consume an Agent's astream, capture events, return the final output.

    Prefer this over ``agent.arun(prompt)`` because it preserves the L1
    event stream — Model deltas, tool calls, approval prompts, errors —
    which would otherwise be discarded at the Airflow boundary.
    """
    from ..core.context import RunContext
    from ..core.events import Error as ErrorEvent
    from ..core.events import Output as OutputEvent

    ctx = RunContext(deps=deps)
    output_value: Any = None
    output_seen = False
    fatal_error: str | None = None

    async for event in agent.astream(ctx, prompt):
        _emit_event(step_id, event, sink)
        if isinstance(event, OutputEvent):
            output_value = event.value
            output_seen = True
        elif isinstance(event, ErrorEvent) and event.fatal:
            fatal_error = event.error

    if fatal_error is not None:
        # Surface the Koala-level error as a Python exception so Airflow
        # marks the task failed and its logs show a meaningful message.
        raise RuntimeError(f"Agent failed: {fatal_error}")
    if not output_seen:
        raise RuntimeError(
            f"Agent step {step_id!r} finished without emitting Output"
        )
    return output_value


def _dispatch(
    action: Any,
    args: dict[str, Any],
    *,
    step_id: str,
    deps: Any,
    sink: Any,
) -> Any:
    """Execute ``action`` with resolved ``args``. Type dispatched.

    Recognised action types:
        - ``BaseAgent``: driven through ``astream`` so events survive.
          Uses ``deps`` for the ``RunContext``.
        - ``BaseTool``: ``run(RunContext(deps=deps), args)``.
        - ``Model``: ``chat([Message.user(prompt)])``, returns ``.text``.
        - Coroutine function: awaited via ``asyncio.run``.
        - Plain callable: called directly with ``**args``.
    """
    # Imported lazily so the runtime doesn't force full koala import for
    # simple plain-callable steps.
    from ..agents.agent import BaseAgent
    from ..core.context import RunContext
    from ..core.messages import Message
    from ..models.model import Model
    from ..tools.base import BaseTool

    if isinstance(action, BaseAgent):
        prompt = _extract_prompt(args)
        return asyncio.run(_drive_agent(action, prompt, deps, step_id, sink))

    if isinstance(action, BaseTool):
        return asyncio.run(action.run(RunContext(deps=deps), args))

    if isinstance(action, Model):
        prompt = _extract_prompt(args)
        message = asyncio.run(action.chat([Message.user(prompt)]))
        return message.text

    if callable(action):
        result = action(**args)
        if asyncio.iscoroutine(result):
            return asyncio.run(result)
        return result

    raise TypeError(
        f"action of type {type(action).__name__!r} is not executable "
        "(expected BaseAgent, BaseTool, Model, or callable)"
    )


# ---------------------------------------------------------------------------
# The one task-time entry point
# ---------------------------------------------------------------------------


def run_step(
    *, spec_path: str, step_id: str, **context: Any
) -> Any:
    """Execute one Flow step. Called by every PythonOperator in a Koala DAG.

    This function is the ONLY thing generated DAG files call at task
    time. Framework updates to argument resolution, DI, event capture,
    or tracing land here — deployed DAG files don't need regeneration.

    Args:
        spec_path: Absolute path to the flow's JSON spec, baked into the
            generated DAG file.
        step_id: Which step to run. The generated DAG passes the value from
            each task's ``task_id``.
        **context: Airflow task context (``dag_run``, ``ti``,
            ``task_instance``, ``params``, etc.).

    Returns:
        The step's return value. Airflow stores this as an XCom under the
        key ``return_value`` automatically.
    """
    spec = load_spec(spec_path)
    _check_spec_version(spec)

    input_key = spec.get("input_key", "input")
    flow_id = spec.get("flow_id", "?")

    step_spec = next(
        (s for s in spec["steps"] if s["id"] == step_id), None
    )
    if step_spec is None:
        raise KeyError(
            f"step {step_id!r} not found in spec at {spec_path}"
        )

    action = _import_ref(step_spec["action_ref"])
    args = _resolve_args(step_spec.get("args", {}), context, input_key)

    # Airflow-only wiring — deps for RunContext, sink for L1 events, and
    # a traceparent bridge for OTel span continuity. All three are opt-in
    # via the executor's ``deps_factory=`` / ``event_sink=`` kwargs and
    # via the traceparent carried in dag_run.conf.
    deps = None
    deps_ref = spec.get("deps_factory")
    if isinstance(deps_ref, str) and deps_ref:
        deps = _load_deps_factory(deps_ref)()

    sink = None
    sink_ref = spec.get("event_sink")
    if isinstance(sink_ref, str) and sink_ref:
        sink = _load_event_sink(sink_ref)

    traceparent = _extract_traceparent(context)
    with _optional_step_span(step_id, flow_id, traceparent):
        return _dispatch(
            action, args, step_id=step_id, deps=deps, sink=sink
        )


__all__ = ["load_spec", "run_step"]
