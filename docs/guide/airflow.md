# Airflow deployment

`Flow.deploy_to_airflow(...)` deploys a Koala `Flow` as an Airflow DAG,
triggers a run, waits for it, and returns each step's result — one method
call.

Under the hood: `AirflowExecutor` compiles your Flow into a thin generated
DAG file plus a JSON spec, writes them to Airflow's `dags` folder, calls
Airflow's REST API to trigger and monitor the run, then pulls each task's
XCom.

## Local vs Airflow

The same `Flow` runs unchanged on `LocalExecutor` and on Airflow, but the
two executors don't have identical capabilities. Only Airflow-side
features are configured through this page; the core framework — Agents,
Tools, Models, Memory, Behaviours — is unchanged.

| Concern | Local (`LocalExecutor`) | Airflow (`AirflowExecutor`) |
|---|---|---|
| Retries per step | `.step(id, action, retries=N)` (honoured) | Same field, rendered into the generated `PythonOperator` |
| Timeout per step | `.step(..., timeout=S)` (honoured) | Same field, becomes `execution_timeout=timedelta(seconds=S)` |
| Dependency injection (`RunContext.deps`) | `LocalExecutor().run(flow, deps=...)` | `deps_factory="module:callable"` — the factory runs on the worker |
| Koala event stream (`ModelDelta`, `ToolCall`, ...) | Consumed in-process by the caller | Captured by `event_sink="module:callable"`; also logged to `koala.airflow` logger |
| OTel tracing | Native | Traceparent bridged via `dag_run.conf["koala_traceparent"]` |
| Pool / queue / priority_weight | Not applicable | Per step via `airflow_step_configs={step_id: {...}}` |
| Cycles / conditional edges | Not supported today | Not supported today |
| Human-in-the-loop approvals | Native via `AwaitingApproval` events | Not bridged — task completes or fails |

Anything not in the Airflow column means: the core framework provides it,
and the Airflow bridge collapses it to what fits inside a task boundary.

## The complete example — one file

```python
# dags/koala_flows/summarize.py
import os
import sys

from koala import Agent, Model, tool
from koala.orchestration import flow


summarizer = Agent(
    Model(f"ollama/{os.getenv('KOALA_OLLAMA_MODEL', 'qwen2.5:7b')}", temperature=0.2),
    name="summarizer",
    instructions="Write exactly two sentences of factual context on the topic.",
)


@tool
def word_count(text: str) -> int:
    """Return the token count."""
    return len(text.split())


pipeline = (
    flow("summarize")
    .step("summarize", summarizer, retries=2, timeout=30, input="$input.topic")
    .step("count",     word_count, text="$result.summarize")
    .edge("summarize", "count")
    .build()
)


if __name__ == "__main__":
    topic = sys.argv[1] if len(sys.argv) > 1 else "koalas"
    results = pipeline.deploy_to_airflow(input={"topic": topic})
    print(results["summarize"])
    print(f"[{results['count']} words]")
```

Run it directly:

```bash
uv run dags/koala_flows/summarize.py "quantum computing"
```

Per-step `retries` and `timeout` in the builder above are actually wired
into the generated `PythonOperator` — a step-level retry doesn't hit the
DAG-wide `default_args["retries"]`, and a step timeout raises an Airflow
`AirflowTaskTimeout` at the worker rather than being silently ignored.

## Prerequisites

1. **Ollama running** on the host with a chat-capable model:

    ```bash
    ollama serve
    ollama pull qwen2.5:7b
    ```

2. **Airflow up** (the shipped `docker-compose.yaml` bakes Koala into the image):

    ```bash
    docker compose up -d --build
    ```

    The compose file sets `OLLAMA_BASE_URL=http://host.docker.internal:11434/v1`
    on every Airflow container, so workers automatically reach the host's
    Ollama daemon.

## Methods on `AirflowExecutor`

`Flow.deploy_to_airflow(...)` wraps `AirflowExecutor(...).run(...)`. For
lower-level control:

| Method | What it does |
|---|---|
| `build_spec(flow)` | Return the JSON-safe spec dict. Pure — no I/O, no HTTP. |
| `render_dag_file(flow)` | Return the DAG-file Python source. Pure. |
| `deploy(flow) -> (dag_path, spec_path)` | Write both artifacts to `dags_folder`. |
| `trigger(flow, *, input=None, conf=None) -> str` | Trigger a run via REST. Returns the run id. |
| `wait(flow, run_id) -> dict` | Poll to completion, fetch every task's XCom. |
| `run(flow, ...)` | `deploy` + `trigger` + `wait`. |
| `arun(flow, ...)` | Async peer. Uses `httpx.AsyncClient` for the poll loop so many `arun` calls in the same event loop don't serialise on the sleep. |

Prefer `write_dag_files(...)` (below) over `deploy(flow)` in CI/CD — it
skips HTTP client construction entirely.

## Configuring the executor

```python
pipeline.deploy_to_airflow(
    input={"topic": "koalas"},

    # Airflow connection
    airflow_url="http://localhost:8080",
    auth=("airflow", "airflow"),
    api_prefix="api/v2",           # or "api/v1" for Airflow 2.x

    # DAG generation
    dags_folder="./dags",          # defaults to AIRFLOW__CORE__DAGS_FOLDER env or ./dags
    dag_id_prefix="koala_",
    tags=["koala", "prod"],
    default_args={"retries": 2},   # DAG-wide fallback; per-step retries win

    # Per-step Airflow overrides — Airflow-only, no leak into LocalExecutor
    airflow_step_configs={
        "summarize": {
            "pool": "ollama_llm",           # rate-limit LLM calls across the cluster
            "queue": "gpu",
            "priority_weight": 10,
            "retry_delay_seconds": 30,
            "on_failure_callback_ref": "my_pkg.callbacks:notify_slack",
        },
    },

    # Framework-level extension hooks
    deps_factory="my_pkg.deps:build_deps",   # RunContext(deps=<returned value>)
    event_sink="my_pkg.events:sink",         # sink(step_id, event) per Koala Event

    # Safety
    auto_unpause=False,            # default; True to override an operator's pause
    poll_interval=2.0,
    timeout=600.0,
)
```

Every kwarg after `input` / `conf` is forwarded to `AirflowExecutor(...)`.

### Dependency injection with `deps_factory`

`RunContext.deps` is a first-class Koala concept — a place for the DB pool,
HTTP client, tenant, or credentials your tools need. On `LocalExecutor` you
pass it directly. On Airflow, you pass a **reference to a factory**:

```python
# my_pkg/deps.py
from functools import lru_cache
import httpx

@lru_cache(maxsize=1)
def build_deps():
    return {"http": httpx.Client(timeout=30.0), "tenant": "acme"}
```

The runtime imports and calls it once per worker (cached by the
`module:attribute` string). The value is passed as `RunContext(deps=...)`
into every Agent and Tool dispatch in that task.

### Capturing the event stream with `event_sink`

Koala Runnables emit an ordered event stream — `Start`, `ModelDelta`,
`ToolCall`, `AwaitingApproval`, `Output`, `Error`, `Done`. Airflow's task
model collapses everything to one return value, so by default that stream
is discarded. `event_sink` gives you a hook to keep it:

```python
# my_pkg/events.py
import json, logging
log = logging.getLogger("my_app.koala")

def sink(step_id: str, event) -> None:
    log.info("event", extra={"step": step_id, "kind": event.kind,
                             "data": _json_safe(event)})
```

The sink is called with each event as it happens. Exceptions from the
sink are logged and swallowed — a broken sink never fails an
otherwise-successful task.

### OpenTelemetry propagation

If OTel is installed on both the trigger side (your Python process) and
the worker side (the Airflow container), `AirflowExecutor.trigger`
injects the current W3C traceparent into `dag_run.conf["koala_traceparent"]`,
and `run_step` uses it as the parent context for the worker-side span. No
config needed — the bridge is best-effort and no-ops when OTel isn't
present.

## Generating DAG files in CI/CD

Application code writing DAG files at runtime is convenient in a REPL,
but it's the wrong pattern for scheduled deployments — the `dags/` folder
is typically git-tracked and CD-deployed. Use `write_dag_files` instead:

```python
# scripts/generate_dags.py
from koala.orchestration import write_dag_files
from koala_flows.ollama_pipeline import pipeline

write_dag_files(
    pipeline,
    dags_folder="./dags",
    tags=["koala", "prod"],
    airflow_step_configs={
        "summarize": {"pool": "ollama_llm"},
    },
    deps_factory="my_pkg.deps:build_deps",
    event_sink="my_pkg.events:sink",
)
```

Run it in your build pipeline, commit the outputs, ship them with your
image. Zero HTTP calls, zero runtime state.

## How action objects get imported

The generated DAG file's `run_step` calls need an `import` statement per
step action so Airflow workers can reload the same object. Koala's
resolver figures this out automatically. Two typical setups:

**Pipeline module inside `dags/`** — the resolver walks the `__init__.py`
chain from the running script up to the package root:

```
dags/
  koala_flows/
    __init__.py
    summarize.py          # ← run this directly
```

The spec records `action_ref: "koala_flows.summarize:summarizer"`. Airflow
puts `/opt/airflow/dags` on `PYTHONPATH` inside every container, so that
import resolves everywhere.

**Pipeline module already installed as a package** — action objects live
in a normal package on `PYTHONPATH`. Nothing special needed; the resolver
picks up `__module__` directly.

If the resolver can't find a module-level binding for an object (e.g. an
inline-constructed `Agent`), the generator raises
`ActionSerializationError` with an actionable message. Fix by moving the
action to module scope or by passing `action_paths=`:

```python
pipeline.deploy_to_airflow(
    action_paths={"summarize": "my_package.pipelines:summarizer_v2"},
)
```

## Passing input

Two ways:

1. **`input=`** (recommended). Delivered under the DAG conf's `input`
   key. `$input.<key>` references in step args resolve from here at
   task-run time.

    ```python
    pipeline.deploy_to_airflow(input={"topic": "koalas", "audience": "adults"})
    ```

2. **`conf=`** — raw dag_run conf override, if you need to set custom keys:

    ```python
    pipeline.deploy_to_airflow(
        conf={"my_key": "value", "input": {"topic": "koalas"}},
    )
    ```

## Reading results

`deploy_to_airflow` returns `{step_id: xcom_value}`. Each value is whatever
the step's action returned, serialized through Airflow's XCom backend.
JSON is the default backend, so Pydantic outputs come back as plain dicts
unless you configure a custom backend. **Watch payload size** — the default
metadata DB has a 48 kB per-XCom limit; write large agent outputs to object
storage and pass a URI through XCom instead.

## Retries and idempotency

Two knobs, distinct behaviours:

- `.step(id, action, retries=N)` — per-step retries, wired into the
  generated `PythonOperator(retries=N)`. Set this deliberately for LLM
  steps: **a retry replays the LLM call**, so cost and non-determinism
  compound. Prefer `retries=0` for expensive Agent steps unless you have a
  cache or deterministic seed strategy.
- `default_args={"retries": N}` — DAG-wide fallback. Per-step retries
  win when both are set.

The framework does **not** stamp a global `retries` in the generated
`default_args` — it lets your per-step value win.

## Waiting for DAG parse

Airflow's dag-processor takes ~30 seconds to pick up a newly-written file.
`trigger` blocks until Airflow reports the DAG parsed. If the DAG doesn't
appear within 45 seconds you'll get `AirflowExecutorError` — usually a
syntax error in the generated file or a missing import on the worker.

By default, a paused DAG is **not** silently un-paused when you trigger —
that used to be a foot-gun for operators who paused a DAG on purpose.
Pass `auto_unpause=True` if you want the old behaviour.

## Run identifiers

Every trigger produces a run id of the form `koala_<flow_id>_<uuid12>`.
UUID rather than timestamp so parallel triggers can't collide.

## Errors

```python
from koala.orchestration import (
    ActionSerializationError,
    AirflowAPIError,
    AirflowExecutorError,
)

try:
    pipeline.deploy_to_airflow(input={"topic": "koalas"})
except ActionSerializationError:
    # An action can't be resolved to a module-level name.
    # Fix by moving it to module scope or passing action_paths=.
    ...
except AirflowAPIError as e:
    # Non-2xx from Airflow's REST API.
    print(e.status, e.body)
except AirflowExecutorError:
    # Timeout, run failed, DAG parse failure, paused DAG without auto_unpause,
    # spec version newer than the worker runtime supports.
    ...
```

## Reference

- `koala.orchestration.Flow.deploy_to_airflow` — the one-line convenience.
- `koala.orchestration.AirflowExecutor` — the underlying class.
- `koala.orchestration.write_dag_files` — CI/CD-friendly file generator.
- `koala.orchestration.airflow_runtime` — the worker-side dispatcher.
- `koala.orchestration.KOALA_SPEC_VERSION` — bumped when the spec schema
  gains a required field.
- `koala.orchestration.ActionSerializationError`,
  `koala.orchestration.AirflowAPIError`,
  `koala.orchestration.AirflowExecutorError`.

See the [API reference for `koala.orchestration`](../reference/orchestration.md).
