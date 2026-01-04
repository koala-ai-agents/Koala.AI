from __future__ import annotations

import asyncio
import concurrent.futures
import importlib
import inspect
import json
import os
import sqlite3
import time
from abc import ABC, abstractmethod
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from . import observability
from .guards import GuardError, GuardsRegistry


class FlowError(Exception):
    pass


@dataclass
class Step:
    id: str
    action: str  # name of the action/function to run
    args: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "action": self.action, "args": self.args}

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Step":
        return Step(id=d["id"], action=d["action"], args=d.get("args", {}))


@dataclass
class DAGFlow:
    id: str
    steps: List[Step] = field(default_factory=list)
    edges: List[tuple] = field(default_factory=list)  # (from_id, to_id)
    version: str = "0.1.0"

    def add_step(self, step: Step) -> None:
        if any(s.id == step.id for s in self.steps):
            raise FlowError(f"Step with id {step.id} already exists")
        self.steps.append(step)

    def add_edge(self, from_id: str, to_id: str) -> None:
        if not any(s.id == from_id for s in self.steps):
            raise FlowError(f"Unknown step {from_id}")
        if not any(s.id == to_id for s in self.steps):
            raise FlowError(f"Unknown step {to_id}")
        self.edges.append((from_id, to_id))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "version": self.version,
            "steps": [s.to_dict() for s in self.steps],
            "edges": [[f, t] for f, t in self.edges],
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DAGFlow":
        flow = DAGFlow(id=d["id"], version=d.get("version", "0.1.0"))
        for s in d.get("steps", []):
            flow.steps.append(Step.from_dict(s))
        for f, t in d.get("edges", []):
            flow.edges.append((f, t))
        return flow

    def dumps(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @staticmethod
    def loads(s: str) -> "DAGFlow":
        return DAGFlow.from_dict(json.loads(s))

    def _toposort(self) -> List[Step]:
        # Kahn's algorithm for simplicity
        nodes = {s.id: s for s in self.steps}
        incoming = dict.fromkeys(nodes, 0)
        outgoing: Dict[str, List[str]] = {nid: [] for nid in nodes}
        for f, t in self.edges:
            outgoing[f].append(t)
            incoming[t] += 1

        queue = [nid for nid, deg in incoming.items() if deg == 0]
        order: List[Step] = []
        while queue:
            n = queue.pop(0)
            order.append(nodes[n])
            for m in outgoing[n]:
                incoming[m] -= 1
                if incoming[m] == 0:
                    queue.append(m)

        if len(order) != len(self.steps):
            raise FlowError("Cycle detected or missing nodes in flow")
        return order

    def run(self, registry: Dict[str, Callable[..., Any]]) -> Dict[str, Any]:
        """Run the flow using functions in registry keyed by action name.

        Returns a mapping from step id to result.
        """
        order = self._toposort()
        results: Dict[str, Any] = {}
        for step in order:
            func = registry.get(step.action)
            if func is None:
                raise FlowError(f"Action {step.action} not found in registry")
            # allow args to reference previous results by using a special syntax
            resolved_args = {}
            for k, v in step.args.items():
                if isinstance(v, str) and v.startswith("$result."):
                    ref_id = v.split("$result.", 1)[1]
                    resolved_args[k] = results.get(ref_id)
                else:
                    resolved_args[k] = v
            results[step.id] = func(**resolved_args)
        return results


class FlowBuilder:
    """Small fluent builder for DAGFlow to provide a compact Python DSL.

    Example:
        f = dag('myflow').step('a', 'noop').step('b', 'noop').edge('a','b').build()
    """

    def __init__(self, id: str, version: Optional[str] = None) -> None:
        self._flow = DAGFlow(id=id, version=version or "0.1.0")

    def step(self, id: str, action: str, **kwargs: Any) -> "FlowBuilder":
        self._flow.add_step(Step(id=id, action=action, args=kwargs))
        return self

    def edge(self, from_id: str, to_id: str) -> "FlowBuilder":
        self._flow.add_edge(from_id, to_id)
        return self

    def build(self) -> DAGFlow:
        return self._flow


def dag(id: str, version: Optional[str] = None) -> FlowBuilder:
    """Convenience helper to start a DAGFlow builder."""
    return FlowBuilder(id=id, version=version)


@dataclass
class State:
    id: str
    action: Optional[str] = None
    on: Dict[str, str] = field(default_factory=dict)  # event -> next_state_id


@dataclass
class StateMachine:
    id: str
    states: List[State] = field(default_factory=list)
    start_state: Optional[str] = None
    version: str = "0.1.0"

    def add_state(self, state: State) -> None:
        if any(s.id == state.id for s in self.states):
            raise FlowError(f"State {state.id} already exists")
        self.states.append(state)
        if self.start_state is None:
            self.start_state = state.id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "version": self.version,
            "start_state": self.start_state,
            "states": [
                {"id": s.id, "action": s.action, "on": s.on} for s in self.states
            ],
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "StateMachine":
        sm = StateMachine(id=d["id"], version=d.get("version", "0.1.0"))
        sm.start_state = d.get("start_state")
        for s in d.get("states", []):
            sm.states.append(
                State(id=s["id"], action=s.get("action"), on=s.get("on", {}))
            )
        return sm

    def dumps(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @staticmethod
    def loads(s: str) -> "StateMachine":
        return StateMachine.from_dict(json.loads(s))

    def run(
        self, registry: Dict[str, Callable[..., Any]], events: List[str]
    ) -> Dict[str, Any]:
        """Run the state machine by feeding a sequence of events.

        Returns mapping state_id -> result.
        """
        if not self.start_state:
            raise FlowError("No start state defined")
        states_map = {s.id: s for s in self.states}
        current = self.start_state
        results: Dict[str, Any] = {}
        for ev in events:
            st = states_map.get(current)
            if st is None:
                raise FlowError(f"Unknown state {current}")
            if st.action:
                func = registry.get(st.action)
                if func is None:
                    raise FlowError(f"Action {st.action} not found in registry")
                results[current] = func(event=ev, state=current)
            nxt = st.on.get(ev)
            if nxt is None:
                break
            current = nxt
        return results


class FlowRepository:
    """Simple persistence for flows using SQLite or filesystem JSON.

    This is intentionally minimal — it's a convenience to persist flow defs during early development.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path
        self.conn: sqlite3.Connection | None = None
        if db_path:
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            self.conn = sqlite3.connect(db_path)
            self._ensure_table()
        else:
            self.conn = None

    def _ensure_table(self) -> None:
        assert self.conn is not None
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS flows (
                id TEXT PRIMARY KEY,
                type TEXT,
                version TEXT,
                payload TEXT
            )
            """
        )
        self.conn.commit()

    def save_flow(self, flow: Any) -> None:
        payload = None
        ftype = type(flow).__name__
        if hasattr(flow, "dumps"):
            payload = flow.dumps()
        else:
            payload = json.dumps(flow)

        if self.conn:
            cur = self.conn.cursor()
            cur.execute(
                "REPLACE INTO flows (id, type, version, payload) VALUES (?, ?, ?, ?)",
                (getattr(flow, "id", ""), ftype, getattr(flow, "version", ""), payload),
            )
            self.conn.commit()
        else:
            # filesystem fallback
            out = f"{getattr(flow, 'id', 'flow')}.{ftype}.json"
            with open(out, "w", encoding="utf-8") as fh:
                fh.write(payload)

    def load_flow(self, flow_id: str) -> Optional[Any]:
        if self.conn:
            cur = self.conn.cursor()
            cur.execute("SELECT type, payload FROM flows WHERE id = ?", (flow_id,))
            row = cur.fetchone()
            if not row:
                return None
            ftype, payload = row
            if ftype == "DAGFlow":
                return DAGFlow.loads(payload)
            if ftype == "StateMachine":
                return StateMachine.loads(payload)
            return json.loads(payload)
        else:
            # filesystem fallback
            for fname in os.listdir("."):
                if fname.startswith(flow_id) and fname.endswith(".json"):
                    with open(fname, "r", encoding="utf-8") as fh:
                        content = fh.read()
                    # try to detect type
                    try:
                        d = json.loads(content)
                    except Exception:
                        return None
                    if "steps" in d:
                        return DAGFlow.from_dict(d)
                    if "states" in d:
                        return StateMachine.from_dict(d)
            return None


class LocalExecutor:
    """A simple local executor for Flow primitives.

    - run_dagflow: runs a DAGFlow using a thread pool to execute independent steps in parallel
    - run_state_machine: runs a StateMachine sequentially
    Supports regular functions and coroutine functions (async defs).
    """

    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers or (min(32, (os.cpu_count() or 1) + 4))
        # global default retry policy for steps (can be overridden per-step)
        from .observability import RetryPolicy

        self.default_retry = RetryPolicy(max_attempts=1, backoff=0.0, multiplier=1.0)

    @staticmethod
    def _is_coroutine(func: Callable[..., Any]) -> bool:
        return inspect.iscoroutinefunction(func)

    def _call_maybe_async(self, func: Callable[..., Any], /, **kwargs) -> Any:
        if self._is_coroutine(func):
            # run coroutine in a fresh event loop
            return asyncio.run(func(**kwargs))
        else:
            return func(**kwargs)

    def run_dagflow(
        self,
        flow: DAGFlow,
        registry: Dict[str, Callable[..., Any]],
        guards: Optional[GuardsRegistry] = None,
    ) -> Dict[str, Any]:
        # Build graph
        nodes = {s.id: s for s in flow.steps}
        incoming = dict.fromkeys(nodes, 0)
        outgoing: Dict[str, List[str]] = {nid: [] for nid in nodes}
        for f, t in flow.edges:
            outgoing[f].append(t)
            incoming[t] += 1

        results: Dict[str, Any] = {}
        trace_id = observability.tracer.start_trace()
        observability.tracer.record(trace_id, "flow_started", flow_id=flow.id)
        observability.logger.info("flow_started", flow_id=flow.id, trace_id=trace_id)
        # queue of ready nodes
        ready = [nid for nid, deg in incoming.items() if deg == 0]

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures: Dict[concurrent.futures.Future, str] = {}
            # keep resolved args per node so post-step guards can inspect them
            step_args_by_nid: Dict[str, Dict[str, Any]] = {}
            # track start timestamps per node for timing
            start_ts_by_nid: Dict[str, float] = {}
            # optional per-step timeouts
            step_timeout_by_nid: Dict[str, Optional[float]] = {}
            # per-step retry policies
            step_retry_by_nid: Dict[str, Optional[observability.RetryPolicy]] = {}

            def submit_step(nid: str):
                step = nodes[nid]
                func = registry.get(step.action)
                if func is None:
                    raise FlowError(f"Action {step.action} not found in registry")

                # resolve args referencing previous results
                resolved_args = {}
                for k, v in step.args.items():
                    if isinstance(v, str) and v.startswith("$result."):
                        ref_id = v.split("$result.", 1)[1]
                        resolved_args[k] = results.get(ref_id)
                    else:
                        resolved_args[k] = v

                # run pre-step guards
                if guards:
                    try:
                        resolved_args = guards.run_pre(
                            step.id, step.action, resolved_args
                        )
                    except GuardError as e:
                        raise FlowError(
                            f"Guard pre-step failed for {step.id}: {e}"
                        ) from e

                # record step started
                observability.tracer.record(
                    trace_id, "step_started", step_id=step.id, action=step.action
                )
                observability.logger.info(
                    "step_started",
                    step_id=step.id,
                    action=step.action,
                    trace_id=trace_id,
                )

                # store resolved args for post-step guards
                step_args_by_nid[nid] = resolved_args

                # extract optional per-step timeout from args (special key)
                timeout: float | None = None
                if "__timeout__" in resolved_args:
                    raw_timeout = resolved_args.pop("__timeout__")
                    if isinstance(raw_timeout, (int, float, str)):
                        try:
                            timeout = float(raw_timeout)
                        except Exception:
                            timeout = None
                    else:
                        timeout = None
                step_timeout_by_nid[nid] = timeout

                # extract optional per-step retry policy from args (special key __retry__)
                retry_policy = None
                if "__retry__" in resolved_args:
                    try:
                        rp = resolved_args.pop("__retry__")
                        retry_policy = observability.RetryPolicy.from_dict(rp)
                    except Exception:
                        retry_policy = None
                if retry_policy is None:
                    retry_policy = self.default_retry
                step_retry_by_nid[nid] = retry_policy

                start_ts_by_nid[nid] = time.perf_counter()

                # wrap execution in a retry loop executed inside the worker thread
                def _run_with_retries():
                    rp = step_retry_by_nid.get(nid)
                    attempts = 0
                    backoff = rp.backoff if rp else 0.0
                    multiplier = rp.multiplier if rp else 1.0
                    max_attempts = rp.max_attempts if rp else 1
                    # last_exc removed; not used
                    while attempts < max_attempts:
                        attempts += 1
                        if attempts > 1:
                            observability.metrics.inc("step_retries")
                            observability.tracer.record(
                                trace_id, "step_retry", step_id=nid, attempt=attempts
                            )
                        try:
                            return self._call_maybe_async(func, **resolved_args)
                        except Exception:
                            # last_exc = ex  # unused; remove assignment
                            if attempts >= max_attempts:
                                raise
                            # backoff before next attempt
                            time.sleep(backoff)
                            backoff = backoff * multiplier

                fut = ex.submit(_run_with_retries)
                futures[fut] = nid

            # submit initial ready steps
            for nid in ready:
                submit_step(nid)

            # process as futures complete
            while futures:
                done, _ = concurrent.futures.wait(
                    list(futures.keys()), return_when=concurrent.futures.FIRST_COMPLETED
                )
                # check for timeouts on running steps
                now = time.perf_counter()
                for fut_obj, nid_obj in list(futures.items()):
                    t0 = start_ts_by_nid.get(nid_obj)
                    tout = step_timeout_by_nid.get(nid_obj)
                    if tout is not None and t0 is not None and (now - t0) > tout:
                        # attempt cancel and raise
                        try:
                            fut_obj.cancel()
                        except Exception:
                            pass
                        observability.metrics.inc("step_timeouts")
                        observability.tracer.record(
                            trace_id, "step_timeout", step_id=nid_obj, timeout=tout
                        )
                        observability.logger.info(
                            "step_timeout",
                            step_id=nid_obj,
                            timeout=tout,
                            trace_id=trace_id,
                        )
                        raise FlowError(
                            f"Step {nid_obj} timed out after {tout} seconds"
                        )
                for fut in done:
                    nid = futures.pop(fut)
                    try:
                        res = fut.result()
                    except Exception as exc:
                        observability.tracer.record(
                            trace_id, "step_failed", step_id=nid, error=str(exc)
                        )
                        observability.logger.info(
                            "step_failed",
                            step_id=nid,
                            error=str(exc),
                            trace_id=trace_id,
                        )
                        raise FlowError(f"Step {nid} failed: {exc}") from exc

                    # run post-step guards if present
                    resolved_args = step_args_by_nid.pop(nid, {})
                    if guards:
                        try:
                            res = guards.run_post(
                                nid, nodes[nid].action, resolved_args, res
                            )
                        except GuardError as e:
                            raise FlowError(
                                f"Guard post-step failed for {nid}: {e}"
                            ) from e

                    # record step completed and metrics
                    start_ts = start_ts_by_nid.pop(nid, time.perf_counter())
                    dur = time.perf_counter() - start_ts
                    observability.tracer.record(
                        trace_id, "step_completed", step_id=nid, duration=dur
                    )
                    observability.metrics.inc("steps_executed")
                    observability.metrics.timing("step_duration_seconds", dur)
                    observability.logger.info(
                        "step_completed", step_id=nid, duration=dur, trace_id=trace_id
                    )

                    results[nid] = res
                    # decrement outgoing
                    for m in outgoing.get(nid, []):
                        incoming[m] -= 1
                        if incoming[m] == 0:
                            submit_step(m)

        # final check
        if len(results) != len(nodes):
            raise FlowError(
                "Not all steps executed; possible cycle or missing dependency"
            )
        observability.tracer.record(trace_id, "flow_completed", flow_id=flow.id)
        observability.logger.info("flow_completed", flow_id=flow.id, trace_id=trace_id)
        return results

    def run_state_machine(
        self,
        sm: StateMachine,
        registry: Dict[str, Callable[..., Any]],
        events: List[str],
        guards: Optional[GuardsRegistry] = None,
    ) -> Dict[str, Any]:
        # reuse StateMachine.run but support async actions
        if not sm.start_state:
            raise FlowError("No start state defined")
        states_map = {s.id: s for s in sm.states}
        current = sm.start_state
        results: Dict[str, Any] = {}
        for ev in events:
            st = states_map.get(current)
            if st is None:
                raise FlowError(f"Unknown state {current}")
            if st.action:
                func = registry.get(st.action)
                if func is None:
                    raise FlowError(f"Action {st.action} not found in registry")

                # prepare args and run pre-step guards
                resolved_args = {"event": ev, "state": current}
                if guards:
                    try:
                        resolved_args = guards.run_pre(
                            current, st.action, resolved_args
                        )
                    except GuardError as e:
                        raise FlowError(
                            f"Guard pre-step failed for state {current}: {e}"
                        ) from e

                # execute the action
                res = func(**resolved_args)

                # run post-step guards
                if guards:
                    try:
                        res = guards.run_post(current, st.action, resolved_args, res)
                    except GuardError as e:
                        raise FlowError(
                            f"Guard post-step failed for state {current}: {e}"
                        ) from e

                results[current] = res

            # determine next state from transition
            nxt = st.on.get(ev)
            if nxt is None:
                break
            current = nxt
        return results


def _process_invoke(action_path: str, kwargs: Dict[str, Any]):
    """Top-level worker function used by ProcessExecutor.

    It imports the callable by dotted path and invokes it with kwargs. This keeps
    the worker picklable and avoids sending callables across process boundaries.
    """
    module_name, func_name = action_path.rsplit(".", 1)
    mod = importlib.import_module(module_name)
    func = getattr(mod, func_name)
    if inspect.iscoroutinefunction(func):
        return asyncio.run(func(**kwargs))
    return func(**kwargs)


class ProcessExecutor:
    """Local executor that runs steps in separate processes.

    Notes:
    - For safety and Windows compatibility, the executor expects registry values
      to be dotted import paths (strings) pointing to importable callables, e.g.
      'kola.actions.add'. If a value in registry is a callable, we will attempt
      to submit it directly, but that may fail on some platforms if the callable
      is not picklable.
    """

    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers or (max(1, (os.cpu_count() or 1) - 1))

    def _resolve_action_path(self, action_val: Any) -> str:
        if isinstance(action_val, str):
            return action_val
        # try to resolve callable to dotted path
        if callable(action_val):
            mod = getattr(action_val, "__module__", None)
            name = getattr(action_val, "__name__", None)
            if mod and name:
                return f"{mod}.{name}"
        raise FlowError(
            "ProcessExecutor requires action values to be dotted import paths or top-level callables"
        )

    def run_dagflow(self, flow: DAGFlow, registry: Dict[str, Any]) -> Dict[str, Any]:
        nodes = {s.id: s for s in flow.steps}
        incoming = dict.fromkeys(nodes, 0)
        outgoing: Dict[str, List[str]] = {nid: [] for nid in nodes}
        for f, t in flow.edges:
            outgoing[f].append(t)
            incoming[t] += 1

        results: Dict[str, Any] = {}

        # prepare mapping from action name to action_path
        action_paths: Dict[str, str] = {}
        for act_name, act_val in registry.items():
            try:
                action_paths[act_name] = self._resolve_action_path(act_val)
            except FlowError:
                # keep as-is to allow direct pickling attempt later
                action_paths[act_name] = act_val

        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as ex:
            futures: Dict[concurrent.futures.Future, str] = {}

            def submit_step(nid: str):
                step = nodes[nid]
                val = action_paths.get(step.action)
                if val is None:
                    raise FlowError(f"Action {step.action} not found in registry")

                # resolve args referencing previous results
                resolved_args = {}
                for k, v in step.args.items():
                    if isinstance(v, str) and v.startswith("$result."):
                        ref_id = v.split("$result.", 1)[1]
                        resolved_args[k] = results.get(ref_id)
                    else:
                        resolved_args[k] = v

                if isinstance(val, str):
                    fut = ex.submit(_process_invoke, val, resolved_args)
                else:
                    # best-effort: try to pickle and run callable directly
                    fut = ex.submit(val, **resolved_args)
                futures[fut] = nid

            ready = [nid for nid, deg in incoming.items() if deg == 0]
            for nid in ready:
                submit_step(nid)

            try:
                while futures:
                    done, _ = concurrent.futures.wait(
                        list(futures.keys()),
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    for fut in done:
                        nid = futures.pop(fut)
                        try:
                            res = fut.result()
                        except BrokenProcessPool as exc:
                            raise FlowError(
                                f"Process pool broken while executing {nid}: {exc}"
                            ) from exc
                        except Exception as exc:
                            raise FlowError(
                                f"Step {nid} failed in process executor: {exc}"
                            ) from exc
                        results[nid] = res
                        for m in outgoing.get(nid, []):
                            incoming[m] -= 1
                            if incoming[m] == 0:
                                submit_step(m)
            finally:
                # ProcessPoolExecutor __exit__ will cleanup
                pass

        if len(results) != len(nodes):
            raise FlowError("Not all steps executed by process executor")
        return results


class RemoteExecutor(ABC):
    """Abstract interface for remote executors/workers.

    Implementations may talk to remote workers over network, RPC, or scheduling
    systems. The contract mirrors LocalExecutor: accept a flow and a registry and
    return results mapping step id -> result.
    """

    @abstractmethod
    def run_dagflow(
        self, flow: DAGFlow, registry: Dict[str, Callable[..., Any]]
    ) -> Dict[str, Any]:
        raise NotImplementedError()


class DummyRemoteExecutor(RemoteExecutor):
    """A lightweight simulated remote executor for testing and dev.

    It executes tasks in a thread pool but can optionally add an artificial
    network latency per task to simulate remote calls. This is intentionally
    simple — a real remote executor would use RPC/HTTP/gRPC and worker
    processes.
    """

    def __init__(self, max_workers: Optional[int] = None, per_call_delay: float = 0.0):
        self.max_workers = max_workers or (min(32, (os.cpu_count() or 1) + 2))
        self.per_call_delay = per_call_delay

    def run_dagflow(
        self, flow: DAGFlow, registry: Dict[str, Callable[..., Any]]
    ) -> Dict[str, Any]:
        # reuse the same dependency resolution as LocalExecutor but dispatch
        # calls into a threadpool to simulate remote behaviour.
        nodes = {s.id: s for s in flow.steps}
        incoming = dict.fromkeys(nodes, 0)
        outgoing: Dict[str, List[str]] = {nid: [] for nid in nodes}
        for f, t in flow.edges:
            outgoing[f].append(t)
            incoming[t] += 1

        results: Dict[str, Any] = {}

        def _invoke(action_name: str, args: Dict[str, Any]):
            func = registry.get(action_name)
            if func is None:
                raise FlowError(f"Action {action_name} not found in registry")
            if self.per_call_delay:
                time.sleep(self.per_call_delay)
            # call synchronously here; remote would be networked
            if inspect.iscoroutinefunction(func):
                return asyncio.run(func(**args))
            return func(**args)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures: Dict[concurrent.futures.Future, str] = {}

            def submit_step(nid: str):
                step = nodes[nid]
                # resolve args referencing previous results
                resolved_args = {}
                for k, v in step.args.items():
                    if isinstance(v, str) and v.startswith("$result."):
                        ref_id = v.split("$result.", 1)[1]
                        resolved_args[k] = results.get(ref_id)
                    else:
                        resolved_args[k] = v
                fut = ex.submit(_invoke, step.action, resolved_args)
                futures[fut] = nid

            ready = [nid for nid, deg in incoming.items() if deg == 0]
            for nid in ready:
                submit_step(nid)

            while futures:
                done, _ = concurrent.futures.wait(
                    list(futures.keys()), return_when=concurrent.futures.FIRST_COMPLETED
                )
                for fut in done:
                    nid = futures.pop(fut)
                    try:
                        res = fut.result()
                    except Exception as exc:
                        raise FlowError(
                            f"Step {nid} failed in remote executor: {exc}"
                        ) from exc
                    results[nid] = res
                    for m in outgoing.get(nid, []):
                        incoming[m] -= 1
                        if incoming[m] == 0:
                            submit_step(m)

        if len(results) != len(nodes):
            raise FlowError("Not all steps executed by remote executor")
        return results
