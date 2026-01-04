"""Simple orchestrator and scheduler for running flows.

This is a lightweight orchestrator intended for local/dev use. It accepts a
flow and an executor instance (LocalExecutor, ProcessExecutor, RemoteExecutor)
and runs flows in a thread pool. A tiny scheduler supports periodic flow runs.
"""

from __future__ import annotations

import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, Optional

from .flow import DAGFlow
from .observability import logger, metrics, tracer
from .run_repo import RunRepository


class Orchestrator:
    def __init__(self, max_workers: int = 4, run_repo: Optional[RunRepository] = None):
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._runs: Dict[str, Future] = {}
        self._results: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._run_repo = run_repo

    def submit_flow(self, flow: DAGFlow, executor, registry: Dict[str, Any]) -> str:
        """Submit a flow for asynchronous execution.

        Returns a run_id which can be used to query status/result.
        """
        run_id = uuid.uuid4().hex

        def _run():
            tracer_id = tracer.start_trace()
            tracer.record(
                tracer_id, "orchestrator_run_started", run_id=run_id, flow_id=flow.id
            )
            metrics.inc("orchestrator_runs")
            # persist initial run state if repo provided
            if self._run_repo:
                try:
                    self._run_repo.create_run(run_id, flow.id, flow.to_dict())
                    self._run_repo.update_status(run_id, "running")
                except Exception:
                    # don't fail the run if persistence isn't available
                    pass
            try:
                res = executor.run_dagflow(flow, registry)
                with self._lock:
                    self._results[run_id] = res
                # persist result
                if self._run_repo:
                    try:
                        self._run_repo.save_result(run_id, res)
                    except Exception:
                        pass
                tracer.record(
                    tracer_id,
                    "orchestrator_run_completed",
                    run_id=run_id,
                    flow_id=flow.id,
                )
                logger.info(
                    "orchestrator_run_completed",
                    run_id=run_id,
                    flow_id=flow.id,
                    trace_id=tracer_id,
                )
                return res
            except Exception as e:
                # persist failure
                if self._run_repo:
                    try:
                        self._run_repo.fail_run(run_id, str(e))
                    except Exception:
                        pass
                tracer.record(
                    tracer_id, "orchestrator_run_failed", run_id=run_id, error=str(e)
                )
                logger.info(
                    "orchestrator_run_failed",
                    run_id=run_id,
                    error=str(e),
                    trace_id=tracer_id,
                )
                raise

        fut = self._executor.submit(_run)
        with self._lock:
            self._runs[run_id] = fut
        return run_id

    def get_status(self, run_id: str) -> str:
        with self._lock:
            fut = self._runs.get(run_id)
            if not fut:
                # consult repo if available
                if self._run_repo:
                    st = self._run_repo.get_status(run_id)
                    return st or "unknown"
                return "unknown"
            if fut.running():
                return "running"
            if fut.done():
                return "done"
            return "pending"

    def get_result(self, run_id: str) -> Optional[Any]:
        with self._lock:
            res = self._results.get(run_id)
            if res is not None:
                return res
        # fallback to repo
        if self._run_repo:
            return self._run_repo.get_result(run_id)
        return None

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)


class Scheduler:
    """Very small periodic scheduler that submits a flow to an Orchestrator.

    This is a convenience for demos and tests. Not a production scheduler.
    """

    def __init__(self, orchestrator: Orchestrator):
        self.orch = orchestrator
        self._timers: Dict[str, threading.Timer] = {}

    def schedule_periodic(
        self, flow: DAGFlow, executor, registry: Dict[str, Any], interval_seconds: float
    ) -> str:
        schedule_id = uuid.uuid4().hex

        def _tick():
            try:
                self.orch.submit_flow(flow, executor, registry)
            finally:
                # reschedule
                t = threading.Timer(interval_seconds, _tick)
                self._timers[schedule_id] = t
                t.daemon = True
                t.start()

        t0 = threading.Timer(interval_seconds, _tick)
        t0.daemon = True
        self._timers[schedule_id] = t0
        t0.start()
        return schedule_id

    def cancel(self, schedule_id: str) -> None:
        t = self._timers.pop(schedule_id, None)
        if t:
            t.cancel()
