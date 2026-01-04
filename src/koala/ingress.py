"""Simple ingress handlers for flows.

This module provides a small, framework-agnostic helper to process REST-like
payloads that request flow execution. It's intentionally minimal so it can be
hooked into any web framework (Flask/FastAPI) later.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .flow import DAGFlow, FlowRepository, LocalExecutor, StateMachine
from .tools import ToolsRegistry, default_registry


class IngressError(Exception):
    pass


def process_rest_payload(
    payload: Dict[str, Any],
    registry: Optional[ToolsRegistry] = None,
    executor: Optional[Any] = None,
    repo: Optional[FlowRepository] = None,
) -> Dict[str, Any]:
    """Process a REST payload requesting flow execution.

    Payload format (either):
      {"flow": <flow_dict>, "type": "dag"}
    or
      {"flow_id": "myflow"}

    If `flow_id` is provided, `repo` is required to load the flow. `registry`
    defaults to the module-level default_registry and `executor` defaults to
    LocalExecutor().
    """
    registry = registry or default_registry
    executor = executor or LocalExecutor()

    if "flow_id" in payload:
        if repo is None:
            raise IngressError("flow_id provided but no FlowRepository configured")
        flow = repo.load_flow(payload["flow_id"])
        if flow is None:
            raise IngressError(f"flow {payload['flow_id']} not found")
    elif "flow" in payload:
        f = payload["flow"]
        if payload.get("type") == "state":
            flow = StateMachine.from_dict(f)
        else:
            flow = DAGFlow.from_dict(f)
    else:
        raise IngressError("payload must contain 'flow' or 'flow_id'")

    # choose executor API style
    if hasattr(executor, "run_dagflow") and isinstance(flow, DAGFlow):
        results = executor.run_dagflow(
            flow, registry={name: meta.func for name, meta in registry._tools.items()}
        )
    elif hasattr(executor, "run_state_machine") and isinstance(flow, StateMachine):
        results = executor.run_state_machine(
            flow,
            registry={name: meta.func for name, meta in registry._tools.items()},
            events=payload.get("events", []),
        )
    else:
        # fallback: try flow.run with function registry
        if isinstance(flow, StateMachine):
            results = flow.run(
                registry={name: meta.func for name, meta in registry._tools.items()},
                events=payload.get("events", []),
            )
        else:
            results = flow.run(
                registry={name: meta.func for name, meta in registry._tools.items()}
            )

    return {"results": results}


def load_flow_from_payload(
    payload: Dict[str, Any], repo: Optional[FlowRepository] = None
):
    """Parse payload and return a Flow object (DAGFlow or StateMachine).

    This extracts the flow without executing it; used by external orchestrators
    that want to submit flows asynchronously.
    """
    if "flow_id" in payload:
        if repo is None:
            raise IngressError("flow_id provided but no FlowRepository configured")
        flow = repo.load_flow(payload["flow_id"])
        if flow is None:
            raise IngressError(f"flow {payload['flow_id']} not found")
    elif "flow" in payload:
        f = payload["flow"]
        if payload.get("type") == "state":
            flow = StateMachine.from_dict(f)
        else:
            flow = DAGFlow.from_dict(f)
    else:
        raise IngressError("payload must contain 'flow' or 'flow_id'")
    return flow
