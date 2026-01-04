"""Small FastAPI wrapper for the ingress helper.

This module exposes a single endpoint `/run-flow` that accepts a JSON payload
and uses `process_rest_payload` to run the flow. It's intentionally tiny so it
can be included in demos or mounted into larger applications.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from .flow import LocalExecutor
from .ingress import IngressError, load_flow_from_payload
from .orchestrator import Orchestrator
from .run_repo import RunRepository
from .tools import default_registry

# create a simple orchestrator and persistent run repo for the API
run_repo = RunRepository()
orch = Orchestrator(run_repo=run_repo)


app = FastAPI(title="Kola Ingress API")


# Simple API-key based auth for the demo. Set env KOLA_API_KEYS to a
# comma-separated list of allowed keys. If unset, a default dev key
# ("dev-key") is allowed to avoid breaking local tests.
_API_KEYS: List[str] = [
    k.strip()
    for k in os.environ.get("KOLA_API_KEYS", "dev-key").split(",")
    if k.strip()
]


def verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    if not x_api_key or x_api_key not in _API_KEYS:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return x_api_key


class RunRequest(BaseModel):
    # The full flow payload (keeps compatibility with existing ingress)
    flow: Dict[str, Any]
    # optional runtime options (executor hints, metadata, etc.)
    options: Optional[Dict[str, Any]] = None


class RunResponse(BaseModel):
    run_id: str
    status: str


class RunStatusResponse(BaseModel):
    run_id: str
    status: str
    result: Optional[Any]


@app.post("/runs", status_code=202, response_model=RunResponse)
async def submit_flow(req: RunRequest, api_key: str = Depends(verify_api_key)):
    try:
        flow = load_flow_from_payload(req.flow, repo=None)
    except IngressError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    try:
        registry = {name: meta.func for name, meta in default_registry._tools.items()}
        run_id = orch.submit_flow(flow, LocalExecutor(), registry=registry)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"run_id": run_id, "status": "accepted"}


@app.get("/runs/{run_id}", response_model=RunStatusResponse)
async def get_run(run_id: str, api_key: str = Depends(verify_api_key)):
    status = orch.get_status(run_id)
    result = orch.get_result(run_id)
    return {"run_id": run_id, "status": status, "result": result}


# Compatibility endpoint used by tests/demos: synchronous execution without auth
@app.post("/run-flow")
async def run_flow_compat(req: Dict[str, Any]):
    try:
        flow_payload = {"flow": req.get("flow", {})}
        flow = load_flow_from_payload(flow_payload, repo=None)
        registry = {name: meta.func for name, meta in default_registry._tools.items()}
        ex = LocalExecutor()
        results = ex.run_dagflow(flow, registry)
        return {"results": results}
    except IngressError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
