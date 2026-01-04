"""Placeholder adapter for integrating with Apache Airflow / Camel.

This module provides a tiny design-level adapter showing how Kola could
trigger Airflow DAG runs (or communicate with Camel) without adding heavy
runtime dependencies. It's intentionally dependency-free and documents the
expected methods for a concrete integration.

Concrete implementations should either:
- implement a REST client that talks to the Airflow REST API (recommended),
  or
- use the Airflow Python client inside an environment that has Airflow
  installed.

Example usage (REST):
    adapter = AirflowAdapter(base_url="http://airflow:8080", auth=(user, pass))
    adapter.trigger_dag("my_dag", conf={"flow_payload": {...}})

This is a placeholder and does not perform network calls.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class AirflowAdapter:
    """Minimal interface for triggering external orchestrators.

    The adapter is intentionally small: concrete code should handle auth,
    error handling, retries, and mapping between Kola flows and external DAGs.
    """

    def __init__(self, base_url: Optional[str] = None, auth: Optional[Any] = None):
        self.base_url = base_url or "http://localhost:8080"
        self.auth = auth

    def trigger_dag(self, dag_id: str, conf: Optional[Dict[str, Any]] = None) -> str:
        """Trigger a DAG run and return a run identifier.

        A real implementation would call the Airflow REST endpoint
        POST /api/v1/dags/{dag_id}/dagRuns with a JSON body containing `conf`.
        """
        raise NotImplementedError(
            "AirflowAdapter.trigger_dag must be implemented by a concrete adapter"
        )

    def get_dag_run_status(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """Fetch status for a DAG run. Return a dict with at least a `state` key."""
        raise NotImplementedError(
            "AirflowAdapter.get_dag_run_status must be implemented by a concrete adapter"
        )


class CamelAdapter:
    """Placeholder for Apache Camel integration (message routing / enterprise integration).

    Camel integration typically involves deploying routes that consume/produce
    messages and may be exposed over HTTP, JMS, or other transports. Concrete
    implementations should provide a thin client that publishes messages to
    the Camel entry points.
    """

    def __init__(self, endpoint: Optional[str] = None):
        self.endpoint = endpoint or "http://localhost:8080"

    def publish(self, route: str, message: Dict[str, Any]) -> None:
        raise NotImplementedError(
            "CamelAdapter.publish must be implemented by a concrete adapter"
        )
