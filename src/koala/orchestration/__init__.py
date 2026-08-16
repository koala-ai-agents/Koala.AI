"""Koala orchestration package (L7).

DAG-based flow orchestration that accepts Runnable steps (Agents, Tools,
Models) directly. Steps can also be plain Python callables or string
handles for a user-provided registry.

Public surface:
    - ``flow(id)`` — fluent builder entry point
    - ``Flow``, ``Step``, ``FlowBuilder`` — declarative types
    - ``StepAction`` — union type describing what a step may reference
    - ``LocalExecutor`` — in-process async executor
    - ``AirflowExecutor`` — deploy a Flow to Airflow (thin DAG file +
      JSON spec + REST-driven trigger / wait)
    - ``spec_from_flow`` / ``render_dag_file`` — pure helpers if you want
      to generate artifacts without an executor
    - Error hierarchy: ``FlowError``, ``StepExecutionError``,
      ``AirflowExecutorError``, ``AirflowAPIError``,
      ``ActionSerializationError``
"""

from __future__ import annotations

from .airflow import (
    KOALA_SPEC_VERSION,
    ActionSerializationError,
    AirflowAPIError,
    AirflowExecutor,
    AirflowExecutorError,
    render_dag_file,
    spec_from_flow,
    write_dag_files,
)
from .errors import FlowError, StepExecutionError
from .executor import LocalExecutor
from .flow import Flow, FlowBuilder, Step, StepAction, flow

__all__ = [
    # Flow types
    "Flow",
    "FlowBuilder",
    "Step",
    "StepAction",
    "flow",
    # Executors
    "LocalExecutor",
    "AirflowExecutor",
    # Airflow helpers
    "spec_from_flow",
    "render_dag_file",
    "write_dag_files",
    "KOALA_SPEC_VERSION",
    # Errors
    "FlowError",
    "StepExecutionError",
    "AirflowExecutorError",
    "AirflowAPIError",
    "ActionSerializationError",
]
