"""Koala orchestration package (L7).

DAG-based flow orchestration that accepts Runnable steps (Agents, Tools,
Models) directly. Steps can also be plain Python callables or string
handles for a user-provided registry.

Public surface:
    - ``flow(id)`` — fluent builder entry point
    - ``Flow``, ``Step``, ``FlowBuilder`` — declarative types
    - ``StepAction`` — union type describing what a step may reference
    - ``LocalExecutor`` — in-process async executor
    - Error hierarchy: ``FlowError``, ``StepExecutionError``
"""

from __future__ import annotations

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
    # Errors
    "FlowError",
    "StepExecutionError",
]
