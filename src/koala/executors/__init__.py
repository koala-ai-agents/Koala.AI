"""Pluggable executor implementations for Kola.

This module provides different execution backends:
- LocalExecutor: Thread-based execution (always available)
- ProcessExecutor: Process-based execution (always available)
- AirflowExecutor: Apache Airflow integration (optional, requires 'airflow' extra)

Example:
    # Always available
    from koala.executors import LocalExecutor, ProcessExecutor

    # Optional - requires: pip install kola[airflow]
    try:
        from koala.executors import AirflowExecutor
    except ImportError:
        print("AirflowExecutor not available. Install with: pip install kola[airflow]")
"""

from __future__ import annotations

# Always available executors (from flow.py)
from ..flow import (
    DummyRemoteExecutor,
    LocalExecutor,
    ProcessExecutor,
    RemoteExecutor,
)

__all__ = ["LocalExecutor", "ProcessExecutor", "RemoteExecutor", "DummyRemoteExecutor"]

# Optional Airflow executor - graceful degradation
try:
    from .airflow_executor import AirflowExecutor  # noqa: F401

    __all__.append("AirflowExecutor")
except ImportError:
    # Airflow dependencies not installed
    pass
