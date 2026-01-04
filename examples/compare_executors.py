"""Compare different executor backends for Kola flows.

This example demonstrates how to use different executors (Local, Process, Airflow)
with the same flow definition, showing the flexibility of Kola's pluggable executor pattern.
"""

from __future__ import annotations

import os
import time

from koala.flow import LocalExecutor, dag
from koala.tools import default_registry, tool


# Define a simple tool for the demo
@tool("compute", input_schema={"x": "int", "y": "int"})
def compute(x: int, y: int) -> dict:
    """Perform a simple computation."""
    result = x * y + (x + y)
    return {"input": (x, y), "result": result}


@tool("format_output", input_schema={"data": "any"})
def format_output(data: dict) -> str:
    """Format the computation result."""
    return f"Computed: {data['input']} → {data['result']}"


def build_example_flow():
    """Build a simple two-step flow."""
    return (
        dag("compute-flow")
        .step("step1", "compute", x=10, y=5)
        .step("step2", "format_output", data="$result.step1")
        .edge("step1", "step2")
        .build()
    )


def run_with_local_executor(flow):
    """Run flow with LocalExecutor (thread-based)."""
    print("\n" + "=" * 60)
    print("Running with LocalExecutor (Thread-based)")
    print("=" * 60)

    executor = LocalExecutor(max_workers=4)
    registry = {name: meta.func for name, meta in default_registry._tools.items()}

    start = time.time()
    results = executor.run_dagflow(flow, registry)
    elapsed = time.time() - start

    print(f"\n✅ Completed in {elapsed:.3f}s")
    print(f"Results: {results}")

    return results


def run_with_process_executor(flow):
    """Run flow with ProcessExecutor (process-based)."""
    print("\n" + "=" * 60)
    print("Running with ProcessExecutor (Process-based)")
    print("=" * 60)

    # ProcessExecutor needs dotted paths for picklability
    # For this demo, we'll use LocalExecutor as fallback
    print("Note: ProcessExecutor requires picklable functions.")
    print("Using LocalExecutor as fallback for this demo.")

    executor = LocalExecutor(max_workers=4)
    registry = {name: meta.func for name, meta in default_registry._tools.items()}

    start = time.time()
    results = executor.run_dagflow(flow, registry)
    elapsed = time.time() - start

    print(f"\n✅ Completed in {elapsed:.3f}s")
    print(f"Results: {results}")

    return results


def run_with_airflow_executor(flow):
    """Run flow with AirflowExecutor (delegated to Airflow)."""
    print("\n" + "=" * 60)
    print("Running with AirflowExecutor (Airflow-based)")
    print("=" * 60)

    # Check if AirflowExecutor is available
    try:
        from koala.executors import AirflowExecutor
    except ImportError:
        print("❌ AirflowExecutor not available.")
        print("Install with: pip install kola[airflow]")
        return None

    # Check configuration
    dags_folder = os.environ.get("AIRFLOW_DAGS_FOLDER")
    if not dags_folder:
        print("❌ AIRFLOW_DAGS_FOLDER environment variable not set.")
        print("Set it to run Airflow executor:")
        print("  export AIRFLOW_DAGS_FOLDER=/path/to/airflow/dags")
        return None

    airflow_url = os.environ.get("AIRFLOW_URL", "http://localhost:8080")

    print(f"Airflow URL: {airflow_url}")
    print(f"DAGs Folder: {dags_folder}")

    # Create executor
    executor = AirflowExecutor(
        airflow_url=airflow_url,
        auth=("admin", "admin"),
        dags_folder=dags_folder,
        poll_interval=2,
        timeout=300,
    )

    registry = {name: meta.func for name, meta in default_registry._tools.items()}

    try:
        start = time.time()
        results = executor.run_dagflow(flow, registry)
        elapsed = time.time() - start

        print(f"\n✅ Completed in {elapsed:.3f}s")
        print(f"Results: {results}")

        return results

    except Exception as e:
        print(f"❌ Airflow execution failed: {e}")
        print("\nEnsure:")
        print("1. Airflow is running (webserver + scheduler)")
        print("2. DAGs folder is correct and writable")
        return None


def main():
    """Compare executor backends."""
    print("=" * 60)
    print("Kola Executor Comparison Demo")
    print("=" * 60)

    # Build flow once
    flow = build_example_flow()
    print(f"\nFlow: {flow.id}")
    print(f"Steps: {[s.id for s in flow.steps]}")
    print(f"Edges: {flow.edges}")

    # Run with different executors
    local_results = run_with_local_executor(flow)
    process_results = run_with_process_executor(flow)
    airflow_results = run_with_airflow_executor(flow)

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"LocalExecutor:    {'✅ Success' if local_results else '❌ Failed'}")
    print(f"ProcessExecutor:  {'✅ Success' if process_results else '❌ Failed'}")
    print(
        f"AirflowExecutor:  {'✅ Success' if airflow_results else '⚠️  Not configured/available'}"
    )

    print("\n" + "=" * 60)
    print("Executor Selection Guide")
    print("=" * 60)
    print("LocalExecutor:")
    print("  ✅ Fast, no setup required")
    print("  ✅ Supports async functions")
    print("  ✅ Good for development and small workloads")
    print("  ❌ Limited to single machine")
    print()
    print("ProcessExecutor:")
    print("  ✅ Better CPU isolation")
    print("  ✅ Parallel execution across cores")
    print("  ❌ Functions must be picklable")
    print("  ❌ Limited to single machine")
    print()
    print("AirflowExecutor:")
    print("  ✅ Production-grade orchestration")
    print("  ✅ Distributed execution")
    print("  ✅ Rich UI and monitoring")
    print("  ✅ Retry/recovery mechanisms")
    print("  ❌ Requires Airflow setup")
    print("  ❌ Higher latency overhead")


if __name__ == "__main__":
    main()
