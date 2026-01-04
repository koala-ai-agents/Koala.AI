"""Simple Airflow demo using Kola's built-in test tools.

This avoids custom tool serialization issues by using tools already
defined in kola.tools module.
"""

from __future__ import annotations

import os
import sys

from koala.flow import dag
from koala.tools import default_registry, tool


# Simple test tools that will work with Airflow
@tool(
    "add_numbers", input_schema={"a": "int", "b": "int"}, description="Add two numbers"
)
def add_numbers(a: int, b: int) -> dict:
    """Add two numbers."""
    result = a + b
    print(f"Adding {a} + {b} = {result}")
    return {"sum": result, "inputs": [a, b]}


@tool("multiply_by_two", input_schema={"value": "int"}, description="Multiply by 2")
def multiply_by_two(value: int) -> dict:
    """Multiply a number by 2."""
    result = value * 2
    print(f"Multiplying {value} * 2 = {result}")
    return {"result": result, "original": value}


@tool("format_result", input_schema={"data": "any"}, description="Format result")
def format_result(data: dict) -> dict:
    """Format the final result."""
    print(f"Formatting result: {data}")
    return {
        "formatted": f"Final result is: {data.get('result', 'N/A')}",
        "raw_data": data,
    }


def main():
    """Run the simple Airflow demo."""

    # Check environment
    airflow_url = os.getenv("AIRFLOW_URL")
    dags_folder = os.getenv("AIRFLOW_DAGS_FOLDER")

    if not dags_folder:
        print("=" * 70)
        print("ERROR: AIRFLOW_DAGS_FOLDER environment variable not set.")
        print("=" * 70)
        print("\nRun this first:")
        print("  . .\\set-airflow-env.ps1")
        print("\nOr set manually:")
        print('  $env:AIRFLOW_DAGS_FOLDER = "$PWD\\airflow\\dags"')
        sys.exit(1)

    print("=" * 70)
    print("Kola Simple Airflow Demo")
    print("=" * 70)
    print(f"\nAirflow URL: {airflow_url or 'http://localhost:8080'}")
    print(f"DAGs Folder: {dags_folder}")

    # Build a simple flow: add -> multiply -> format
    print("\n1. Building flow...")
    flow = (
        dag("simple-math")
        .step("add", "add_numbers", a=5, b=3)
        .step("multiply", "multiply_by_two", value="$result.add.sum")
        .step("format", "format_result", data="$result.multiply")
        .edge("add", "multiply")
        .edge("multiply", "format")
        .build()
    )

    print(f"   Flow ID: {flow.id}")
    print(f"   Steps: {len(flow.steps)}")
    for step in flow.steps:
        print(f"      - {step.id}: {step.action}")

    # Create executor
    print("\n2. Creating AirflowExecutor...")
    from koala.executors import AirflowExecutor

    executor = AirflowExecutor(
        airflow_url=airflow_url or "http://localhost:8080",
        auth=(
            os.getenv("AIRFLOW_USERNAME", "admin"),
            os.getenv("AIRFLOW_PASSWORD", "admin"),
        ),
        dags_folder=dags_folder,
        poll_interval=2,
        timeout=60,
    )

    # Build registry
    registry = {name: meta.func for name, meta in default_registry._tools.items()}

    # Execute
    print("\n3. Executing flow via Airflow...")
    print("   Please wait...")

    try:
        results = executor.run_dagflow(flow, registry)

        print("\n" + "=" * 70)
        print("✅ Execution successful!")
        print("=" * 70)
        print("\nResults:")
        for step_id, result in results.items():
            print(f"\n{step_id}:")
            print(f"  {result}")

        print("\n" + "=" * 70)
        print("Check Airflow UI for details:")
        print(f"  {airflow_url or 'http://localhost:8080'}")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Execution failed: {e}")
        print("\nTroubleshooting:")
        print("1. Check Airflow UI task logs")
        print("2. Verify DAG is unpaused")
        print("3. Check scheduler is running")
        sys.exit(1)


if __name__ == "__main__":
    main()
