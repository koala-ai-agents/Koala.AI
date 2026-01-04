"""Demo: Using AirflowExecutor to execute Kola flows.

This example shows how to use the AirflowExecutor to run Kola DAG flows
via Apache Airflow.

Prerequisites:
1. Install Airflow support:
   pip install kola[airflow]

2. Have Airflow running (separate installation):
   pip install apache-airflow
   airflow db init
   airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
   airflow webserver --port 8080 (in one terminal)
   airflow scheduler (in another terminal)

3. Ensure you have access to Airflow's dags folder
"""

from __future__ import annotations

import os
import sys

# import koala components
from koala.flow import dag
from koala.tools import default_registry, tool

# Try to import AirflowExecutor
try:
    from koala.executors import AirflowExecutor
except ImportError:
    print("ERROR: AirflowExecutor not available.")
    print("Install with: pip install kola[airflow]")
    sys.exit(1)


# Define some sample tools
@tool("fetch_data", input_schema={"url": "str"}, description="Fetch data from URL")
def fetch_data(url: str) -> dict:
    """Simulate fetching data from a URL."""
    print(f"Fetching data from: {url}")
    return {
        "url": url,
        "data": ["item1", "item2", "item3"],
        "count": 3,
    }


@tool("process_data", input_schema={"data": "any"}, description="Process fetched data")
def process_data(data: dict) -> dict:
    """Process the fetched data."""
    print(f"Processing data with {data.get('count', 0)} items")
    items = data.get("data", [])
    processed = [item.upper() for item in items]
    return {
        "original": items,
        "processed": processed,
        "transformation": "uppercase",
    }


@tool(
    "save_results",
    input_schema={"results": "any"},
    description="Save processed results",
)
def save_results(results: dict) -> dict:
    """Save the processed results."""
    print(f"Saving results: {results.get('transformation')} transformation")
    return {
        "saved": True,
        "items_saved": len(results.get("processed", [])),
        "location": "/tmp/results.json",
    }


def build_data_pipeline_flow():
    """Build a simple data processing pipeline flow."""
    return (
        dag("data-pipeline")
        .step("fetch", "fetch_data", url="https://api.example.com/data")
        .step("process", "process_data", data="$result.fetch")
        .step("save", "save_results", results="$result.process")
        .edge("fetch", "process")
        .edge("process", "save")
        .build()
    )


def main():
    """Run the demo."""
    print("=" * 70)
    print("Kola AirflowExecutor Demo")
    print("=" * 70)

    # Configuration
    airflow_url = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
    airflow_user = os.environ.get("AIRFLOW_USER", "admin")
    airflow_password = os.environ.get("AIRFLOW_PASSWORD", "admin")
    dags_folder = os.environ.get("AIRFLOW_DAGS_FOLDER")

    if not dags_folder:
        print("\nERROR: AIRFLOW_DAGS_FOLDER environment variable not set.")
        print("Please set it to your Airflow dags directory, e.g.:")
        print("  export AIRFLOW_DAGS_FOLDER=/path/to/airflow/dags")
        print("\nFor default Airflow standalone setup, try:")
        print("  export AIRFLOW_DAGS_FOLDER=~/airflow/dags")
        sys.exit(1)

    print(f"\nAirflow URL: {airflow_url}")
    print(f"DAGs Folder: {dags_folder}")

    # Build the flow
    print("\n1. Building flow...")
    flow = build_data_pipeline_flow()
    print(f"   Flow ID: {flow.id}")
    print(f"   Steps: {len(flow.steps)}")
    print(f"   Edges: {len(flow.edges)}")

    # Create executor
    print("\n2. Creating AirflowExecutor...")
    executor = AirflowExecutor(
        airflow_url=airflow_url,
        auth=(airflow_user, airflow_password),
        dags_folder=dags_folder,
        poll_interval=2,
        timeout=300,  # 5 minutes
    )

    # Build registry
    registry = {name: meta.func for name, meta in default_registry._tools.items()}

    # Execute
    print("\n3. Executing flow via Airflow...")
    print("   This will:")
    print("   - Translate the flow to Airflow DAG code")
    print("   - Write the DAG file to Airflow's dags folder")
    print("   - Trigger the DAG via REST API")
    print("   - Poll for completion")
    print("   - Return results")
    print("\n   Please wait...")

    try:
        results = executor.run_dagflow(flow, registry)

        print("\n4. Execution completed successfully!")
        print("\nResults:")
        for step_id, result in results.items():
            print(f"\n   {step_id}:")
            print(f"      {result}")

        print("\n" + "=" * 70)
        print("Demo completed successfully!")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Execution failed: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure Airflow is running: airflow webserver & airflow scheduler")
        print("2. Check Airflow UI at http://localhost:8080")
        print("3. Verify dags folder is correct and writable")
        print("4. Check Airflow logs for errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
