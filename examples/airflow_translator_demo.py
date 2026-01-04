"""Demo: AirflowExecutor DAG Translation (No Airflow Installation Required)

This example shows how the AirflowExecutor translates Kola flows to Airflow DAG code,
without requiring an actual Airflow installation.

This is useful for:
- Understanding how Kola flows map to Airflow DAGs
- Verifying DAG generation before deploying to Airflow
- Learning the AirflowExecutor without setting up infrastructure
"""

from __future__ import annotations

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
    print("Or: uv pip install requests")
    sys.exit(1)


# Define some sample tools
@tool("fetch_data", input_schema={"url": "str"}, description="Fetch data from URL")
def fetch_data(url: str) -> dict:
    """Simulate fetching data from a URL."""
    return {
        "url": url,
        "data": ["item1", "item2", "item3"],
        "count": 3,
    }


@tool("process_data", input_schema={"data": "any"}, description="Process fetched data")
def process_data(data: dict) -> dict:
    """Process the fetched data."""
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
    return {
        "saved": True,
        "items_saved": len(results.get("processed", [])),
        "location": "/tmp/results.json",
    }


def main():
    """Run the translation demo."""
    print("=" * 70)
    print("Kola → Airflow DAG Translation Demo")
    print("=" * 70)
    print("\nThis demo shows how Kola flows are translated to Airflow DAG code.")
    print("No Airflow installation required!")
    print()

    # Build a data processing pipeline flow
    print("1. Building Kola Flow...")
    flow = (
        dag("data-pipeline")
        .step("fetch", "fetch_data", url="https://api.example.com/data")
        .step("process", "process_data", data="$result.fetch")
        .step("save", "save_results", results="$result.process")
        .edge("fetch", "process")
        .edge("process", "save")
        .build()
    )

    print(f"   Flow ID: {flow.id}")
    print(f"   Version: {flow.version}")
    print(f"   Steps: {len(flow.steps)}")
    for step in flow.steps:
        print(f"      - {step.id}: {step.action}")
    print("   Dependencies:")
    for from_id, to_id in flow.edges:
        print(f"      {from_id} → {to_id}")

    # Create executor (just for translation, no execution)
    print("\n2. Creating AirflowExecutor...")
    executor = AirflowExecutor(
        airflow_url="http://localhost:8080",
        dags_folder="/tmp/test",  # Not actually used for translation
    )

    # Build registry
    registry = {name: meta.func for name, meta in default_registry._tools.items()}

    # Translate to Airflow DAG code
    print("\n3. Translating to Airflow DAG code...")
    dag_code = executor._translate_to_airflow(flow, registry)

    print("   ✅ Translation successful!")
    print(f"   Generated {len(dag_code)} characters of Airflow DAG code")
    print(f"   Generated {dag_code.count('PythonOperator')} PythonOperators")

    # Show the generated code
    print("\n4. Generated Airflow DAG Code:")
    print("=" * 70)
    print(dag_code)
    print("=" * 70)

    # Explain what happens next
    print("\n5. What Happens Next (when deployed to Airflow):")
    print("   - This DAG file would be saved to Airflow's dags/ folder")
    print("   - Airflow would discover and parse the DAG")
    print("   - You could trigger it via REST API or Airflow UI")
    print("   - Tasks would execute on Airflow workers")
    print("   - Results would be passed via XCom")

    print("\n" + "=" * 70)
    print("Demo Complete!")
    print("=" * 70)
    print("\nTo actually run this on Airflow:")
    print("1. Install Airflow: pip install apache-airflow")
    print("2. Start Airflow: airflow standalone")
    print("3. Set AIRFLOW_DAGS_FOLDER environment variable")
    print("4. Run: python examples/airflow_demo.py")


if __name__ == "__main__":
    main()
