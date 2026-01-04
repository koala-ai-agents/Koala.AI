"""Web Search Agent - Airflow DAG Generator (Simplified & Clean).

This script generates an Airflow DAG from the Koala workflow.
Uses the AirflowExecutor from Koala framework - no manual DAG writing needed!

Usage:
    python web_search_agent_airflow_clean.py

The script will:
1. Generate an Airflow DAG file
2. Copy necessary files to Airflow dags folder
3. Ready to trigger in Airflow UI with: {"question": "your question"}
"""

from __future__ import annotations

import shutil
from pathlib import Path

# Import workflow from web_search_agent
from web_search_agent import _load_env, build_web_search_flow

from koala.executors import AirflowExecutor
from koala.tools import default_registry

# ============================================================================
# DAG GENERATION
# ============================================================================


def generate_airflow_dag(
    dag_id: str = "koala_web_search", dags_folder: str = None
) -> str:
    """Generate Airflow DAG from Koala workflow.

    This is the ONLY function you need! The framework handles everything:
    - DAG code generation
    - Tool registration
    - File deployment

    Args:
        dag_id: Name for the generated DAG
        dags_folder: Path to Airflow dags folder (auto-detects if None)

    Returns:
        Path to generated DAG file
    """
    # Load configuration
    _load_env()

    # Auto-detect dags folder
    if not dags_folder:
        project_root = Path(__file__).parent.parent
        dags_folder = str(project_root / "dags")

    Path(dags_folder).mkdir(parents=True, exist_ok=True)

    print("\n🐨 Koala → Airflow DAG Generator")
    print(f"{'=' * 60}\n")

    # Build workflow with placeholder question
    # The actual question comes from dag_run.conf at runtime
    flow = build_web_search_flow(question="<<FROM_DAG_CONFIG>>")

    # Get tool registry
    registry = {name: meta.func for name, meta in default_registry._tools.items()}

    print(f"📦 Tools registered: {len(registry)}")
    print(f"📊 Workflow steps: {len(flow.steps)}")
    print(f"🎯 DAG ID: {dag_id}\n")

    # Create Airflow executor
    executor = AirflowExecutor(
        airflow_url="http://localhost:8080",
        auth=("admin", "admin"),
        dags_folder=dags_folder,
    )

    # Use the new generate_airflow_dag method from executor
    print("🔄 Generating DAG using AirflowExecutor.generate_airflow_dag()...")

    # Rename flow to match dag_id
    flow.id = dag_id

    # Generate DAG file using the executor's method
    dag_path = executor.generate_airflow_dag(flow, registry)

    # Copy web_search_agent.py to dags folder (contains all tools)
    source_file = Path(__file__).parent / "web_search_agent.py"
    if source_file.exists():
        dest_file = Path(dags_folder) / "web_search_agent.py"
        shutil.copy(source_file, dest_file)
        print(f"✅ Tools file copied: {dest_file}\n")

    return str(dag_path)


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================


def quick_generate():
    """Quick generation with defaults - just run and go!"""
    return generate_airflow_dag()


def custom_generate(dag_id: str, dags_folder: str = None):
    """Generate with custom settings."""
    return generate_airflow_dag(dag_id=dag_id, dags_folder=dags_folder)


# ============================================================================
# MAIN
# ============================================================================


def main():
    """Main entry point - simple and clean!"""
    import sys

    if len(sys.argv) > 1:
        dag_id = sys.argv[1]
        dags_folder = sys.argv[2] if len(sys.argv) > 2 else None
        custom_generate(dag_id, dags_folder)
    else:
        quick_generate()


if __name__ == "__main__":
    main()
