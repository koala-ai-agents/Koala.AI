"""Apache Airflow executor for Kola flows - GENERALIZED VERSION.

This is a production-ready, version-independent implementation that works across
Python 3.8, 3.11, and 3.12 by using dynamic imports instead of function serialization.

Key Design Decisions:
1. NO pickle/dill serialization - avoids Python version incompatibility
2. Dynamic import of tools from koala.tools at Airflow runtime
3. Tools must be importable (PYTHONPATH properly configured)
4. Generalized for any flow - no per-file customization needed

Requirements:
    - Apache Airflow running (separate installation)
    - pip install kola[airflow]  # Installs requests
    - PYTHONPATH includes kola source in Airflow environment

Example:
    >>> from koala.executors import AirflowExecutor
    >>> from koala.flow import dag
    >>> from koala.tools import default_registry, tool
    >>>
    >>> @tool("my_func")
    >>> def my_func(x: int) -> int:
    ...     return x * 2
    >>>
    >>> flow = dag("my-flow").step("double", "my_func", x=5).build()
    >>> registry = {name: meta.func for name, meta in default_registry._tools.items()}
    >>>
    >>> executor = AirflowExecutor(
    ...     airflow_url="http://localhost:8080",
    ...     auth=("admin", "admin"),
    ...     dags_folder="/path/to/airflow/dags"
    ... )
    >>> results = executor.run_dagflow(flow, registry)
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Dict, Optional

try:
    import requests
except ImportError:
    raise ImportError(
        "AirflowExecutor requires requests library. "
        "Install with: pip install kola[airflow]"
    )

from ..flow import DAGFlow, FlowError
from ..guards import GuardsRegistry
from ..observability import logger, metrics, tracer


class AirflowExecutor:
    """Execute Kola flows by delegating to Apache Airflow.

    This executor is GENERALIZED and VERSION-INDEPENDENT:
    - Works with any Python 3.8+ version (no serialization issues)
    - No per-flow customization needed
    - Tools are imported dynamically at Airflow runtime
    - Single implementation for all flows

    Architecture:
    1. Translate Kola DAGFlow → Airflow DAG Python code
    2. Deploy DAG file to Airflow's dags folder
    3. Trigger DAG via REST API
    4. Poll for completion
    5. Fetch results via XCom

    Attributes:
        airflow_url: Base URL of Airflow webserver (e.g., http://localhost:8080)
        auth: Tuple of (username, password) for Airflow basic auth
        poll_interval: Seconds to wait between status checks
        dags_folder: Path to Airflow's dags directory for DAG deployment
        timeout: Maximum seconds to wait for DAG completion (None = no timeout)
    """

    def __init__(
        self,
        airflow_url: str = "http://localhost:8080",
        auth: Optional[tuple] = None,
        poll_interval: int = 2,
        dags_folder: Optional[str] = None,
        timeout: Optional[int] = 300,
    ):
        """Initialize AirflowExecutor.

        Args:
            airflow_url: Airflow webserver URL
            auth: Basic auth credentials (username, password)
            poll_interval: Polling interval in seconds
            dags_folder: Path to Airflow dags directory
            timeout: Max execution time in seconds (None = no timeout)
        """
        self.airflow_url = airflow_url.rstrip("/")
        self.auth = auth or ("admin", "admin")
        self.poll_interval = poll_interval
        self.dags_folder = dags_folder
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = self.auth

    def run_dagflow(
        self,
        flow: DAGFlow,
        registry: Dict[str, Callable[..., Any]],
        guards: Optional[GuardsRegistry] = None,
    ) -> Dict[str, Any]:
        """Execute a Kola DAGFlow using Apache Airflow.

        Steps:
        1. Validate tools are in registry
        2. Translate DAGFlow to Airflow Python code
        3. Deploy DAG file to Airflow's dags folder
        4. Wait for Airflow to parse DAG
        5. Trigger DAG execution via REST API
        6. Poll until completion or timeout
        7. Fetch and return results

        Args:
            flow: The DAGFlow to execute
            registry: Tool registry mapping names to functions
            guards: Optional guards (not yet supported for Airflow)

        Returns:
            Dictionary mapping step IDs to their results

        Raises:
            FlowError: If execution fails, times out, or DAG not found
        """
        if not self.dags_folder:
            raise FlowError(
                "dags_folder must be specified for AirflowExecutor. "
                "Point it to your Airflow installation's dags directory."
            )

        trace_id = tracer.start_trace()
        logger.info("airflow_executor_started", flow_id=flow.id, trace_id=trace_id)
        metrics.inc("airflow_executor_starts")

        try:
            # Step 1: Translate flow to Airflow DAG code
            dag_code = self._translate_to_airflow(flow, registry)

            # Step 2: Write DAG file
            self._write_dag_file(flow.id, dag_code)

            # Step 3: Trigger DAG
            run_id = self._trigger_dag(flow.id)
            tracer.record(trace_id, "dag_triggered", flow_id=flow.id, run_id=run_id)
            logger.info(
                "dag_triggered", flow_id=flow.id, run_id=run_id, trace_id=trace_id
            )

            # Step 4: Poll until complete
            results = self._poll_until_complete(flow.id, run_id, trace_id)

            # Step 5: Success
            tracer.record(trace_id, "airflow_executor_completed", flow_id=flow.id)
            logger.info(
                "airflow_executor_completed", flow_id=flow.id, trace_id=trace_id
            )
            metrics.inc("airflow_executor_success")

            return results

        except Exception as e:
            tracer.record(trace_id, "airflow_executor_failed", error=str(e))
            logger.info("airflow_executor_failed", error=str(e), trace_id=trace_id)
            metrics.inc("airflow_executor_failures")
            raise

    def _translate_to_airflow(
        self, flow: DAGFlow, registry: Dict[str, Callable[..., Any]]
    ) -> str:
        """Translate Kola DAGFlow to Airflow DAG Python code.

        DYNAMIC APPROACH:
        - Import tools from source modules (no code embedding)
        - Works with any Python version
        - DAG stays small and maintainable
        - Changes to tools automatically reflected

        Args:
            flow: DAGFlow to translate
            registry: Tool registry (validates all tools exist)

        Returns:
            Python code as string

        Raises:
            ValueError: If tool not found in registry
        """
        import inspect

        # Validate all tools exist before generating
        for step in flow.steps:
            if step.action not in registry:
                raise ValueError(
                    f"Tool '{step.action}' not found in registry. "
                    f"Available: {list(registry.keys())}"
                )

        # Get unique import locations for tools
        tool_imports = {}  # module_name -> list of (tool_name, func_name)
        tools_used = set(step.action for step in flow.steps)

        for tool_name in tools_used:
            func = registry[tool_name]
            module = inspect.getmodule(func)

            # Handle __main__ module - get actual file path
            if module and module.__name__ == "__main__":
                if hasattr(module, "__file__") and module.__file__:
                    # Convert file path to module name
                    file_path = Path(module.__file__)
                    module_name = file_path.stem  # Get filename without extension
                else:
                    module_name = None
            elif module:
                module_name = module.__name__
            else:
                module_name = None

            if module_name:
                if module_name not in tool_imports:
                    tool_imports[module_name] = []
                tool_imports[module_name].append((tool_name, func.__name__))

        # Generate DAG with dynamic imports
        dag_code = f"""# Auto-generated Airflow DAG from koala flow: {flow.id}
# Generated by Kola-AI AirflowExecutor (Dynamic Version)
# This DAG imports tools dynamically - no embedded code

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Dict, Any
import sys
from pathlib import Path

# Add cookbook to path for importing tools
cookbook_path = Path(__file__).parent.parent / "cookbook"
if cookbook_path.exists() and str(cookbook_path) not in sys.path:
    sys.path.insert(0, str(cookbook_path))

# Also try current directory
dags_path = Path(__file__).parent
if str(dags_path) not in sys.path:
    sys.path.insert(0, str(dags_path))

# Dynamic imports of tools
TOOL_REGISTRY = {{}}
"""

        # Add dynamic imports
        if tool_imports:
            dag_code += "\n# Import tools from source modules\n"
            for module_name, tools in sorted(tool_imports.items()):
                dag_code += "try:\n"
                dag_code += f"    import {module_name}\n"

                for tool_name, func_name in tools:
                    dag_code += f"    TOOL_REGISTRY['{tool_name}'] = {module_name}.{func_name}\n"

                dag_code += "except ImportError as e:\n"
                dag_code += (
                    f"    print(f'Warning: Could not import {module_name}: {{e}}')\n"
                )
                dag_code += "    import traceback\n"
                dag_code += "    traceback.print_exc()\n\n"

        dag_code += '''
def execute_kola_step(**context):
    """Execute a Kola step using dynamically imported tools."""
    step_id = context['params']['step_id']
    action = context['params']['action']
    args = context['params']['args']

    # Get tool function from registry
    if action not in TOOL_REGISTRY:
        available = list(TOOL_REGISTRY.keys())
        raise ValueError(f"Tool '{action}' not found. Available: {available}")

    func = TOOL_REGISTRY[action]

    # Resolve $result references via XCom
    resolved_args = {}
    for key, value in args.items():
        if isinstance(value, str) and value.startswith("$result."):
            ref_parts = value.split("$result.", 1)[1].split(".", 1)
            upstream_step = ref_parts[0]
            ti = context['ti']
            result = ti.xcom_pull(task_ids=upstream_step)

            if len(ref_parts) > 1:
                for key_part in ref_parts[1].split("."):
                    result = result[key_part] if isinstance(result, dict) else getattr(result, key_part)
                resolved_args[key] = result
            else:
                resolved_args[key] = result
        elif isinstance(value, str) and "<<FROM_DAG_CONFIG>>" in value:
            # Get question from dag_run.conf
            dag_run = context.get('dag_run')
            if dag_run and hasattr(dag_run, 'conf') and dag_run.conf:
                question = dag_run.conf.get('question')
                if question:
                    print(f"📝 Using question from config: {question}")
                    resolved_args[key] = question
                else:
                    print(f"⚠️  No 'question' in config, using default")
                    resolved_args[key] = "What is AI?"
            else:
                print(f"⚠️  No DAG run config, using default")
                resolved_args[key] = "What is AI?"
        else:
            resolved_args[key] = value

    # Execute tool
    return func(**resolved_args)

# Define DAG
default_args = {
    'owner': 'kola',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

'''

        # Add DAG definition
        dag_code += f"""with DAG(
    dag_id='{flow.id}',
    default_args=default_args,
    description='Kola flow: {flow.id}',
    schedule=None,
    catchup=False,
    tags=['kola-generated'],
) as dag:
"""

        # Generate tasks
        for step in flow.steps:
            args_str = json.dumps(step.args)
            dag_code += f"""
    {step.id} = PythonOperator(
        task_id='{step.id}',
        python_callable=execute_kola_step,
        params={{
            'step_id': '{step.id}',
            'action': '{step.action}',
            'args': {args_str},
        }},
    )
"""

        # Generate dependencies
        if flow.edges:
            dag_code += "\n    # Task dependencies\n"
            for from_id, to_id in flow.edges:
                dag_code += f"    {from_id} >> {to_id}\n"

        return dag_code

    def _write_dag_file(self, dag_id: str, code: str) -> None:
        """Write DAG code to Airflow's dags folder.

        Args:
            dag_id: Unique DAG identifier
            code: Python code for the DAG
        """
        os.makedirs(self.dags_folder, exist_ok=True)
        file_path = os.path.join(self.dags_folder, f"kola_{dag_id}.py")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(code)

        logger.info("dag_file_written", path=file_path, dag_id=dag_id)

        # Wait for Airflow to parse the DAG
        self._wait_for_dag_parsed(dag_id, timeout=45)

    def _wait_for_dag_parsed(self, dag_id: str, timeout: int = 45) -> None:
        """Wait for Airflow to parse and recognize the DAG.

        Args:
            dag_id: DAG identifier to wait for
            timeout: Maximum seconds to wait

        Raises:
            FlowError: If DAG not found after timeout
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}"
        start_time = time.time()

        logger.info("waiting_for_dag_parse", dag_id=dag_id, timeout=timeout)

        while time.time() - start_time < timeout:
            try:
                response = self.session.get(url, timeout=5)
                if response.status_code == 200:
                    dag_info = response.json()

                    # Check if DAG is active (required for triggering)
                    if not dag_info.get("is_active", False):
                        # DAG parsed but not active yet, wait longer
                        logger.info(
                            "dag_inactive", dag_id=dag_id, waiting_for_activation=True
                        )
                        time.sleep(3)
                        continue

                    # Check if DAG is unpaused
                    if dag_info.get("is_paused", True):
                        # Unpause it automatically
                        unpause_url = f"{self.airflow_url}/api/v1/dags/{dag_id}"
                        self.session.patch(
                            unpause_url, json={"is_paused": False}, timeout=5
                        )
                        logger.info("dag_unpaused", dag_id=dag_id)

                    logger.info(
                        "dag_parsed", dag_id=dag_id, elapsed=time.time() - start_time
                    )
                    return
                elif response.status_code == 404:
                    time.sleep(3)
                    continue
                else:
                    response.raise_for_status()
            except requests.RequestException:
                time.sleep(3)
                continue

        raise FlowError(
            f"DAG '{dag_id}' not found in Airflow after {timeout}s. "
            f"Check for import errors: docker exec airflow-standalone airflow dags list-import-errors"
        )

    def _trigger_dag(self, dag_id: str, conf: Optional[Dict[str, Any]] = None) -> str:
        """Trigger a DAG run via Airflow REST API.

        Args:
            dag_id: DAG identifier to trigger
            conf: Optional configuration dict

        Returns:
            DAG run ID from Airflow

        Raises:
            FlowError: If trigger fails
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns"

        try:
            response = self.session.post(url, json={"conf": conf or {}}, timeout=10)

            if response.status_code == 404:
                raise FlowError(f"DAG '{dag_id}' not found in Airflow")

            response.raise_for_status()
            return response.json()["dag_run_id"]

        except requests.RequestException as e:
            raise FlowError(f"Failed to trigger DAG '{dag_id}': {e}")

    def _get_dag_run_status(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """Get DAG run status from Airflow.

        Args:
            dag_id: DAG identifier
            run_id: Run identifier

        Returns:
            Status dictionary from Airflow API

        Raises:
            FlowError: If status check fails
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise FlowError(
                f"Failed to get status for DAG '{dag_id}' run '{run_id}': {e}"
            )

    def _poll_until_complete(
        self, dag_id: str, run_id: str, trace_id: str
    ) -> Dict[str, Any]:
        """Poll Airflow until DAG run completes or times out.

        Args:
            dag_id: DAG identifier
            run_id: Run identifier
            trace_id: Trace ID for observability

        Returns:
            Results dictionary from completed DAG

        Raises:
            FlowError: If DAG fails or times out
        """
        start_time = time.time()

        while True:
            # Check timeout
            if self.timeout and (time.time() - start_time) > self.timeout:
                raise FlowError(
                    f"DAG '{dag_id}' run '{run_id}' timed out after {self.timeout} seconds"
                )

            # Get status
            status = self._get_dag_run_status(dag_id, run_id)
            state = status.get("state")

            if state == "success":
                # Fetch results
                return self._fetch_results(dag_id, run_id)
            elif state == "failed":
                raise FlowError(
                    f"Airflow DAG '{dag_id}' run '{run_id}' failed with state: {state}"
                )
            elif state in ["running", "queued"]:
                time.sleep(self.poll_interval)
            else:
                # Unknown state
                time.sleep(self.poll_interval)

    def _fetch_results(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """Fetch task results from Airflow XCom.

        Args:
            dag_id: DAG identifier
            run_id: Run identifier

        Returns:
            Dictionary mapping task IDs to their XCom results
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            results = {}
            for task_instance in response.json().get("task_instances", []):
                task_id = task_instance["task_id"]

                # Fetch XCom for this task
                xcom_url = (
                    f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/"
                    f"taskInstances/{task_id}/xcomEntries/return_value"
                )
                xcom_response = self.session.get(xcom_url, timeout=10)

                if xcom_response.status_code == 200:
                    results[task_id] = xcom_response.json().get("value")

            return results

        except requests.RequestException as e:
            logger.warning("failed_to_fetch_results", error=str(e))
            return {}
