# Apache Airflow Integration Guide

Complete guide to using Kola-AI with Apache Airflow for production-scale workflow orchestration.

## 📚 Table of Contents

- [What is This?](#what-is-this)
- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Setup Guide](#setup-guide)
- [Creating Workflows](#creating-workflows)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## 🎯 What is This?

The Airflow integration allows you to run your Kola workflows on Apache Airflow, giving you:

- **🔄 Distributed Execution**: Run tasks across multiple workers
- **📊 Visual Monitoring**: Track workflows in Airflow's web UI
- **🔁 Retry & Recovery**: Automatic retries and failure handling
- **📈 Scalability**: Handle hundreds of concurrent workflows
- **📅 Scheduling**: Run workflows on a schedule (cron-like)

**When to use Airflow?**
- Production environments
- Workflows with >10 steps
- Need for distributed processing
- Require monitoring dashboard
- Want automatic retries/alerts

**When to use LocalExecutor?**
- Development and testing
- Simple workflows (<10 steps)
- Single-machine execution
- Quick prototyping

## ⚡ Quick Start

### 1. Start Airflow (Docker)

\\\ash
# Start Airflow container
docker-compose up -d

# Wait ~3 minutes for startup
docker logs -f airflow-standalone

# Check health
curl http://localhost:8080/health
\\\

### 2. Create Your Workflow

\\\python
# my_workflow.py
from kola.flow import dag
from kola.tools import tool, default_registry
from kola.executors.airflow_executor import AirflowExecutor

# Define tools
@tool("add")
def add(x: int, y: int) -> int:
    return x + y

@tool("multiply")
def multiply(x: int, factor: int) -> int:
    return x * factor

# Build workflow
flow = (
    dag("my-workflow")
    .step("add_numbers", "add", x=10, y=5)
    .step("multiply_result", "multiply", x="\$result.add_numbers\", factor=2)
    .build()
)

# Execute on Airflow
executor = AirflowExecutor(
    airflow_url="http://localhost:8080",
    dags_folder="./airflow/dags"
)

registry = {name: meta.func for name, meta in default_registry._tools.items()}
results = executor.run_dagflow(flow, registry)

print(f"✅ Results: {results}")
\\\

### 3. Run It

\\\ash
python my_workflow.py
\\\

That's it! Your workflow is now running on Airflow. Check the UI at http://localhost:8080

## 🔍 How It Works

### Architecture

\\\
Your Python Script (Windows/Linux)          Airflow (Docker Container)
┌───────────────────────────┐              ┌───────────────────────────┐
│ 1. Define Workflow        │              │                           │
│    @tool("add")           │              │                           │
│    def add(x, y): ...     │              │                           │
│                           │              │                           │
│ 2. Build Flow             │              │                           │
│    flow = dag(...)        │              │                           │
│                           │              │                           │
│ 3. Execute                │──────────▶   │ 4. Generate DAG File      │
│    executor.run_dagflow() │   REST API   │    kola_my-workflow.py    │
│                           │              │                           │
│                           │              │ 5. Parse & Execute        │
│                           │              │    - Run tasks            │
│                           │              │    - Store results        │
│                           │              │                           │
│ 6. Get Results            │◀─────────────│ 7. Return via XCom       │
│    results = {...}        │   REST API   │    Task outputs           │
└───────────────────────────┘              └───────────────────────────┘
\\\

### Key Concepts

1. **DAG Generation**: Kola translates your workflow into an Airflow DAG (Python file)
2. **Tool Embedding**: Tool functions are embedded in the DAG file (no serialization issues!)
3. **REST API**: Communication between your script and Airflow via HTTP
4. **XCom**: Airflow's way of passing data between tasks
5. **Version Independent**: Works across Python 3.8, 3.11, 3.12

## 🛠️ Setup Guide

### Option 1: Docker (Recommended)

**Requirements:**
- Docker Desktop installed
- 4GB+ RAM allocated to Docker

**Steps:**

1. **Start Airflow**:
\\\ash
cd /path/to/kola
docker-compose up -d
\\\

2. **Verify**:
\\\ash
# Check container is running
docker ps | grep airflow

# Check logs
docker logs airflow-standalone

# Access UI
open http://localhost:8080
# Login: admin / admin
\\\

3. **Configuration**:

The \docker-compose.yaml\ is pre-configured with:
- ✅ SQLite database (simple, no setup)
- ✅ SequentialExecutor (single-worker, reliable)
- ✅ Auto-unpause DAGs
- ✅ REST API enabled
- ✅ Kola source mounted (tools available)

**Important Paths:**
- DAGs folder: \./airflow/dags\ (created automatically)
- Logs: \./airflow/logs\
- Database: \./airflow/airflow.db\

### Option 2: Local Installation

**Requirements:**
- Python 3.8-3.11 (Airflow doesn't support 3.12 yet)

**Steps:**

1. **Install Airflow**:
\\\ash
pip install apache-airflow==2.8.1
\\\

2. **Initialize**:
\\\ash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
\\\

3. **Start Services**:
\\\ash
# Terminal 1: Webserver
airflow webserver -p 8080

# Terminal 2: Scheduler
airflow scheduler
\\\

4. **Configure Kola Executor**:
\\\python
executor = AirflowExecutor(
    airflow_url="http://localhost:8080",
    dags_folder=os.path.expanduser("~/airflow/dags")
)
\\\

## 📝 Creating Workflows

### Basic Workflow

\\\python
from kola.flow import dag
from kola.tools import tool, default_registry
from kola.executors.airflow_executor import AirflowExecutor

# Step 1: Define your tools
@tool("extract")
def extract(source: str) -> dict:
    \"\"\"Extract data from source.\"\"\"
    return {"source": source, "data": [1, 2, 3, 4, 5]}

@tool("transform")
def transform(data: dict) -> dict:
    \"\"\"Transform the data.\"\"\"
    data["data"] = [x * 2 for x in data["data"]]
    return data

@tool("load")
def load(data: dict, destination: str) -> str:
    \"\"\"Load data to destination.\"\"\"
    return f"Loaded {len(data['data'])} items to {destination}"

# Step 2: Build the workflow
flow = (
    dag("etl-pipeline")
    .step("extract", "extract", source="database")
    .step("transform", "transform", data="\$result.extract\")
    .step("load", "load", data="\$result.transform\", destination="warehouse")
    .build()
)

# Step 3: Execute
executor = AirflowExecutor(
    airflow_url="http://localhost:8080",
    dags_folder="./airflow/dags"
)

registry = {name: meta.func for name, meta in default_registry._tools.items()}
results = executor.run_dagflow(flow, registry)
\\\

### Passing Data Between Steps

Use \\$result.step_id\\ to reference previous step results:

\\\python
flow = (
    dag("data-flow")
    .step("step1", "process", data="input")
    .step("step2", "combine",
          result1="\$result.step1\",  # Get full result from step1
          other="value")
    .step("step3", "use_field",
          value="\$result.step1.field\")  # Get specific field
    .build()
)
\\\

### Parallel Execution

Steps without dependencies run in parallel:

\\\python
flow = (
    dag("parallel-workflow")
    .step("fetch_a", "fetch", source="api_a")
    .step("fetch_b", "fetch", source="api_b")  # Runs parallel with fetch_a
    .step("fetch_c", "fetch", source="api_c")  # Runs parallel with fetch_a/b
    .step("combine", "merge",
          a="\$result.fetch_a\",
          b="\$result.fetch_b\",
          c="\$result.fetch_c\")  # Waits for all three
    .build()
)
\\\

### Complex Example: ML Pipeline

\\\python
@tool("load_data")
def load_data(path: str) -> dict:
    return {"features": [...], "labels": [...]}

@tool("train_model")
def train_model(data: dict) -> dict:
    # Training logic
    return {"model_id": "model_123", "accuracy": 0.95}

@tool("evaluate")
def evaluate(model: dict, test_data: dict) -> dict:
    return {"accuracy": 0.94, "f1": 0.93}

@tool("deploy")
def deploy(model: dict, metrics: dict) -> str:
    if metrics["accuracy"] > 0.90:
        return f"Deployed {model['model_id']}"
    return "Deployment skipped - low accuracy"

flow = (
    dag("ml-pipeline")
    .step("load_train", "load_data", path="train.csv")
    .step("load_test", "load_data", path="test.csv")
    .step("train", "train_model", data="\$result.load_train\")
    .step("evaluate", "evaluate",
          model="\$result.train\",
          test_data="\$result.load_test\")
    .step("deploy", "deploy",
          model="\$result.train\",
          metrics="\$result.evaluate\")
    .build()
)
\\\

## ⚙️ Configuration

### Executor Options

\\\python
executor = AirflowExecutor(
    airflow_url="http://localhost:8080",     # Airflow web server URL
    auth=("admin", "admin"),                 # Basic auth credentials
    dags_folder="./airflow/dags",            # Where to write DAG files
    poll_interval=2,                         # Status check interval (seconds)
    timeout=300                              # Max execution time (seconds)
)
\\\

### Docker Configuration

Edit \docker-compose.yaml\:

\\\yaml
services:
  airflow:
    image: apache/airflow:2.8.1
    environment:
      # Change executor type (for parallel execution)
      AIRFLOW__CORE__EXECUTOR: LocalExecutor  # or SequentialExecutor

      # Auto-unpause new DAGs
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'

      # Increase worker timeout
      AIRFLOW__CORE__DAGRUN_TIMEOUT: 600

    ports:
      - "8080:8080"  # Change port if needed
\\\

### Advanced: Production Setup

For production, use PostgreSQL instead of SQLite:

\\\yaml
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow

  airflow:
    depends_on:
      - postgres
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor  # or CeleryExecutor for distributed
\\\

## 🔧 Troubleshooting

### Problem: DAG Not Found

**Symptom:** \FlowError: DAG 'my-workflow' not found\

**Solutions:**

1. **Check DAG was created**:
\\\ash
ls -la ./airflow/dags/
# Should see: kola_my-workflow.py
\\\

2. **Check for import errors**:
\\\ash
docker exec airflow-standalone airflow dags list-import-errors
\\\

3. **Wait longer** - Airflow needs ~30s to parse new DAGs:
\\\python
executor = AirflowExecutor(
    ...,
    timeout=120  # Increase timeout
)
\\\

### Problem: Tasks Failing

**Symptom:** Tasks show as failed in Airflow UI

**Solutions:**

1. **Check task logs**:
\\\ash
# Via Docker
docker exec airflow-standalone airflow tasks test my-workflow my-task 2025-01-01

# Or check logs folder
cat ./airflow/logs/my-workflow/my-task/.../1.log
\\\

2. **Test tool locally first**:
\\\python
# Make sure tool works outside Airflow
from my_tools import my_tool
result = my_tool(arg1="test")
print(result)
\\\

3. **Check for missing dependencies**:
\\\ash
# Install in Airflow container
docker exec airflow-standalone pip install <package>
\\\

### Problem: Connection Refused

**Symptom:** \ConnectionError: Failed to connect to http://localhost:8080\

**Solutions:**

1. **Check Airflow is running**:
\\\ash
docker ps | grep airflow
# Should show container running
\\\

2. **Check port is correct**:
\\\ash
curl http://localhost:8080/health
# Should return: {"status": "healthy"}
\\\

3. **Check firewall/antivirus** - May block port 8080

### Problem: Results are Strings

**Symptom:** Results come back as strings instead of expected types

**Cause:** Airflow XCom serializes values to JSON

**Solution:** Convert types in your code:
\\\python
results = executor.run_dagflow(flow, registry)

# Convert if needed
value = int(results["my_step"])  # If expecting int
value = json.loads(results["my_step"])  # If expecting dict
\\\

### Problem: Slow Execution

**Symptom:** Workflows take longer than expected

**Solutions:**

1. **Use LocalExecutor** (instead of SequentialExecutor):
\\\yaml
# docker-compose.yaml
environment:
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://...  # Requires PostgreSQL
\\\

2. **Reduce polling interval**:
\\\python
executor = AirflowExecutor(..., poll_interval=1)  # Check every 1s instead of 2s
\\\

3. **Check Airflow scheduler logs**:
\\\ash
docker logs airflow-standalone | grep ERROR
\\\

## 📊 Monitoring

### Airflow UI

Access at http://localhost:8080 (admin/admin)

- **DAGs View**: See all your workflows
- **Graph View**: Visual representation of task dependencies
- **Task Logs**: Detailed execution logs for each task
- **Gantt Chart**: Timeline of task execution

### Programmatic Monitoring

\\\python
# Get DAG status
import requests

response = requests.get(
    "http://localhost:8080/api/v1/dags/my-workflow",
    auth=("admin", "admin")
)
dag_info = response.json()
print(f"DAG paused: {dag_info['is_paused']}")

# Get task status
response = requests.get(
    f"http://localhost:8080/api/v1/dags/my-workflow/dagRuns/{run_id}/taskInstances",
    auth=("admin", "admin")
)
tasks = response.json()["task_instances"]
for task in tasks:
    print(f"{task['task_id']}: {task['state']}")
\\\

## 🎓 Best Practices

### 1. Tool Design

✅ **DO**:
- Keep tools small and focused
- Use type hints
- Add docstrings
- Handle errors gracefully

❌ **DON'T**:
- Create tools that depend on external state
- Use global variables
- Have side effects without documentation

### 2. Workflow Design

✅ **DO**:
- Break complex workflows into smaller steps
- Use descriptive step IDs
- Leverage parallelism where possible
- Add retry logic for flaky operations

❌ **DON'T**:
- Create circular dependencies
- Pass large data between steps (use references)
- Mix business logic in workflow definition

### 3. Production

✅ **DO**:
- Use PostgreSQL instead of SQLite
- Set up proper monitoring/alerting
- Configure retries and timeouts
- Use secrets management for credentials
- Test workflows locally first

❌ **DON'T**:
- Use default passwords (admin/admin)
- Skip error handling
- Ignore failed tasks
- Deploy without testing

## 📖 Examples

See complete examples in \xamples/\:
- \irflow_test.py\ - Basic Airflow integration test
- \gent_a.py\ - Data pipeline example
- \gent_b.py\ - Human-in-loop workflow

## 🚀 Next Steps

1. ✅ Start Airflow: \docker-compose up -d\
2. ✅ Run test: \python examples/airflow_test.py\
3. ✅ Create your own workflow
4. ✅ Monitor in UI: http://localhost:8080
5. ✅ Scale to production

## 💡 Tips

- **Development**: Use LocalExecutor for faster iteration
- **Testing**: Use \docker-compose\ for consistency
- **Production**: Use PostgreSQL + LocalExecutor or CeleryExecutor
- **Debugging**: Check Airflow UI logs before code
- **Performance**: Profile your tools, not the workflow

## 🆘 Need Help?

- **Documentation**: This guide + [README.md](../README.md)
- **Examples**: \xamples/\ folder
- **Issues**: [GitHub Issues](https://github.com/PR-HARIHARAN/Kola-AI/issues)
- **Airflow Docs**: https://airflow.apache.org/docs/

---

**Happy Orchestrating! 🎉**
