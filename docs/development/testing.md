# Testing

Koala ships with a broad pytest suite covering every non-legacy module.
The suite is fast (~7 seconds), fully offline by default, and gated at 70%
coverage on non-legacy code — currently reporting ~91%.

## Layout

```
tests/
├── conftest.py
├── test_sdk.py                 # package metadata sanity
├── core/                       # L1 primitives
├── models/                     # L2 (provider, registry, keys, ...)
├── tools/                      # L3 (@tool, approval, schema, MCP)
├── memory/                     # L4 (backends + agent integration)
├── behaviors/                  # L5 (builtins + agent integration)
├── agents/                     # L6 (tool loop, structured output, handoffs)
├── orchestration/              # L7 (flow, executor)
├── harness/                    # L8 (session, checkpointing, HITL)
├── ui/                         # show / ashow
├── observability/              # OpenTelemetry span attributes
└── integration/                # env-gated live-provider smoke tests
```

## Running the suite

```bash
# Everything, with coverage (default)
make test                       # or:  pytest tests/

# No coverage (faster)
make test-fast                  # or:  pytest tests/ --no-cov

# One module
pytest tests/agents/ --no-cov -v

# One test
pytest tests/tools/test_function_tool.py::test_tool_derives_schema_from_signature --no-cov -v
```

## Markers

Declared in `pyproject.toml`:

```toml
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "integration: marks tests that hit real LLM providers (needs env keys)",
    "unit: marks pure unit tests",
]
```

`integration` is the only marker actively used today — see the
[Integration tests](#integration-tests) section.

## Coverage

Gated at 70% via `pyproject.toml`:

```toml
[tool.coverage.run]
source = ["src/koala"]
omit = ["src/koala/_legacy/*"]

[tool.pytest.ini_options]
addopts = [
    "-ra", "-q", "--strict-markers",
    "--cov=src/koala", "--cov-report=term-missing", "--cov-report=html",
    "--cov-fail-under=70",
]
```

Legacy modules under `src/koala/_legacy/` are excluded — they're
quarantined pending removal and have no test coverage.

HTML report lives at `htmlcov/index.html` after every run.

## Integration tests

Env-gated smoke tests hit real LLM providers. Skipped by default — never
run in offline CI.

```bash
# Groq
$env:GROQ_API_KEY = "gsk_..."
pytest tests/integration/test_groq_smoke.py -m integration --no-cov -v

# OpenAI
$env:OPENAI_API_KEY = "sk-..."
pytest tests/integration/test_openai_smoke.py -m integration --no-cov -v

# Ollama (needs a local `ollama serve` on port 11434)
pytest tests/integration/test_ollama_smoke.py -m integration --no-cov -v

# All three via make target
make test-integration
```

Each test file uses `pytest.mark.skipif` on its module-level `pytestmark`
so tests silently skip when the required env is missing.

## Test doubles

Unit tests use two patterns to avoid network calls:

### `MockTransport` on `httpx`

Wire a fake HTTP transport into `Model(http_client=...)` to script
`UniversalProvider` responses. Used in `tests/models/`.

### `ScriptedProvider`

A `BaseProvider` subclass that yields pre-canned events. Used across
`tests/agents/`, `tests/harness/`, `tests/observability/`, etc. Lives in
`tests/agents/conftest.py`:

```python
from tests.agents.conftest import (
    assistant_text,
    assistant_tool_call,
    make_scripted_model,
)

model, provider = make_scripted_model([
    assistant_tool_call("add", {"a": 7, "b": 5}, call_id="c1"),
    assistant_text("12"),
])

agent = Agent(model, tools=[add])
```

`make_scripted_model` returns a `(Model, ScriptedProvider)` pair. Each
iteration of the agent's loop consumes the next scripted response.

## MCP tests

The MCP tests fully mock the `ClientSession` and transport — no
subprocess, no network. See `tests/tools/test_mcp.py` for the pattern.

## OpenTelemetry tests

`tests/observability/conftest.py` installs a session-scoped in-memory
span exporter (OTel enforces set-once on the global TracerProvider) and
`.clear()`s it per test.

## Async patterns

Every async test uses `@pytest.mark.asyncio`. `asyncio_mode` is left at
the default (strict) — tests that use the sync `Agent.run(...)` or
`show(...)` entry points must **not** be marked async, because those
call `asyncio.run()` internally and a running loop conflicts.

## Adding a test

1. Put it in the layer's directory (e.g. `tests/agents/` for L6).
2. Use `ScriptedProvider` or `MockTransport` — no live calls.
3. Prefer clear behavioral assertions over implementation details.
4. Keep it fast — the entire suite finishes in seconds.

## Type checking

```bash
make type-check                  # runs mypy on src/koala
```

Configured in `pyproject.toml` `[tool.mypy]`. `disallow_untyped_defs` is
off for now; `ignore_missing_imports` is on because several optional
dependencies (mcp, opentelemetry) may not always be installed.

## Linting and formatting

```bash
make lint                        # ruff check
make lint-fix                    # ruff check --fix
make format                      # black + isort
make format-check                # black --check + isort --check
```

Ruff config is in `pyproject.toml` `[tool.ruff]`. `_legacy/` is excluded.

## CI-shaped local run

```bash
make ci           # format-check + lint + type-check + test
```

## Reference

- The full test file list is under [`tests/`](https://github.com/PR-HARIHARAN/Koala.AI/tree/main/tests).
- The `tests/agents/conftest.py` `ScriptedProvider` is the workhorse
  for offline agent tests.
