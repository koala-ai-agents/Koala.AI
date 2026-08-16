# Installation

Koala requires **Python 3.12 or newer**. The base install has two runtime
dependencies: `httpx` and `pydantic`. Everything else is optional and
lives behind an extra.

## From source (recommended while pre-1.0)

```bash
git clone https://github.com/PR-HARIHARAN/Koala.AI
cd Koala.AI

# With uv (fastest)
uv venv
uv pip install -e ".[dev]"

# Or with plain pip
python -m venv .venv
.venv/Scripts/activate       # Windows
# source .venv/bin/activate   # macOS/Linux
pip install -e ".[dev]"
```

## Optional extras

| Extra | Installs | Enables |
|---|---|---|
| `dev` | pytest, pytest-asyncio, pytest-cov, ruff, black, isort, mypy, pre-commit | Development toolchain |
| `mcp` | `mcp>=1.0` | `koala.tools.mcp.MCPToolset` (stdio, SSE, streamable HTTP) |
| `otel` | `opentelemetry-api`, `-sdk`, `-semantic-conventions` | Real GenAI-semconv span export from `koala.observability` |
| `docs` | mkdocs, mkdocs-material, mkdocstrings | Building this documentation site |

Install multiple at once:

```bash
uv pip install -e ".[dev,mcp,otel]"
```

## Verify the install

```python
>>> from koala import Agent, tool
>>> from koala.models import BUILTIN_PROVIDERS
>>> sorted(BUILTIN_PROVIDERS)
['custom', 'deepseek', 'fireworks', 'groq', 'lmstudio', 'ollama',
 'openai', 'openrouter', 'together', 'xai']
```

If that runs without an import error, the install is good.

## Running tests

```bash
# Full suite with coverage (excludes _legacy/)
make test

# Fast — no coverage
make test-fast

# Live-provider smoke tests (env-gated)
$env:GROQ_API_KEY = "..."
make test-integration
```

The gate is 70% coverage on non-legacy code. The suite currently reports
~91%.

## Editor / IDE

`koala` ships a `py.typed` marker, so mypy / Pyright / Pylance will pick
up type information automatically once the package is installed
(editable install works too).
