"""Shared helpers for env-gated live-provider integration tests.

Every test module in ``tests/integration/`` is opt-in and skipped unless the
required environment variable is present. To run them::

    # Groq
    $env:GROQ_API_KEY = "..."
    uv run pytest tests/integration/test_groq_smoke.py -m integration --no-cov

    # OpenAI
    $env:OPENAI_API_KEY = "..."
    uv run pytest tests/integration/test_openai_smoke.py -m integration --no-cov

    # Ollama (requires a local `ollama serve`)
    uv run pytest tests/integration/test_ollama_smoke.py -m integration --no-cov

CI should only run these on main / release branches with the appropriate
secrets injected.
"""

from __future__ import annotations
