"""Ollama live-provider smoke test.

Skipped unless a local Ollama daemon is reachable at ``OLLAMA_HOST`` (default
``http://localhost:11434``). When no ``KOALA_TEST_OLLAMA_MODEL`` env var is
set, the test auto-picks the first non-cloud model returned by
``/api/tags`` so it works with whatever the developer already has pulled.
"""

from __future__ import annotations

import os
import socket
from urllib.parse import urlparse

import httpx
import pytest


def _ollama_base() -> str:
    return os.getenv("OLLAMA_HOST", "http://localhost:11434")


def _ollama_reachable() -> bool:
    url = _ollama_base()
    p = urlparse(url)
    host = p.hostname or "localhost"
    port = p.port or 11434
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _pick_model() -> str | None:
    """Return an installed non-cloud Ollama model, or ``None`` if none exist."""
    explicit = os.getenv("KOALA_TEST_OLLAMA_MODEL")
    if explicit:
        return explicit
    try:
        r = httpx.get(f"{_ollama_base()}/api/tags", timeout=3.0)
        r.raise_for_status()
    except Exception:  # noqa: BLE001
        return None
    for m in r.json().get("models", []):
        name = m.get("name", "")
        if name and "cloud" not in name:
            return name
    return None


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _ollama_reachable(), reason="no local Ollama daemon"),
]


def test_ollama_agent_hello_world() -> None:
    from koala import Agent, Model

    model_name = _pick_model()
    if model_name is None:
        pytest.skip("no local Ollama models installed")
    base_url = _ollama_base() + "/v1"

    model = Model(f"ollama/{model_name}", base_url=base_url)
    agent = Agent(model, instructions="Reply in one short sentence.")
    result = agent.run("Say hi.")
    assert result.stop_reason == "final_output", (
        f"agent stopped with {result.stop_reason!r}: {result.error!r}"
    )
    assert isinstance(result.output, str)
    assert len(result.output) > 0
