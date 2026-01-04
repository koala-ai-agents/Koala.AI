"""Minimal demo using an OpenAI-compatible LLM endpoint (e.g., Groq).

Set env vars `LLM_API_KEY` and optionally `LLM_BASE_URL`.
"""

from __future__ import annotations

import os
from pathlib import Path

from koala import LLMClient


def _load_env() -> None:
    """Load simple KEY=VALUE lines from project .env without extra deps."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ[key] = val


def main() -> None:
    _load_env()
    api_key = os.getenv("LLM_API_KEY")
    base_url = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")
    if not api_key:
        raise SystemExit("LLM_API_KEY not set")

    client = LLMClient(api_key=api_key, base_url=base_url)
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Say hello and one fun fact."},
    ]

    default_model = "openai/gpt-oss-120b" if "groq" in base_url else "gpt-4o-mini"
    model = os.getenv("LLM_MODEL", default_model)
    data = client.chat(model=model, messages=messages)
    try:
        print(data["choices"][0]["message"]["content"])
    except Exception:
        print(data)


if __name__ == "__main__":
    main()
