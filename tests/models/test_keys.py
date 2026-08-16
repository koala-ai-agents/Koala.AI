"""Tests for koala.models.keys — the api key resolution chain."""

from __future__ import annotations

import pytest

from koala.models import MissingApiKey, resolve_api_key


def test_explicit_key_wins_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "from-env")
    monkeypatch.setenv("LLM_API_KEY", "from-universal")

    result = resolve_api_key(
        explicit="explicit-key",
        env_key="GROQ_API_KEY",
        provider_slug="groq",
    )
    assert result == "explicit-key"


def test_provider_env_key_used_when_no_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "from-groq-env")
    monkeypatch.setenv("LLM_API_KEY", "from-universal")

    result = resolve_api_key(
        explicit=None, env_key="GROQ_API_KEY", provider_slug="groq"
    )
    assert result == "from-groq-env"


def test_universal_key_used_when_provider_env_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.setenv("LLM_API_KEY", "from-universal")

    result = resolve_api_key(
        explicit=None, env_key="GROQ_API_KEY", provider_slug="groq"
    )
    assert result == "from-universal"


def test_universal_key_used_when_provider_env_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "")
    monkeypatch.setenv("LLM_API_KEY", "from-universal")

    result = resolve_api_key(
        explicit=None, env_key="GROQ_API_KEY", provider_slug="groq"
    )
    assert result == "from-universal"


def test_missing_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)

    with pytest.raises(MissingApiKey, match="GROQ_API_KEY"):
        resolve_api_key(
            explicit=None, env_key="GROQ_API_KEY", provider_slug="groq"
        )


def test_missing_key_error_mentions_universal_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)

    with pytest.raises(MissingApiKey, match="LLM_API_KEY"):
        resolve_api_key(
            explicit=None, env_key="GROQ_API_KEY", provider_slug="groq"
        )


def test_key_optional_returns_none_when_nothing_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LLM_API_KEY", raising=False)

    result = resolve_api_key(
        explicit=None,
        env_key=None,  # keyless provider like Ollama
        provider_slug="ollama",
        key_optional=True,
    )
    assert result is None


def test_key_optional_still_prefers_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LLM_API_KEY", raising=False)

    result = resolve_api_key(
        explicit="my-key",
        env_key=None,
        provider_slug="ollama",
        key_optional=True,
    )
    assert result == "my-key"


def test_empty_explicit_treated_as_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_API_KEY", "from-universal")

    result = resolve_api_key(
        explicit="",  # empty string, treat as not-provided
        env_key=None,
        provider_slug="custom",
    )
    assert result == "from-universal"
