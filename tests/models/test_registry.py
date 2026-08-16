"""Tests for koala.models.registry."""

from __future__ import annotations

import pytest

from koala.core import Capability
from koala.models import (
    BUILTIN_PROVIDERS,
    ProviderProfile,
    get_provider_profile,
    list_providers,
    register_provider,
    unregister_provider,
)


def test_known_builtins_include_expected_slugs() -> None:
    slugs = set(BUILTIN_PROVIDERS.keys())
    for expected in {
        "openai",
        "groq",
        "deepseek",
        "xai",
        "together",
        "fireworks",
        "openrouter",
        "ollama",
        "lmstudio",
        "custom",
    }:
        assert expected in slugs


def test_get_provider_profile_returns_none_for_unknown() -> None:
    assert get_provider_profile("this-provider-does-not-exist") is None


def test_openai_profile_shape() -> None:
    p = get_provider_profile("openai")
    assert p is not None
    assert p.base_url == "https://api.openai.com/v1"
    assert p.env_key == "OPENAI_API_KEY"
    assert Capability.TOOL_CALLING in p.capabilities


def test_ollama_is_keyless() -> None:
    p = get_provider_profile("ollama")
    assert p is not None
    assert p.env_key is None
    assert p.base_url == "http://localhost:11434/v1"


def test_custom_has_no_default_base_url() -> None:
    p = get_provider_profile("custom")
    assert p is not None
    assert p.base_url is None
    assert p.env_key is None


def test_register_provider_adds_a_new_slug() -> None:
    unregister_provider("test-corp")
    try:
        register_provider(
            "test-corp",
            base_url="https://api.testcorp.local/v1",
            env_key="TESTCORP_KEY",
            capabilities=frozenset({Capability.STREAMING}),
        )
        p = get_provider_profile("test-corp")
        assert p is not None
        assert p.base_url == "https://api.testcorp.local/v1"
        assert p.env_key == "TESTCORP_KEY"
        assert p.capabilities == frozenset({Capability.STREAMING})
    finally:
        unregister_provider("test-corp")


def test_register_provider_normalizes_slug() -> None:
    unregister_provider("myco")
    try:
        register_provider(
            "  MyCo  ", base_url="https://x.example/v1", env_key="MYCO_KEY"
        )
        assert get_provider_profile("myco") is not None
        assert get_provider_profile("MYCO") is not None
    finally:
        unregister_provider("myco")


def test_register_provider_rejects_empty_slug() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        register_provider("   ", base_url="https://x.example/v1")


def test_user_provider_overrides_builtin() -> None:
    try:
        register_provider(
            "openai",  # override built-in
            base_url="https://custom-openai-proxy.example/v1",
            env_key="OPENAI_API_KEY",
        )
        p = get_provider_profile("openai")
        assert p is not None
        assert p.base_url == "https://custom-openai-proxy.example/v1"
    finally:
        unregister_provider("openai")
    # Built-in restored
    p_after = get_provider_profile("openai")
    assert p_after is not None
    assert p_after.base_url == "https://api.openai.com/v1"


def test_unregister_returns_false_for_unknown() -> None:
    assert unregister_provider("never-registered") is False


def test_unregister_cannot_remove_builtin() -> None:
    # Built-ins are always visible via get_provider_profile, even after
    # calling unregister_provider on the same slug.
    assert unregister_provider("openai") is False
    assert get_provider_profile("openai") is not None


def test_list_providers_includes_builtin_and_user() -> None:
    try:
        register_provider("aaa-test-slug", base_url="http://x")
        listed = list_providers()
        assert "openai" in listed
        assert "aaa-test-slug" in listed
        assert listed == sorted(listed)
    finally:
        unregister_provider("aaa-test-slug")


def test_provider_profile_is_frozen() -> None:
    import dataclasses

    p = ProviderProfile(base_url="x", env_key="Y")
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.base_url = "z"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# base_url_env — env-driven URL override for local providers
# ---------------------------------------------------------------------------


def test_ollama_profile_declares_base_url_env() -> None:
    """Local providers advertise an env var to override the base URL."""
    p = get_provider_profile("ollama")
    assert p is not None
    assert p.base_url_env == "OLLAMA_BASE_URL"


def test_lmstudio_profile_declares_base_url_env() -> None:
    p = get_provider_profile("lmstudio")
    assert p is not None
    assert p.base_url_env == "LMSTUDIO_BASE_URL"


def test_model_reads_base_url_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Model construction picks up ``base_url`` from the profile's env var."""
    from koala.models import Model

    monkeypatch.setenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434/v1")
    m = Model("ollama/qwen2.5:7b")
    assert m.provider.base_url == "http://host.docker.internal:11434/v1"


def test_explicit_base_url_wins_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The explicit ``base_url=`` kwarg still wins over the env var."""
    from koala.models import Model

    monkeypatch.setenv("OLLAMA_BASE_URL", "http://from-env:11434/v1")
    m = Model("ollama/qwen2.5:7b", base_url="http://explicit:11434/v1")
    assert m.provider.base_url == "http://explicit:11434/v1"


def test_env_falls_back_to_profile_default_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the env var is not set, the profile's static default wins."""
    from koala.models import Model

    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    m = Model("ollama/qwen2.5:7b")
    assert m.provider.base_url == "http://localhost:11434/v1"


def test_empty_env_var_falls_back_to_profile_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Whitespace-only env var is treated as unset."""
    from koala.models import Model

    monkeypatch.setenv("OLLAMA_BASE_URL", "   ")
    m = Model("ollama/qwen2.5:7b")
    assert m.provider.base_url == "http://localhost:11434/v1"
