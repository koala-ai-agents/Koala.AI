"""API-key resolution.

Resolution order (first non-empty value wins):
    1. Explicit `api_key=` argument on `Model(...)`.
    2. Provider-specific env var (`GROQ_API_KEY`, `OPENAI_API_KEY`, ...) if set.
    3. Universal `LLM_API_KEY` env var — the "one to rule them all".
    4. `None` if the provider is keyless (Ollama, LM Studio).
    5. Raise `MissingApiKey` with a clear message naming every var that would
       have worked.
"""

from __future__ import annotations

import os

from .errors import MissingApiKey

UNIVERSAL_ENV_KEY: str = "LLM_API_KEY"
"""The single universal API-key env var. Set this once and it works across
providers that don't require a provider-specific key."""


def resolve_api_key(
    *,
    explicit: str | None,
    env_key: str | None,
    provider_slug: str,
    key_optional: bool = False,
) -> str | None:
    """Resolve an API key using the standard fallback chain.

    Args:
        explicit: The `api_key=` argument passed by the user, or None.
        env_key: The provider-specific env var name (e.g. ``"GROQ_API_KEY"``),
            or None if the provider doesn't have one.
        provider_slug: Provider slug used only for error messages.
        key_optional: When True, return None instead of raising when no key
            can be resolved. Used for keyless local providers.

    Raises:
        MissingApiKey: If no key can be resolved and ``key_optional`` is False.
    """
    if explicit is not None and explicit != "":
        return explicit

    if env_key:
        value = os.environ.get(env_key, "").strip()
        if value:
            return value

    universal = os.environ.get(UNIVERSAL_ENV_KEY, "").strip()
    if universal:
        return universal

    if key_optional:
        return None

    tried: list[str] = []
    if env_key:
        tried.append(f"${env_key}")
    tried.append(f"${UNIVERSAL_ENV_KEY}")
    raise MissingApiKey(
        f"No API key for provider {provider_slug!r}. "
        f"Set {' or '.join(tried)}, or pass api_key= to Model()."
    )
