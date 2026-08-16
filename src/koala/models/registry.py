"""Built-in provider registry.

Every well-known provider (OpenAI, Groq, DeepSeek, Ollama, ...) ships with a
`ProviderProfile` giving its default base URL, env var name, and capability
set. Users can register their own providers at runtime with
`register_provider(...)`.

The registry is what makes `Model("groq/llama-3.3-70b-versatile")` "just
work" without the user having to spell out base_url or api key.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..core.capabilities import Capability


@dataclass(frozen=True, slots=True)
class ProviderProfile:
    """Static defaults for one provider.

    Args:
        base_url: Default HTTP endpoint. `None` means the user MUST pass
            `base_url=` when constructing a Model for this provider (e.g.
            Azure OpenAI, or a custom endpoint).
        env_key: Provider-specific env var name for the API key, or None
            if the provider is keyless (Ollama, LM Studio).
        base_url_env: Optional env var that overrides ``base_url`` at
            ``Model`` construction time. Useful for local providers
            (Ollama, LM Studio) that live on the host but need a
            different URL when accessed from Docker containers — set
            ``OLLAMA_BASE_URL=http://host.docker.internal:11434/v1`` and
            every ``Model("ollama/...")`` picks it up automatically.
        default_headers: Extra headers sent on every request.
        api_style: Wire format. `"openai"` is the only value at L2; room for
            `"anthropic"` and others in future.
        capabilities: Default capability set for this provider. Overridable
            per-model.
    """

    base_url: str | None
    env_key: str | None
    base_url_env: str | None = None
    default_headers: dict[str, str] = field(default_factory=dict)
    api_style: str = "openai"
    capabilities: frozenset[Capability] = field(default_factory=frozenset)


# Common capability set for full-featured OpenAI-compatible providers.
_STANDARD_CAPS = frozenset(
    {
        Capability.STREAMING,
        Capability.TOOL_CALLING,
        Capability.PARALLEL_TOOL_CALLS,
        Capability.STRUCTURED_OUTPUT,
        Capability.JSON_MODE,
        Capability.LONG_CONTEXT,
        Capability.IMAGE_INPUT,
    }
)


BUILTIN_PROVIDERS: dict[str, ProviderProfile] = {
    "openai": ProviderProfile(
        base_url="https://api.openai.com/v1",
        env_key="OPENAI_API_KEY",
        capabilities=_STANDARD_CAPS,
    ),
    "groq": ProviderProfile(
        base_url="https://api.groq.com/openai/v1",
        env_key="GROQ_API_KEY",
        capabilities=frozenset(
            {
                Capability.STREAMING,
                Capability.TOOL_CALLING,
                Capability.PARALLEL_TOOL_CALLS,
                Capability.STRUCTURED_OUTPUT,
                Capability.JSON_MODE,
                Capability.IMAGE_INPUT,
            }
        ),
    ),
    "deepseek": ProviderProfile(
        base_url="https://api.deepseek.com/v1",
        env_key="DEEPSEEK_API_KEY",
        capabilities=frozenset(
            {
                Capability.STREAMING,
                Capability.TOOL_CALLING,
                Capability.JSON_MODE,
                Capability.THINKING,
            }
        ),
    ),
    "xai": ProviderProfile(
        base_url="https://api.x.ai/v1",
        env_key="XAI_API_KEY",
        capabilities=_STANDARD_CAPS,
    ),
    "together": ProviderProfile(
        base_url="https://api.together.xyz/v1",
        env_key="TOGETHER_API_KEY",
        capabilities=frozenset(
            {
                Capability.STREAMING,
                Capability.TOOL_CALLING,
                Capability.JSON_MODE,
            }
        ),
    ),
    "fireworks": ProviderProfile(
        base_url="https://api.fireworks.ai/inference/v1",
        env_key="FIREWORKS_API_KEY",
        capabilities=frozenset(
            {
                Capability.STREAMING,
                Capability.TOOL_CALLING,
                Capability.JSON_MODE,
            }
        ),
    ),
    "openrouter": ProviderProfile(
        base_url="https://openrouter.ai/api/v1",
        env_key="OPENROUTER_API_KEY",
        default_headers={
            "HTTP-Referer": "https://github.com/PR-HARIHARAN/Koala.AI",
            "X-Title": "Koala.AI",
        },
        capabilities=frozenset(
            {
                Capability.STREAMING,
                Capability.TOOL_CALLING,
                Capability.JSON_MODE,
            }
        ),
    ),
    "ollama": ProviderProfile(
        base_url="http://localhost:11434/v1",
        env_key=None,
        base_url_env="OLLAMA_BASE_URL",
        capabilities=frozenset(
            {
                Capability.STREAMING,
                Capability.TOOL_CALLING,
                Capability.JSON_MODE,
            }
        ),
    ),
    "lmstudio": ProviderProfile(
        base_url="http://localhost:1234/v1",
        env_key=None,
        base_url_env="LMSTUDIO_BASE_URL",
        capabilities=frozenset({Capability.STREAMING, Capability.TOOL_CALLING}),
    ),
    "custom": ProviderProfile(
        base_url=None,  # user MUST supply
        env_key=None,
        capabilities=frozenset({Capability.STREAMING, Capability.TOOL_CALLING}),
    ),
}


# User-registered providers live here. Kept separate from the built-in table
# so lookups can prefer user overrides for the same slug.
_USER_PROVIDERS: dict[str, ProviderProfile] = {}


def register_provider(
    slug: str,
    *,
    base_url: str | None,
    env_key: str | None = None,
    default_headers: dict[str, str] | None = None,
    api_style: str = "openai",
    capabilities: frozenset[Capability] | None = None,
) -> None:
    """Register (or override) a provider profile at runtime.

    After registration, `Model("<slug>/<name>")` uses these defaults. Passing
    a slug that already exists (built-in or user) replaces its profile.
    """
    slug = slug.strip().lower()
    if not slug:
        raise ValueError("Provider slug must be non-empty")
    _USER_PROVIDERS[slug] = ProviderProfile(
        base_url=base_url,
        env_key=env_key,
        default_headers=default_headers or {},
        api_style=api_style,
        capabilities=capabilities if capabilities is not None else frozenset(),
    )


def unregister_provider(slug: str) -> bool:
    """Remove a user-registered provider. Returns True if it was present.

    Built-in providers cannot be removed (only overridden).
    """
    return _USER_PROVIDERS.pop(slug.strip().lower(), None) is not None


def get_provider_profile(slug: str) -> ProviderProfile | None:
    """Look up a provider profile. User overrides win over built-ins.

    Returns None if the slug is unknown.
    """
    slug = slug.strip().lower()
    if slug in _USER_PROVIDERS:
        return _USER_PROVIDERS[slug]
    return BUILTIN_PROVIDERS.get(slug)


def list_providers() -> list[str]:
    """Return every known provider slug (built-in + user-registered), sorted."""
    return sorted(set(BUILTIN_PROVIDERS.keys()) | set(_USER_PROVIDERS.keys()))
