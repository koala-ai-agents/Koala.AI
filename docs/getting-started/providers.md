# Providers

Every `Model` is backed by a **provider profile** — a small dataclass with
the default base URL, env-var name, and default capability set for one
provider. Koala ships ten built-in profiles and lets you register more at
runtime.

## Built-in registry

| Slug | Base URL | Env var | Capabilities |
|---|---|---|---|
| `openai` | `https://api.openai.com/v1` | `OPENAI_API_KEY` | streaming, tool_calling, parallel_tool_calls, structured_output, json_mode, long_context, image_input |
| `groq` | `https://api.groq.com/openai/v1` | `GROQ_API_KEY` | streaming, tool_calling, parallel_tool_calls, structured_output, json_mode, image_input |
| `deepseek` | `https://api.deepseek.com/v1` | `DEEPSEEK_API_KEY` | streaming, tool_calling, json_mode, thinking |
| `xai` | `https://api.x.ai/v1` | `XAI_API_KEY` | full standard set |
| `together` | `https://api.together.xyz/v1` | `TOGETHER_API_KEY` | streaming, tool_calling, json_mode |
| `fireworks` | `https://api.fireworks.ai/inference/v1` | `FIREWORKS_API_KEY` | streaming, tool_calling, json_mode |
| `openrouter` | `https://openrouter.ai/api/v1` | `OPENROUTER_API_KEY` | streaming, tool_calling, json_mode (adds `HTTP-Referer` + `X-Title` headers) |
| `ollama` | `http://localhost:11434/v1` (or `$OLLAMA_BASE_URL`) | *keyless* | streaming, tool_calling, json_mode |
| `lmstudio` | `http://localhost:1234/v1` (or `$LMSTUDIO_BASE_URL`) | *keyless* | streaming, tool_calling |
| `custom` | *user must supply* | *user may supply* | streaming, tool_calling |

## Capability flags

Defined in `koala.core.capabilities.Capability` (a `StrEnum`):

```python
class Capability(StrEnum):
    STREAMING           = "streaming"
    TOOL_CALLING        = "tool_calling"
    PARALLEL_TOOL_CALLS = "parallel_tool_calls"
    STRUCTURED_OUTPUT   = "structured_output"
    JSON_MODE           = "json_mode"
    LONG_CONTEXT        = "long_context"
    IMAGE_INPUT         = "image_input"
    IMAGE_OUTPUT        = "image_output"
    AUDIO_INPUT         = "audio_input"
    AUDIO_OUTPUT        = "audio_output"
    EMBEDDING           = "embedding"
    RERANKING           = "reranking"
    THINKING            = "thinking"
```

Currently the framework dispatches on `TOOL_CALLING` (to enable tool
schemas) and `STRUCTURED_OUTPUT` (to prefer native `response_format` over
prompt-engineered JSON hints). The rest are informational and used by
your own code when planning.

## Env-driven base URLs

Local providers (`ollama`, `lmstudio`) declare a `base_url_env` field so
you can point them at a different host without touching code. Set
`OLLAMA_BASE_URL=http://host.docker.internal:11434/v1` in your Airflow
container and every `Model("ollama/...")` inside picks it up automatically.

Resolution order for `base_url`:

1. Explicit `base_url=` on the `Model(...)` call.
2. Provider's `base_url_env` env var (if set + non-empty).
3. Provider's static `base_url` default from the registry.
4. `ValueError` if none of the above yield a URL.

## Registering a provider

```python
from koala import register_provider
from koala.core import Capability

register_provider(
    slug="mycompany",
    base_url="https://llm.mycompany.internal/v1",
    env_key="MYCOMPANY_API_KEY",
    default_headers={"X-Org": "koala"},
    capabilities=frozenset({
        Capability.STREAMING,
        Capability.TOOL_CALLING,
    }),
)

# Now provider/name shorthand works.
model = Model("mycompany/our-model")
```

To make your custom provider's base URL env-overridable, add
`base_url_env="MYCOMPANY_BASE_URL"` when registering.

Passing an existing slug **replaces** the profile — useful for redirecting
`openai` to an internal gateway. Built-in providers cannot be removed,
only overridden.

```python
from koala.models import unregister_provider

unregister_provider("mycompany")   # returns True if it was present
```

## Introspection

```python
from koala.models import (
    BUILTIN_PROVIDERS,
    get_provider_profile,
    list_providers,
)

print(list_providers())            # every slug, sorted
print(get_provider_profile("openai").base_url)
print(BUILTIN_PROVIDERS["ollama"].env_key is None)   # True — keyless
```

## Adding a non-OpenAI-compatible provider

The current `UniversalProvider` speaks OpenAI wire format only. If you
need to talk to a provider with a different wire shape (Anthropic native,
Google Gemini native), subclass `BaseProvider` and provide your own
`chat` / `stream_chat` implementation, then hand your instance in via
`Model(..., provider_instance=my_provider)`.

See `koala.models.base.BaseProvider` in the [API reference](../reference/models.md).
