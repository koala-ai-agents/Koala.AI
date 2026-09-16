# Models

A `Model` composes:

- A `BaseProvider` — owns HTTP and wire format.
- A specific model name — e.g. `"gpt-4o-mini"`.
- Default `ChatSettings` — temperature, max_tokens, etc.

Model instances satisfy `Runnable[list[Message], Message]`.

## Constructing

```python
from koala import Model
from koala.models.settings import ChatSettings

# Shorthand — resolves via the provider registry
m = Model("groq/llama-3.3-70b-versatile")

# Explicit endpoint — any OpenAI-compatible URL
m = Model(name="my-model", base_url="http://localhost:8000/v1", api_key="sk-x")

# Inline settings
m = Model("openai/gpt-4o-mini", temperature=0.2, max_tokens=512, top_p=0.9)

# Or via a settings object
m = Model(
    "openai/gpt-4o-mini",
    settings=ChatSettings(temperature=0.7, stop=["\n\n"], seed=42),
)
```

Constructor kwargs (all keyword-only after `ref`):

- `name`, `provider` — alternatives to the `"provider/name"` shorthand.
- `base_url`, `api_key`, `default_headers` — override registry defaults.
- `capabilities` — override the profile's capability set (rare).
- `temperature`, `max_tokens`, `top_p`, `stop`, `seed`, `frequency_penalty`,
  `presence_penalty` — inline ChatSettings values.
- `settings` — a full `ChatSettings` object.
- `extra` — raw dict passed unchanged to the provider (`response_format`,
  `tool_choice`, custom fields).
- `retry_policy` — optional `RetryPolicy` for automatic exponential backoff with full jitter and `Retry-After` compliance (defaults to active `RetryPolicy()`).
- `provider_instance` — inject your own `BaseProvider` for testing or a
  non-OpenAI wire format.

See [Providers](../getting-started/providers.md) for the registry and [Retries & resilience](resilience.md) for retry details.

## Non-streaming chat

```python
from koala.core import Message

msg = await model.chat([Message.user("Hi")])
print(msg.text)
```

Returns a single `Message`. No events emitted.

## Streaming

```python
async for event in model.stream([Message.user("Hi")]):
    match event:
        case ModelDelta(text=t):
            print(t, end="", flush=True)
        case ModelMessage(message=m):
            final = m
```

`.stream()` delegates directly to the provider's `stream_chat` — no
`Start`/`Done` bookends. Use `astream(ctx, messages)` if you want the full
`Runnable` contract (bookended, `Output` event, error handling).

## `ChatSettings` semantics

```python
@dataclass
class ChatSettings:
    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    stop: list[str] | None = None
    seed: int | None = None
    frequency_penalty: float | None = None
    presence_penalty: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)
```

`None` means "don't send this field". Providers use their own defaults.

**Merging**: settings compose. `Model.settings` is the base; per-call
`.chat(..., settings=...)` layers on top; `Agent.settings` layers again
when the model is used inside an Agent.

```python
base = ChatSettings(temperature=0.0)
override = ChatSettings(temperature=0.7, max_tokens=256)
merged = base.merge(override)   # temperature=0.7, max_tokens=256
```

## Automatic retries & backoff

Every `Model` automatically retries transient failures (HTTP 429 rate limits, 500, 502, 503, 504 server errors, connect errors, and network timeouts). Koala applies exponential backoff with full jitter and parses RFC 7231 `Retry-After` headers:

```python
from koala import Model
from koala.core import RetryPolicy

# Configure custom retry behavior
m = Model(
    "groq/llama-3.3-70b-versatile",
    retry_policy=RetryPolicy(
        max_retries=5,
        initial_delay=0.5,
        max_delay=30.0,
        jitter=True,
    ),
)
```

Both standard non-streaming requests (`model.chat`) and streaming requests (`model.stream` / `model.astream`) are protected. See the [Retries & resilience guide](resilience.md) for full details.

## Capabilities

```python
from koala.core import Capability

if Capability.STRUCTURED_OUTPUT in model.capabilities:
    # Use native response_format
    ...
```

Providers advertise a default capability set (see the
[registry table](../getting-started/providers.md)). Agents dispatch on
`STRUCTURED_OUTPUT` and `TOOL_CALLING` today. The rest are informational.

## Base-URL resolution

Order (first non-empty wins):

1. Explicit `base_url=` argument.
2. Provider's `base_url_env` env var — `OLLAMA_BASE_URL` for ollama,
   `LMSTUDIO_BASE_URL` for lmstudio, or whatever you configured on a
   custom provider via `register_provider(..., base_url_env=...)`.
3. Provider profile's static `base_url` from the registry.
4. `ValueError` if none of the above yield a URL.

Useful for Docker: set `OLLAMA_BASE_URL=http://host.docker.internal:11434/v1`
on the container and every `Model("ollama/...")` picks it up without a
code change.

## API-key resolution

Order (first non-empty wins):

1. Explicit `api_key=` argument.
2. Provider-specific env var (`OPENAI_API_KEY`, `GROQ_API_KEY`, ...).
3. Universal `LLM_API_KEY`.
4. `None` if the provider is keyless (Ollama, LM Studio).
5. `MissingApiKey` exception with a useful message.

```python
from koala.models import UNIVERSAL_ENV_KEY, resolve_api_key

resolve_api_key(
    explicit=None,
    env_key="OPENAI_API_KEY",
    provider_slug="openai",
    key_optional=False,
)
```

## Custom endpoints

Any OpenAI-compatible endpoint works out of the box:

```python
# Local vLLM
Model(name="qwen", base_url="http://localhost:8000/v1", api_key="EMPTY")

# Corporate LLM gateway
Model(
    name="prod-model",
    base_url="https://llm-gateway.internal/v1",
    api_key="...",
    default_headers={"X-Org": "koala"},
)

# Register once, use forever
from koala import register_provider
register_provider(
    slug="internal",
    base_url="https://llm-gateway.internal/v1",
    env_key="INTERNAL_LLM_KEY",
)
Model("internal/prod-model")
```

## Non-OpenAI wire formats

`UniversalProvider` speaks OpenAI wire format only. For Anthropic native
or Google Gemini native, subclass `BaseProvider` and inject:

```python
from koala.models.base import BaseProvider


class MyProvider(BaseProvider):
    async def chat(self, model, messages, settings, *, tools=None,
                   response_format=None):
        ...  # your implementation
        return message, raw_response

    async def stream_chat(self, model, messages, settings, *, tools=None,
                          response_format=None):
        ...  # yields Event

    async def close(self):
        ...


model = Model(
    name="claude-3-5-sonnet-latest",
    provider="anthropic",
    provider_instance=MyProvider(...),
)
```

## Errors

```python
from koala.models import (
    ProviderError,
    AuthenticationError,
    RateLimitError,
    BadRequestError,
    ProviderTimeoutError,
    ProviderConnectionError,
    ProviderServerError,
    MissingApiKey,
)
```

All provider errors inherit `ProviderError`. Non-2xx HTTP responses are
mapped to the appropriate subclass with the response body attached.

## Model observability

If OpenTelemetry is installed and configured, `Model.astream` emits a
`chat <model>` span per call with GenAI semantic conventions. See the
[Observability guide](observability.md).

## Reference

See [`koala.models` API reference](../reference/models.md).
