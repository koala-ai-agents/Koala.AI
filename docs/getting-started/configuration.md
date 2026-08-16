# Configuration

Koala has no config file. Behavior is set through:

1. **Constructor arguments** on `Agent`, `Model`, `AgentSession`, `Flow`.
2. **Environment variables** for API keys and provider selection.
3. **`ChatSettings`** for per-request LLM parameters (temperature,
   max_tokens, etc.).

## API keys

Every provider except `ollama` and `lmstudio` needs a key. Resolution
order — first non-empty value wins:

1. Explicit `api_key=` argument on `Model(...)` or `Agent(...)`.
2. Provider-specific env var (`GROQ_API_KEY`, `OPENAI_API_KEY`,
   `DEEPSEEK_API_KEY`, `XAI_API_KEY`, `TOGETHER_API_KEY`,
   `FIREWORKS_API_KEY`, `OPENROUTER_API_KEY`).
3. `LLM_API_KEY` — the **universal** fallback that works across
   providers.
4. `None` for keyless providers (Ollama, LM Studio).
5. `MissingApiKey` exception with a message that names every env var
   that would have worked.

The universal env var name is exposed as `koala.models.UNIVERSAL_ENV_KEY`.

### Setting keys

**Windows (PowerShell)**:

```powershell
$env:GROQ_API_KEY = "gsk_..."
$env:LLM_API_KEY  = "gsk_..."   # or a shared one across providers
```

**Bash**:

```bash
export GROQ_API_KEY="gsk_..."
export LLM_API_KEY="gsk_..."
```

**`.env` files**: Koala does NOT auto-load `.env`. Use `python-dotenv`
in your own entry point if you want that behavior.

## `ChatSettings` — per-request LLM parameters

```python
from koala import Model
from koala.models.settings import ChatSettings

# Set defaults on the Model
model = Model(
    "openai/gpt-4o-mini",
    temperature=0.2,
    max_tokens=256,
    top_p=0.9,
)

# Or pass a ChatSettings object
settings = ChatSettings(
    temperature=0.7,
    stop=["\n\n"],
    seed=42,
    frequency_penalty=0.1,
)
model = Model("openai/gpt-4o-mini", settings=settings)

# Override at Agent level (merges with the Model's defaults)
agent = Agent(model, settings=ChatSettings(temperature=0.0))
```

Fields: `temperature`, `max_tokens`, `top_p`, `stop`, `seed`,
`frequency_penalty`, `presence_penalty`, `extra` (raw dict passed to
the provider unchanged).

## Base URL / custom endpoints

Any OpenAI-compatible endpoint works:

```python
model = Model(
    name="my-model",
    base_url="http://localhost:8000/v1",
    api_key="sk-anything",
)
```

For providers already in the registry, you can override just the URL:

```python
model = Model(
    "openai/gpt-4o-mini",
    base_url="https://gateway.internal/openai/v1",
)
```

## Registering a custom provider

```python
from koala import register_provider
from koala.core import Capability

register_provider(
    slug="mycompany",
    base_url="https://llm.mycompany.internal/v1",
    env_key="MYCOMPANY_API_KEY",
    capabilities=frozenset({Capability.TOOLS, Capability.STREAMING}),
    default_headers={"X-Org": "koala"},
)

model = Model("mycompany/our-model")
```

See the [Providers page](providers.md) for the full built-in registry
and capability semantics.

## Observability

For distributed tracing, use OpenTelemetry — see the
[Observability guide](../guide/observability.md).
