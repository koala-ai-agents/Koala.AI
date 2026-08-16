# Quickstart

Five things you'll actually use.

## 1. `Model` — one class, ten providers

```python
from koala import Model

# provider/name shorthand — resolves base_url + env var from the registry
m = Model("groq/llama-3.3-70b-versatile")
m = Model("openai/gpt-4o-mini")
m = Model("deepseek/deepseek-chat")
m = Model("ollama/qwen2.5:7b")          # no key needed

# Explicit endpoint (any OpenAI-compatible URL)
m = Model(
    name="my-model",
    base_url="http://localhost:8000/v1",
    api_key="...",
)
```

Built-in providers: `openai`, `groq`, `deepseek`, `xai`, `together`,
`fireworks`, `openrouter`, `ollama`, `lmstudio`, `custom`. Register more
at runtime with `koala.register_provider(...)` — see [Providers](providers.md).

## 2. `@tool` — a Python function is a tool

```python
from koala import tool


@tool
def get_weather(city: str, units: str = "celsius") -> str:
    """Return the current weather for a city.

    Args:
        city: City name, e.g. "Tokyo".
        units: "celsius" or "fahrenheit".
    """
    return f"18 {units} and clear in {city}"
```

Koala derives the JSON schema from your signature and Google-style
docstring. `RunContext` deps can be injected by adding a parameter typed
as `RunContext`. See the [Tools guide](../guide/tools.md).

## 3. `Agent` — tool loop, structured output, handoffs

```python
from pydantic import BaseModel
from koala import Agent


class Weather(BaseModel):
    city: str
    temp_c: float
    condition: str


agent = Agent(
    "openai/gpt-4o-mini",
    instructions="Reply with structured weather data.",
    tools=[get_weather],
    output_type=Weather,           # validated Pydantic output
    max_iterations=10,
)

result = agent.run("What's the weather in Tokyo?")
print(result.output.temp_c)        # 18.0
print(result.usage.total_tokens)   # cumulative usage across the run
```

## 4. `AgentSession` — streaming + human-in-the-loop

```python
import asyncio
from koala import Agent
from koala.core import AwaitingApproval, Done, ModelDelta
from koala.tools import RequireApprovalFor


agent = Agent(
    "openai/gpt-4o-mini",
    tools=[charge_card],
    approval_rules=[RequireApprovalFor(names=frozenset({"charge_card"}))],
)


async def main() -> None:
    async with agent.session() as s:
        await s.send("Buy me a coffee.")
        async for event in s.events():
            match event:
                case ModelDelta(text=t):
                    print(t, end="", flush=True)
                case AwaitingApproval(call=c, request_id=r):
                    ok = input(f"\nApprove {c.name}? y/n ")
                    await s.reply_approval(r, "allow" if ok == "y" else "deny")
                case Done():
                    break


asyncio.run(main())
```

Approval defaults to deny after `approval_timeout` — the resolver raises
`ResolverTimeoutError` so the deny reason on the ToolResult is informative.

## 5. `flow(...)` — DAG orchestration

```python
from koala import Agent, tool
from koala.orchestration import LocalExecutor, flow


@tool
def word_count(text: str) -> int:
    """Count words."""
    return len(text.split())


researcher = Agent("groq/llama-3.3-70b-versatile", name="researcher")
writer = Agent("groq/llama-3.3-70b-versatile", name="writer")

pipeline = (
    flow("research-and-write")
    .step("research", researcher, input="$input.topic")
    .step("tweet", writer, input="$result.research")
    .step("length", word_count, text="$result.tweet")
    .edge("research", "tweet")
    .edge("tweet", "length")
    .build()
)
print(LocalExecutor().run(pipeline, input={"topic": "koalas"}))
```

Steps can be Agents, Tools, Models, plain callables, or anything that
satisfies the L1 `Runnable` protocol. `$input.<key>` and
`$result.<step_id>` references are substituted at run time. Independent
branches run concurrently.



## Next steps

- The [Agents guide](../guide/agents.md) unpacks the tool-calling loop
- The [Sessions guide](../guide/sessions.md) covers streaming + HITL
- The [MCP guide](../guide/mcp.md) hooks in remote MCP tool servers
- The [OpenTelemetry guide](../guide/observability.md) exports spans with
  GenAI semantic conventions
