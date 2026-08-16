# Koala.AI

A Python framework for building AI agents.

One `Agent` class, one `Model` wrapper covering ten OpenAI-compatible
providers, one `@tool` decorator that derives its schema from your function
signature, and one event-stream contract that every layer speaks.

## Hello agent

```python
from koala import Agent, tool


@tool
def add(a: int, b: int) -> int:
    """Add two integers and return the sum."""
    return a + b


agent = Agent(
    "groq/llama-3.3-70b-versatile",
    instructions="You are a precise assistant. Use tools when helpful.",
    tools=[add],
)
agent.show("What is 7 plus 5?")
```

Set `LLM_API_KEY` (universal) or a provider-specific key like `GROQ_API_KEY` /
`OPENAI_API_KEY`. Local providers (`ollama`, `lmstudio`) need no key.

## What each layer does

| Layer | Import | What it does |
|---|---|---|
| Primitives | `koala.core` | `Message`, `Event`, `Runnable`, `RunContext`, `Capability`, `ApprovalRule` |
| Models | `koala.models` | `Model` + `UniversalProvider` + 10-provider registry + `LLM_API_KEY` chain |
| Tools | `koala.tools` | `@tool` decorator, Pydantic schemas, 5 approval rules |
| MCP client | `koala.tools.mcp` | `MCPToolset` over stdio / SSE / streamable HTTP |
| Memory | `koala.memory` | `InMemoryMemory`, `SQLiteMemory` — conversation history |
| Behaviors | `koala.behaviors` | `Persona`, `ToolPack`, `ApprovalPolicy`, `OutputSchema`, `ModelSettings` |
| Agents | `koala.agents` | `Agent` — tool loop, structured output, handoffs |
| Orchestration | `koala.orchestration` | `flow()` + `LocalExecutor` |
| Harness | `koala.harness` | `AgentSession` — streaming, HITL, `SQLiteCheckpointer` for durable sessions |
| UI | `koala.ui` | `show` / `ashow` — one-line terminal renderer |
| Observability | `koala.observability` | OpenTelemetry spans with GenAI semantic conventions |

## Design at a glance

```text
koala/
  core/             L1  primitives
  models/           L2  model + providers
  tools/            L3  tools + approvals + MCP client
  memory/           L4  conversation history
  behaviors/        L5  composable agent config
  agents/           L6  tool-calling loop
  orchestration/    L7  DAG flows
  harness/          L8  sessions + checkpointing
  ui/                   show / ashow renderer
  observability/        OpenTelemetry emitter
```

Every layer speaks the same event vocabulary from `core`. `Agent`, `Model`,
`Tool`, and `Flow` all satisfy the `Runnable` protocol.

## Next steps

- [Installation](getting-started/installation.md) — clone, venv, extras
- [Quickstart](getting-started/quickstart.md) — the five things you'll use
- [Architecture](concepts/architecture.md) — why the layers look this way
- [API reference](reference/core.md) — module-by-module surface
