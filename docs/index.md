# <span class="gradient-text">Koala.AI</span>

<p style="font-size: 1.25rem; font-weight: 500; color: var(--md-default-fg-color--light); margin-top: -0.5rem; margin-bottom: 1.5rem;">
The modular, event-driven Python framework for production-grade AI agents.
</p>

<div>
  <span class="badge badge-green">Python 3.10+</span>
  <span class="badge badge-blue">10+ Built-in Providers</span>
  <span class="badge badge-purple">Zero Callback Spaghetti</span>
  <span class="badge">OpenTelemetry Native</span>
</div>

One unified `Agent` class, one universal `Model` gateway covering 10+ providers with automatic jittered backoff, one `@tool` decorator that derives schemas from Python type hints, and an immutable event-stream contract that every layer speaks.

---

<div class="grid-cards">
  <div class="feature-card">
    <h3>⚡ Bounded Parallel Tools</h3>
    <p>Executes multi-tool model turns concurrently via <code>asyncio.gather</code> with semaphore concurrency controls, strictly preserving event order and response pairing.</p>
  </div>
  <div class="feature-card">
    <h3>🔄 Automatic Retries & Resilience</h3>
    <p>Transparent exponential backoff with full jitter and RFC 7231 <code>Retry-After</code> parsing protects against 429 rate limits, server drops, and network timeouts.</p>
  </div>
  <div class="feature-card">
    <h3>🧠 Atomic Context Pruning</h3>
    <p><code>ContextPolicy</code> compacts history into atomic turns, preventing orphaned tool call errors that break OpenAI, Anthropic, and Groq APIs.</p>
  </div>
  <div class="feature-card">
    <h3>🎯 Self-Correction Reflection</h3>
    <p>Automated reflection loop intercepts schema validation failures, feeds structured error prompts back to the LLM, and enables self-correction without crashes.</p>
  </div>
</div>

---

## Hello Agent

```python
from koala import Agent, tool


@tool
def add(a: int, b: int) -> int:
    """Add two integers and return the sum."""
    return a + b


# Universal provider shorthand: resolves base_url and API key from env
agent = Agent(
    "groq/llama-3.3-70b-versatile",
    instructions="You are a precise assistant. Use tools when helpful.",
    tools=[add],
)

# Stream live tokens and tool events directly to the console
agent.show("What is 7 plus 5?")
```

Set `LLM_API_KEY` (universal fallback) or provider-specific keys like `GROQ_API_KEY` / `OPENAI_API_KEY`. Local providers (`ollama`, `lmstudio`) work out of the box with no key required.

---

## The Layer Stack

Koala is structured as strict unidirectionally dependent layers. Every layer is independent and reusable in isolation:

| Layer | Module | Key Primitives | Responsibility |
|---|---|---|---|
| **L1 Primitives** | `koala.core` | `Message`, `Event`, `Runnable`, `RunContext`, `ModelRetry`, `RetryPolicy` | Pure types, dataclasses, and standard event streaming contract. Zero non-stdlib deps. |
| **L2 Models** | `koala.models` | `Model`, `UniversalProvider`, `ChatSettings`, `registry` | Universal HTTP client with automatic jittered retry and multi-provider registry. |
| **L3 Tools** | `koala.tools` | `@tool`, `BaseTool`, approval chains, `MCPToolset` | Signature inspection, JSON schema derivation, and human approval rules. |
| **L4 Memory** | `koala.memory` | `InMemoryMemory`, `SQLiteMemory` | Conversation history persistence and multi-turn state recall. |
| **L5 Behaviors** | `koala.behaviors` | `Persona`, `ToolPack`, `ApprovalPolicy`, `OutputSchema` | Composable, reusable agent personality and constraint bundles. |
| **L6 Agents** | `koala.agents` | `Agent`, `BaseAgent`, `ContextPolicy`, `AgentTool` | Tool-calling loop, parallel execution, atomic pruning, and reflection. |
| **L7 Orchestration** | `koala.orchestration` | `flow()`, `LocalExecutor` | DAG-based task orchestration and fan-out/fan-in execution. |
| **L8 Harness** | `koala.harness` | `AgentSession`, `SQLiteCheckpointer` | Bidirectional channels, human-in-the-loop interaction, and durable checkpoints. |
| **Observability** | `koala.observability` | `agent_span`, `model_span`, `tool_span` | Native OpenTelemetry GenAI semantic conventions (degrades to no-ops if SDK absent). |
| **UI** | `koala.ui` | `show()`, `ashow()` | One-line terminal streaming renderer for any component or event iterator. |

---

## Architectural Principles

1. **`Runnable` is a Protocol, not an Inheritance Trap**: Any class implementing `astream(ctx, input) -> AsyncIterator[Event]` satisfies `Runnable`. Models, Tools, Agents, and Flows compose seamlessly.
2. **Events Are Ground Truth**: No opaque callbacks or hidden global variables. Everything that happens (token deltas, reasoning blocks, tool invocations, approval requests) is an immutable `Event` in a stream.
3. **Fail-Safe Self-Correction**: Agents recover gracefully from API glitches, rate limits, schema mismatches, and domain validation failures through built-in transport retries, `ModelRetry`, and reflection loops.

---

## Getting Started

- [Installation](getting-started/installation.md) — Quick install via pip, uv, or git
- [Quickstart](getting-started/quickstart.md) — The five patterns you will use daily
- [Architecture](concepts/architecture.md) — How the layered architecture works
- [Context Management & Pruning](guide/context-policy.md) — Prevent context window overflow safely
- [Retries & Resilience](guide/resilience.md) — Transport retries and reflection loops
- [API Reference](reference/core.md) — Detailed class and function reference
