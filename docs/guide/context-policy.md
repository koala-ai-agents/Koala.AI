# Context Management & Pruning

As agents run multi-step tool loops or long multi-turn sessions, conversation history accumulates tokens rapidly. Left unmanaged, requests soon exceed model context limits or dramatically increase latency and token costs.

Koala provides `ContextPolicy` to manage context windows deterministically without breaking API contracts.

---

## The "Orphaned Tool Call" Problem

Most LLM providers (OpenAI, Anthropic, Groq, DeepSeek) enforce strict structural validation on chat messages:

1. Every `tool_calls` block from an assistant message **must** be immediately followed by corresponding `role: "tool"` messages with matching `tool_call_id`s.
2. An isolated `role: "tool"` message without a preceding assistant call causes an immediate **HTTP 400 Bad Request**.
3. Naive sliding-window truncation (e.g. `messages[-10:]`) cuts arbitrarily through tool call sequences, corrupting conversation history.

```text
❌ Naive Truncation:
... [Assistant: call tool_1, tool_2] (TRUNCATED OUT)
    [ToolResult: result 1]           <- 400 Bad Request: missing tool_calls!
    [ToolResult: result 2]
    [User: "Now what?"]

✔ Koala Atomic Turn Compaction:
    [System prompt]                  <- Always preserved
    ... (older turns pruned cleanly as whole atomic units) ...
    [Assistant: call tool_1, tool_2]
    [ToolResult: result 1]           <- Kept together as an unbreakable atomic turn
    [ToolResult: result 2]
    [User: "Now what?"]
```

`ContextPolicy` solves this by grouping assistant calls and their tool results into **atomic turns**. An atomic turn is either kept in its entirety or cleanly pruned in its entirety.

---

## Quickstart

```python
from koala import Agent
from koala.agents import ContextPolicy

# Prune context to 8,000 tokens while keeping the system prompt and latest turns intact
policy = ContextPolicy(
    max_tokens=8000,
    keep_last_turns=4,              # Always preserve at least the last 4 complete turns
    max_tool_output_tokens=500,     # Truncate individual massive tool results (e.g. large JSON/HTML)
    token_estimator="fast",         # 4 chars/token heuristic (or "tiktoken")
)

agent = Agent(
    "openai/gpt-4o-mini",
    instructions="You are an autonomous research agent.",
    tools=[search_web, scrape_page],
    context_policy=policy,
)
```

---

## Configuration Options

| Option | Type | Default | Description |
|---|---|---|---|
| `max_tokens` | `int \| None` | `None` | Maximum cumulative token budget for the context window. |
| `keep_last_turns` | `int` | `2` | Minimum number of recent complete conversation turns to unconditionally retain. |
| `preserve_system` | `bool` | `True` | Whether the initial system prompt is permanently pinned and never pruned. |
| `max_tool_output_tokens` | `int \| None` | `None` | Cap on individual tool result token lengths. |
| `max_tool_output_chars` | `int \| None` | `None` | Cap on individual tool result character lengths. |
| `token_estimator` | `str \| Callable` | `"fast"` | Token counter: `"fast"` (4 chars/token heuristic), `"tiktoken"`, or a custom function `(Message) -> int`. |

---

## How Turn Grouping Works

`ContextPolicy.group_turns()` segments message history into structured turn records:

- **System Turn**: The initial system message (if present).
- **User Turn**: A single user message initiating an interaction.
- **Assistant Simple Turn**: An assistant message with text only (final reply).
- **Assistant Tool Turn**: An assistant message containing `tool_calls` plus all immediate subsequent `role: "tool"` result messages.

When the total estimated tokens exceed `max_tokens`:

1. System messages are locked in place.
2. The `keep_last_turns` most recent turns are locked in place.
3. The oldest intermediate turns are dropped one full atomic turn at a time until the conversation fits within `max_tokens`.

---

## Tool Result Truncation

When tools return large outputs (e.g. database dumps, web page contents, or API responses), a single tool result can consume thousands of tokens.

`ContextPolicy` can truncate oversized tool outputs inline before pruning:

```python
policy = ContextPolicy(
    max_tokens=16000,
    max_tool_output_tokens=1000,   # Caps individual tool results at ~1,000 tokens
)
```

When truncated, Koala replaces the trailing portion with a clear notice:
`"... [truncated: output exceeded 1000 tokens]"` so the model understands that the payload was intentionally bounded.

---

## Token Estimation Strategies

### Fast Heuristic (`"fast"`)
Uses standard 4 characters per token estimation (`len(text) // 4`). Extremely fast, zero external dependencies, and suitable for high-throughput loops.

### Precise Tiktoken (`"tiktoken"`)
Uses OpenAI's `tiktoken` tokenizer (`cl100k_base` / `o200k_base`) when installed:

```bash
pip install tiktoken
```

```python
policy = ContextPolicy(
    max_tokens=8192,
    token_estimator="tiktoken",
)
```

### Custom Estimator Function
Provide your own tokenizer or model-specific counter:

```python
def my_estimator(msg: Message) -> int:
    # Custom token counting logic
    return len(msg.text.split())

policy = ContextPolicy(
    max_tokens=4000,
    token_estimator=my_estimator,
)
```
