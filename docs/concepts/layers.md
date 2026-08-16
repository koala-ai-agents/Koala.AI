# The layer stack

What each layer actually contains. See the [API reference](../reference/core.md)
for full signatures — this is the map.

## L1 — `koala.core`

Pure types and Protocols. Zero runtime deps beyond stdlib + `pydantic`
(for `Message` content blocks).

- **Messages**: `Message`, `Role` (`"system" | "user" | "assistant" | "tool"`),
  and content blocks: `TextBlock`, `ThinkingBlock`, `ImageBlock`, `ToolCallBlock`,
  `ToolResultBlock`.
- **Events**: `Event` union with `Start`, `ModelDelta`, `ThinkingDelta`,
  `ModelMessage`, `ToolCall`, `ToolResult`, `AwaitingApproval`, `UsageEvent`,
  `Output`, `Error`, `Done`.
- **Runnable + Channel**: `Runnable[I, O]` and `Channel[I, O]` Protocols,
  plus consumer helpers `ainvoke`, `invoke`, `acollect`.
- **Context**: `RunContext[DepsT]` (deps, usage, session_id, cancel token,
  metadata, approval_resolver), `CancelToken`.
- **Types**: `Usage`, `ModelRef`, `parse_model_ref`.
- **Capabilities**: `Capability` StrEnum with 13 flags.
- **Approval**: `ApprovalRule` Protocol, `ApprovalDecision` literal
  (`"allow" | "deny" | "ask"`), `ApprovalResult` dataclass.

## L2 — `koala.models`

One-shot chat completion, streaming or not.

- **`Model`** — user-facing wrapper. Constructor accepts `"provider/name"`
  shorthand, explicit `base_url`, and inline `ChatSettings` fields.
  Implements `Runnable[list[Message], Message]`.
- **`BaseProvider`** (ABC) — abstract wire-format layer. Two required
  async methods: `chat(...)` (non-streaming) and `stream_chat(...)`
  (async iterator of Events).
- **`UniversalProvider`** — concrete OpenAI-compatible provider using
  `httpx`. Handles tool calling, streaming, structured output, and
  usage extraction.
- **Registry**: `BUILTIN_PROVIDERS` (10 profiles), `register_provider`,
  `unregister_provider`, `get_provider_profile`, `list_providers`.
- **Settings**: `ChatSettings` dataclass — temperature, max_tokens, top_p,
  stop, seed, penalties, and a raw `extra` dict.
- **Keys**: `UNIVERSAL_ENV_KEY = "LLM_API_KEY"`, `resolve_api_key(...)`.
- **Errors**: `ProviderError`, `AuthenticationError`, `RateLimitError`,
  `BadRequestError`, `ProviderTimeoutError`, `ProviderConnectionError`,
  `ProviderServerError`, `MissingApiKey`.

## L3 — `koala.tools`

- **`BaseTool`** (ABC) — sets `name`, `description`, `schema`; implements
  `run(ctx, arguments)` and derives `astream` for free.
- **`FunctionTool`** + **`@tool`** decorator — wrap a Python function.
  Schema is derived from signature + Google docstring via `pydantic`.
  Three call forms: `@tool`, `@tool(name="...")`, or plain
  `FunctionTool(fn)`.
- **Schema utilities**: `ToolSpec`, `build_tool_spec`, `parse_google_docstring`.
- **Approval rules**: `DenyList`, `AllowList`, `AlwaysAsk`, `AlwaysAllow`,
  `RequireApprovalFor`, plus the chain runner `evaluate_approval_chain`.
- **Errors**: `ToolError`, `ToolValidationError`, `ToolExecutionError`,
  `ToolNotFoundError`.
- **MCP client** (`koala.tools.mcp`, requires the `[mcp]` extra):
  `MCPTool`, `MCPToolset` with `.stdio()`, `.sse()`, `.http()`
  constructors.

## L4 — `koala.memory`

Conversation-history persistence keyed by session id.

- **`BaseMemory`** (ABC) — `append(session_id, messages)`, `get(session_id)`,
  `clear(session_id)`, `sessions()`.
- **`InMemoryMemory`** — dict-backed.
- **`SQLiteMemory`** — stdlib `sqlite3` wrapped in `asyncio.to_thread`.
  All content-block variants (Text/Thinking/Image/ToolCall/ToolResult)
  round-trip losslessly.

No vector store, no RAG helpers. Bring your own.

## L5 — `koala.behaviors`

Composable agent configuration. Each `Behavior` gets one shot at
mutating an `AgentSpec` before the agent is finalized.

- **`Behavior`** Protocol with `apply(spec: AgentSpec) -> None`.
- **`AgentSpec`** dataclass — mutable builder holding
  `instructions_parts`, `tools`, `approval_rules`, `settings`, `output_type`.
- Built-ins: `Persona`, `ToolPack`, `ApprovalPolicy`, `OutputSchema`,
  `ModelSettings`.

## L6 — `koala.agents`

- **`Agent`** — the standard tool-calling loop with structured output,
  handoffs, memory, and behaviors. Implements
  `Runnable[str | list[Message], Any]`. Sync `run()`, async `arun()`,
  async iterator `astream()`, session factory `session()`, and
  UI helpers `show()` / `ashow()`.
- **`BaseAgent`** (ABC) — subclass this only if you want to swap out
  the whole tool-calling loop.
- **`AgentTool`** — Rig-style "Agent-as-Tool" wrapper used to implement
  handoffs (`Agent(handoffs=[other])`).
- **`RunResult`** dataclass — `output`, `messages`, `usage`, `iterations`,
  `stop_reason`, `error`, `metadata`.
- **Output helpers**: `build_response_format`, `build_prompt_schema_hint`,
  `parse_output`.
- **Errors**: `AgentError`, `MaxIterationsError`, `HandoffError`,
  `OutputParseError`.

## L7 — `koala.orchestration`

DAG flows over any `Runnable`.

- **`flow(id)`** — fluent builder entry point.
- **`Flow`**, **`Step`**, **`FlowBuilder`** — declarative types.
- **`StepAction`** — union: `str | Runnable | Callable`.
- **`LocalExecutor`** — in-process async executor with concurrent
  independent-branch execution and `$input.<key>` / `$result.<step_id>`
  reference substitution.
- **Errors**: `FlowError`, `StepExecutionError`.

## L8 — `koala.harness`

- **`AgentSession`** — implements `Channel[str | list[Message], Event]`.
  Bidirectional, streaming, multi-turn, HITL-capable. Background worker
  task processes queued inputs one turn at a time.
- **`Checkpointer`** Protocol + `InMemoryCheckpointer` + `SQLiteCheckpointer`.
- **`Checkpoint`**, **`PendingApproval`** dataclasses.
- `AgentSession.resume(agent, session_id, checkpointer)` async classmethod.
- **Errors**: `SessionError`, `SessionClosedError`, `ResolverTimeoutError`.

## UI — `koala.ui`

- **`show`**, **`ashow`** — one function that renders any Koala thing to
  stdout. Auto-detects Agent, Model, Tool, RunResult, async event iterator,
  Message, RunContext callable. Reasoning ("thinking") tokens get the
  `[reason]` prefix, tool calls get `[tool]`, errors get `[error]`.

## Observability — `koala.observability`

- **`is_available()`**, **`model_span(...)`**, **`agent_span(...)`**,
  **`tool_span(...)`** — context managers that emit OpenTelemetry spans
  with GenAI semantic conventions (`gen_ai.system`, `gen_ai.request.model`,
  `gen_ai.usage.input_tokens`, etc.). Degrades to no-op when OTel isn't
  installed.


