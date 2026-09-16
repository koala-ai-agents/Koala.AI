# Tools

A tool is a callable capability the model can invoke. In Koala, three
things matter:

1. Every tool is a `BaseTool` — a small abstract interface.
2. `@tool` derives the JSON schema from your function signature and
   Google-style docstring, so you don't hand-write it.
3. `RunContext` can be injected — reach into your DB pool, config, or
   any user-provided deps.

## The `@tool` decorator

Three forms, all valid:

```python
from koala import tool

# 1. Bare — name comes from the function
@tool
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b

# 2. With a public name
@tool("public_name")
def internal_name(x: int) -> int:
    """Foo."""
    return x * 2

# 3. With explicit metadata
@tool(name="do_things", description="Override docstring description")
async def custom(x: int) -> int:
    return x + 1
```

Async functions are supported natively. Sync functions are executed via
`asyncio.to_thread` so they don't block the event loop.

## Schema derivation

Koala reads your function signature and docstring to build a JSON schema
that the LLM can see. Types map straight through:

| Python annotation | JSON schema |
|---|---|
| `int` | `{"type": "integer"}` |
| `float` | `{"type": "number"}` |
| `str` | `{"type": "string"}` |
| `bool` | `{"type": "boolean"}` |
| `list[T]` | `{"type": "array", "items": ...}` |
| `dict[str, T]` | `{"type": "object", ...}` |
| `Literal["a", "b"]` | `{"enum": ["a", "b"]}` |
| `Optional[T]` / `T | None` | schema for `T` with `nullable` semantics |
| Pydantic `BaseModel` | Its `model_json_schema()` |

Google-style docstrings feed parameter descriptions:

```python
@tool
def get_weather(city: str, units: str = "celsius") -> str:
    """Return the current weather for a city.

    Args:
        city: City name, e.g. "Tokyo".
        units: "celsius" or "fahrenheit". Defaults to celsius.

    Returns:
        Free-text weather description.
    """
    ...
```

The `Args:` block descriptions become the JSON schema's per-property
descriptions. The docstring summary (the first line) becomes the tool's
top-level `description`.

## Inspecting the derived schema

```python
add.name           # "add"
add.description    # "Add two integers."
add.schema         # dict: {"type": "object", "properties": {...}, "required": [...]}
add.to_openai_schema()  # full OpenAI function-calling wire shape
```

## Injecting `RunContext`

Add a parameter annotated `RunContext` (or `RunContext[YourDeps]`) and
Koala will pass the current run context there — invisible to the model:

```python
from koala import tool
from koala.core import RunContext


@tool
async def fetch_user(ctx: RunContext, user_id: str) -> dict:
    """Fetch a user by id.

    Args:
        user_id: The user's id.
    """
    return await ctx.deps.db.fetch_user(user_id)
```

The model sees only `user_id` in the schema. Any `pool`, `client`, or
config on `ctx.deps` is available to the function.

## Error handling & `ModelRetry`

Three exceptions get special treatment inside the Agent's tool loop:

- **`ModelRetry`** (`from koala.core import ModelRetry`) — raised intentionally by your tool code when argument values fail business logic (e.g., record not found, ambiguous query). The agent converts this into a direct instruction for the model: `"Tool requested retry: <message>"`, allowing the LLM to correct its input parameters in the next turn.
- **`ToolValidationError`** — arguments failed Pydantic schema validation. The agent appends a `ToolResult` with `is_error=True` and lets the model retry with corrected arguments.
- **`ToolExecutionError`** — an unexpected exception was raised inside the tool function. Wrapped automatically with `is_error=True`, preserving the original exception in `.original`.

```python
from koala import tool
from koala.core import ModelRetry


@tool
def lookup_customer(customer_id: str) -> dict:
    """Look up customer details by their 6-digit ID."""
    if not customer_id.isdigit() or len(customer_id) != 6:
        # Prompt the model to retry with a formatted error
        raise ModelRetry(
            f"'{customer_id}' is invalid. Customer IDs must be exactly 6 digits (e.g. 104829)."
        )
    ...
```

## Subclassing `BaseTool`

`@tool` covers 95% of cases. If you need a stateful tool, or a tool
backed by something other than a Python function, subclass:

```python
from koala.tools import BaseTool
from koala.core import RunContext


class SearchTool(BaseTool):
    name = "search"
    description = "Full-text search over the local index."
    schema = {
        "type": "object",
        "properties": {"query": {"type": "string"}},
        "required": ["query"],
    }

    def __init__(self, index):
        self.index = index

    async def run(self, ctx: RunContext, arguments: dict) -> str:
        return self.index.search(arguments["query"])
```

Because `BaseTool.astream` is derived from `run`, a `BaseTool` satisfies
the `Runnable` protocol for free — you can hand it directly to a `Flow`
step or a `show()` call.

## Approval

Every tool call passes through the approval chain if the agent has
`approval_rules=[...]`. See the [Approval + HITL](approval-hitl.md)
guide for the built-in rules.

## MCP tools

Tools hosted on an MCP server can be pulled in as regular `BaseTool`
instances via `koala.tools.mcp.MCPToolset` — see the [MCP guide](mcp.md).

## Reference

See the [API reference for `koala.tools`](../reference/tools.md) for
signatures.
