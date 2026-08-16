# MCP tools

Model Context Protocol (MCP) is an open standard for exposing tools to
LLM agents over stdio, SSE, or streamable HTTP. Koala speaks the client
side: point a `MCPToolset` at an MCP server and its tools show up as
regular `BaseTool` instances that you can pass to any `Agent`.

Requires the `[mcp]` extra:

```bash
uv pip install -e ".[mcp]"
```

## Three transports

### Stdio

Spawns the MCP server as a child process and talks over stdin/stdout.
The pattern used by the reference filesystem, git, and sqlite servers.

```python
from koala import Agent
from koala.tools.mcp import MCPToolset


async def main():
    async with MCPToolset.stdio(
        command="npx",
        args=["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
    ) as fs_tools:
        agent = Agent(
            "openai/gpt-4o-mini",
            tools=list(fs_tools),
        )
        result = await agent.arun("List files in /tmp")
```

Args:

- `command` — executable to spawn.
- `args` — list of command-line args.
- `env` — env vars for the child (defaults to inheriting the parent's).
- `cwd` — working directory for the child.
- `include`, `exclude` — tool-name filters.

### SSE

For servers that expose an HTTP + Server-Sent-Events endpoint.

```python
async with MCPToolset.sse(
    "http://localhost:8080/sse",
    headers={"Authorization": "Bearer secret"},
) as tools:
    ...
```

### Streamable HTTP (the newer standard)

```python
async with MCPToolset.http(
    "http://localhost:8080/mcp",
    headers={"X-Api-Key": "..."},
) as tools:
    ...
```

## Lifecycle

`MCPToolset` is an async context manager. On `__aenter__` it:

1. Opens the transport (subprocess or HTTP client).
2. Constructs an `mcp.ClientSession` over the read/write streams.
3. Runs `session.initialize()` — the MCP handshake.
4. Calls `session.list_tools()` and wraps each result in an `MCPTool`.

On `__aexit__` (or `close()`), the whole stack tears down cleanly. Using
the toolset outside its `async with` block raises `RuntimeError`.

## Filtering

Servers often expose more tools than you need. `include` / `exclude`
narrow the surface handed to the agent:

```python
# Only read + list — never write or delete
async with MCPToolset.stdio(
    command="npx",
    args=["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
    include=["read_file", "list_directory"],
) as safe_fs:
    agent = Agent("openai/gpt-4o-mini", tools=list(safe_fs))
```

## What `MCPTool` looks like

Each entry is a `BaseTool` with:

- `name` — the remote tool's name.
- `description` — the remote description.
- `schema` — the remote `inputSchema`, already a JSON schema so
  `to_openai_schema()` works with no conversion.
- `run(ctx, arguments)` — forwards to `session.call_tool(name, arguments)`,
  flattens the response content to a string, and raises
  `ToolExecutionError` on `isError=True` or RPC failure.

Content-block flattening:

| MCP content type | Rendered as |
|---|---|
| `text` | concatenated verbatim |
| `image` | `[image mime=... bytes=N]` placeholder |
| `audio` | `[audio mime=... bytes=N]` placeholder |
| `resource` (embedded resource) | `[resource uri=...]` |
| unknown | `repr(block)` |

Text is lossless; binary is described but not embedded. If you need raw
bytes, reach into the underlying session:

```python
raw_result = await mcp_tool._session.call_tool(mcp_tool.name, arguments={...})
```

## Combining MCP tools with your own tools

Nothing special — they're all `BaseTool` instances:

```python
from koala import Agent, tool


@tool
def local_add(a: int, b: int) -> int:
    """Add."""
    return a + b


async def main():
    async with MCPToolset.stdio(command="mcp-server-time") as time_tools:
        agent = Agent(
            "openai/gpt-4o-mini",
            tools=[local_add, *time_tools],  # mix and match
        )
        await agent.ashow("What time is it? Also compute 7 + 5.")
```

## Approval on MCP tools

MCP tools go through the same approval chain as any other tool. Their
`name` is what the rule sees:

```python
from koala.tools import DenyList, RequireApprovalFor

rules = [
    DenyList(frozenset({"write_file", "delete_file"})),
    RequireApprovalFor(prefixes=frozenset({"execute_"})),
]

async with MCPToolset.stdio(command="...") as tools:
    agent = Agent("...", tools=list(tools), approval_rules=rules)
```

See [Approval + HITL](approval-hitl.md).

## Long-lived agents

The toolset holds the transport open. If you need an agent that survives
across many requests, keep the `async with` block open for the whole
agent lifetime:

```python
async def app():
    async with MCPToolset.stdio(...) as tools:
        agent = Agent("...", tools=list(tools))
        for request in incoming():
            await agent.arun(request)
```

Killing the toolset invalidates the wrapped tools — you cannot reuse
them after `close()`.

## Reference

- `koala.tools.mcp.MCPTool` — the `BaseTool` wrapper.
- `koala.tools.mcp.MCPToolset` — connection manager.
- `koala.tools.mcp.list_mcp_tools` — one-shot listing (doesn't hold the
  connection).

See the [API reference for `koala.tools`](../reference/tools.md).
