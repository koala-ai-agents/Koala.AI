"""MCP (Model Context Protocol) tool integration.

Bring tools hosted on an MCP server into Koala as regular ``BaseTool``
instances. The bridge is transparent — once ``MCPToolset`` has connected,
the tools it exposes behave exactly like ``@tool``-decorated Python
functions: same schema shape, same approval-chain semantics, same event
stream.

Requires the optional ``mcp`` extra::

    pip install koala[mcp]

Example — stdio transport::

    from koala import Agent
    from koala.tools.mcp import MCPToolset

    async with MCPToolset.stdio(
        command="npx",
        args=["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
    ) as fs_tools:
        agent = Agent("openai/gpt-4o-mini", tools=list(fs_tools))
        result = await agent.arun("List files in /tmp")

Example — SSE transport::

    async with MCPToolset.sse("http://localhost:8080/sse") as ts:
        ...

Example — Streamable HTTP transport::

    async with MCPToolset.http("http://localhost:8080/mcp") as ts:
        ...

Design
------
* ``MCPToolset`` is an async context manager. On enter it opens the
  transport, creates an ``mcp.ClientSession``, initialises the MCP
  handshake, calls ``list_tools()``, and materialises one ``MCPTool``
  per remote tool. On exit it tears the whole stack down.
* ``MCPTool`` is a thin ``BaseTool`` subclass that forwards ``run()`` to
  ``session.call_tool(...)`` and flattens the returned content blocks
  into a single string the LLM can consume.
* No polling / no reconnection logic. If the underlying transport dies,
  subsequent tool calls raise. Callers using ``MCPToolset`` in a long-lived
  agent are expected to wrap it in their own supervision policy.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from ..core.context import RunContext
from .base import BaseTool
from .errors import ToolExecutionError

if TYPE_CHECKING:
    from mcp import ClientSession


class MCPTool(BaseTool):
    """A single remote tool exposed by an MCP server.

    Constructed by ``MCPToolset`` after ``list_tools()`` — users rarely build
    these directly.

    Attributes:
        name: The remote tool's name.
        description: The remote tool's description (empty string if absent).
        schema: The remote tool's ``inputSchema`` — already a JSON Schema
            object, so ``to_openai_schema()`` works with no conversion.
    """

    def __init__(
        self,
        *,
        session: "ClientSession",
        name: str,
        description: str,
        schema: dict[str, Any],
    ) -> None:
        self._session = session
        self.name = name
        self.description = description
        self.schema = schema

    async def run(
        self, ctx: RunContext, arguments: dict[str, Any]
    ) -> Any:
        """Invoke the remote MCP tool and flatten the response to a string.

        Args:
            ctx: Ignored — the MCP session is owned by the toolset.
            arguments: JSON-encodable dict matching ``self.schema``.

        Raises:
            ToolExecutionError: If the server returns ``isError=True`` or the
                RPC itself raises.
        """
        try:
            result = await self._session.call_tool(self.name, arguments=arguments)
        except Exception as e:  # noqa: BLE001
            raise ToolExecutionError(tool_name=self.name, original=e) from e

        text = _flatten_content(result.content)

        if getattr(result, "isError", False):
            # Preserve the server's error text as the original message so
            # the Agent's loop can show it to the model.
            raise ToolExecutionError(
                tool_name=self.name,
                original=RuntimeError(text or "MCP tool returned isError=True"),
            )

        return text


class MCPToolset:
    """A live connection to an MCP server, exposing its tools as ``BaseTool``s.

    Use one of the transport classmethods to construct — ``stdio``, ``sse``,
    or ``http`` — and enter the resulting object as an async context manager.

        async with MCPToolset.stdio(command="mcp-server-git") as ts:
            for tool in ts:
                print(tool.name)

    After entry, ``list(toolset)`` gives you the ``MCPTool`` instances you
    can hand to ``Agent(tools=...)``.
    """

    def __init__(
        self,
        *,
        transport_factory: Any,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] | None = None,
    ) -> None:
        """Not for direct use — see the transport classmethods.

        Args:
            transport_factory: A zero-arg callable that returns an async
                context manager yielding a ``(read, write)`` stream pair
                compatible with ``mcp.ClientSession``. This indirection lets
                the same class handle stdio / SSE / streamable-http without
                importing them eagerly.
            include: If given, only tools whose ``name`` is in this set are
                exposed.
            exclude: Tool names to skip.
        """
        self._transport_factory = transport_factory
        self._include: frozenset[str] | None = (
            frozenset(include) if include is not None else None
        )
        self._exclude: frozenset[str] = frozenset(exclude or ())

        self._stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None
        self._tools: list[MCPTool] = []

    # ------------------------------------------------------------------
    # Transport constructors
    # ------------------------------------------------------------------

    @classmethod
    def stdio(
        cls,
        command: str,
        args: Iterable[str] | None = None,
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] | None = None,
    ) -> "MCPToolset":
        """Connect to an MCP server over stdio.

        Args:
            command: Executable to spawn (e.g. ``"npx"``, ``"python"``).
            args: Command arguments.
            env: Environment variables passed to the child process. When
                ``None``, the parent's environment is inherited.
            cwd: Working directory for the child process.
        """
        from mcp import StdioServerParameters
        from mcp.client.stdio import stdio_client

        params = StdioServerParameters(
            command=command,
            args=list(args or ()),
            env=dict(env) if env is not None else None,
            cwd=cwd,
        )
        return cls(
            transport_factory=lambda: stdio_client(params),
            include=include,
            exclude=exclude,
        )

    @classmethod
    def sse(
        cls,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] | None = None,
    ) -> "MCPToolset":
        """Connect to an MCP server over Server-Sent Events."""
        from mcp.client.sse import sse_client

        return cls(
            transport_factory=lambda: sse_client(
                url, headers=dict(headers) if headers is not None else None
            ),
            include=include,
            exclude=exclude,
        )

    @classmethod
    def http(
        cls,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        include: Iterable[str] | None = None,
        exclude: Iterable[str] | None = None,
    ) -> "MCPToolset":
        """Connect to an MCP server over Streamable HTTP (the successor to SSE).

        Custom headers are applied by constructing a preconfigured
        ``httpx.AsyncClient`` — the MCP SDK's streamable-http transport takes
        an ``http_client=`` argument rather than a raw ``headers`` dict.
        """
        from mcp.client.streamable_http import streamable_http_client

        def factory() -> Any:
            client_kwargs: dict[str, Any] = {}
            if headers:
                client_kwargs["headers"] = dict(headers)
            if client_kwargs:
                import httpx

                return streamable_http_client(
                    url, http_client=httpx.AsyncClient(**client_kwargs)
                )
            return streamable_http_client(url)

        return cls(
            transport_factory=factory,
            include=include,
            exclude=exclude,
        )

    # ------------------------------------------------------------------
    # Public introspection
    # ------------------------------------------------------------------

    @property
    def tools(self) -> list[MCPTool]:
        """The list of remote tools exposed by this toolset.

        Empty until the toolset is entered as an async context manager.
        """
        return list(self._tools)

    @property
    def session(self) -> "ClientSession":
        """The underlying MCP client session. Raises if not entered yet."""
        if self._session is None:
            raise RuntimeError(
                "MCPToolset session is not available. Enter the toolset as "
                "an async context manager first: `async with toolset as ts:`"
            )
        return self._session

    def __iter__(self) -> Iterator[MCPTool]:
        return iter(self._tools)

    def __len__(self) -> int:
        return len(self._tools)

    def __repr__(self) -> str:
        return (
            f"MCPToolset(connected={self._session is not None}, "
            f"tools={len(self._tools)})"
        )

    # ------------------------------------------------------------------
    # Async lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "MCPToolset":
        from mcp import ClientSession

        stack = AsyncExitStack()
        try:
            # 1. Open the transport (yields read/write streams).
            transport = await stack.enter_async_context(self._transport_factory())
            # Some transports yield a 2-tuple, streamable-http yields 3.
            read, write = transport[0], transport[1]

            # 2. Wrap in a ClientSession and initialise.
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()

            # 3. Enumerate remote tools and build wrappers.
            list_result = await session.list_tools()
            wrappers: list[MCPTool] = []
            for tool in list_result.tools:
                if self._include is not None and tool.name not in self._include:
                    continue
                if tool.name in self._exclude:
                    continue
                wrappers.append(
                    MCPTool(
                        session=session,
                        name=tool.name,
                        description=tool.description or "",
                        schema=dict(tool.inputSchema or {}),
                    )
                )
        except Exception:
            await stack.aclose()
            raise

        self._stack = stack
        self._session = session
        self._tools = wrappers
        return self

    async def __aexit__(
        self, exc_type: object, exc: object, tb: object
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Tear down the transport and session. Idempotent."""
        if self._stack is not None:
            await self._stack.aclose()
        self._stack = None
        self._session = None
        self._tools = []


# ---------------------------------------------------------------------------
# Content flattening
# ---------------------------------------------------------------------------


def _flatten_content(content: Iterable[Any]) -> str:
    """Turn a list of MCP content blocks into a single string for the LLM.

    MCP ``call_tool`` results are a list of ``TextContent | ImageContent |
    EmbeddedResource | ...``. Models want strings, so:

        - Text blocks are concatenated verbatim.
        - Image / binary blocks are replaced with a compact ``[image ...]``
          placeholder that names the mime type and size, so the model at
          least knows something non-text was returned.

    This is intentionally lossy — callers who need the raw content should
    reach into ``mcp_tool._session.call_tool(...)`` directly.
    """
    parts: list[str] = []
    for block in content:
        block_type = getattr(block, "type", None)
        if block_type == "text":
            parts.append(getattr(block, "text", ""))
        elif block_type == "image":
            mime = getattr(block, "mimeType", "image/unknown")
            data = getattr(block, "data", "") or ""
            parts.append(f"[image mime={mime} bytes={len(data)}]")
        elif block_type == "audio":
            mime = getattr(block, "mimeType", "audio/unknown")
            data = getattr(block, "data", "") or ""
            parts.append(f"[audio mime={mime} bytes={len(data)}]")
        elif block_type == "resource":
            # EmbeddedResource — grab the resource URI + a description.
            resource = getattr(block, "resource", None)
            uri = getattr(resource, "uri", "unknown://") if resource else "unknown://"
            parts.append(f"[resource uri={uri}]")
        else:
            # Unknown / future content type — fall back to repr.
            parts.append(repr(block))
    return "\n".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Convenience for one-shot listing without holding the connection.
# ---------------------------------------------------------------------------


async def list_mcp_tools(toolset: MCPToolset) -> AsyncIterator[MCPTool]:
    """Yield the tools of a toolset that hasn't been entered yet.

    Opens the toolset, yields each tool, then closes. Only useful for
    introspection; do NOT hand the yielded tools to an Agent — they will be
    invalid once the toolset closes.
    """
    async with toolset as ts:
        for t in ts:
            yield t


__all__ = [
    "MCPTool",
    "MCPToolset",
    "list_mcp_tools",
]
