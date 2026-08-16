"""Tests for koala.tools.mcp — MCPToolset + MCPTool.

Uses fully-mocked transport + session objects — no subprocess, no network.
The MCPToolset accepts a ``transport_factory`` callable in __init__, which
we override with our own fake to drive the full connect / list / call path.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import patch

import pytest

from koala.core.context import RunContext
from koala.tools.errors import ToolExecutionError
from koala.tools.mcp import MCPTool, MCPToolset, _flatten_content

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeContent:
    """Duck-typed stand-in for mcp.types.TextContent / ImageContent / etc."""

    def __init__(self, **fields: Any) -> None:
        for k, v in fields.items():
            setattr(self, k, v)


class _FakeCallResult:
    def __init__(self, content: list[Any], is_error: bool = False) -> None:
        self.content = content
        self.isError = is_error


class _FakeTool:
    def __init__(
        self, name: str, description: str = "", input_schema: dict | None = None
    ) -> None:
        self.name = name
        self.description = description
        self.inputSchema = input_schema or {"type": "object", "properties": {}}


class _FakeListToolsResult:
    def __init__(self, tools: list[_FakeTool]) -> None:
        self.tools = tools


class FakeSession:
    """Duck-typed stand-in for mcp.ClientSession usable as an async ctx manager."""

    def __init__(self, tools: list[_FakeTool], call_result: _FakeCallResult) -> None:
        self._tools = tools
        self._call_result = call_result
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.initialized = False

    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def initialize(self) -> None:
        self.initialized = True

    async def list_tools(self) -> _FakeListToolsResult:
        return _FakeListToolsResult(self._tools)

    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> _FakeCallResult:
        self.calls.append((name, arguments))
        return self._call_result


@asynccontextmanager
async def _fake_transport() -> Any:
    """Yields a dummy (read, write) pair — the FakeSession ignores these."""
    yield (object(), object())


def _make_toolset(
    tools: list[_FakeTool],
    call_result: _FakeCallResult | None = None,
    *,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> tuple[MCPToolset, FakeSession]:
    """Build a toolset with a stubbed transport + session."""
    session = FakeSession(
        tools, call_result or _FakeCallResult(content=[], is_error=False)
    )
    toolset = MCPToolset(
        transport_factory=_fake_transport, include=include, exclude=exclude
    )
    # Patch `from mcp import ClientSession` inside __aenter__ to return
    # our fake — the fake already supports async ctx and initialize/list/call.
    ctx_patch = patch(
        "koala.tools.mcp.MCPToolset.__aenter__",
        _make_patched_aenter(toolset, session),
    )
    ctx_patch.start()
    return toolset, session


def _make_patched_aenter(toolset: MCPToolset, session: FakeSession):
    """Build an __aenter__ that uses our FakeSession instead of ClientSession."""
    from contextlib import AsyncExitStack

    async def aenter(self: MCPToolset) -> MCPToolset:
        stack = AsyncExitStack()
        try:
            transport = await stack.enter_async_context(
                self._transport_factory()
            )
            _ = transport  # noqa: F841
            fake = await stack.enter_async_context(session)
            await fake.initialize()
            list_result = await fake.list_tools()
            wrappers: list[MCPTool] = []
            for t in list_result.tools:
                if self._include is not None and t.name not in self._include:
                    continue
                if t.name in self._exclude:
                    continue
                wrappers.append(
                    MCPTool(
                        session=fake,
                        name=t.name,
                        description=t.description or "",
                        schema=dict(t.inputSchema or {}),
                    )
                )
        except Exception:
            await stack.aclose()
            raise
        self._stack = stack
        self._session = fake
        self._tools = wrappers
        return self

    return aenter


# ---------------------------------------------------------------------------
# _flatten_content
# ---------------------------------------------------------------------------


def test_flatten_content_joins_text_blocks() -> None:
    blocks = [
        _FakeContent(type="text", text="hello"),
        _FakeContent(type="text", text="world"),
    ]
    assert _flatten_content(blocks) == "hello\nworld"


def test_flatten_content_describes_images_compactly() -> None:
    blocks = [
        _FakeContent(type="image", mimeType="image/png", data="AAAA"),
    ]
    out = _flatten_content(blocks)
    assert "[image mime=image/png bytes=4]" == out


def test_flatten_content_mixed_text_and_binary() -> None:
    blocks = [
        _FakeContent(type="text", text="here you go:"),
        _FakeContent(type="image", mimeType="image/jpeg", data="XY"),
    ]
    out = _flatten_content(blocks)
    assert "here you go:" in out
    assert "[image" in out


def test_flatten_content_resource_uri() -> None:
    resource = _FakeContent(uri="file:///tmp/report.pdf")
    blocks = [_FakeContent(type="resource", resource=resource)]
    assert _flatten_content(blocks) == "[resource uri=file:///tmp/report.pdf]"


def test_flatten_content_audio_block() -> None:
    blocks = [_FakeContent(type="audio", mimeType="audio/wav", data="12345")]
    assert _flatten_content(blocks) == "[audio mime=audio/wav bytes=5]"


def test_flatten_content_unknown_falls_back_to_repr() -> None:
    class Unknown:
        def __repr__(self) -> str:
            return "<Unknown>"

    assert _flatten_content([Unknown()]) == "<Unknown>"


def test_flatten_content_skips_empty_text() -> None:
    blocks = [
        _FakeContent(type="text", text=""),
        _FakeContent(type="text", text="only-me"),
    ]
    assert _flatten_content(blocks) == "only-me"


# ---------------------------------------------------------------------------
# MCPTool.run
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mcp_tool_run_forwards_arguments_and_returns_text() -> None:
    session = FakeSession(
        tools=[],
        call_result=_FakeCallResult(
            content=[_FakeContent(type="text", text="ok")]
        ),
    )
    tool = MCPTool(
        session=session,
        name="echo",
        description="",
        schema={"type": "object", "properties": {}},
    )
    result = await tool.run(RunContext(deps=None), {"msg": "hi"})
    assert result == "ok"
    assert session.calls == [("echo", {"msg": "hi"})]


@pytest.mark.asyncio
async def test_mcp_tool_run_raises_tool_execution_error_on_iserror() -> None:
    session = FakeSession(
        tools=[],
        call_result=_FakeCallResult(
            content=[_FakeContent(type="text", text="disk full")],
            is_error=True,
        ),
    )
    tool = MCPTool(session=session, name="write", description="", schema={})
    with pytest.raises(ToolExecutionError) as exc:
        await tool.run(RunContext(deps=None), {})
    assert "disk full" in str(exc.value.original)


@pytest.mark.asyncio
async def test_mcp_tool_run_wraps_rpc_exception() -> None:
    class BoomSession(FakeSession):
        async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
            raise RuntimeError("connection reset")

    tool = MCPTool(
        session=BoomSession([], _FakeCallResult([])),
        name="x",
        description="",
        schema={},
    )
    with pytest.raises(ToolExecutionError) as exc:
        await tool.run(RunContext(deps=None), {})
    assert isinstance(exc.value.original, RuntimeError)
    assert "connection reset" in str(exc.value.original)


# ---------------------------------------------------------------------------
# MCPToolset lifecycle + filtering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_toolset_lists_all_tools_when_no_filter() -> None:
    tools = [_FakeTool("read"), _FakeTool("write"), _FakeTool("list")]
    toolset, _ = _make_toolset(tools)
    async with toolset as ts:
        names = [t.name for t in ts]
    assert names == ["read", "write", "list"]


@pytest.mark.asyncio
async def test_toolset_include_filters_by_name() -> None:
    tools = [_FakeTool("read"), _FakeTool("write"), _FakeTool("delete")]
    toolset, _ = _make_toolset(tools, include=["read", "write"])
    async with toolset as ts:
        assert sorted(t.name for t in ts) == ["read", "write"]


@pytest.mark.asyncio
async def test_toolset_exclude_filters_by_name() -> None:
    tools = [_FakeTool("read"), _FakeTool("write"), _FakeTool("delete")]
    toolset, _ = _make_toolset(tools, exclude=["delete"])
    async with toolset as ts:
        assert sorted(t.name for t in ts) == ["read", "write"]


@pytest.mark.asyncio
async def test_toolset_len_and_iter_reflect_wrappers() -> None:
    tools = [_FakeTool("a"), _FakeTool("b")]
    toolset, _ = _make_toolset(tools)
    async with toolset as ts:
        assert len(ts) == 2
        assert [t.name for t in ts] == ["a", "b"]


@pytest.mark.asyncio
async def test_toolset_wrappers_carry_schema_and_description() -> None:
    tools = [
        _FakeTool(
            "search",
            description="Search the web",
            input_schema={
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        )
    ]
    toolset, _ = _make_toolset(tools)
    async with toolset as ts:
        (t,) = list(ts)
        assert t.description == "Search the web"
        assert t.schema["required"] == ["query"]
        openai_shape = t.to_openai_schema()
        assert openai_shape["type"] == "function"
        assert openai_shape["function"]["name"] == "search"


def test_toolset_session_property_raises_before_enter() -> None:
    toolset = MCPToolset(transport_factory=_fake_transport)
    with pytest.raises(RuntimeError, match="not available"):
        _ = toolset.session


@pytest.mark.asyncio
async def test_toolset_close_clears_state() -> None:
    tools = [_FakeTool("read")]
    toolset, _ = _make_toolset(tools)
    async with toolset as ts:
        assert len(ts) == 1
    # After __aexit__ -> close(), tools list is empty and session is gone.
    assert len(toolset.tools) == 0
    with pytest.raises(RuntimeError):
        _ = toolset.session


@pytest.mark.asyncio
async def test_tool_run_calls_underlying_session() -> None:
    """End-to-end: entered toolset -> tool.run -> session.call_tool."""
    tools = [_FakeTool("get_time")]
    call_result = _FakeCallResult(
        content=[_FakeContent(type="text", text="2026-08-07T10:00Z")]
    )
    toolset, session = _make_toolset(tools, call_result=call_result)
    async with toolset as ts:
        tool = next(iter(ts))
        out = await tool.run(RunContext(deps=None), {"tz": "UTC"})
    assert out == "2026-08-07T10:00Z"
    assert session.calls == [("get_time", {"tz": "UTC"})]


# ---------------------------------------------------------------------------
# Transport constructors — smoke-check they build without contacting anything
# ---------------------------------------------------------------------------


def test_stdio_constructor_captures_command_and_args() -> None:
    ts = MCPToolset.stdio(command="python", args=["-m", "mymcp"])
    # Constructing must not spawn a subprocess; the factory is lazy.
    assert callable(ts._transport_factory)


def test_sse_constructor_accepts_headers() -> None:
    ts = MCPToolset.sse(
        "https://example.com/sse", headers={"Authorization": "Bearer x"}
    )
    assert callable(ts._transport_factory)


def test_http_constructor_accepts_headers() -> None:
    ts = MCPToolset.http(
        "https://example.com/mcp", headers={"X-Api-Key": "abc"}
    )
    assert callable(ts._transport_factory)
