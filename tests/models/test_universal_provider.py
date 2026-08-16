"""Tests for koala.models.universal — UniversalProvider against httpx.MockTransport.

No live API calls. Every test constructs a UniversalProvider with an injected
`httpx.MockTransport` that returns pre-built OpenAI-shaped responses.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from koala.core import (
    Message,
    ModelDelta,
    ModelMessage,
    TextBlock,
    ThinkingBlock,
    ToolCall,
    ToolCallBlock,
    UsageEvent,
)
from koala.models import (
    AuthenticationError,
    BadRequestError,
    ChatSettings,
    ProviderServerError,
    RateLimitError,
    UniversalProvider,
)

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _make_provider(handler: Any, **overrides: Any) -> UniversalProvider:
    """Build a UniversalProvider wired to a MockTransport handler."""
    kwargs: dict[str, Any] = {
        "slug": "test",
        "base_url": "https://mock.example/v1",
        "api_key": "test-key",
        "transport": httpx.MockTransport(handler),
    }
    kwargs.update(overrides)
    return UniversalProvider(**kwargs)


def _sse_body(*chunks: dict[str, Any]) -> bytes:
    """Encode a sequence of chunks as OpenAI-style SSE."""
    lines: list[str] = []
    for c in chunks:
        lines.append(f"data: {json.dumps(c)}\n")
    lines.append("data: [DONE]\n")
    return "\n".join(lines).encode("utf-8")


# ---------------------------------------------------------------------------
# Non-streaming chat
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_returns_assistant_message_and_usage() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-1",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "Hi!"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 5, "completion_tokens": 3},
            },
        )

    provider = _make_provider(handler)
    try:
        msg, usage = await provider.chat(
            "test-model", [Message.user("Hello")], ChatSettings()
        )
        assert msg.role == "assistant"
        assert msg.text == "Hi!"
        assert usage.input_tokens == 5
        assert usage.output_tokens == 3
        assert usage.requests == 1
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_chat_sends_correct_request_payload() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["auth"] = request.headers.get("Authorization")
        return httpx.Response(
            200,
            json={
                "choices": [
                    {"message": {"role": "assistant", "content": "ok"}}
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            },
        )

    provider = _make_provider(handler)
    try:
        await provider.chat(
            "gpt-4o-mini",
            [Message.system("be nice"), Message.user("hi")],
            ChatSettings(temperature=0.7, max_tokens=50),
        )
    finally:
        await provider.close()

    body = captured["body"]
    assert captured["method"] == "POST"
    assert captured["path"] == "/v1/chat/completions"
    assert captured["auth"] == "Bearer test-key"
    assert body["model"] == "gpt-4o-mini"
    assert body["temperature"] == 0.7
    assert body["max_tokens"] == 50
    assert body["messages"][0] == {"role": "system", "content": "be nice"}
    assert body["messages"][1] == {"role": "user", "content": "hi"}
    assert "stream" not in body


@pytest.mark.asyncio
async def test_chat_parses_tool_calls() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "type": "function",
                                    "function": {
                                        "name": "add",
                                        "arguments": '{"a": 1, "b": 2}',
                                    },
                                }
                            ],
                        }
                    }
                ],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5},
            },
        )

    provider = _make_provider(handler)
    try:
        msg, _ = await provider.chat(
            "test-model", [Message.user("add 1 and 2")], ChatSettings()
        )
        assert msg.tool_calls
        tc = msg.tool_calls[0]
        assert tc.id == "call_1"
        assert tc.name == "add"
        assert tc.arguments == {"a": 1, "b": 2}
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_chat_parses_thinking_content() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "Final answer.",
                            "reasoning_content": "Let me think...",
                        }
                    }
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            },
        )

    provider = _make_provider(handler)
    try:
        msg, _ = await provider.chat(
            "reasoner-1", [Message.user("hi")], ChatSettings()
        )
        blocks = msg.content
        assert isinstance(blocks[0], ThinkingBlock)
        assert blocks[0].text == "Let me think..."
        assert isinstance(blocks[1], TextBlock)
        assert blocks[1].text == "Final answer."
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_chat_forwards_tools_and_response_format() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"role": "assistant", "content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            },
        )

    provider = _make_provider(handler)
    try:
        await provider.chat(
            "m",
            [Message.user("x")],
            ChatSettings(),
            tools=[{"type": "function", "function": {"name": "f"}}],
            response_format={"type": "json_object"},
        )
    finally:
        await provider.close()

    assert captured["body"]["tools"][0]["function"]["name"] == "f"
    assert captured["body"]["response_format"] == {"type": "json_object"}


# ---------------------------------------------------------------------------
# HTTP error mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_401_maps_to_authentication_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="invalid api key")

    provider = _make_provider(handler)
    try:
        with pytest.raises(AuthenticationError) as exc:
            await provider.chat("m", [Message.user("x")], ChatSettings())
        assert exc.value.status == 401
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_429_maps_to_rate_limit_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(429, text="slow down")

    provider = _make_provider(handler)
    try:
        with pytest.raises(RateLimitError):
            await provider.chat("m", [Message.user("x")], ChatSettings())
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_400_maps_to_bad_request_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="bad model name")

    provider = _make_provider(handler)
    try:
        with pytest.raises(BadRequestError):
            await provider.chat("m", [Message.user("x")], ChatSettings())
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_500_maps_to_server_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="oops")

    provider = _make_provider(handler)
    try:
        with pytest.raises(ProviderServerError):
            await provider.chat("m", [Message.user("x")], ChatSettings())
    finally:
        await provider.close()


# ---------------------------------------------------------------------------
# Streaming chat
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_chat_emits_deltas_and_final_message() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        body = _sse_body(
            {"choices": [{"delta": {"role": "assistant", "content": "Hi"}}]},
            {"choices": [{"delta": {"content": " there"}}]},
            {"choices": [{"delta": {"content": "!"}}]},
            {"usage": {"prompt_tokens": 5, "completion_tokens": 3}, "choices": []},
        )
        return httpx.Response(
            200,
            content=body,
            headers={"Content-Type": "text/event-stream"},
        )

    provider = _make_provider(handler)
    try:
        events = []
        async for e in provider.stream_chat(
            "m", [Message.user("hi")], ChatSettings()
        ):
            events.append(e)
    finally:
        await provider.close()

    deltas = [e for e in events if isinstance(e, ModelDelta)]
    usage_events = [e for e in events if isinstance(e, UsageEvent)]
    final = [e for e in events if isinstance(e, ModelMessage)]

    assert [d.text for d in deltas] == ["Hi", " there", "!"]
    assert len(usage_events) == 1
    assert usage_events[0].usage.input_tokens == 5
    assert len(final) == 1
    assert final[0].message.text == "Hi there!"


@pytest.mark.asyncio
async def test_stream_chat_accumulates_tool_call_across_chunks() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        body = _sse_body(
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call_abc",
                                    "function": {
                                        "name": "add",
                                        "arguments": '{"a":',
                                    },
                                }
                            ]
                        }
                    }
                ]
            },
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "function": {"arguments": ' 1, "b": 2}'},
                                }
                            ]
                        }
                    }
                ]
            },
        )
        return httpx.Response(
            200, content=body, headers={"Content-Type": "text/event-stream"}
        )

    provider = _make_provider(handler)
    try:
        events = []
        async for e in provider.stream_chat(
            "m", [Message.user("compute")], ChatSettings()
        ):
            events.append(e)
    finally:
        await provider.close()

    tool_calls = [e for e in events if isinstance(e, ToolCall)]
    assert len(tool_calls) == 1
    assert tool_calls[0].call.id == "call_abc"
    assert tool_calls[0].call.name == "add"
    assert tool_calls[0].call.arguments == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_stream_chat_error_status_still_raises() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="nope")

    provider = _make_provider(handler)
    try:
        with pytest.raises(AuthenticationError):
            async for _ in provider.stream_chat(
                "m", [Message.user("hi")], ChatSettings()
            ):
                pass
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_stream_chat_sends_stream_flag_and_include_usage() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            content=_sse_body(
                {"choices": [{"delta": {"content": "ok"}}]}
            ),
            headers={"Content-Type": "text/event-stream"},
        )

    provider = _make_provider(handler)
    try:
        async for _ in provider.stream_chat(
            "m", [Message.user("hi")], ChatSettings()
        ):
            pass
    finally:
        await provider.close()

    assert captured["body"]["stream"] is True
    assert captured["body"]["stream_options"] == {"include_usage": True}


# ---------------------------------------------------------------------------
# Auth / no-auth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_api_key_omits_authorization_header() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers.get("Authorization")
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"role": "assistant", "content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            },
        )

    provider = _make_provider(handler, api_key=None)
    try:
        await provider.chat("m", [Message.user("hi")], ChatSettings())
    finally:
        await provider.close()

    assert captured["auth"] is None


@pytest.mark.asyncio
async def test_default_headers_are_sent() -> None:
    captured: dict[str, str | None] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["referer"] = request.headers.get("HTTP-Referer")
        captured["title"] = request.headers.get("X-Title")
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"role": "assistant", "content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            },
        )

    provider = _make_provider(
        handler,
        default_headers={
            "HTTP-Referer": "https://myapp.example",
            "X-Title": "My App",
        },
    )
    try:
        await provider.chat("m", [Message.user("hi")], ChatSettings())
    finally:
        await provider.close()

    assert captured["referer"] == "https://myapp.example"
    assert captured["title"] == "My App"


# ---------------------------------------------------------------------------
# Message encoding (via chat payloads)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_result_message_serializes_correctly() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"role": "assistant", "content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            },
        )

    provider = _make_provider(handler)
    try:
        await provider.chat(
            "m",
            [
                Message.user("compute"),
                Message.assistant("calling..."),
                Message.tool("call_1", "42"),
            ],
            ChatSettings(),
        )
    finally:
        await provider.close()

    tool_msg = captured["body"]["messages"][-1]
    assert tool_msg == {"role": "tool", "tool_call_id": "call_1", "content": "42"}


@pytest.mark.asyncio
async def test_assistant_message_with_tool_calls_serializes() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"role": "assistant", "content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            },
        )

    provider = _make_provider(handler)
    try:
        assistant_msg = Message(
            role="assistant",
            content=[
                TextBlock(text="Let me compute."),
                ToolCallBlock(
                    id="c1", name="add", arguments={"a": 1, "b": 2}
                ),
            ],
        )
        await provider.chat(
            "m", [Message.user("hi"), assistant_msg], ChatSettings()
        )
    finally:
        await provider.close()

    encoded = captured["body"]["messages"][1]
    assert encoded["role"] == "assistant"
    assert encoded["content"] == "Let me compute."
    assert encoded["tool_calls"][0]["id"] == "c1"
    assert encoded["tool_calls"][0]["function"]["name"] == "add"
    args = json.loads(encoded["tool_calls"][0]["function"]["arguments"])
    assert args == {"a": 1, "b": 2}
