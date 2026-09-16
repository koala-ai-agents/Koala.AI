"""OpenAI-compatible universal provider.

Speaks the OpenAI Chat Completions wire format. Works transparently with:
    - OpenAI, Groq, DeepSeek, xAI, Together, Fireworks, OpenRouter
    - Local endpoints: Ollama, LM Studio, vLLM, TGI (with OpenAI compat)
    - Any other endpoint that mimics OpenAI's /chat/completions shape

One class covers everything with a base_url + api_key. Provider-specific
behavior (headers, streaming quirks) is negligible enough to fit here; if we
ever need a truly different wire format (Anthropic native, Gemini native)
that's a separate BaseProvider subclass.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any, cast

import httpx

from ..core.capabilities import Capability
from ..core.events import (
    Event,
    ModelDelta,
    ModelMessage,
    ThinkingDelta,
    ToolCall,
    UsageEvent,
)
from ..core.messages import (
    ImageBlock,
    Message,
    TextBlock,
    ThinkingBlock,
    ToolCallBlock,
    ToolResultBlock,
)
from ..core.retry import RetryPolicy, parse_retry_after, retry_async
from ..core.types import Usage
from .base import BaseProvider
from .errors import (
    AuthenticationError,
    BadRequestError,
    ProviderConnectionError,
    ProviderError,
    ProviderServerError,
    ProviderTimeoutError,
    RateLimitError,
)
from .settings import ChatSettings

DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0, read=300.0)


class UniversalProvider(BaseProvider):
    """OpenAI-compatible provider client.

    Args:
        slug: Provider slug (for tracing/errors, e.g. "groq").
        base_url: Endpoint root, e.g. ``https://api.groq.com/openai/v1``.
        api_key: Bearer token. `None` for keyless local endpoints.
        default_headers: Extra headers on every request.
        capabilities: Capability set this provider advertises.
        timeout: httpx timeout config, or a float for a simple overall timeout.
        transport: Optional httpx transport for testing (`httpx.MockTransport`).
        retry_policy: Optional retry policy for transient errors (429, 5xx, timeouts).
    """

    def __init__(
        self,
        *,
        slug: str,
        base_url: str,
        api_key: str | None,
        default_headers: dict[str, str] | None = None,
        capabilities: frozenset[Capability] = frozenset(),
        timeout: httpx.Timeout | float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.slug = slug
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.capabilities = capabilities
        self.retry_policy: RetryPolicy = (
            retry_policy if retry_policy is not None else RetryPolicy()
        )
        self._default_headers = dict(default_headers or {})

        headers: dict[str, str] = {
            "Content-Type": "application/json",
            **self._default_headers,
        }
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=timeout if timeout is not None else DEFAULT_TIMEOUT,
            transport=transport,
        )

    async def close(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Non-streaming chat
    # ------------------------------------------------------------------

    async def chat(
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> tuple[Message, Usage]:
        payload = self._build_payload(
            model_name, messages, settings, tools, response_format, stream=False
        )
        data = await self._post_json("/chat/completions", payload)
        return self._parse_completion(data)

    # ------------------------------------------------------------------
    # Streaming chat
    # ------------------------------------------------------------------

    async def stream_chat(
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> AsyncIterator[Event]:
        payload = self._build_payload(
            model_name, messages, settings, tools, response_format, stream=True
        )

        text_buf: list[str] = []
        thinking_buf: list[str] = []
        # tool_call index -> partial state: {id, name, arguments}
        tool_calls: dict[int, dict[str, Any]] = {}

        async def _open_stream() -> httpx.Response:
            try:
                req = self._client.build_request("POST", "/chat/completions", json=payload)
                resp = await self._client.send(req, stream=True)
            except httpx.TimeoutException as e:
                raise ProviderTimeoutError(
                    f"Request to {self.slug!r} timed out: {e}"
                ) from e
            except httpx.ConnectError as e:
                raise ProviderConnectionError(
                    f"Could not connect to {self.slug!r}: {e}"
                ) from e

            if resp.status_code >= 400:
                body_bytes = await resp.aread()
                await resp.aclose()
                retry_after = parse_retry_after(resp.headers.get("retry-after"))
                self._raise_for_status(
                    resp.status_code,
                    body_bytes.decode(errors="replace"),
                    retry_after=retry_after,
                )
            return resp

        try:
            resp = await retry_async(
                _open_stream, self.retry_policy, is_retryable=self._is_retryable_exception
            )
            try:

                async for line in resp.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    data_str = line[len("data:") :].strip()
                    if data_str == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    # Usage may arrive on the final chunk when
                    # `stream_options.include_usage` is set.
                    if usage_data := chunk.get("usage"):
                        yield UsageEvent(usage=self._parse_usage(usage_data))

                    choices = chunk.get("choices") or []
                    if not choices:
                        continue
                    delta = (choices[0] or {}).get("delta") or {}

                    if content := delta.get("content"):
                        text_buf.append(content)
                        yield ModelDelta(text=content)

                    # Reasoning content (DeepSeek-R1, Qwen QwQ, ...)
                    if reasoning := delta.get("reasoning_content"):
                        thinking_buf.append(reasoning)
                        yield ThinkingDelta(text=reasoning)

                    if tc_deltas := delta.get("tool_calls"):
                        for tc_delta in tc_deltas:
                            idx = tc_delta.get("index", 0)
                            slot = tool_calls.setdefault(
                                idx, {"id": None, "name": None, "arguments": ""}
                            )
                            if tc_id := tc_delta.get("id"):
                                slot["id"] = tc_id
                            fn = tc_delta.get("function") or {}
                            if name := fn.get("name"):
                                slot["name"] = name
                            if args := fn.get("arguments"):
                                slot["arguments"] += args
            finally:
                await resp.aclose()
        except httpx.TimeoutException as e:
            raise ProviderTimeoutError(
                f"Request to {self.slug!r} timed out: {e}"
            ) from e
        except httpx.ConnectError as e:
            raise ProviderConnectionError(
                f"Could not connect to {self.slug!r}: {e}"
            ) from e

        # Assemble the final ModelMessage and emit ToolCall events for each
        # accumulated tool call.
        content_blocks: list[Any] = []
        if thinking_buf:
            content_blocks.append(ThinkingBlock(text="".join(thinking_buf)))
        if text_buf:
            content_blocks.append(TextBlock(text="".join(text_buf)))

        for idx in sorted(tool_calls.keys()):
            call = tool_calls[idx]
            if not (call["id"] and call["name"]):
                continue
            try:
                args = json.loads(call["arguments"]) if call["arguments"] else {}
            except json.JSONDecodeError:
                args = {"__raw__": call["arguments"]}
            tc_block = ToolCallBlock(
                id=call["id"], name=call["name"], arguments=args
            )
            content_blocks.append(tc_block)
            yield ToolCall(call=tc_block)

        yield ModelMessage(message=Message(role="assistant", content=content_blocks))

    # ------------------------------------------------------------------
    # Payload building / response parsing
    # ------------------------------------------------------------------

    def _build_payload(
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None,
        response_format: dict[str, Any] | None,
        stream: bool,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": model_name,
            "messages": [_encode_message(m) for m in messages],
            **settings.to_payload(),
        }
        if tools:
            payload["tools"] = tools
        if response_format:
            payload["response_format"] = response_format
        if stream:
            payload["stream"] = True
            payload["stream_options"] = {"include_usage": True}
        return payload

    def _is_retryable_exception(self, exc: Exception) -> tuple[bool, float | None]:
        if isinstance(exc, RateLimitError):
            return True, exc.retry_after
        if isinstance(exc, ProviderServerError):
            return True, exc.retry_after
        if isinstance(exc, (ProviderTimeoutError, ProviderConnectionError)):
            return True, None
        if isinstance(exc, (httpx.TimeoutException, httpx.ConnectError)):
            return True, None
        return False, None

    async def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        async def _attempt() -> dict[str, Any]:
            try:
                r = await self._client.post(path, json=payload)
            except httpx.TimeoutException as e:
                raise ProviderTimeoutError(
                    f"Request to {self.slug!r} timed out: {e}"
                ) from e
            except httpx.ConnectError as e:
                raise ProviderConnectionError(
                    f"Could not connect to {self.slug!r}: {e}"
                ) from e

            if r.status_code >= 400:
                retry_after = parse_retry_after(r.headers.get("retry-after"))
                self._raise_for_status(r.status_code, r.text, retry_after=retry_after)
            return cast(dict[str, Any], r.json())

        return await retry_async(
            _attempt, self.retry_policy, is_retryable=self._is_retryable_exception
        )

    def _raise_for_status(
        self, status: int, body: str, retry_after: float | None = None
    ) -> None:
        snippet = body[:500] if body else ""
        message = f"{self.slug} returned HTTP {status}: {snippet}"
        if status in (401, 403):
            raise AuthenticationError(
                message, status=status, body=body, retry_after=retry_after
            )
        if status == 429:
            raise RateLimitError(
                message, status=status, body=body, retry_after=retry_after
            )
        if 400 <= status < 500:
            raise BadRequestError(
                message, status=status, body=body, retry_after=retry_after
            )
        if 500 <= status < 600:
            raise ProviderServerError(
                message, status=status, body=body, retry_after=retry_after
            )
        raise ProviderError(
            message, status=status, body=body, retry_after=retry_after
        )

    def _parse_completion(self, data: dict[str, Any]) -> tuple[Message, Usage]:
        """Parse a non-streaming /chat/completions response body."""
        choices = data.get("choices") or []
        if not choices:
            raise BadRequestError(
                f"{self.slug} returned no choices",
                body=str(data)[:500],
            )
        msg = choices[0].get("message") or {}

        content_blocks: list[Any] = []
        if reasoning := msg.get("reasoning_content"):
            content_blocks.append(ThinkingBlock(text=reasoning))
        if text := msg.get("content"):
            content_blocks.append(TextBlock(text=text))
        for tc in msg.get("tool_calls") or []:
            fn = tc.get("function") or {}
            args_str = fn.get("arguments", "")
            try:
                args = json.loads(args_str) if args_str else {}
            except json.JSONDecodeError:
                args = {"__raw__": args_str}
            content_blocks.append(
                ToolCallBlock(
                    id=tc.get("id", ""),
                    name=fn.get("name", ""),
                    arguments=args,
                )
            )

        message = Message(role="assistant", content=content_blocks)
        usage = self._parse_usage(data.get("usage") or {})
        return message, usage

    def _parse_usage(self, data: dict[str, Any]) -> Usage:
        prompt_details = data.get("prompt_tokens_details") or {}
        completion_details = data.get("completion_tokens_details") or {}
        return Usage(
            input_tokens=int(data.get("prompt_tokens", 0) or 0),
            output_tokens=int(data.get("completion_tokens", 0) or 0),
            cached_input_tokens=int(prompt_details.get("cached_tokens", 0) or 0),
            reasoning_tokens=int(completion_details.get("reasoning_tokens", 0) or 0),
            requests=1,
        )


# ---------------------------------------------------------------------------
# Message encoding (koala Message -> OpenAI chat wire format)
# ---------------------------------------------------------------------------


def _encode_message(m: Message) -> dict[str, Any]:
    """Serialize a koala Message to the OpenAI chat wire format."""
    # Tool result messages have a distinct shape.
    if m.role == "tool":
        for block in m.content:
            if isinstance(block, ToolResultBlock):
                content = block.content
                if isinstance(content, (dict, list)):
                    content = json.dumps(content)
                return {
                    "role": "tool",
                    "tool_call_id": block.tool_call_id,
                    "content": content,
                }
        raise ValueError("Tool message must contain a ToolResultBlock")

    text_parts: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    images: list[dict[str, Any]] = []

    for block in m.content:
        if isinstance(block, TextBlock):
            text_parts.append(block.text)
        elif isinstance(block, ToolCallBlock):
            tool_calls.append(
                {
                    "id": block.id,
                    "type": "function",
                    "function": {
                        "name": block.name,
                        "arguments": json.dumps(block.arguments),
                    },
                }
            )
        elif isinstance(block, ImageBlock):
            images.append(
                {"type": "image_url", "image_url": {"url": block.source}}
            )
        # ThinkingBlock is intentionally not sent back to the model; it's
        # display-only output.

    encoded: dict[str, Any] = {"role": m.role}

    if images:
        # Multimodal: content is a list of typed parts.
        parts: list[dict[str, Any]] = []
        if text_parts:
            parts.append({"type": "text", "text": "".join(text_parts)})
        parts.extend(images)
        encoded["content"] = parts
    else:
        encoded["content"] = "".join(text_parts) if text_parts else None

    if tool_calls:
        encoded["tool_calls"] = tool_calls
    if m.name:
        encoded["name"] = m.name

    return encoded
