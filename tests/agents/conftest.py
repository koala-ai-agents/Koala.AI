"""Shared fixtures for agent tests — the scripted fake provider + Model factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from koala.core import (
    Capability,
    Event,
    Message,
    ModelDelta,
    ModelMessage,
    TextBlock,
    ToolCall,
    ToolCallBlock,
    Usage,
    UsageEvent,
)
from koala.models import BaseProvider, ChatSettings, Model


class ScriptedProvider(BaseProvider):
    """A BaseProvider that returns predetermined messages one per chat call.

    Useful for testing agent loops without any HTTP or provider math:
    describe the exact sequence of assistant messages you expect the model
    to produce, then let the agent drive.
    """

    def __init__(
        self,
        responses: list[Message],
        *,
        slug: str = "scripted",
        capabilities: frozenset[Capability] = frozenset(),
        usage_per_call: Usage | None = None,
    ) -> None:
        self.slug = slug
        self.base_url = "http://scripted.test"
        self.capabilities = capabilities
        self._responses = list(responses)
        self._index = 0
        self._usage_per_call = usage_per_call or Usage(
            input_tokens=5, output_tokens=5, requests=1
        )
        # Test-inspectable state
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    def _record_call(
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None,
        response_format: dict[str, Any] | None,
    ) -> Message:
        self.calls.append(
            {
                "model_name": model_name,
                "messages": list(messages),
                "settings": settings,
                "tools": tools,
                "response_format": response_format,
            }
        )
        if self._index >= len(self._responses):
            raise RuntimeError(
                f"ScriptedProvider exhausted after {self._index} calls; "
                "add more responses or reduce agent iterations."
            )
        msg = self._responses[self._index]
        self._index += 1
        return msg

    async def chat(
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> tuple[Message, Usage]:
        msg = self._record_call(
            model_name, messages, settings, tools, response_format
        )
        return msg, self._usage_per_call

    async def stream_chat(  # type: ignore[override]
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> AsyncIterator[Event]:
        msg = self._record_call(
            model_name, messages, settings, tools, response_format
        )
        # Yield text content as a single delta for observability.
        for block in msg.content:
            if isinstance(block, TextBlock) and block.text:
                yield ModelDelta(text=block.text)
        # Match the real UniversalProvider: emit ToolCall events for every
        # tool_call block AFTER text streaming, then close with ModelMessage.
        for block in msg.content:
            if isinstance(block, ToolCallBlock):
                yield ToolCall(call=block)
        yield UsageEvent(usage=self._usage_per_call)
        yield ModelMessage(message=msg)

    async def close(self) -> None:
        self.closed = True


def make_scripted_model(
    responses: list[Message],
    *,
    capabilities: frozenset[Capability] = frozenset(),
    name: str = "test-model",
) -> tuple[Model, ScriptedProvider]:
    """Build a Model with an injected ScriptedProvider.

    Returns (model, provider) so tests can inspect calls / advance state.
    """
    provider = ScriptedProvider(responses=responses, capabilities=capabilities)
    model = Model(
        name=name,
        provider="scripted",
        provider_instance=provider,
        base_url="http://scripted.test",
    )
    return model, provider


# Convenience factories for building assistant messages -----------------------


def assistant_text(text: str) -> Message:
    return Message(role="assistant", content=[TextBlock(text=text)])


def assistant_tool_call(
    tool_name: str,
    arguments: dict[str, Any],
    *,
    call_id: str = "call_1",
    text: str = "",
) -> Message:
    content: list[Any] = []
    if text:
        content.append(TextBlock(text=text))
    content.append(
        ToolCallBlock(id=call_id, name=tool_name, arguments=arguments)
    )
    return Message(role="assistant", content=content)


# Re-export for convenience in tests
__all__ = [
    "ScriptedProvider",
    "make_scripted_model",
    "assistant_text",
    "assistant_tool_call",
]



