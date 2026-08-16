"""Abstract provider interface.

A `BaseProvider` owns an HTTP client and knows how to speak one wire format
(OpenAI-compatible for L2). Concrete providers implement `chat()` for
non-streaming completion and `stream_chat()` for streaming.

Providers are async context managers so the HTTP client lifecycle is explicit.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

from ..core.capabilities import Capability
from ..core.events import Event
from ..core.messages import Message
from ..core.types import Usage
from .settings import ChatSettings


class BaseProvider(ABC):
    """Contract for all provider clients.

    Concrete implementations must set `slug`, `base_url`, and `capabilities`
    in `__init__` before returning.
    """

    slug: str
    base_url: str
    capabilities: frozenset[Capability]

    @abstractmethod
    async def chat(
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> tuple[Message, Usage]:
        """Send a non-streaming chat completion.

        Args:
            model_name: The specific model on this provider.
            messages: Full conversation history to send.
            settings: Merged chat settings (temperature, max_tokens, ...).
            tools: Optional list of tool schemas in the provider's wire format.
            response_format: Optional structured-output spec (e.g. json_schema).

        Returns:
            Tuple of (assistant Message, Usage for the call).
        """
        raise NotImplementedError

    @abstractmethod
    def stream_chat(
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> AsyncIterator[Event]:
        """Stream a chat completion as core Events.

        The returned async iterator yields:
            - `ModelDelta` for each text chunk
            - `ToolCall` when a tool-call block finalizes (accumulated across chunks)
            - `UsageEvent` when usage arrives
            - `ModelMessage` with the fully-assembled assistant message at end

        `Start`/`Done` bookends are the caller's responsibility (see `Model.astream`).
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Release HTTP resources."""
        raise NotImplementedError

    async def __aenter__(self) -> "BaseProvider":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()
