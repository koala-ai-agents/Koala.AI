"""Model — the user-facing wrapper around a Provider + specific model name.

A `Model` composes:
    - a `BaseProvider` (owns HTTP, wire format)
    - a specific `name` (which model on that provider)
    - default `ChatSettings` (temperature, max_tokens, ...)

It satisfies L1's `Runnable[list[Message], Message]` protocol via `astream`,
so anything that takes a Runnable (Flow steps, harness, observability) can
treat a Model uniformly.

Construction shorthand:
    Model("groq/llama-3.3-70b-versatile")                        # from registry
    Model("groq/llama-3.3", api_key="...")                       # explicit key
    Model("custom/my-model", base_url="http://...", api_key=...) # custom endpoint
    Model(name="my-model", base_url="http://...")                # slug defaults to "custom"
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

from ..core.capabilities import Capability
from ..core.context import RunContext
from ..core.events import Done, Error, Event, ModelMessage, Output, Start, UsageEvent
from ..core.messages import Message
from ..core.types import ModelRef, parse_model_ref
from ..observability.otel import model_span
from .base import BaseProvider
from .errors import ProviderError
from .keys import resolve_api_key
from .registry import get_provider_profile
from .settings import ChatSettings
from .universal import UniversalProvider


class Model:
    """A ready-to-call LLM. Combines a Provider with a specific model + settings.

    All constructor arguments after `ref` are keyword-only.
    """

    def __init__(
        self,
        ref: str | None = None,
        *,
        # Explicit routing (alternatives to `ref`)
        name: str | None = None,
        provider: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        default_headers: dict[str, str] | None = None,
        capabilities: frozenset[Capability] | None = None,
        # Common ChatSettings, inline for ergonomics
        temperature: float | None = None,
        max_tokens: int | None = None,
        top_p: float | None = None,
        stop: list[str] | None = None,
        seed: int | None = None,
        frequency_penalty: float | None = None,
        presence_penalty: float | None = None,
        # Escape hatches
        settings: ChatSettings | None = None,
        extra: dict[str, Any] | None = None,
        provider_instance: BaseProvider | None = None,
    ) -> None:
        # 1. Resolve provider slug + model name.
        if ref is not None:
            parsed = parse_model_ref(ref)
            self.provider_slug: str = parsed.provider
            self.name: str = parsed.name
        else:
            if not name:
                raise ValueError(
                    "Model() requires either a `ref` string ('provider/name') "
                    "or explicit `name=...` (with optional `provider=...`)."
                )
            self.provider_slug = (provider or "custom").strip().lower()
            self.name = name

        # 2. Load provider profile from the registry (may be None for unknown slug).
        profile = get_provider_profile(self.provider_slug)

        # 3. Resolve base_url. Order:
        #    explicit `base_url=` >
        #    env var named by `profile.base_url_env` (if set + non-empty) >
        #    profile.base_url >
        #    error.
        resolved_base_url = base_url
        if not resolved_base_url and profile is not None:
            if profile.base_url_env:
                env_value = os.environ.get(profile.base_url_env, "").strip()
                if env_value:
                    resolved_base_url = env_value
            if not resolved_base_url:
                resolved_base_url = profile.base_url
        if not resolved_base_url:
            raise ValueError(
                f"No base_url available for provider {self.provider_slug!r}. "
                "Pass base_url= or call register_provider() first."
            )

        # 4. Resolve api_key using the standard chain.
        env_key = profile.env_key if profile else None
        # Providers with no env_key in the profile are treated as key-optional
        # (Ollama, LM Studio, unknown custom endpoints). Users can still pass
        # api_key= explicitly for private endpoints.
        key_optional = env_key is None
        resolved_api_key = resolve_api_key(
            explicit=api_key,
            env_key=env_key,
            provider_slug=self.provider_slug,
            key_optional=key_optional,
        )

        # 5. Capabilities: explicit override > profile default > empty.
        resolved_caps = (
            capabilities
            if capabilities is not None
            else (profile.capabilities if profile else frozenset())
        )

        # 6. Merge default headers.
        resolved_headers = {
            **(profile.default_headers if profile else {}),
            **(default_headers or {}),
        }

        # 7. Build the provider (or accept an injected one for testing).
        if provider_instance is None:
            self.provider: BaseProvider = UniversalProvider(
                slug=self.provider_slug,
                base_url=resolved_base_url,
                api_key=resolved_api_key,
                default_headers=resolved_headers,
                capabilities=resolved_caps,
            )
        else:
            self.provider = provider_instance

        # 8. Build default ChatSettings from inline kwargs + escape hatches.
        base = settings or ChatSettings()
        inline = ChatSettings(
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            stop=stop,
            seed=seed,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
            extra=dict(extra or {}),
        )
        self.settings: ChatSettings = base.merge(inline)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def capabilities(self) -> frozenset[Capability]:
        return self.provider.capabilities

    @property
    def ref(self) -> ModelRef:
        return ModelRef(provider=self.provider_slug, name=self.name)

    def __repr__(self) -> str:
        return f"Model({self.provider_slug}/{self.name})"

    # ------------------------------------------------------------------
    # Convenience API
    # ------------------------------------------------------------------

    async def chat(
        self,
        messages: list[Message],
        *,
        settings: ChatSettings | None = None,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> Message:
        """Non-streaming chat completion. Returns the assistant Message."""
        merged = (
            self.settings if settings is None else self.settings.merge(settings)
        )
        message, _ = await self.provider.chat(
            self.name,
            messages,
            merged,
            tools=tools,
            response_format=response_format,
        )
        return message

    def stream(
        self,
        messages: list[Message],
        *,
        settings: ChatSettings | None = None,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> AsyncIterator[Event]:
        """Streaming chat completion. Returns an async iterator of core Events.

        Delegates directly to the provider's `stream_chat` — no Start/Done
        bookends. Use `astream` if you want the full Runnable event contract.
        """
        merged = (
            self.settings if settings is None else self.settings.merge(settings)
        )
        return self.provider.stream_chat(
            self.name,
            messages,
            merged,
            tools=tools,
            response_format=response_format,
        )

    # ------------------------------------------------------------------
    # L1 Runnable[list[Message], Message] interface
    # ------------------------------------------------------------------

    async def astream(
        self,
        ctx: RunContext,
        input: list[Message],
        /,
    ) -> AsyncIterator[Event]:
        """Runnable protocol implementation.

        Emits `Start` first, then streams model events, then a final `Output`
        carrying the assistant Message, then `Done`. Usage from the model is
        accumulated into `ctx.usage`.
        """
        run_id = ctx.session_id
        yield Start(
            run_id=run_id,
            name=f"Model({self.provider_slug}/{self.name})",
            input=None,
        )
        final_message: Message | None = None
        turn_input_tokens = 0
        turn_output_tokens = 0

        with model_span(
            system=self.provider_slug,
            model=self.name,
            settings=self.settings,
        ) as span:
            try:
                async for event in self.provider.stream_chat(
                    self.name, input, self.settings
                ):
                    yield event
                    if isinstance(event, ModelMessage):
                        final_message = event.message
                    elif isinstance(event, UsageEvent):
                        ctx.usage.add(event.usage)
                        turn_input_tokens = event.usage.input_tokens
                        turn_output_tokens = event.usage.output_tokens
            except ProviderError as e:
                span.record_error(e)
                yield Error(error=str(e), exc_type=type(e).__name__, fatal=True)
                yield Done(run_id=run_id)
                return

            finish_reason: str | None = None
            if final_message is not None and final_message.tool_calls:
                finish_reason = "tool_calls"
            elif final_message is not None:
                finish_reason = "stop"
            span.record_response(
                input_tokens=turn_input_tokens or None,
                output_tokens=turn_output_tokens or None,
                finish_reason=finish_reason,
                response_model=self.name,
            )

        if final_message is not None:
            yield Output(value=final_message)
        yield Done(run_id=run_id)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        await self.provider.close()

    async def __aenter__(self) -> "Model":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # UI convenience — same as `koala.show(model, input)` but discoverable
    # ------------------------------------------------------------------

    def show(
        self,
        input: str | list[Message],
        *,
        end: str = "\n",
    ) -> str:
        """Sync convenience: stream a completion and print it live."""
        from ..ui import show as _show

        return _show(self, input, end=end)

    async def ashow(
        self,
        input: str | list[Message],
        *,
        end: str = "\n",
    ) -> str:
        """Async version of :meth:`show`."""
        from ..ui import ashow as _ashow

        return await _ashow(self, input, end=end)
