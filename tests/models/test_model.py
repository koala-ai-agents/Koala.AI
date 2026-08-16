"""Tests for koala.models.model — Model construction + Runnable behavior."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from koala.core import (
    Capability,
    Done,
    Event,
    Message,
    ModelDelta,
    ModelMessage,
    RunContext,
    Runnable,
    Start,
    Usage,
    UsageEvent,
    acollect,
    ainvoke,
)
from koala.models import (
    BaseProvider,
    ChatSettings,
    MissingApiKey,
    Model,
    register_provider,
    unregister_provider,
)

# ---------------------------------------------------------------------------
# Fake provider — bypasses HTTP so Model tests focus on Model behavior.
# ---------------------------------------------------------------------------


class FakeProvider(BaseProvider):
    """Records calls and returns scripted responses."""

    def __init__(
        self,
        *,
        slug: str = "fake",
        response_text: str = "hi",
        capabilities: frozenset[Capability] = frozenset(),
        deltas: list[str] | None = None,
    ) -> None:
        self.slug = slug
        self.base_url = "http://fake"
        self.capabilities = capabilities
        self.response_text = response_text
        self.deltas = deltas or ["hi"]
        self.chat_calls: list[dict[str, Any]] = []
        self.stream_calls: list[dict[str, Any]] = []
        self.closed = False

    async def chat(
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> tuple[Message, Usage]:
        self.chat_calls.append(
            {
                "model_name": model_name,
                "messages": messages,
                "settings": settings,
                "tools": tools,
                "response_format": response_format,
            }
        )
        return (
            Message.assistant(self.response_text),
            Usage(input_tokens=3, output_tokens=2, requests=1),
        )

    async def stream_chat(  # type: ignore[override]
        self,
        model_name: str,
        messages: list[Message],
        settings: ChatSettings,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> AsyncIterator[Event]:
        self.stream_calls.append(
            {"model_name": model_name, "messages": messages, "settings": settings}
        )
        text_buf: list[str] = []
        for chunk in self.deltas:
            text_buf.append(chunk)
            yield ModelDelta(text=chunk)
        yield UsageEvent(
            usage=Usage(input_tokens=3, output_tokens=len(self.deltas), requests=1)
        )
        yield ModelMessage(
            message=Message.assistant("".join(text_buf))
        )

    async def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Construction / resolution
# ---------------------------------------------------------------------------


def test_model_from_ref_uses_registry_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "gsk_test")
    m = Model("groq/llama-3.3-70b-versatile")
    assert m.provider_slug == "groq"
    assert m.name == "llama-3.3-70b-versatile"
    assert m.provider.base_url.endswith("api.groq.com/openai/v1")
    assert Capability.TOOL_CALLING in m.capabilities


def test_model_uses_universal_env_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.setenv("LLM_API_KEY", "universal_test_key")
    m = Model("groq/llama-3.3-70b-versatile")
    assert m.provider.api_key == "universal_test_key"  # type: ignore[attr-defined]


def test_model_explicit_key_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "env_key")
    m = Model("groq/x", api_key="explicit")
    assert m.provider.api_key == "explicit"  # type: ignore[attr-defined]


def test_model_missing_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    with pytest.raises(MissingApiKey):
        Model("groq/x")


def test_model_ollama_needs_no_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    m = Model("ollama/llama3.2")
    assert m.provider.api_key is None  # type: ignore[attr-defined]
    assert m.provider.base_url == "http://localhost:11434/v1"


def test_model_custom_endpoint_no_slug() -> None:
    m = Model(
        name="my-model",
        base_url="http://custom.example/v1",
        api_key="local",
    )
    assert m.provider_slug == "custom"
    assert m.name == "my-model"
    assert m.provider.base_url == "http://custom.example/v1"


def test_model_custom_endpoint_with_slug() -> None:
    m = Model(
        "myco/my-model",
        base_url="http://myco.internal/v1",
        api_key="local",
    )
    assert m.provider_slug == "myco"
    assert m.name == "my-model"


def test_model_registered_provider_works(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register_provider(
        "vllm-prod",
        base_url="http://vllm.svc:8000/v1",
        env_key="VLLM_KEY",
    )
    try:
        monkeypatch.setenv("VLLM_KEY", "secret")
        m = Model("vllm-prod/my-tuned-model")
        assert m.provider.base_url == "http://vllm.svc:8000/v1"
        assert m.provider.api_key == "secret"  # type: ignore[attr-defined]
    finally:
        unregister_provider("vllm-prod")


def test_model_missing_ref_and_name_raises() -> None:
    with pytest.raises(ValueError, match="either a `ref`"):
        Model(base_url="http://x")


def test_model_missing_base_url_for_unknown_slug_raises() -> None:
    with pytest.raises(ValueError, match="No base_url"):
        Model("unknown-provider-xyz/model", api_key="k")


def test_inline_settings_kwargs_become_chat_settings() -> None:
    m = Model(
        "ollama/llama3.2",
        temperature=0.7,
        max_tokens=256,
        seed=42,
    )
    assert m.settings.temperature == 0.7
    assert m.settings.max_tokens == 256
    assert m.settings.seed == 42


def test_extra_kwargs_land_in_settings_extra() -> None:
    m = Model(
        "ollama/llama3.2",
        extra={"reasoning_effort": "high"},
    )
    assert m.settings.extra == {"reasoning_effort": "high"}


def test_capabilities_from_profile_default_are_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "k")
    m = Model("groq/llama-3.3")
    assert Capability.STREAMING in m.capabilities


def test_explicit_capabilities_override_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "k")
    m = Model(
        "groq/llama-3.3",
        capabilities=frozenset({Capability.STREAMING}),
    )
    assert m.capabilities == frozenset({Capability.STREAMING})


def test_model_repr() -> None:
    m = Model(name="m", base_url="http://x", api_key="k")
    assert repr(m) == "Model(custom/m)"


# ---------------------------------------------------------------------------
# Convenience API — chat / stream against FakeProvider
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_returns_assistant_message() -> None:
    fake = FakeProvider(response_text="hello!")
    m = Model(name="m", provider="fake", provider_instance=fake, base_url="http://x")
    result = await m.chat([Message.user("hi")])
    assert result.text == "hello!"
    assert fake.chat_calls[0]["model_name"] == "m"


@pytest.mark.asyncio
async def test_chat_forwards_tools() -> None:
    fake = FakeProvider()
    m = Model(name="m", provider="fake", provider_instance=fake, base_url="http://x")
    tools = [{"type": "function", "function": {"name": "f"}}]
    await m.chat([Message.user("hi")], tools=tools)
    assert fake.chat_calls[0]["tools"] == tools


@pytest.mark.asyncio
async def test_chat_per_call_settings_override_model_defaults() -> None:
    fake = FakeProvider()
    m = Model(
        name="m",
        provider="fake",
        provider_instance=fake,
        base_url="http://x",
        temperature=0.5,
    )
    await m.chat(
        [Message.user("hi")],
        settings=ChatSettings(temperature=0.9),
    )
    assert fake.chat_calls[0]["settings"].temperature == 0.9


@pytest.mark.asyncio
async def test_stream_yields_provider_events_directly() -> None:
    fake = FakeProvider(deltas=["Hi ", "there"])
    m = Model(name="m", provider="fake", provider_instance=fake, base_url="http://x")
    events = []
    async for e in m.stream([Message.user("hi")]):
        events.append(e)
    deltas = [e for e in events if isinstance(e, ModelDelta)]
    assert [d.text for d in deltas] == ["Hi ", "there"]


# ---------------------------------------------------------------------------
# L1 Runnable protocol conformance
# ---------------------------------------------------------------------------


def test_model_satisfies_runnable_protocol() -> None:
    fake = FakeProvider()
    m = Model(name="m", provider="fake", provider_instance=fake, base_url="http://x")
    assert isinstance(m, Runnable)


@pytest.mark.asyncio
async def test_astream_emits_start_and_done_bookends() -> None:
    fake = FakeProvider(deltas=["hi"])
    m = Model(name="m", provider="fake", provider_instance=fake, base_url="http://x")
    ctx: RunContext[None] = RunContext(deps=None)
    events = await acollect(m, ctx, [Message.user("hi")])
    kinds = [e.kind for e in events]
    assert kinds[0] == "start"
    assert kinds[-1] == "done"
    assert "output" in kinds
    assert isinstance(events[0], Start)
    assert isinstance(events[-1], Done)


@pytest.mark.asyncio
async def test_astream_output_carries_assistant_message() -> None:
    fake = FakeProvider(deltas=["Hello ", "world"])
    m = Model(name="m", provider="fake", provider_instance=fake, base_url="http://x")
    ctx: RunContext[None] = RunContext(deps=None)
    result = await ainvoke(m, ctx, [Message.user("hi")])
    assert isinstance(result, Message)
    assert result.text == "Hello world"


@pytest.mark.asyncio
async def test_astream_accumulates_usage_into_context() -> None:
    fake = FakeProvider(deltas=["a", "b"])
    m = Model(name="m", provider="fake", provider_instance=fake, base_url="http://x")
    ctx: RunContext[None] = RunContext(deps=None)
    await ainvoke(m, ctx, [Message.user("hi")])
    assert ctx.usage.requests == 1
    assert ctx.usage.output_tokens == 2  # two deltas
