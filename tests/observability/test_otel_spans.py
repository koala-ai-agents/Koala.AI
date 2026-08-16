"""Tests for koala.observability.otel — GenAI-semconv OpenTelemetry spans."""

from __future__ import annotations

import pytest

from koala.models.settings import ChatSettings
from koala.observability import agent_span, is_available, model_span, tool_span
from koala.observability.otel import _safe_json_dumps, _trim


def test_otel_is_available_when_dep_installed() -> None:
    # The koala[otel] extra is installed in this test env, so this must be True.
    assert is_available() is True


def _attrs(span) -> dict:
    return dict(span.attributes)


# ---------------------------------------------------------------------------
# model_span
# ---------------------------------------------------------------------------


def test_model_span_emits_gen_ai_request_attributes(span_exporter) -> None:
    settings = ChatSettings(temperature=0.2, max_tokens=128, top_p=0.9)
    with model_span(
        system="groq",
        model="llama-3.3-70b-versatile",
        settings=settings,
    ) as span:
        span.record_response(
            input_tokens=42,
            output_tokens=7,
            finish_reason="stop",
            response_model="llama-3.3-70b-versatile",
        )

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    s = spans[0]
    assert s.name == "chat llama-3.3-70b-versatile"
    a = _attrs(s)
    assert a["gen_ai.operation.name"] == "chat"
    assert a["gen_ai.system"] == "groq"
    assert a["gen_ai.provider.name"] == "groq"
    assert a["gen_ai.request.model"] == "llama-3.3-70b-versatile"
    assert a["gen_ai.request.temperature"] == pytest.approx(0.2)
    assert a["gen_ai.request.max_tokens"] == 128
    assert a["gen_ai.request.top_p"] == pytest.approx(0.9)
    assert a["gen_ai.usage.input_tokens"] == 42
    assert a["gen_ai.usage.output_tokens"] == 7
    assert list(a["gen_ai.response.finish_reasons"]) == ["stop"]
    assert a["gen_ai.response.model"] == "llama-3.3-70b-versatile"


def test_model_span_records_exception_as_error(span_exporter) -> None:
    with pytest.raises(RuntimeError):
        with model_span(system="openai", model="gpt-4o-mini"):
            raise RuntimeError("boom")

    (s,) = span_exporter.get_finished_spans()
    assert s.status.status_code.name == "ERROR"
    assert any("boom" in e.name or "boom" in str(e.attributes) for e in s.events)


def test_model_span_without_settings_still_records(span_exporter) -> None:
    with model_span(system="ollama", model="qwen2.5:7b") as span:
        span.record_response(finish_reason="stop")
    (s,) = span_exporter.get_finished_spans()
    a = _attrs(s)
    assert a["gen_ai.request.model"] == "qwen2.5:7b"
    # No temperature attribute was set.
    assert "gen_ai.request.temperature" not in a


# ---------------------------------------------------------------------------
# agent_span
# ---------------------------------------------------------------------------


def test_agent_span_carries_conversation_id_and_name(span_exporter) -> None:
    with agent_span(
        name="planner",
        description="planning specialist",
        conversation_id="conv-1",
    ) as span:
        span.record_completion(
            iterations=3,
            input_tokens=100,
            output_tokens=50,
            stop_reason="final_output",
        )

    (s,) = span_exporter.get_finished_spans()
    assert s.name == "invoke_agent planner"
    a = _attrs(s)
    assert a["gen_ai.operation.name"] == "invoke_agent"
    assert a["gen_ai.agent.name"] == "planner"
    assert a["gen_ai.agent.description"] == "planning specialist"
    assert a["gen_ai.conversation.id"] == "conv-1"
    assert a["gen_ai.usage.input_tokens"] == 100
    assert a["gen_ai.usage.output_tokens"] == 50
    assert a["koala.agent.iterations"] == 3
    assert a["koala.agent.stop_reason"] == "final_output"


# ---------------------------------------------------------------------------
# tool_span
# ---------------------------------------------------------------------------


def test_tool_span_records_arguments_and_result(span_exporter) -> None:
    with tool_span(
        name="add",
        call_id="call-1",
        arguments={"a": 7, "b": 5},
        description="Add two ints",
    ) as span:
        span.record_result("12", is_error=False)

    (s,) = span_exporter.get_finished_spans()
    assert s.name == "execute_tool add"
    a = _attrs(s)
    assert a["gen_ai.operation.name"] == "execute_tool"
    assert a["gen_ai.tool.name"] == "add"
    assert a["gen_ai.tool.call.id"] == "call-1"
    assert a["gen_ai.tool.description"] == "Add two ints"
    assert '"a": 7' in a["gen_ai.tool.call.arguments"]
    assert a["gen_ai.tool.call.result"] == "12"


def test_tool_span_is_error_sets_status(span_exporter) -> None:
    with tool_span(name="rm", call_id="c1", arguments={"path": "/etc"}) as span:
        span.record_result("permission denied", is_error=True)

    (s,) = span_exporter.get_finished_spans()
    assert s.status.status_code.name == "ERROR"


# ---------------------------------------------------------------------------
# End-to-end: real Agent run through OTel
# ---------------------------------------------------------------------------


def test_agent_run_emits_invoke_agent_and_chat_spans(span_exporter) -> None:
    # Sync entry point — Agent.run wraps asyncio.run() internally.
    from tests.agents.conftest import (
        assistant_text,
        make_scripted_model,
    )

    from koala import Agent

    model, _ = make_scripted_model([assistant_text("hi there")])
    agent = Agent(model, name="greeter")

    result = agent.run("say hi")
    assert result.output == "hi there"

    span_names = {s.name for s in span_exporter.get_finished_spans()}
    assert any(n.startswith("invoke_agent") for n in span_names)
    assert any(n.startswith("chat") for n in span_names)


def test_agent_with_tool_emits_execute_tool_span(span_exporter) -> None:
    from tests.agents.conftest import (
        assistant_text,
        assistant_tool_call,
        make_scripted_model,
    )

    from koala import Agent, tool

    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 7, "b": 5}, call_id="c1"),
            assistant_text("12"),
        ]
    )
    agent = Agent(model, tools=[add], name="calc")
    result = agent.run("what is 7+5?")
    assert result.stop_reason == "final_output"

    spans = span_exporter.get_finished_spans()
    tool_spans = [s for s in spans if s.name == "execute_tool add"]
    assert len(tool_spans) == 1
    a = _attrs(tool_spans[0])
    assert a["gen_ai.tool.name"] == "add"
    assert a["gen_ai.tool.call.id"] == "c1"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def test_safe_json_dumps_falls_back_on_non_serializable() -> None:
    class X:
        def __repr__(self) -> str:
            return "<X>"

    assert _safe_json_dumps({"x": X()}) == '{"x": "<X>"}'


def test_trim_keeps_short_text_unchanged() -> None:
    assert _trim("short", 100) == "short"


def test_trim_truncates_long_text_with_ellipsis() -> None:
    out = _trim("a" * 200, 50)
    assert len(out) == 50
    assert out.endswith("…")
