"""Tests for FunctionTool + the @tool decorator."""

from __future__ import annotations

from typing import Annotated

import pytest

from koala import tool
from koala.core import (
    Done,
    Output,
    RunContext,
    Runnable,
    Start,
    acollect,
    ainvoke,
)
from koala.tools import (
    BaseTool,
    FunctionTool,
    ToolExecutionError,
    ToolValidationError,
)

# ---------------------------------------------------------------------------
# Decorator variants
# ---------------------------------------------------------------------------


def test_bare_decorator_uses_function_name() -> None:
    @tool
    def multiply(a: int, b: int) -> int:
        """Multiply two numbers."""
        return a * b

    assert multiply.name == "multiply"
    assert multiply.description == "Multiply two numbers."


def test_decorator_with_string_name() -> None:
    @tool("mul")
    def multiply(a: int, b: int) -> int:
        return a * b

    assert multiply.name == "mul"


def test_decorator_with_kwargs() -> None:
    @tool(name="mul", description="Custom description.")
    def multiply(a: int, b: int) -> int:
        return a * b

    assert multiply.name == "mul"
    assert multiply.description == "Custom description."


def test_decorator_kwargs_only_no_positional() -> None:
    @tool(description="from kwargs")
    def foo(a: int) -> int:
        return a

    assert foo.name == "foo"
    assert foo.description == "from kwargs"


def test_bare_decorator_rejects_kwargs_via_typeerror() -> None:
    def foo(a: int) -> int:
        return a

    with pytest.raises(TypeError):
        # Simulate the (invalid) form: tool(foo, name="x")
        tool(foo, name="x")  # type: ignore[call-overload]


def test_decorator_type_error_on_bad_arg() -> None:
    with pytest.raises(TypeError):
        tool(123)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Sync + async execution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_function_tool_runs() -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    result = await add.run(RunContext(deps=None), {"a": 2, "b": 3})
    assert result == 5


@pytest.mark.asyncio
async def test_async_function_tool_runs() -> None:
    @tool
    async def greet(name: str) -> str:
        """Greet."""
        return f"hello, {name}"

    result = await greet.run(RunContext(deps=None), {"name": "world"})
    assert result == "hello, world"


@pytest.mark.asyncio
async def test_sync_tool_does_not_block_event_loop() -> None:
    """Sync tools execute in a worker thread so a blocking call doesn't
    freeze the event loop."""
    import time

    @tool
    def blocker(ms: int) -> str:
        """Sleep, return 'ok'."""
        time.sleep(ms / 1000)
        return "ok"

    result = await blocker.run(RunContext(deps=None), {"ms": 10})
    assert result == "ok"


# ---------------------------------------------------------------------------
# RunContext injection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_context_is_injected_by_type_annotation() -> None:
    @tool
    async def fetch(ctx: RunContext, key: str) -> str:
        """Fetch."""
        return f"{key}:{ctx.session_id[:4]}"

    ctx = RunContext(deps=None)
    result = await fetch.run(ctx, {"key": "hello"})
    assert result.startswith("hello:")
    # ctx must NOT appear in the schema (model shouldn't see it)
    assert "ctx" not in fetch.schema["properties"]


@pytest.mark.asyncio
async def test_run_context_generic_hint_is_detected() -> None:
    @tool
    async def with_deps(ctx: RunContext[dict], key: str) -> str:
        """With deps."""
        return f"{key}:{ctx.deps.get('name', '?')}"

    ctx: RunContext[dict] = RunContext(deps={"name": "koala"})
    result = await with_deps.run(ctx, {"key": "x"})
    assert result == "x:koala"


@pytest.mark.asyncio
async def test_ctx_by_convention_without_type_hint_not_injected() -> None:
    """An untyped `ctx` param without a RunContext hint is a schema error."""
    with pytest.raises(TypeError, match="no type hint"):

        @tool
        def wrong(ctx, x: int) -> int:  # type: ignore[no-untyped-def]
            return x


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_required_arg_raises_validation_error() -> None:
    @tool
    def foo(a: int, b: int) -> int:
        """Foo."""
        return a + b

    with pytest.raises(ToolValidationError) as exc:
        await foo.run(RunContext(deps=None), {"a": 1})
    assert exc.value.tool_name == "foo"
    assert any("b" in e for e in exc.value.errors)


@pytest.mark.asyncio
async def test_wrong_type_raises_validation_error() -> None:
    @tool
    def foo(a: int) -> int:
        """Foo."""
        return a

    with pytest.raises(ToolValidationError):
        await foo.run(RunContext(deps=None), {"a": "not-a-number"})


@pytest.mark.asyncio
async def test_defaults_are_applied() -> None:
    @tool
    def foo(a: int, b: int = 10) -> int:
        """Foo."""
        return a + b

    assert await foo.run(RunContext(deps=None), {"a": 1}) == 11


@pytest.mark.asyncio
async def test_types_are_coerced_in_lax_mode() -> None:
    @tool
    def foo(a: int) -> int:
        """Foo."""
        return a * 2

    # Pydantic default lax mode coerces "5" -> 5
    assert await foo.run(RunContext(deps=None), {"a": "5"}) == 10


# ---------------------------------------------------------------------------
# Execution errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_function_exception_wrapped_in_execution_error() -> None:
    @tool
    def divide(a: int, b: int) -> float:
        """Divide."""
        return a / b

    with pytest.raises(ToolExecutionError) as exc:
        await divide.run(RunContext(deps=None), {"a": 1, "b": 0})
    assert exc.value.tool_name == "divide"
    assert isinstance(exc.value.original, ZeroDivisionError)


@pytest.mark.asyncio
async def test_validation_error_not_double_wrapped() -> None:
    """If a tool internally raises ToolValidationError, don't re-wrap it."""

    @tool
    def foo(a: int) -> int:
        """Foo."""
        raise ToolValidationError("foo", ["custom validation msg"])

    with pytest.raises(ToolValidationError) as exc:
        await foo.run(RunContext(deps=None), {"a": 1})
    assert exc.value.errors == ["custom validation msg"]


# ---------------------------------------------------------------------------
# Schema surface
# ---------------------------------------------------------------------------


def test_openai_schema_shape() -> None:
    @tool
    def get_weather(
        city: Annotated[str, "The city to look up."],
    ) -> str:
        """Get the weather."""
        return "sunny"

    schema = get_weather.to_openai_schema()
    assert schema["type"] == "function"
    fn = schema["function"]
    assert fn["name"] == "get_weather"
    assert fn["description"] == "Get the weather."
    assert fn["parameters"]["properties"]["city"]["type"] == "string"
    assert (
        fn["parameters"]["properties"]["city"]["description"]
        == "The city to look up."
    )
    assert fn["parameters"]["required"] == ["city"]


# ---------------------------------------------------------------------------
# L1 Runnable conformance
# ---------------------------------------------------------------------------


def test_tool_satisfies_runnable_protocol() -> None:
    @tool
    def echo(text: str) -> str:
        """Echo."""
        return text

    assert isinstance(echo, Runnable)


@pytest.mark.asyncio
async def test_astream_emits_start_output_done() -> None:
    @tool
    def echo(text: str) -> str:
        """Echo."""
        return text

    events = await acollect(echo, RunContext(deps=None), {"text": "hi"})
    kinds = [e.kind for e in events]
    assert kinds == ["start", "output", "done"]
    assert isinstance(events[0], Start)
    assert isinstance(events[-1], Done)
    assert isinstance(events[1], Output)
    assert events[1].value == "hi"


@pytest.mark.asyncio
async def test_ainvoke_returns_tool_result() -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    result = await ainvoke(add, RunContext(deps=None), {"a": 2, "b": 3})
    assert result == 5


@pytest.mark.asyncio
async def test_astream_emits_error_event_on_exception() -> None:
    from koala.core import Error

    @tool
    def bad(a: int) -> int:
        """Bad."""
        raise RuntimeError("boom")

    events = await acollect(bad, RunContext(deps=None), {"a": 1})
    error_events = [e for e in events if isinstance(e, Error)]
    assert len(error_events) == 1
    assert "boom" in error_events[0].error
    # No Output when there was a fatal Error
    assert not any(e.kind == "output" for e in events)


# ---------------------------------------------------------------------------
# BaseTool remains abstract
# ---------------------------------------------------------------------------


def test_base_tool_is_abstract() -> None:
    with pytest.raises(TypeError):
        BaseTool()  # type: ignore[abstract]


def test_repr_uses_class_name_and_tool_name() -> None:
    @tool
    def foo(a: int) -> int:
        """Foo."""
        return a

    assert repr(foo) == "FunctionTool(name='foo')"


def test_function_tool_direct_construction() -> None:
    def my_func(x: int) -> int:
        """My."""
        return x

    ft = FunctionTool(my_func, name="custom", description="d")
    assert ft.name == "custom"
    assert ft.description == "d"
