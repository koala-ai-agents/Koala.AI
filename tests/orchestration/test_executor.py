"""Tests for koala.orchestration.executor.LocalExecutor.

Covers every dispatch path (str/callable/Runnable/BaseTool/BaseAgent/Model),
$result and $input references, parallel execution, cycle guards, and errors.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

# Reuse the scripted provider from the agents test suite so we can drive
# agents deterministically inside flow tests.
_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))
from conftest import assistant_text, make_scripted_model  # noqa: E402

from koala import Agent, tool  # noqa: E402
from koala.core import Message, RunContext  # noqa: E402
from koala.orchestration import (  # noqa: E402
    LocalExecutor,
    StepExecutionError,
    flow,
)

# ---------------------------------------------------------------------------
# Plain-callable steps
# ---------------------------------------------------------------------------


def test_sync_callable_step_runs() -> None:
    def add(a: int, b: int) -> int:
        return a + b

    f = flow("math").step("sum", add, a=2, b=3).build()
    out = LocalExecutor().run(f)
    assert out == {"sum": 5}


@pytest.mark.asyncio
async def test_async_callable_step_runs() -> None:
    async def add(a: int, b: int) -> int:
        return a + b

    f = flow("math").step("sum", add, a=1, b=1).build()
    out = await LocalExecutor().arun(f)
    assert out == {"sum": 2}


def test_multiple_independent_steps_all_run() -> None:
    def one() -> int:
        return 1

    def two() -> int:
        return 2

    f = flow("x").step("a", one).step("b", two).build()
    out = LocalExecutor().run(f)
    assert out == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# $result references
# ---------------------------------------------------------------------------


def test_result_reference_resolves() -> None:
    def source() -> int:
        return 5

    def double(x: int) -> int:
        return x * 2

    f = (
        flow("x")
        .step("src", source)
        .step("dbl", double, x="$result.src")
        .edge("src", "dbl")
        .build()
    )
    out = LocalExecutor().run(f)
    assert out == {"src": 5, "dbl": 10}


def test_result_reference_dotted_dict_access() -> None:
    def make_dict() -> dict:
        return {"a": {"b": 42}}

    def read(value: int) -> int:
        return value

    f = (
        flow("x")
        .step("d", make_dict)
        .step("r", read, value="$result.d.a.b")
        .edge("d", "r")
        .build()
    )
    out = LocalExecutor().run(f)
    assert out["r"] == 42


def test_result_reference_to_unknown_step_fails() -> None:
    def read(x: int) -> int:
        return x

    f = flow("x").step("r", read, x="$result.missing").build()
    with pytest.raises(StepExecutionError):
        LocalExecutor().run(f)


# ---------------------------------------------------------------------------
# $input references
# ---------------------------------------------------------------------------


def test_input_reference_resolves() -> None:
    def echo(msg: str) -> str:
        return f"got: {msg}"

    f = flow("x").step("e", echo, msg="$input.topic").build()
    out = LocalExecutor().run(f, input={"topic": "koalas"})
    assert out == {"e": "got: koalas"}


def test_input_reference_missing_key_fails() -> None:
    def echo(msg: str) -> str:
        return msg

    f = flow("x").step("e", echo, msg="$input.missing").build()
    with pytest.raises(StepExecutionError):
        LocalExecutor().run(f, input={})


# ---------------------------------------------------------------------------
# Legacy string-registry dispatch
# ---------------------------------------------------------------------------


def test_string_action_resolves_via_registry() -> None:
    def add(a: int, b: int) -> int:
        return a + b

    f = flow("x").step("s", "add_op", a=3, b=4).build()
    out = LocalExecutor(registry={"add_op": add}).run(f)
    assert out == {"s": 7}


def test_string_action_not_in_registry_fails() -> None:
    f = flow("x").step("s", "missing_op").build()
    with pytest.raises(StepExecutionError):
        LocalExecutor().run(f)


# ---------------------------------------------------------------------------
# BaseTool step
# ---------------------------------------------------------------------------


def test_basetool_step_receives_args_dict() -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    f = flow("x").step("t", add, a=10, b=5).build()
    out = LocalExecutor().run(f)
    assert out == {"t": 15}


# ---------------------------------------------------------------------------
# Agent step
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_step_with_prompt_kwarg() -> None:
    model, _ = make_scripted_model([assistant_text("bright and shiny")])
    agent = Agent(model, name="poet")

    f = flow("x").step("write", agent, prompt="describe a koala").build()
    out = await LocalExecutor().arun(f)
    assert out == {"write": "bright and shiny"}


@pytest.mark.asyncio
async def test_agent_step_with_single_kwarg_shorthand() -> None:
    """Single-kwarg calls unambiguously feed the value as the agent's prompt."""
    model, _ = make_scripted_model([assistant_text("42")])
    agent = Agent(model)

    f = flow("x").step("q", agent, question="what is meaning?").build()
    out = await LocalExecutor().arun(f)
    assert out == {"q": "42"}


@pytest.mark.asyncio
async def test_agent_step_result_flows_into_next_agent() -> None:
    model_1, _ = make_scripted_model([assistant_text("koalas sleep 20h a day")])
    model_2, provider_2 = make_scripted_model(
        [assistant_text("[polished] koalas sleep 20h a day")]
    )
    research = Agent(model_1, name="research")
    write = Agent(model_2, name="write")

    f = (
        flow("pipeline")
        .step("research", research, prompt="tell me about koalas")
        .step("write", write, prompt="$result.research")
        .edge("research", "write")
        .build()
    )
    out = await LocalExecutor().arun(f)

    assert out["research"] == "koalas sleep 20h a day"
    assert out["write"].startswith("[polished]")
    # Second agent's model received the first's output as its user prompt.
    sent = provider_2.calls[0]["messages"]
    joined = " ".join(m.text for m in sent if m.text)
    assert "koalas sleep 20h a day" in joined


@pytest.mark.asyncio
async def test_agent_step_no_args_fails() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(model)

    f = flow("x").step("bad", agent).build()
    with pytest.raises(StepExecutionError):
        await LocalExecutor().arun(f)


@pytest.mark.asyncio
async def test_agent_step_multiple_ambiguous_kwargs_fails() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(model)

    f = flow("x").step("bad", agent, a="one", b="two").build()
    with pytest.raises(StepExecutionError):
        await LocalExecutor().arun(f)


# ---------------------------------------------------------------------------
# Model step
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_model_step_with_prompt() -> None:
    model, provider = make_scripted_model([assistant_text("hi from model")])

    f = flow("x").step("m", model, prompt="say hi").build()
    out = await LocalExecutor().arun(f)
    assert out == {"m": "hi from model"}
    # Ensure the model actually saw a user message with our prompt.
    sent = provider.calls[0]["messages"]
    assert any(m.role == "user" and m.text == "say hi" for m in sent)


@pytest.mark.asyncio
async def test_model_step_with_explicit_messages() -> None:
    model, provider = make_scripted_model([assistant_text("ok")])

    msgs = [Message.system("be brief"), Message.user("hi")]
    f = flow("x").step("m", model, messages=msgs).build()
    out = await LocalExecutor().arun(f)
    assert out == {"m": "ok"}
    sent = provider.calls[0]["messages"]
    assert [m.role for m in sent] == ["system", "user"]


@pytest.mark.asyncio
async def test_model_step_requires_prompt_or_messages() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])

    f = flow("x").step("m", model).build()
    with pytest.raises(StepExecutionError):
        await LocalExecutor().arun(f)


# ---------------------------------------------------------------------------
# Cycle detection at execution time (edge cases missed by build)
# ---------------------------------------------------------------------------


def test_empty_flow_returns_empty_dict() -> None:
    f = flow("empty").build()
    assert LocalExecutor().run(f) == {}


# ---------------------------------------------------------------------------
# Failure propagation + cleanup
# ---------------------------------------------------------------------------


def test_step_exception_wrapped_in_step_execution_error() -> None:
    def boom() -> None:
        raise ValueError("kaboom")

    f = flow("x").step("bad", boom).build()
    with pytest.raises(StepExecutionError) as exc:
        LocalExecutor().run(f)
    assert exc.value.step_id == "bad"
    assert isinstance(exc.value.original, ValueError)


def test_downstream_steps_dont_run_after_failure() -> None:
    ran: list[str] = []

    def crash() -> None:
        ran.append("crash")
        raise RuntimeError("stop")

    def should_not_run() -> None:
        ran.append("second")

    f = (
        flow("x")
        .step("a", crash)
        .step("b", should_not_run)
        .edge("a", "b")
        .build()
    )
    with pytest.raises(StepExecutionError):
        LocalExecutor().run(f)
    assert ran == ["crash"]


# ---------------------------------------------------------------------------
# Parallel execution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_independent_async_steps_run_concurrently() -> None:
    """Two independent async steps that each sleep 0.1s should finish in
    ~0.1s total (not 0.2s), proving concurrency."""

    async def slow(x: int) -> int:
        await asyncio.sleep(0.1)
        return x

    f = (
        flow("parallel")
        .step("a", slow, x=1)
        .step("b", slow, x=2)
        .step("c", slow, x=3)
        .build()
    )
    start = time.perf_counter()
    out = await LocalExecutor().arun(f)
    elapsed = time.perf_counter() - start
    assert out == {"a": 1, "b": 2, "c": 3}
    # Wall-clock should be closer to 0.1s than 0.3s — allow generous headroom
    # for slow CI without letting a sequential regression pass.
    assert elapsed < 0.25, f"expected concurrent execution (~0.1s), got {elapsed:.2f}s"


# ---------------------------------------------------------------------------
# Type-dispatch sanity
# ---------------------------------------------------------------------------


def test_unsupported_action_type_raises() -> None:
    # int is not callable / str / Runnable — should error clearly.
    f = flow("x").step("bad", 42).build()  # type: ignore[arg-type]
    with pytest.raises(StepExecutionError):
        LocalExecutor().run(f)


# ---------------------------------------------------------------------------
# Deps propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deps_reach_agent_via_run_context() -> None:
    """deps= on run/arun is threaded through ctx to agents/tools."""

    seen: dict[str, object] = {}

    @tool
    def peek(ctx: RunContext, marker: str) -> str:
        """Read deps out of the run context."""
        seen["deps"] = ctx.deps
        return marker

    f = flow("x").step("t", peek, marker="ok").build()
    result = await LocalExecutor().arun(f, deps={"db": "pool"})
    assert result == {"t": "ok"}
    assert seen["deps"] == {"db": "pool"}
