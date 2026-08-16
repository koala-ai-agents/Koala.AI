"""Integration tests — Agent + behaviors composed together."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import BaseModel

# Reuse the scripted provider from the agents tests.
_AGENTS_DIR = Path(__file__).resolve().parent.parent / "agents"
sys.path.insert(0, str(_AGENTS_DIR))
from conftest import (  # noqa: E402
    assistant_text,
    assistant_tool_call,
    make_scripted_model,
)

from koala import (  # noqa: E402  # noqa: E402
    Agent,
    ApprovalPolicy,
    ModelSettings,
    OutputSchema,
    Persona,
    ToolPack,
    tool,
)
from koala.behaviors import AgentSpec, Behavior  # noqa: E402
from koala.models.settings import ChatSettings  # noqa: E402
from koala.tools import DenyList  # noqa: E402

# ---------------------------------------------------------------------------
# Behaviors compose into the agent's final state
# ---------------------------------------------------------------------------


def test_agent_persona_contributes_to_instructions() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(
        model,
        behaviors=[Persona("You are a haiku poet.")],
    )
    assert agent.instructions == "You are a haiku poet."


def test_explicit_instructions_and_persona_combine() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(
        model,
        instructions="Be brief.",
        behaviors=[Persona("You are Sam.")],
    )
    # Explicit instructions come first; behavior contributions follow.
    assert agent.instructions == "Be brief.\n\nYou are Sam."


def test_multiple_personas_stack_in_order() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(
        model,
        behaviors=[
            Persona("Base identity: helpful."),
            Persona("Domain: customer support."),
        ],
    )
    assert (
        agent.instructions
        == "Base identity: helpful.\n\nDomain: customer support."
    )


def test_toolpack_contributes_tools() -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    @tool
    def sub(a: int, b: int) -> int:
        """Subtract."""
        return a - b

    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(model, behaviors=[ToolPack(add, sub)])
    assert [t.name for t in agent.tools] == ["add", "sub"]


def test_explicit_tools_and_toolpack_both_contribute() -> None:
    @tool
    def a() -> int:
        """A."""
        return 1

    @tool
    def b() -> int:
        """B."""
        return 2

    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(model, tools=[a], behaviors=[ToolPack(b)])
    assert [t.name for t in agent.tools] == ["a", "b"]


def test_duplicate_tool_names_across_explicit_and_behavior_fails() -> None:
    @tool
    def dup() -> int:
        """dup."""
        return 1

    model, _ = make_scripted_model([assistant_text("ok")])
    with pytest.raises(ValueError, match="Duplicate tool name"):
        Agent(model, tools=[dup], behaviors=[ToolPack(dup)])


def test_approval_policy_contributes_rules() -> None:
    rule = DenyList(names=frozenset({"rm"}))
    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(model, behaviors=[ApprovalPolicy(rule)])
    assert agent.approval_rules == [rule]


def test_output_schema_sets_agent_output_type() -> None:
    class Report(BaseModel):
        title: str

    model, _ = make_scripted_model([assistant_text('{"title": "hi"}')])
    agent = Agent(model, behaviors=[OutputSchema(Report)])
    assert agent.output_type is Report


def test_behavior_overrides_explicit_output_type() -> None:
    class A(BaseModel):
        x: int

    class B(BaseModel):
        y: str

    model, _ = make_scripted_model([assistant_text("{}")])
    agent = Agent(model, output_type=A, behaviors=[OutputSchema(B)])
    # Behaviors apply after explicit args; later wins.
    assert agent.output_type is B


def test_model_settings_layers_over_agent_settings() -> None:
    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(
        model,
        settings=ChatSettings(temperature=0.1, max_tokens=100),
        behaviors=[ModelSettings(ChatSettings(temperature=0.9))],
    )
    assert agent.settings.temperature == 0.9  # overridden
    assert agent.settings.max_tokens == 100  # preserved


# ---------------------------------------------------------------------------
# Agent runs correctly after behaviors are applied
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_with_behaviors_actually_runs() -> None:
    @tool
    def add(a: int, b: int) -> int:
        """Add."""
        return a + b

    model, _ = make_scripted_model(
        [
            assistant_tool_call("add", {"a": 2, "b": 3}, call_id="c1"),
            assistant_text("5"),
        ]
    )
    agent = Agent(
        model,
        behaviors=[
            Persona("You are a math helper."),
            ToolPack(add),
        ],
    )
    result = await agent.arun("2 + 3")
    assert result.output == "5"


@pytest.mark.asyncio
async def test_persona_appears_in_first_model_call() -> None:
    model, provider = make_scripted_model([assistant_text("ok")])
    agent = Agent(
        model,
        behaviors=[Persona("You are Sam.")],
    )
    await agent.arun("hi")

    sent = provider.calls[0]["messages"]
    system = next(m for m in sent if m.role == "system")
    assert "You are Sam." in system.text


# ---------------------------------------------------------------------------
# Reusability — same behaviors across multiple agents
# ---------------------------------------------------------------------------


def test_behaviors_can_be_shared_across_agents() -> None:
    @tool
    def shared_tool() -> int:
        """A shared tool."""
        return 1

    common_tools = ToolPack(shared_tool)
    common_safety = ApprovalPolicy(DenyList(names=frozenset({"rm"})))

    model1, _ = make_scripted_model([assistant_text("a")])
    model2, _ = make_scripted_model([assistant_text("b")])

    agent1 = Agent(
        model1,
        behaviors=[Persona("Agent one."), common_tools, common_safety],
    )
    agent2 = Agent(
        model2,
        behaviors=[Persona("Agent two."), common_tools, common_safety],
    )

    assert [t.name for t in agent1.tools] == ["shared_tool"]
    assert [t.name for t in agent2.tools] == ["shared_tool"]
    assert agent1.approval_rules == agent2.approval_rules
    assert agent1.instructions == "Agent one."
    assert agent2.instructions == "Agent two."


# ---------------------------------------------------------------------------
# Custom user-defined behaviors work
# ---------------------------------------------------------------------------


def test_user_defined_behavior_via_protocol() -> None:
    class TenantTagger:
        """Custom behavior that stashes a tenant id in metadata."""

        name = "tenant_tagger"

        def __init__(self, tenant_id: str) -> None:
            self.tenant_id = tenant_id

        def apply(self, spec: AgentSpec) -> None:
            spec.metadata["tenant_id"] = self.tenant_id
            spec.instructions_parts.append(
                f"Serving tenant {self.tenant_id}."
            )

    inst = TenantTagger("acme-42")
    assert isinstance(inst, Behavior)  # structural check

    model, _ = make_scripted_model([assistant_text("ok")])
    agent = Agent(model, behaviors=[inst])
    assert agent.instructions is not None
    assert "acme-42" in agent.instructions


def test_behavior_without_apply_method_raises() -> None:
    class NotABehavior:
        name = "nope"

    model, _ = make_scripted_model([assistant_text("ok")])
    with pytest.raises(TypeError, match="apply"):
        Agent(model, behaviors=[NotABehavior()])  # type: ignore[list-item]


# ---------------------------------------------------------------------------
# Behaviors compose with handoffs
# ---------------------------------------------------------------------------


def test_behaviors_apply_after_handoff_tools() -> None:
    """Handoff-agent tools go in first; ToolPack tools stack on top."""

    @tool
    def extra() -> int:
        """Extra."""
        return 1

    inner_model, _ = make_scripted_model([assistant_text("inner")])
    inner = Agent(inner_model, name="inner")

    outer_model, _ = make_scripted_model([assistant_text("outer")])
    outer = Agent(
        outer_model,
        handoffs=[inner],
        behaviors=[ToolPack(extra)],
    )

    names = [t.name for t in outer.tools]
    assert "transfer_to_inner" in names
    assert "extra" in names
    # Handoff comes before behavior-added tools.
    assert names.index("transfer_to_inner") < names.index("extra")
