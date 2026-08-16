"""Unit tests for koala.behaviors built-ins and the Behavior protocol."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from koala import tool
from koala.behaviors import (
    AgentSpec,
    ApprovalPolicy,
    Behavior,
    ModelSettings,
    OutputSchema,
    Persona,
    ToolPack,
)
from koala.models.settings import ChatSettings
from koala.tools import AlwaysAllow, DenyList

# ---------------------------------------------------------------------------
# AgentSpec + Behavior protocol
# ---------------------------------------------------------------------------


def test_agent_spec_defaults_are_empty() -> None:
    spec = AgentSpec()
    assert spec.instructions_parts == []
    assert spec.tools == []
    assert spec.approval_rules == []
    assert spec.output_type is None
    assert isinstance(spec.settings, ChatSettings)


def test_all_builtins_satisfy_behavior_protocol() -> None:
    for b in [
        Persona("x"),
        ToolPack(),
        ApprovalPolicy(),
        OutputSchema(_TestSchema),
        ModelSettings(ChatSettings(temperature=0.5)),
    ]:
        assert isinstance(b, Behavior)


class _TestSchema(BaseModel):
    x: int


def test_custom_behavior_satisfies_protocol() -> None:
    class MyCap:
        name = "custom"

        def apply(self, spec: AgentSpec) -> None:
            spec.metadata["custom"] = True

    inst = MyCap()
    assert isinstance(inst, Behavior)

    spec = AgentSpec()
    inst.apply(spec)
    assert spec.metadata == {"custom": True}


# ---------------------------------------------------------------------------
# Persona
# ---------------------------------------------------------------------------


def test_persona_appends_instructions() -> None:
    spec = AgentSpec()
    Persona("You are Sam.").apply(spec)
    assert spec.instructions_parts == ["You are Sam."]


def test_persona_default_name() -> None:
    assert Persona("x").name == "persona"


def test_persona_custom_name() -> None:
    assert Persona("x", name="voice").name == "voice"


def test_persona_ignores_empty_or_whitespace() -> None:
    spec = AgentSpec()
    Persona("").apply(spec)
    Persona("   ").apply(spec)
    assert spec.instructions_parts == []


def test_multiple_personas_stack() -> None:
    spec = AgentSpec()
    Persona("Persona A").apply(spec)
    Persona("Persona B").apply(spec)
    assert spec.instructions_parts == ["Persona A", "Persona B"]


# ---------------------------------------------------------------------------
# ToolPack
# ---------------------------------------------------------------------------


def test_toolpack_extends_tools_via_varargs() -> None:
    @tool
    def a() -> int:
        """A."""
        return 1

    @tool
    def b() -> int:
        """B."""
        return 2

    spec = AgentSpec()
    ToolPack(a, b).apply(spec)
    assert [t.name for t in spec.tools] == ["a", "b"]


def test_empty_toolpack_is_noop() -> None:
    spec = AgentSpec()
    ToolPack().apply(spec)
    assert spec.tools == []


def test_toolpack_repr_shows_tool_names() -> None:
    @tool
    def x() -> int:
        """X."""
        return 1

    tp = ToolPack(x, name="mypack")
    assert "mypack" in repr(tp)
    assert "'x'" in repr(tp)


def test_multiple_toolpacks_accumulate() -> None:
    @tool
    def a() -> int:
        """A."""
        return 1

    @tool
    def b() -> int:
        """B."""
        return 2

    spec = AgentSpec()
    ToolPack(a, name="p1").apply(spec)
    ToolPack(b, name="p2").apply(spec)
    assert [t.name for t in spec.tools] == ["a", "b"]


# ---------------------------------------------------------------------------
# ApprovalPolicy
# ---------------------------------------------------------------------------


def test_approval_policy_extends_rules() -> None:
    spec = AgentSpec()
    rule = DenyList(names=frozenset({"rm"}))
    ApprovalPolicy(rule).apply(spec)
    assert spec.approval_rules == [rule]


def test_approval_policy_multiple_rules_in_order() -> None:
    spec = AgentSpec()
    r1 = DenyList(names=frozenset({"rm"}))
    r2 = AlwaysAllow()
    ApprovalPolicy(r1, r2).apply(spec)
    assert spec.approval_rules == [r1, r2]


def test_approval_policy_repr_lists_rule_types() -> None:
    p = ApprovalPolicy(
        DenyList(names=frozenset({"rm"})),
        AlwaysAllow(),
        name="prod_safety",
    )
    r = repr(p)
    assert "prod_safety" in r
    assert "DenyList" in r
    assert "AlwaysAllow" in r


# ---------------------------------------------------------------------------
# OutputSchema
# ---------------------------------------------------------------------------


def test_output_schema_sets_output_type() -> None:
    class Report(BaseModel):
        title: str

    spec = AgentSpec()
    OutputSchema(Report).apply(spec)
    assert spec.output_type is Report


def test_output_schema_later_overrides_earlier() -> None:
    class A(BaseModel):
        x: int

    class B(BaseModel):
        y: str

    spec = AgentSpec()
    OutputSchema(A).apply(spec)
    OutputSchema(B).apply(spec)
    assert spec.output_type is B


def test_output_schema_rejects_non_basemodel() -> None:
    with pytest.raises(TypeError, match="Pydantic BaseModel"):
        OutputSchema(int)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# ModelSettings
# ---------------------------------------------------------------------------


def test_model_settings_merges() -> None:
    spec = AgentSpec(settings=ChatSettings(temperature=0.1, max_tokens=100))
    ModelSettings(ChatSettings(temperature=0.9)).apply(spec)
    assert spec.settings.temperature == 0.9  # overridden
    assert spec.settings.max_tokens == 100  # preserved


def test_model_settings_multiple_stack() -> None:
    spec = AgentSpec()
    ModelSettings(ChatSettings(temperature=0.5)).apply(spec)
    ModelSettings(ChatSettings(max_tokens=50)).apply(spec)
    assert spec.settings.temperature == 0.5
    assert spec.settings.max_tokens == 50
