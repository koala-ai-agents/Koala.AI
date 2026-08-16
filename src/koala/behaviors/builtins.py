"""Built-in behaviors for common composition patterns.

These cover the 80/20 cases users hit when they start composing agents:
persona, reusable toolpacks, approval policies, structured output schemas,
and per-agent model-setting overrides. Users add their own by writing any
class with a ``name`` attribute and an ``apply(spec)`` method — see
:class:`~koala.behaviors.base.Behavior`.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel

from ..core.approval import ApprovalRule
from ..models.settings import ChatSettings
from ..tools.base import BaseTool
from .base import AgentSpec


@dataclass
class Persona:
    """Append persona text to the agent's system prompt.

    Args:
        description: Text describing the persona / voice / identity.
        name: Behavior name for introspection (default ``"persona"``).
    """

    description: str
    name: str = "persona"

    def apply(self, spec: AgentSpec) -> None:
        text = self.description.strip()
        if text:
            spec.instructions_parts.append(text)


class ToolPack:
    """A named collection of tools that plug into an agent as a unit.

    Constructed with tools as positional args::

        pack = ToolPack(get_weather, search_web, name="research")
    """

    __slots__ = ("tools", "name")

    def __init__(
        self,
        *tools: BaseTool,
        name: str = "toolpack",
    ) -> None:
        self.tools: list[BaseTool] = list(tools)
        self.name = name

    def apply(self, spec: AgentSpec) -> None:
        spec.tools.extend(self.tools)

    def __repr__(self) -> str:
        return (
            f"ToolPack(name={self.name!r}, "
            f"tools={[t.name for t in self.tools]})"
        )


class ApprovalPolicy:
    """A named chain of approval rules for tool calls.

    Constructed with rules as positional args::

        policy = ApprovalPolicy(
            DenyList({"rm", "drop_table"}),
            RequireApprovalFor(prefixes={"delete_"}),
            name="production_safety",
        )
    """

    __slots__ = ("rules", "name")

    def __init__(
        self,
        *rules: ApprovalRule,
        name: str = "approval_policy",
    ) -> None:
        self.rules: list[ApprovalRule] = list(rules)
        self.name = name

    def apply(self, spec: AgentSpec) -> None:
        spec.approval_rules.extend(self.rules)

    def __repr__(self) -> str:
        return (
            f"ApprovalPolicy(name={self.name!r}, "
            f"rules={[type(r).__name__ for r in self.rules]})"
        )


@dataclass
class OutputSchema:
    """Force the agent to return a structured Pydantic model.

    Args:
        model: Pydantic ``BaseModel`` subclass — becomes ``Agent.output_type``.
        name: Behavior name (default ``"output_schema"``).

    Note: if the caller also passes ``output_type=...`` explicitly to the
    Agent, whichever runs later wins (behaviors apply after explicit args,
    so this behavior will override).
    """

    model: type[BaseModel]
    name: str = "output_schema"

    def __post_init__(self) -> None:
        if not (isinstance(self.model, type) and issubclass(self.model, BaseModel)):
            raise TypeError(
                "OutputSchema.model must be a Pydantic BaseModel subclass."
            )

    def apply(self, spec: AgentSpec) -> None:
        spec.output_type = self.model


@dataclass
class ModelSettings:
    """Layer chat settings (temperature, max_tokens, ...) onto the agent.

    Merges over any settings the caller passed to ``Agent(settings=...)`` and
    over any earlier behavior's ``ModelSettings``.
    """

    settings: ChatSettings
    name: str = "model_settings"

    def apply(self, spec: AgentSpec) -> None:
        spec.settings = spec.settings.merge(self.settings)
