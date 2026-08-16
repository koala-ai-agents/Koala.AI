"""Behavior protocol and the AgentSpec builder.

A ``Behavior`` is a small composable bundle that contributes to an agent's
configuration — instructions, tools, approval rules, output type, and model
settings. Behaviors are applied in order when an ``Agent`` is constructed,
each getting a chance to mutate a shared ``AgentSpec``.

The idea is to compose reusable slices of agent config so you don't have to
copy the same tools/rules/persona across every agent that shares them:

    acme_toolpack = ToolPack(lookup_order, refund)
    acme_safety   = ApprovalPolicy(DenyList({"delete_customer"}))

    support = Agent(model="...", behaviors=[
        Persona("You are Sam, a friendly support agent."),
        acme_toolpack,
        acme_safety,
    ])
    sales = Agent(model="...", behaviors=[
        Persona("You are Alex, a sales agent."),
        acme_toolpack,     # reused
        acme_safety,       # reused
    ])
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from ..core.approval import ApprovalRule
from ..models.settings import ChatSettings
from ..tools.base import BaseTool


@dataclass
class AgentSpec:
    """Mutable builder passed through each behavior in order.

    Attributes:
        instructions_parts: Ordered list of instruction strings that will be
            joined with double newlines to form the final system prompt.
            Empty entries are filtered out.
        tools: Ordered list of tools available to the agent.
        approval_rules: Ordered chain of approval rules — first non-None
            decision wins.
        settings: Merged chat settings layered onto the model's defaults.
        output_type: Optional Pydantic model class for structured output.
            Later writers win (behaviors applied later override earlier).
        metadata: Free-form dict for behaviors to stash bookkeeping.
    """

    instructions_parts: list[str] = field(default_factory=list)
    tools: list[BaseTool] = field(default_factory=list)
    approval_rules: list[ApprovalRule] = field(default_factory=list)
    settings: ChatSettings = field(default_factory=ChatSettings)
    output_type: type | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Behavior(Protocol):
    """A composable bundle of agent behavior.

    Implementations set a ``name`` attribute for introspection and implement
    ``apply(spec)`` to mutate the shared ``AgentSpec``. Behaviors are applied
    in the order they appear in ``Agent(behaviors=[...])``, so later
    behaviors can layer on top of or override earlier ones.
    """

    name: str

    def apply(self, spec: AgentSpec) -> None:
        """Mutate the spec — append instructions, extend tools, etc."""
        ...
