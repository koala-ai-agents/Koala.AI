"""Approval rules for tool calls.

Each tool call runs through an ordered list of `ApprovalRule`s. Each rule
returns one of {"allow", "deny", "ask"}, or `None` to defer to the next rule.
If every rule defers, the default is `"ask"` — a safe fallback that surfaces
the decision to the caller via an `AwaitingApproval` event.

The rule chain runner itself lives in a higher layer (harness / tools); L1
defines only the shape so nothing downstream has to invent its own vocabulary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

from .context import RunContext
from .messages import ToolCallBlock

ApprovalDecision = Literal["allow", "deny", "ask"]


@runtime_checkable
class ApprovalRule(Protocol):
    """A rule that decides whether a proposed tool call should proceed.

    Return an `ApprovalDecision` to make a call. Return `None` to defer the
    decision to the next rule in the chain.
    """

    def check(
        self, ctx: RunContext, call: ToolCallBlock, /
    ) -> ApprovalDecision | None:
        ...


@dataclass(frozen=True, slots=True)
class ApprovalResult:
    """The final decision from evaluating a chain of ApprovalRules."""

    decision: ApprovalDecision
    reason: str = ""
    rule_name: str | None = None
