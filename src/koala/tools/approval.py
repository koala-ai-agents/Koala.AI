"""Approval chain runner + built-in ApprovalRule helpers.

The runner walks an ordered list of rules. First rule to return a non-None
decision wins. If every rule defers, the safe default is ``"ask"``.

Built-in rules cover the common patterns:
    - ``DenyList``  — deny specific names, defer otherwise
    - ``AllowList`` — allow specific names, defer otherwise
    - ``AlwaysAsk`` — force human approval for every call (terminal)
    - ``AlwaysAllow`` — YOLO / auto-approve (terminal). Useful for CI and demos.
    - ``RequireApprovalFor`` — force ``ask`` for names or prefix matches

Compose to taste. Recommended default::

    rules = [DenyList({"rm", "drop_table"}), RequireApprovalFor(prefixes={"delete_"})]
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..core.approval import ApprovalDecision, ApprovalResult, ApprovalRule
from ..core.context import RunContext
from ..core.messages import ToolCallBlock


def evaluate_approval_chain(
    rules: list[ApprovalRule],
    ctx: RunContext,
    call: ToolCallBlock,
) -> ApprovalResult:
    """Walk ``rules`` in order. First non-None decision wins.

    If every rule defers (returns None), the default is ``"ask"``. This is a
    safe default: if you want silent auto-approve for everything not deny-listed,
    place an ``AlwaysAllow()`` at the end of your chain.
    """
    for rule in rules:
        decision = rule.check(ctx, call)
        if decision is not None:
            return ApprovalResult(
                decision=decision,
                rule_name=type(rule).__name__,
            )
    return ApprovalResult(
        decision="ask",
        reason="no rule matched; defaulting to ask",
    )


# ---------------------------------------------------------------------------
# Built-in rules
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DenyList:
    """Deny any tool whose name is in ``names``. Defer otherwise."""

    names: frozenset[str] = field(default_factory=frozenset)

    def check(
        self, ctx: RunContext, call: ToolCallBlock, /
    ) -> ApprovalDecision | None:
        if call.name in self.names:
            return "deny"
        return None


@dataclass(frozen=True, slots=True)
class AllowList:
    """Allow any tool whose name is in ``names``. Defer otherwise."""

    names: frozenset[str] = field(default_factory=frozenset)

    def check(
        self, ctx: RunContext, call: ToolCallBlock, /
    ) -> ApprovalDecision | None:
        if call.name in self.names:
            return "allow"
        return None


@dataclass(frozen=True, slots=True)
class AlwaysAsk:
    """Terminal rule — force human approval for every tool call."""

    def check(self, ctx: RunContext, call: ToolCallBlock, /) -> ApprovalDecision:
        return "ask"


@dataclass(frozen=True, slots=True)
class AlwaysAllow:
    """Terminal rule — auto-approve every tool call.

    Useful for CI, demos, and dev loops. Never ship as the sole rule for a
    production agent that runs destructive tools.
    """

    def check(self, ctx: RunContext, call: ToolCallBlock, /) -> ApprovalDecision:
        return "allow"


@dataclass(frozen=True, slots=True)
class RequireApprovalFor:
    """Force ``ask`` for tool names matching either an exact name or prefix.

    Compose with a `DenyList` or `AllowList` before this rule to short-circuit
    for names you already have an opinion about.
    """

    names: frozenset[str] = field(default_factory=frozenset)
    prefixes: frozenset[str] = field(default_factory=frozenset)

    def check(
        self, ctx: RunContext, call: ToolCallBlock, /
    ) -> ApprovalDecision | None:
        if call.name in self.names:
            return "ask"
        if any(call.name.startswith(p) for p in self.prefixes):
            return "ask"
        return None
