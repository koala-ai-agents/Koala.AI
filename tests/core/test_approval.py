"""Tests for koala.core.approval."""

from __future__ import annotations

from koala.core import (
    ApprovalDecision,
    ApprovalResult,
    ApprovalRule,
    RunContext,
    ToolCallBlock,
)


class AllowAll:
    def check(self, ctx: RunContext, call: ToolCallBlock, /) -> ApprovalDecision:
        return "allow"


class DenyDestructive:
    """Deny known-destructive tool names; defer on everything else."""

    def check(
        self, ctx: RunContext, call: ToolCallBlock, /
    ) -> ApprovalDecision | None:
        if call.name in {"rm", "drop_table", "delete_user"}:
            return "deny"
        return None


class AskForPayments:
    def check(
        self, ctx: RunContext, call: ToolCallBlock, /
    ) -> ApprovalDecision | None:
        if call.name.startswith("payment_"):
            return "ask"
        return None


def test_rule_satisfies_protocol_structurally():
    assert isinstance(AllowAll(), ApprovalRule)
    assert isinstance(DenyDestructive(), ApprovalRule)
    assert isinstance(AskForPayments(), ApprovalRule)


def test_arbitrary_object_does_not_satisfy_rule_protocol():
    assert not isinstance(object(), ApprovalRule)


def test_defer_by_returning_none():
    ctx = RunContext(deps=None)
    assert (
        DenyDestructive().check(
            ctx, ToolCallBlock(id="1", name="read_file")
        )
        is None
    )


def test_deny_matches_name():
    ctx = RunContext(deps=None)
    assert (
        DenyDestructive().check(ctx, ToolCallBlock(id="1", name="rm")) == "deny"
    )


def test_ask_matches_prefix():
    ctx = RunContext(deps=None)
    assert (
        AskForPayments().check(
            ctx, ToolCallBlock(id="1", name="payment_charge")
        )
        == "ask"
    )


def test_approval_result_defaults():
    ar = ApprovalResult(decision="allow")
    assert ar.reason == ""
    assert ar.rule_name is None


def test_approval_result_with_reason_and_rule_name():
    ar = ApprovalResult(
        decision="deny", reason="destructive tool", rule_name="DenyDestructive"
    )
    assert ar.decision == "deny"
    assert ar.reason == "destructive tool"
    assert ar.rule_name == "DenyDestructive"
