"""Tests for approval rule chain runner + built-in rules."""

from __future__ import annotations

from koala.core import RunContext, ToolCallBlock
from koala.tools import (
    AllowList,
    AlwaysAllow,
    AlwaysAsk,
    DenyList,
    RequireApprovalFor,
    evaluate_approval_chain,
)


def _call(name: str) -> ToolCallBlock:
    return ToolCallBlock(id="c1", name=name, arguments={})


def _ctx() -> RunContext[None]:
    return RunContext(deps=None)


# ---------------------------------------------------------------------------
# Individual rules
# ---------------------------------------------------------------------------


def test_deny_list_denies_matching_names() -> None:
    rule = DenyList(names=frozenset({"rm", "drop_table"}))
    assert rule.check(_ctx(), _call("rm")) == "deny"
    assert rule.check(_ctx(), _call("drop_table")) == "deny"
    # Defers on unknown
    assert rule.check(_ctx(), _call("read_file")) is None


def test_allow_list_allows_matching_names() -> None:
    rule = AllowList(names=frozenset({"read_file", "list_dir"}))
    assert rule.check(_ctx(), _call("read_file")) == "allow"
    assert rule.check(_ctx(), _call("write_file")) is None


def test_always_ask_forces_ask() -> None:
    assert AlwaysAsk().check(_ctx(), _call("anything")) == "ask"


def test_always_allow_forces_allow() -> None:
    assert AlwaysAllow().check(_ctx(), _call("anything")) == "allow"


def test_require_approval_for_name_match() -> None:
    rule = RequireApprovalFor(names=frozenset({"pay_customer"}))
    assert rule.check(_ctx(), _call("pay_customer")) == "ask"
    assert rule.check(_ctx(), _call("read_file")) is None


def test_require_approval_for_prefix_match() -> None:
    rule = RequireApprovalFor(prefixes=frozenset({"delete_", "drop_"}))
    assert rule.check(_ctx(), _call("delete_user")) == "ask"
    assert rule.check(_ctx(), _call("drop_table")) == "ask"
    assert rule.check(_ctx(), _call("list_users")) is None


# ---------------------------------------------------------------------------
# Chain runner
# ---------------------------------------------------------------------------


def test_chain_first_non_none_wins() -> None:
    rules = [
        DenyList(names=frozenset({"rm"})),
        AllowList(names=frozenset({"rm"})),  # would allow, but deny wins
    ]
    result = evaluate_approval_chain(rules, _ctx(), _call("rm"))
    assert result.decision == "deny"
    assert result.rule_name == "DenyList"


def test_chain_defers_to_next_when_first_returns_none() -> None:
    rules = [
        DenyList(names=frozenset({"rm"})),
        AllowList(names=frozenset({"read_file"})),
    ]
    result = evaluate_approval_chain(rules, _ctx(), _call("read_file"))
    assert result.decision == "allow"
    assert result.rule_name == "AllowList"


def test_chain_default_is_ask_when_all_defer() -> None:
    rules = [
        DenyList(names=frozenset({"rm"})),
        AllowList(names=frozenset({"read_file"})),
    ]
    result = evaluate_approval_chain(rules, _ctx(), _call("write_file"))
    assert result.decision == "ask"
    assert "no rule matched" in result.reason


def test_chain_empty_defaults_to_ask() -> None:
    result = evaluate_approval_chain([], _ctx(), _call("anything"))
    assert result.decision == "ask"


def test_chain_terminal_rule_at_end_catches_everything() -> None:
    """Pattern: deny known-destructive, then auto-allow everything else."""
    rules = [
        DenyList(names=frozenset({"rm"})),
        AlwaysAllow(),  # terminal — everything else auto-approves
    ]
    assert evaluate_approval_chain(rules, _ctx(), _call("rm")).decision == "deny"
    assert (
        evaluate_approval_chain(rules, _ctx(), _call("read_file")).decision == "allow"
    )
