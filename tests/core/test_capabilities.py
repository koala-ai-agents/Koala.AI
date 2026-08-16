"""Tests for koala.core.capabilities."""

from __future__ import annotations

from koala.core import Capability


def test_capability_is_str_enum_member():
    assert Capability.STREAMING == "streaming"
    assert Capability.TOOL_CALLING == "tool_calling"
    assert isinstance(Capability.THINKING.value, str)


def test_all_capability_values_are_unique():
    values = [c.value for c in Capability]
    assert len(values) == len(set(values))


def test_capability_frozenset_membership():
    caps: frozenset[Capability] = frozenset(
        {Capability.STREAMING, Capability.TOOL_CALLING}
    )
    assert Capability.STREAMING in caps
    assert Capability.EMBEDDING not in caps


def test_all_capability_values_are_lowercase_snake_case():
    for c in Capability:
        assert c.value == c.value.lower()
        assert " " not in c.value
