"""Tests for koala.models.settings — ChatSettings merge + payload conversion."""

from __future__ import annotations

from koala.models import ChatSettings


def test_none_fields_omitted_from_payload() -> None:
    s = ChatSettings()
    assert s.to_payload() == {}


def test_only_set_fields_appear_in_payload() -> None:
    s = ChatSettings(temperature=0.7, max_tokens=100)
    payload = s.to_payload()
    assert payload == {"temperature": 0.7, "max_tokens": 100}


def test_extra_fields_included_in_payload() -> None:
    s = ChatSettings(extra={"reasoning_effort": "high"})
    assert s.to_payload() == {"reasoning_effort": "high"}


def test_extra_can_override_first_class_field_in_payload() -> None:
    # Explicit intent — user typed extra to shadow a field. Last-writer wins.
    s = ChatSettings(temperature=0.5, extra={"temperature": 0.9})
    assert s.to_payload()["temperature"] == 0.9


def test_merge_other_overrides_self_for_non_none_fields() -> None:
    base = ChatSettings(temperature=0.5, max_tokens=100)
    override = ChatSettings(temperature=0.9)
    merged = base.merge(override)
    assert merged.temperature == 0.9
    assert merged.max_tokens == 100  # inherited


def test_merge_preserves_self_when_other_is_none() -> None:
    base = ChatSettings(temperature=0.5, seed=42)
    override = ChatSettings()  # everything None
    merged = base.merge(override)
    assert merged.temperature == 0.5
    assert merged.seed == 42


def test_merge_extras_are_shallow_merged() -> None:
    base = ChatSettings(extra={"a": 1, "b": 2})
    override = ChatSettings(extra={"b": 20, "c": 3})
    merged = base.merge(override)
    assert merged.extra == {"a": 1, "b": 20, "c": 3}


def test_merge_returns_new_instance() -> None:
    base = ChatSettings(temperature=0.5)
    override = ChatSettings(temperature=0.9)
    merged = base.merge(override)
    assert merged is not base
    assert merged is not override
    assert base.temperature == 0.5  # unchanged


def test_all_first_class_fields_serialize_correctly() -> None:
    s = ChatSettings(
        temperature=0.5,
        max_tokens=100,
        top_p=0.9,
        stop=["\n\n"],
        seed=42,
        frequency_penalty=0.1,
        presence_penalty=0.2,
    )
    payload = s.to_payload()
    assert payload["temperature"] == 0.5
    assert payload["max_tokens"] == 100
    assert payload["top_p"] == 0.9
    assert payload["stop"] == ["\n\n"]
    assert payload["seed"] == 42
    assert payload["frequency_penalty"] == 0.1
    assert payload["presence_penalty"] == 0.2
