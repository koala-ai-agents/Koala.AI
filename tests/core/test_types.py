"""Tests for koala.core.types."""

from __future__ import annotations

import pytest

from koala.core import Usage, parse_model_ref


def test_parse_model_ref_basic():
    ref = parse_model_ref("groq/llama-3.3-70b-versatile")
    assert ref.provider == "groq"
    assert ref.name == "llama-3.3-70b-versatile"


def test_parse_model_ref_preserves_colon_in_name():
    ref = parse_model_ref("ollama/llama3:8b")
    assert ref.provider == "ollama"
    assert ref.name == "llama3:8b"


def test_parse_model_ref_splits_on_first_slash_only():
    ref = parse_model_ref("openrouter/anthropic/claude-3.5")
    assert ref.provider == "openrouter"
    assert ref.name == "anthropic/claude-3.5"


def test_parse_model_ref_requires_slash():
    with pytest.raises(ValueError, match="provider/name"):
        parse_model_ref("gpt-4o-mini")


def test_parse_model_ref_rejects_empty_parts():
    with pytest.raises(ValueError, match="empty"):
        parse_model_ref("/name")
    with pytest.raises(ValueError, match="empty"):
        parse_model_ref("provider/")


def test_model_ref_str_roundtrip():
    original = "groq/llama-3.3"
    assert str(parse_model_ref(original)) == original


def test_usage_add_accumulates_in_place():
    u = Usage(input_tokens=10, output_tokens=5, cost_usd=0.01)
    u.add(Usage(input_tokens=3, output_tokens=2, cost_usd=0.005, requests=1))
    assert u.input_tokens == 13
    assert u.output_tokens == 7
    assert u.cost_usd == pytest.approx(0.015)
    assert u.requests == 1


def test_usage_total_tokens():
    assert Usage(input_tokens=100, output_tokens=50).total_tokens == 150


def test_usage_defaults_are_zero():
    u = Usage()
    assert u.total_tokens == 0
    assert u.cost_usd == 0.0
    assert u.requests == 0
