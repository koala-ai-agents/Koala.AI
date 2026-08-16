"""Tests for koala.tools.schema — docstring parsing + Pydantic schema derivation."""

from __future__ import annotations

from typing import Annotated

import pytest

from koala.core import RunContext
from koala.tools import build_tool_spec, parse_google_docstring

# ---------------------------------------------------------------------------
# Docstring parsing
# ---------------------------------------------------------------------------


def test_parse_docstring_summary_only() -> None:
    summary, params = parse_google_docstring("Just a summary.")
    assert summary == "Just a summary."
    assert params == {}


def test_parse_docstring_returns_empty_for_none() -> None:
    assert parse_google_docstring(None) == ("", {})
    assert parse_google_docstring("") == ("", {})


def test_parse_docstring_extracts_google_args_block() -> None:
    doc = """Do a thing.

    A multi-line summary
    with two lines.

    Args:
        x: The x value.
        y: The y value,
            wrapped onto two lines.
        z (int): The z value with a type suffix.

    Returns:
        Some result.
    """
    summary, params = parse_google_docstring(doc)
    assert "Do a thing" in summary
    assert "multi-line" in summary
    assert "Returns" not in summary  # Args section ended when Returns arrived
    assert params["x"] == "The x value."
    assert "wrapped onto two lines" in params["y"]
    assert params["z"] == "The z value with a type suffix."


def test_parse_docstring_stops_args_at_other_sections() -> None:
    doc = """Summary.

    Args:
        a: First.
        b: Second.

    Raises:
        ValueError: On bad input.

    Returns:
        A number.
    """
    _, params = parse_google_docstring(doc)
    assert set(params.keys()) == {"a", "b"}


def test_parse_docstring_accepts_parameters_and_arguments_headers() -> None:
    doc1 = "Summary.\n\nArguments:\n    x: The x.\n"
    doc2 = "Summary.\n\nParameters:\n    x: The x.\n"
    for doc in (doc1, doc2):
        _, params = parse_google_docstring(doc)
        assert params == {"x": "The x."}


# ---------------------------------------------------------------------------
# Schema derivation
# ---------------------------------------------------------------------------


def test_build_spec_basic_types() -> None:
    def foo(a: int, b: str, c: float) -> str:
        """Foo."""
        return ""

    spec = build_tool_spec(foo)
    assert spec.description == "Foo."
    props = spec.schema["properties"]
    assert props["a"]["type"] == "integer"
    assert props["b"]["type"] == "string"
    assert props["c"]["type"] == "number"


def test_build_spec_required_vs_optional_defaults() -> None:
    def foo(required: int, optional: int = 42) -> int:
        return 0

    spec = build_tool_spec(foo)
    assert spec.schema["required"] == ["required"]
    assert spec.schema["properties"]["optional"]["default"] == 42


def test_build_spec_uses_docstring_descriptions() -> None:
    def foo(a: int, b: int) -> int:
        """Add two.

        Args:
            a: The first number.
            b: The second number.
        """
        return a + b

    spec = build_tool_spec(foo)
    assert spec.schema["properties"]["a"]["description"] == "The first number."
    assert spec.schema["properties"]["b"]["description"] == "The second number."


def test_build_spec_prefers_annotated_metadata_over_docstring() -> None:
    def foo(a: Annotated[int, "From annotation"]) -> int:
        """Foo.

        Args:
            a: From docstring.
        """
        return a

    spec = build_tool_spec(foo)
    # Inline Annotated string wins over the docstring
    assert spec.schema["properties"]["a"]["description"] == "From annotation"


def test_build_spec_injects_run_context() -> None:
    def with_ctx(ctx: RunContext, x: int) -> int:
        return x

    spec = build_tool_spec(with_ctx)
    assert spec.injected_params == ["ctx"]
    # The `ctx` parameter must not appear in the tool schema
    assert "ctx" not in spec.schema["properties"]
    assert "x" in spec.schema["properties"]


def test_build_spec_ignores_untyped_ctx() -> None:
    def foo(ctx, x: int) -> int:  # type: ignore[no-untyped-def]
        return x

    with pytest.raises(TypeError, match="no type hint"):
        build_tool_spec(foo)


def test_build_spec_rejects_untyped_param() -> None:
    def foo(x) -> int:  # type: ignore[no-untyped-def]
        return 0

    with pytest.raises(TypeError, match="no type hint"):
        build_tool_spec(foo)


def test_build_spec_supports_list_and_dict_types() -> None:
    def foo(items: list[str], meta: dict[str, int]) -> None:
        """Foo."""

    spec = build_tool_spec(foo)
    assert spec.schema["properties"]["items"]["type"] == "array"
    assert spec.schema["properties"]["meta"]["type"] == "object"


def test_build_spec_supports_optional() -> None:
    def foo(x: int | None = None) -> None:
        """Foo."""

    spec = build_tool_spec(foo)
    # Pydantic v2 emits an anyOf for int | None
    prop = spec.schema["properties"]["x"]
    assert "anyOf" in prop or prop.get("type") in ("integer", None)


def test_build_spec_pydantic_model_validates_arguments() -> None:
    def foo(a: int, b: str = "x") -> None:
        """Foo."""

    spec = build_tool_spec(foo)
    validated = spec.param_model.model_validate({"a": 5}).model_dump()
    assert validated == {"a": 5, "b": "x"}


def test_build_spec_pydantic_model_coerces_types() -> None:
    def foo(a: int) -> None:
        """Foo."""

    spec = build_tool_spec(foo)
    # "5" is coerced to int(5) in Pydantic default lax mode
    validated = spec.param_model.model_validate({"a": "5"}).model_dump()
    assert validated == {"a": 5}


def test_build_spec_no_docstring_gives_empty_description() -> None:
    def foo(a: int) -> int:
        return a

    spec = build_tool_spec(foo)
    assert spec.description == ""


def test_build_spec_skips_var_args_and_var_kwargs() -> None:
    def foo(a: int, *args, **kwargs) -> int:  # type: ignore[no-untyped-def]
        return a

    spec = build_tool_spec(foo)
    assert set(spec.schema["properties"].keys()) == {"a"}
