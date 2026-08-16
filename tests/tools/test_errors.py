"""Tests for koala.tools.errors."""

from __future__ import annotations

from koala.tools import (
    ToolError,
    ToolExecutionError,
    ToolNotFoundError,
    ToolValidationError,
)


def test_all_tool_errors_subclass_tool_error() -> None:
    for cls in (ToolValidationError, ToolExecutionError, ToolNotFoundError):
        assert issubclass(cls, ToolError)


def test_tool_validation_error_carries_name_and_errors() -> None:
    err = ToolValidationError("add", ["a: field required", "b: not an int"])
    assert err.tool_name == "add"
    assert err.errors == ["a: field required", "b: not an int"]
    assert "'add'" in str(err)
    assert "field required" in str(err)


def test_tool_validation_error_empty_details() -> None:
    err = ToolValidationError("noop", [])
    assert "invalid arguments" in str(err)


def test_tool_execution_error_wraps_original() -> None:
    original = ValueError("boom")
    err = ToolExecutionError("divide", original)
    assert err.tool_name == "divide"
    assert err.original is original
    assert "ValueError" in str(err)
    assert "boom" in str(err)


def test_tool_not_found_error_lists_available_names() -> None:
    err = ToolNotFoundError("weather", ["math", "search"])
    assert err.tool_name == "weather"
    assert err.available == ["math", "search"]
    assert "weather" in str(err)
    assert "math" in str(err)


def test_tool_not_found_error_with_empty_available() -> None:
    err = ToolNotFoundError("weather", [])
    assert "(none)" in str(err)
