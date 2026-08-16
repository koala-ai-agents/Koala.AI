"""Tool-level exception hierarchy.

Distinct exception types let the agent loop decide what to do:
    - `ToolValidationError` -> feed the error back to the model so it can retry
      with corrected arguments.
    - `ToolExecutionError` -> feed the error back OR bubble up, depending on
      the agent's error policy.
    - `ToolNotFoundError` -> the model asked for a tool that doesn't exist;
      typically feed a "unknown tool" message back.
"""

from __future__ import annotations


class ToolError(Exception):
    """Base for all tool-related errors."""


class ToolValidationError(ToolError):
    """Arguments failed schema validation. Recoverable — feed back to the model."""

    def __init__(self, tool_name: str, errors: list[str]) -> None:
        self.tool_name = tool_name
        self.errors = list(errors)
        detail = "; ".join(self.errors) if self.errors else "invalid arguments"
        super().__init__(f"Validation failed for tool {tool_name!r}: {detail}")


class ToolExecutionError(ToolError):
    """The tool function raised while running. Preserves the original exception."""

    def __init__(self, tool_name: str, original: BaseException) -> None:
        self.tool_name = tool_name
        self.original = original
        super().__init__(
            f"Tool {tool_name!r} raised {type(original).__name__}: {original}"
        )


class ToolNotFoundError(ToolError):
    """The model requested a tool that isn't registered on the agent."""

    def __init__(self, tool_name: str, available: list[str]) -> None:
        self.tool_name = tool_name
        self.available = list(available)
        available_str = ", ".join(self.available) if self.available else "(none)"
        super().__init__(
            f"Tool {tool_name!r} not found. Available: {available_str}"
        )
