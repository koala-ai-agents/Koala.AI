"""Koala tools package (L3).

Exports:
    - ``@tool``: the decorator that turns a function into a Tool
    - ``BaseTool`` / ``FunctionTool``: the tool interface + concrete impl
    - ``build_tool_spec``, ``ToolSpec``: low-level schema derivation
    - Built-in approval rules: ``DenyList``, ``AllowList``, ``AlwaysAsk``,
      ``AlwaysAllow``, ``RequireApprovalFor`` + the chain runner
    - Error hierarchy: ``ToolError``, ``ToolValidationError``,
      ``ToolExecutionError``, ``ToolNotFoundError``
"""

from __future__ import annotations

from .approval import (
    AllowList,
    AlwaysAllow,
    AlwaysAsk,
    DenyList,
    RequireApprovalFor,
    evaluate_approval_chain,
)
from .base import BaseTool
from .errors import (
    ToolError,
    ToolExecutionError,
    ToolNotFoundError,
    ToolValidationError,
)
from .function_tool import FunctionTool, tool
from .schema import ToolSpec, build_tool_spec, parse_google_docstring

__all__ = [
    # decorator + main classes
    "tool",
    "FunctionTool",
    "BaseTool",
    # schema utilities
    "ToolSpec",
    "build_tool_spec",
    "parse_google_docstring",
    # approval
    "evaluate_approval_chain",
    "DenyList",
    "AllowList",
    "AlwaysAsk",
    "AlwaysAllow",
    "RequireApprovalFor",
    # errors
    "ToolError",
    "ToolValidationError",
    "ToolExecutionError",
    "ToolNotFoundError",
]
