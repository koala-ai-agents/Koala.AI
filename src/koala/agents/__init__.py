"""Koala agents package (L6).

Exports:
    - ``Agent``: the standard tool-calling agent
    - ``BaseAgent``: the abstract agent contract
    - ``AgentTool``: wraps an Agent so it can be called by another agent
    - ``RunResult``: the summary object returned by ``run`` / ``arun``
    - Errors: ``AgentError``, ``MaxIterationsError``, ``HandoffError``,
      ``OutputParseError``
    - Structured output helpers: ``build_response_format``,
      ``build_prompt_schema_hint``, ``parse_output``
"""

from __future__ import annotations

from .agent import Agent, AgentTool, BaseAgent
from .errors import (
    AgentError,
    HandoffError,
    MaxIterationsError,
    OutputParseError,
)
from .output import build_prompt_schema_hint, build_response_format, parse_output
from .result import RunResult, StopReason

__all__ = [
    # main
    "Agent",
    "BaseAgent",
    "AgentTool",
    "RunResult",
    "StopReason",
    # errors
    "AgentError",
    "MaxIterationsError",
    "HandoffError",
    "OutputParseError",
    # output helpers
    "build_response_format",
    "build_prompt_schema_hint",
    "parse_output",
]
