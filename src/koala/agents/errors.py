"""Agent-level exception hierarchy."""

from __future__ import annotations


class AgentError(Exception):
    """Base for all agent-related errors."""


class MaxIterationsError(AgentError):
    """The tool-calling loop hit its iteration cap without producing an answer."""

    def __init__(self, agent_name: str, max_iterations: int) -> None:
        self.agent_name = agent_name
        self.max_iterations = max_iterations
        super().__init__(
            f"Agent {agent_name!r} exceeded max_iterations={max_iterations} "
            "without producing a final answer."
        )


class HandoffError(AgentError):
    """A handoff to another agent failed."""


class OutputParseError(AgentError):
    """The model's final message could not be parsed into the requested output_type."""

    def __init__(self, agent_name: str, raw: str, errors: list[str]) -> None:
        self.agent_name = agent_name
        self.raw = raw
        self.errors = list(errors)
        detail = "; ".join(self.errors) if self.errors else "unparseable"
        super().__init__(
            f"Agent {agent_name!r} could not parse output: {detail}. "
            f"Raw: {raw[:200]}"
        )
