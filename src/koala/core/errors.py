"""Core error types for Koala."""

from __future__ import annotations


class ModelRetry(Exception):
    """Raised by a tool function or validator to ask the model to retry.

    When raised during tool execution or structured output validation,
    the agent catches it, feeds the error message back to the LLM, and
    allows the model another iteration to correct its tool parameters or output.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
