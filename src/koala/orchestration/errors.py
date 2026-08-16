"""Orchestration-level exception hierarchy."""

from __future__ import annotations


class FlowError(Exception):
    """Base for flow / orchestration errors."""


class StepExecutionError(FlowError):
    """A step raised during execution. Preserves the original exception."""

    def __init__(self, step_id: str, original: BaseException) -> None:
        self.step_id = step_id
        self.original = original
        super().__init__(
            f"Step {step_id!r} raised {type(original).__name__}: {original}"
        )
