"""RunResult — the summary object returned by Agent.run / Agent.arun."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from ..core.messages import Message
from ..core.types import Usage

StopReason = Literal[
    "final_output",
    "max_iterations",
    "error",
    "cancelled",
]


@dataclass(slots=True)
class RunResult:
    """The result of running an Agent to completion.

    Args:
        output: The final answer. Type depends on the agent's ``output_type``:
            a plain ``str`` when no output_type is set, else the validated
            Pydantic model instance.
        messages: Full conversation history — initial system+user messages,
            every assistant message (including any with tool_calls), and
            every tool_result message. Ready to feed back into another
            ``arun`` call for continuation.
        usage: Cumulative token / cost usage across every model call in the run.
        iterations: Number of model calls (a.k.a. tool-call rounds) taken.
            1 = model answered without calling any tools.
        stop_reason: Why the agent stopped. ``final_output`` is the happy path.
        error: When ``stop_reason`` is ``error`` or ``max_iterations``, the
            human-readable reason. ``None`` otherwise.
        metadata: Free-form dict for tags, tracing ids, etc.
    """

    output: Any
    messages: list[Message]
    usage: Usage
    iterations: int
    stop_reason: StopReason
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
