"""Event discriminated union.

Every Runnable emits an ordered stream of Events through an AsyncIterator.
Events describe the lifecycle of a run: model deltas, tool calls, HITL
prompts, usage updates, errors, and the final output.

The `kind` string on each event is the discriminator: consumers can pattern-
match with `isinstance` or dispatch on `event.kind` — whichever suits.

Contract:
    - The first event of a run is `Start`.
    - The last event of a run is `Done`.
    - If the runnable produces a final value, it emits an `Output(value=...)`
      event before `Done`.
    - A fatal `Error` must be followed by `Done` (no more processing).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Union

from .messages import Message, ToolCallBlock, ToolResultBlock
from .types import Usage


@dataclass(frozen=True, slots=True)
class Start:
    """A runnable has begun. Carries name + input for tracing."""

    run_id: str
    name: str
    input: Any = None
    kind: Literal["start"] = "start"


@dataclass(frozen=True, slots=True)
class ModelDelta:
    """Incremental token or content chunk streamed from a model.

    Represents *visible* assistant content — the text the model wants the
    user to see. For chain-of-thought reasoning tokens that shouldn't be
    rendered as the answer, see :class:`ThinkingDelta`.
    """

    text: str
    kind: Literal["model_delta"] = "model_delta"


@dataclass(frozen=True, slots=True)
class ThinkingDelta:
    """Incremental reasoning / thinking content streamed from a model.

    Emitted by providers when a reasoning-capable model streams its chain
    of thought (DeepSeek-R1, Qwen QwQ, OpenAI o-series with reasoning
    output, and similar). Non-reasoning models never emit this event —
    consumers can safely subscribe unconditionally.
    """

    text: str
    kind: Literal["thinking_delta"] = "thinking_delta"


@dataclass(frozen=True, slots=True)
class ModelMessage:
    """A complete assistant message from the model.

    Emitted once streaming for a turn finishes, so downstream consumers that
    don't care about deltas can key off this event alone.
    """

    message: Message
    kind: Literal["model_message"] = "model_message"


@dataclass(frozen=True, slots=True)
class ToolCall:
    """A model has requested a tool call. Emitted before invocation."""

    call: ToolCallBlock
    kind: Literal["tool_call"] = "tool_call"


@dataclass(frozen=True, slots=True)
class ToolResult:
    """A tool invocation has completed."""

    result: ToolResultBlock
    kind: Literal["tool_result"] = "tool_result"


@dataclass(frozen=True, slots=True)
class AwaitingApproval:
    """A tool call is awaiting human approval per the ApprovalRule chain."""

    call: ToolCallBlock
    request_id: str
    reason: str = ""
    kind: Literal["awaiting_approval"] = "awaiting_approval"


@dataclass(frozen=True, slots=True)
class UsageEvent:
    """A usage snapshot. Multiple may be emitted per run (per model call)."""

    usage: Usage
    kind: Literal["usage"] = "usage"


@dataclass(frozen=True, slots=True)
class Output:
    """The final validated output value produced by the runnable."""

    value: Any
    kind: Literal["output"] = "output"


@dataclass(frozen=True, slots=True)
class Error:
    """An error occurred during the run.

    If `fatal` is True, `Done` follows immediately and no further events fire.
    If False, the runnable may attempt to recover and continue.
    """

    error: str
    fatal: bool = True
    exc_type: str | None = None
    kind: Literal["error"] = "error"


@dataclass(frozen=True, slots=True)
class Done:
    """Terminal event. Always the last item in a run's stream."""

    run_id: str
    kind: Literal["done"] = "done"


Event = Union[
    Start,
    ModelDelta,
    ThinkingDelta,
    ModelMessage,
    ToolCall,
    ToolResult,
    AwaitingApproval,
    UsageEvent,
    Output,
    Error,
    Done,
]
