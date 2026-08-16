"""Koala core primitives (L1).

Pure types, protocols, and small dataclasses. Zero runtime dependencies
beyond stdlib. Higher layers (Models, Tools, Agents, Harness, Orchestration)
build on top of this layer.

The public surface of `koala.core` is intentionally narrow — only the names
that other layers or user code will reference. Anything not re-exported here
is an internal implementation detail of a specific submodule.
"""

from __future__ import annotations

from .approval import ApprovalDecision, ApprovalResult, ApprovalRule
from .capabilities import Capability
from .context import CancelToken, RunContext
from .events import (
    AwaitingApproval,
    Done,
    Error,
    Event,
    ModelDelta,
    ModelMessage,
    Output,
    Start,
    ThinkingDelta,
    ToolCall,
    ToolResult,
    UsageEvent,
)
from .messages import (
    ContentBlock,
    ImageBlock,
    Message,
    Role,
    TextBlock,
    ThinkingBlock,
    ToolCallBlock,
    ToolResultBlock,
)
from .runnable import Channel, Runnable, acollect, ainvoke, invoke
from .types import ModelRef, Usage, parse_model_ref

__all__ = [
    # messages
    "Message",
    "Role",
    "ContentBlock",
    "TextBlock",
    "ThinkingBlock",
    "ImageBlock",
    "ToolCallBlock",
    "ToolResultBlock",
    # events
    "Event",
    "Start",
    "ModelDelta",
    "ThinkingDelta",
    "ModelMessage",
    "ToolCall",
    "ToolResult",
    "AwaitingApproval",
    "UsageEvent",
    "Output",
    "Error",
    "Done",
    # runnable
    "Runnable",
    "Channel",
    "ainvoke",
    "invoke",
    "acollect",
    # context
    "RunContext",
    "CancelToken",
    # types
    "Usage",
    "ModelRef",
    "parse_model_ref",
    # capabilities
    "Capability",
    # approval
    "ApprovalRule",
    "ApprovalDecision",
    "ApprovalResult",
]
