"""Message and content-block types.

Every LLM interaction produces or consumes Messages. A Message has a role and
a list of ContentBlocks so we can preserve tool calls, tool results, thinking
tokens, and images alongside text without lossy string-flattening across
providers.

The block set is Anthropic-inspired because it's the most expressive: OpenAI /
Groq / Ollama content maps down to a subset (Text + ToolCall + ToolResult),
while Claude-specific features (Thinking) and multimodal blocks (Image) fit
naturally without a second Message type.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Union

Role = Literal["system", "user", "assistant", "tool"]


@dataclass(frozen=True, slots=True)
class TextBlock:
    """Plain text content."""

    text: str
    kind: Literal["text"] = "text"


@dataclass(frozen=True, slots=True)
class ThinkingBlock:
    """Reasoning/thought content emitted by models that expose thinking.

    Applies to Claude with extended thinking, OpenAI o-series, DeepSeek-R1, etc.
    Kept as a distinct block type so downstream code can choose to show, hide,
    or persist it separately from user-visible text.
    """

    text: str
    kind: Literal["thinking"] = "thinking"


@dataclass(frozen=True, slots=True)
class ImageBlock:
    """Image content, either as a URL or a base64 data URI."""

    source: str
    media_type: str = "image/png"
    kind: Literal["image"] = "image"


@dataclass(frozen=True, slots=True)
class ToolCallBlock:
    """A model-emitted request to invoke a tool."""

    id: str
    name: str
    arguments: dict[str, Any] = field(default_factory=dict)
    kind: Literal["tool_call"] = "tool_call"


@dataclass(frozen=True, slots=True)
class ToolResultBlock:
    """The result of executing a tool, tied back to a tool_call by id."""

    tool_call_id: str
    content: str | dict[str, Any] | list[Any]
    is_error: bool = False
    kind: Literal["tool_result"] = "tool_result"


ContentBlock = Union[
    TextBlock,
    ThinkingBlock,
    ImageBlock,
    ToolCallBlock,
    ToolResultBlock,
]


@dataclass(frozen=True, slots=True)
class Message:
    """A single conversation turn.

    Args:
        role: Who produced this message.
        content: Ordered list of content blocks. May mix text with tool calls,
            thinking, images, etc.
        name: Optional participant name for multi-agent scenarios.
    """

    role: Role
    content: list[ContentBlock] = field(default_factory=list)
    name: str | None = None

    # -- ergonomic constructors -------------------------------------------------

    @staticmethod
    def system(text: str) -> "Message":
        return Message(role="system", content=[TextBlock(text=text)])

    @staticmethod
    def user(text: str) -> "Message":
        return Message(role="user", content=[TextBlock(text=text)])

    @staticmethod
    def assistant(text: str) -> "Message":
        return Message(role="assistant", content=[TextBlock(text=text)])

    @staticmethod
    def tool(
        tool_call_id: str, content: str | dict[str, Any] | list[Any]
    ) -> "Message":
        """Build a tool-response message tied to a prior tool call."""
        return Message(
            role="tool",
            content=[ToolResultBlock(tool_call_id=tool_call_id, content=content)],
        )

    # -- convenience views ------------------------------------------------------

    @property
    def text(self) -> str:
        """Concatenation of TextBlock contents. Excludes thinking, tool calls,
        and other non-text blocks. Convenient for the common case where you
        just want the visible message text.
        """
        return "".join(b.text for b in self.content if isinstance(b, TextBlock))

    @property
    def tool_calls(self) -> list[ToolCallBlock]:
        """All ToolCallBlocks in this message, in order."""
        return [b for b in self.content if isinstance(b, ToolCallBlock)]
