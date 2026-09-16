"""Context window management: atomic turn compaction, token budgeting, and tool truncation.

Key principles:
1. Atomic turn preservation: An assistant message with tool calls and all its
   corresponding tool result messages are treated as a single indivisible unit.
   They are kept together or pruned together. This prevents "orphaned tool call"
   or "orphaned tool result" errors that break OpenAI and Anthropic wire formats.
2. System instruction pinning: System messages are always preserved at the start.
3. Tool output truncation: Massive tool returns (e.g. webpage scrapes or large dumps)
   are safely truncated with an informative notice before entering the prompt.
4. Zero dependencies: Fast, robust character-based token estimation (~4 chars/token).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from ..core.messages import (
    ContentBlock,
    ImageBlock,
    Message,
    TextBlock,
    ThinkingBlock,
    ToolCallBlock,
    ToolResultBlock,
)

if TYPE_CHECKING:
    from ..behaviors.base import AgentSpec


def estimate_tokens(text: str) -> int:
    """Estimate token count for a text string using standard ~4 char/token heuristic.

    Fast and zero-dependency. Minimum 1 token for non-empty text.
    """
    if not text:
        return 0
    return max(1, len(text) // 4)


def estimate_message_tokens(message: Message) -> int:
    """Estimate total tokens consumed by a Message and its ContentBlocks."""
    # 4 tokens overhead per message for role/formatting in chat formats
    tokens = 4
    if message.name:
        tokens += estimate_tokens(message.name)

    for block in message.content:
        if isinstance(block, TextBlock):
            tokens += estimate_tokens(block.text)
        elif isinstance(block, ThinkingBlock):
            tokens += estimate_tokens(block.text)
        elif isinstance(block, ToolCallBlock):
            tokens += estimate_tokens(block.name)
            if block.arguments:
                tokens += estimate_tokens(json.dumps(block.arguments))
        elif isinstance(block, ToolResultBlock):
            content = block.content
            if isinstance(content, (dict, list)):
                content_str = json.dumps(content)
            else:
                content_str = str(content)
            tokens += estimate_tokens(content_str)
        elif isinstance(block, ImageBlock):
            tokens += 65  # Approximate base tokens for an image reference

    return tokens


@dataclass(frozen=True, slots=True)
class ContextPolicy:
    """Policy for pruning conversation history to fit context windows.

    Args:
        max_tokens: Maximum allowed tokens for the conversation messages.
            When exceeded, oldest atomic turns are pruned (while keeping system messages).
        max_turns: Maximum number of user/assistant conversational turns to keep.
        max_tool_output_chars: Maximum characters allowed in a single ToolResultBlock.
            Longer outputs are truncated with an informative notice.
        preserve_system: If True, system messages are always pinned at the start.
    """

    max_tokens: int | None = None
    max_turns: int | None = None
    max_tool_output_chars: int = 12_000
    preserve_system: bool = True
    name: str = "context_policy"

    def apply(self, spec: AgentSpec) -> None:
        """Behavior protocol support: allows passing ContextPolicy in behaviors list."""
        # Attached to spec or consumed by Agent
        pass

    def prune(self, messages: list[Message]) -> list[Message]:
        """Prune a message list according to this policy.

        Guarantees atomic turn pairing (never splits assistant tool call from tool results)
        and preserves pinned system instructions.
        """
        if not messages:
            return []

        # Step 1: Truncate oversized tool result blocks
        processed_messages: list[Message] = []
        for m in messages:
            if m.role == "tool":
                new_blocks: list[ContentBlock] = []
                for b in m.content:
                    if isinstance(b, ToolResultBlock):
                        new_blocks.append(self._truncate_tool_block(b))
                    else:
                        new_blocks.append(b)
                processed_messages.append(replace(m, content=new_blocks))
            else:
                processed_messages.append(m)

        # Step 2: Separate pinned system messages from conversational turns
        system_messages: list[Message] = []
        conversation_messages: list[Message] = []

        for m in processed_messages:
            if m.role == "system" and self.preserve_system:
                system_messages.append(m)
            else:
                conversation_messages.append(m)

        # Step 3: Group conversational messages into atomic units
        atomic_units = self._group_into_atomic_units(conversation_messages)

        # Step 4: Apply max_turns filter if configured
        if self.max_turns is not None and self.max_turns > 0:
            atomic_units = atomic_units[-self.max_turns :]

        # Step 5: Apply max_tokens budget if configured
        if self.max_tokens is not None and self.max_tokens > 0:
            system_tokens = sum(estimate_message_tokens(m) for m in system_messages)
            available_tokens = max(0, self.max_tokens - system_tokens)

            selected_units: list[list[Message]] = []
            accumulated_tokens = 0

            # Traverse from newest to oldest
            for unit in reversed(atomic_units):
                unit_tokens = sum(estimate_message_tokens(m) for m in unit)
                if accumulated_tokens + unit_tokens <= available_tokens or not selected_units:
                    selected_units.append(unit)
                    accumulated_tokens += unit_tokens
                else:
                    break

            selected_units.reverse()
            atomic_units = selected_units

        # Step 6: Reconstruct flat message list
        flattened: list[Message] = list(system_messages)
        for unit in atomic_units:
            flattened.extend(unit)

        return flattened

    def _truncate_tool_block(self, block: ToolResultBlock) -> ToolResultBlock:
        content = block.content
        if isinstance(content, str) and len(content) > self.max_tool_output_chars:
            allowed = self.max_tool_output_chars
            omitted = len(content) - allowed
            truncated_text = (
                content[:allowed]
                + f"\n... [Output truncated: {omitted:,} characters omitted by context policy]"
            )
            return ToolResultBlock(
                tool_call_id=block.tool_call_id,
                content=truncated_text,
                is_error=block.is_error,
            )
        elif isinstance(content, (dict, list)):
            dumped = json.dumps(content)
            if len(dumped) > self.max_tool_output_chars:
                allowed = self.max_tool_output_chars
                omitted = len(dumped) - allowed
                truncated_text = (
                    dumped[:allowed]
                    + f"\n... [JSON output truncated: {omitted:,} characters omitted by context policy]"
                )
                return ToolResultBlock(
                    tool_call_id=block.tool_call_id,
                    content=truncated_text,
                    is_error=block.is_error,
                )
        return block

    @staticmethod
    def _group_into_atomic_units(messages: list[Message]) -> list[list[Message]]:
        """Group messages into atomic turns.

        An assistant message with tool calls and all subsequent matching tool results
        belong to the same atomic unit.
        """
        units: list[list[Message]] = []
        i = 0
        n = len(messages)

        while i < n:
            msg = messages[i]
            if msg.role == "user":
                unit = [msg]
                i += 1
                while i < n and messages[i].role != "user":
                    unit.append(messages[i])
                    i += 1
                units.append(unit)
            elif msg.role == "assistant" and msg.tool_calls:
                unit = [msg]
                i += 1
                # Consume all consecutive tool result messages
                while i < n and messages[i].role == "tool":
                    unit.append(messages[i])
                    i += 1
                units.append(unit)
            else:
                units.append([msg])
                i += 1

        return units
