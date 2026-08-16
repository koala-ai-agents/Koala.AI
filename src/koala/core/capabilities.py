"""Capability flags for models and tools.

Rather than a fat abstract base class where every implementation must either
provide or `NotImplementedError`-out every feature, implementations advertise
the set of capabilities they support. Higher layers dispatch on membership:

    if Capability.TOOL_CALLING in model.capabilities:
        # use native function calling
    else:
        # fall back to prompt-engineered tool use

This is a growing set. Add new members as new provider features emerge; never
remove members (that would break advertised contracts of existing models).
"""

from __future__ import annotations

from enum import StrEnum


class Capability(StrEnum):
    """Feature flags a Model or Tool may advertise."""

    # Streaming
    STREAMING = "streaming"

    # Function / tool calling
    TOOL_CALLING = "tool_calling"
    PARALLEL_TOOL_CALLS = "parallel_tool_calls"

    # Structured / JSON output
    STRUCTURED_OUTPUT = "structured_output"
    JSON_MODE = "json_mode"

    # Reasoning / thinking
    THINKING = "thinking"

    # Prompt / context features
    CACHING = "caching"
    LONG_CONTEXT = "long_context"

    # Modalities
    IMAGE_INPUT = "image_input"
    IMAGE_OUTPUT = "image_output"
    AUDIO_INPUT = "audio_input"
    AUDIO_OUTPUT = "audio_output"

    # Non-chat models
    EMBEDDING = "embedding"
    RERANKING = "reranking"
