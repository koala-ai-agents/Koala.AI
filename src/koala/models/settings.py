"""Chat-completion settings.

`ChatSettings` is the common knobs users pass to a model. Every field defaults
to `None`, meaning "use whatever the layer below has set." This makes the
three-tier override (Model default → Agent override → per-call override)
clean via `.merge()`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ChatSettings:
    """Settings for one chat completion.

    All fields default to `None`; a `None` field inherits from the layer
    above during a merge. `extra` is a dict for provider-specific params
    that don't have a first-class field yet (reasoning_effort, etc.).
    """

    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    stop: list[str] | None = None
    seed: int | None = None
    frequency_penalty: float | None = None
    presence_penalty: float | None = None

    # Provider-specific extras. Shallow-merged on `.merge()`, with `other`
    # winning on key conflict.
    extra: dict[str, Any] = field(default_factory=dict)

    def merge(self, other: "ChatSettings") -> "ChatSettings":
        """Return a new ChatSettings with `other` layered over `self`.

        Non-None fields in `other` override `self`. `extra` dicts are
        shallow-merged (other wins on conflicting keys).
        """
        return ChatSettings(
            temperature=other.temperature if other.temperature is not None else self.temperature,
            max_tokens=other.max_tokens if other.max_tokens is not None else self.max_tokens,
            top_p=other.top_p if other.top_p is not None else self.top_p,
            stop=other.stop if other.stop is not None else self.stop,
            seed=other.seed if other.seed is not None else self.seed,
            frequency_penalty=(
                other.frequency_penalty
                if other.frequency_penalty is not None
                else self.frequency_penalty
            ),
            presence_penalty=(
                other.presence_penalty
                if other.presence_penalty is not None
                else self.presence_penalty
            ),
            extra={**self.extra, **other.extra},
        )

    def to_payload(self) -> dict[str, Any]:
        """Convert to OpenAI-compatible request fields, omitting None values."""
        payload: dict[str, Any] = {}
        if self.temperature is not None:
            payload["temperature"] = self.temperature
        if self.max_tokens is not None:
            payload["max_tokens"] = self.max_tokens
        if self.top_p is not None:
            payload["top_p"] = self.top_p
        if self.stop is not None:
            payload["stop"] = self.stop
        if self.seed is not None:
            payload["seed"] = self.seed
        if self.frequency_penalty is not None:
            payload["frequency_penalty"] = self.frequency_penalty
        if self.presence_penalty is not None:
            payload["presence_penalty"] = self.presence_penalty
        payload.update(self.extra)
        return payload
