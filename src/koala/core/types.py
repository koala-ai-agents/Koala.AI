"""Small shared types: Usage, ModelRef, and the string-shorthand parser."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Usage:
    """Cumulative usage across a run.

    Mutable on purpose: callees can accumulate into a caller-provided Usage
    via `add()` without allocating a fresh object per turn.
    """

    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0
    requests: int = 0
    cost_usd: float = 0.0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def add(self, other: "Usage") -> None:
        """Accumulate `other` into this instance in place."""
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.cached_input_tokens += other.cached_input_tokens
        self.reasoning_tokens += other.reasoning_tokens
        self.requests += other.requests
        self.cost_usd += other.cost_usd


@dataclass(frozen=True, slots=True)
class ModelRef:
    """A parsed `provider/name` shorthand.

    The `provider` is the routing key ("openai", "groq", "ollama", "anthropic",
    ...). `name` is whatever the provider recognises as a model identifier
    ("gpt-4o-mini", "llama-3.3-70b-versatile", "llama3:8b", ...).
    """

    provider: str
    name: str

    def __str__(self) -> str:
        return f"{self.provider}/{self.name}"


def parse_model_ref(ref: str) -> ModelRef:
    """Parse a `provider/model-name` shorthand into a ModelRef.

    The split is on the first `/` only, so model names containing `/` or `:`
    (e.g. `ollama/library/llama3:8b`) are preserved intact after the provider.

    Raises:
        ValueError: If the string doesn't contain a `/`, or if either the
            provider or the name portion is empty after stripping.
    """
    if "/" not in ref:
        raise ValueError(
            f"Model reference must be in `provider/name` form, got {ref!r}"
        )
    provider, name = ref.split("/", 1)
    provider = provider.strip()
    name = name.strip()
    if not provider or not name:
        raise ValueError(f"Model reference has an empty provider or name: {ref!r}")
    return ModelRef(provider=provider, name=name)
