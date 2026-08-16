"""Structured output helpers.

If a Pydantic model is set as ``output_type`` on an Agent:
    1. When the model advertises ``Capability.STRUCTURED_OUTPUT``, we send a
       native ``response_format={"type": "json_schema", ...}`` to force JSON
       matching the schema.
    2. Otherwise, we prompt-engineer: append a schema hint to the system
       message and parse+validate the model's final text as JSON.

Both paths ultimately flow through ``parse_output`` which tolerates common
wrapping quirks (whitespace, markdown fences).
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ValidationError

from .errors import OutputParseError


def build_response_format(output_type: type[BaseModel]) -> dict[str, Any]:
    """Build an OpenAI-style ``response_format`` payload for structured output.

    Note: we do NOT set ``strict: true`` because provider support for strict
    mode varies (OpenAI supports it; Groq/OpenRouter often reject the request
    body when strict is combined with certain schemas). Users who need strict
    can override by passing ``response_format=`` on the model directly.
    """
    schema = output_type.model_json_schema()
    return {
        "type": "json_schema",
        "json_schema": {
            "name": output_type.__name__,
            "schema": schema,
        },
    }


def build_prompt_schema_hint(output_type: type[BaseModel]) -> str:
    """Return a system-prompt fragment for prompt-engineered JSON output.

    Used when the model doesn't advertise native structured-output support.
    """
    schema = json.dumps(output_type.model_json_schema(), indent=2)
    return (
        "\n\nRespond ONLY with a JSON object matching this schema. "
        "Do not wrap in markdown code fences. Do not include any prose "
        "outside the JSON.\n"
        f"Schema:\n{schema}"
    )


def _strip_markdown_fences(text: str) -> str:
    """Strip ``` ...``` or ```json ...``` fences from a text block."""
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.split("\n")
    # Drop opening fence (```json / ```)
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    # Drop trailing fence
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def parse_output(
    text: str,
    output_type: type[BaseModel],
    *,
    agent_name: str,
) -> Any:
    """Parse a model's text into an ``output_type`` instance.

    Tolerates leading/trailing whitespace and markdown code fences. Raises
    ``OutputParseError`` on invalid JSON or Pydantic validation failure.
    """
    cleaned = _strip_markdown_fences(text)

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise OutputParseError(agent_name, text, [f"invalid JSON: {e}"]) from e

    try:
        return output_type.model_validate(parsed)
    except ValidationError as e:
        details = [
            f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
            for err in e.errors()
        ]
        raise OutputParseError(agent_name, text, details) from e
