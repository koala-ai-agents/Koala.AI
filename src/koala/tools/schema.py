"""Function signature -> JSON Schema conversion via Pydantic.

Given a Python function, `build_tool_spec` produces:
    - a summary (docstring pre-Args section)
    - a Pydantic model that validates the tool's arguments at call time
    - the corresponding JSON schema (OpenAI-compatible `parameters`)
    - the list of parameter names Koala will inject at runtime (currently
      just the `RunContext` param, if any)

Parameter descriptions come from Google-style `Args:` docstring blocks, or
inline from `Annotated[T, "description"]`.
"""

from __future__ import annotations

import inspect
import re
from dataclasses import dataclass
from typing import Annotated, Any, Callable, get_args, get_origin, get_type_hints

from pydantic import BaseModel, Field, create_model

from ..core.context import RunContext


@dataclass(slots=True)
class ToolSpec:
    """Everything a tool needs, derived once from a function."""

    description: str
    param_model: type[BaseModel]
    schema: dict[str, Any]
    injected_params: list[str]


# ---------------------------------------------------------------------------
# Docstring parsing
# ---------------------------------------------------------------------------

_SECTION_HEADER_RE = re.compile(
    r"^(Args|Arguments|Parameters|Returns?|Raises?|Yields?|Examples?|Notes?|"
    r"Attributes?|See Also):\s*$"
)
_ARGS_HEADER_RE = re.compile(r"^(Args|Arguments|Parameters):\s*$")
_PARAM_LINE_RE = re.compile(r"^(\w+)(?:\s*\([^)]*\))?:\s*(.*)$")


def parse_google_docstring(docstring: str | None) -> tuple[str, dict[str, str]]:
    """Split a Google-style docstring into (summary, {param_name: description}).

    - Anything before the `Args:` section becomes the summary.
    - Lines inside `Args:` are parsed as `param_name: description`, with
      continuation lines joined by spaces.
    - The Args block ends at any other recognized section header
      (Returns, Raises, ...).
    """
    if not docstring:
        return "", {}

    summary_lines: list[str] = []
    param_descs: dict[str, str] = {}

    lines = inspect.cleandoc(docstring).splitlines()

    in_args = False
    current_param: str | None = None
    current_desc_parts: list[str] = []

    def flush() -> None:
        nonlocal current_param, current_desc_parts
        if current_param:
            joined = " ".join(p.strip() for p in current_desc_parts).strip()
            param_descs[current_param] = joined
        current_param = None
        current_desc_parts = []

    for line in lines:
        stripped = line.strip()

        if _ARGS_HEADER_RE.match(stripped):
            flush()
            in_args = True
            continue

        if in_args and _SECTION_HEADER_RE.match(stripped):
            flush()
            in_args = False
            continue

        if in_args:
            m = _PARAM_LINE_RE.match(stripped)
            if m:
                flush()
                current_param = m.group(1)
                first = m.group(2)
                current_desc_parts = [first] if first else []
            elif current_param and stripped:
                current_desc_parts.append(stripped)
        else:
            summary_lines.append(line)

    flush()
    summary = "\n".join(summary_lines).strip()
    return summary, param_descs


# ---------------------------------------------------------------------------
# Type-hint helpers
# ---------------------------------------------------------------------------


def _unwrap_annotated(hint: Any) -> tuple[Any, str | None]:
    """Return (base_type, inline_description) from `Annotated[T, "desc"]`.

    If `hint` is not annotated, returns (hint, None).
    """
    if get_origin(hint) is Annotated or hasattr(hint, "__metadata__"):
        args = get_args(hint)
        if args:
            base = args[0]
            for meta in args[1:]:
                if isinstance(meta, str):
                    return base, meta
            return base, None
    return hint, None


def _is_run_context_hint(hint: Any) -> bool:
    """Detect if a type hint is `RunContext` or `RunContext[T]`."""
    if hint is RunContext:
        return True
    origin = get_origin(hint)
    if origin is RunContext:
        return True
    return False


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------


def build_tool_spec(func: Callable[..., Any]) -> ToolSpec:
    """Introspect `func` and return everything a Tool needs.

    Raises:
        TypeError: If a non-injected parameter lacks a type hint (we need
            hints to build the argument schema).
    """
    sig = inspect.signature(func)
    hints = get_type_hints(func, include_extras=True)
    summary, docstring_descs = parse_google_docstring(func.__doc__)

    injected_params: list[str] = []
    fields: dict[str, Any] = {}

    for name, param in sig.parameters.items():
        # Skip *args / **kwargs — not representable in JSON schema
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        # Skip `self` on bound methods
        if name == "self":
            continue

        raw_hint = hints.get(name, param.annotation)

        # RunContext detection uses the raw hint (may be Annotated wrapper too)
        base_hint, inline_desc = _unwrap_annotated(raw_hint)
        if _is_run_context_hint(base_hint):
            injected_params.append(name)
            continue

        if base_hint is inspect.Parameter.empty:
            raise TypeError(
                f"Parameter {name!r} of {func.__name__!r} has no type hint. "
                "All non-injected tool parameters must be typed."
            )

        description = inline_desc or docstring_descs.get(name)
        default = (
            ...  # Ellipsis marks a required field in pydantic v2
            if param.default is inspect.Parameter.empty
            else param.default
        )

        if description is not None:
            fields[name] = (
                base_hint,
                Field(default=default, description=description),
            )
        else:
            fields[name] = (base_hint, default)

    # Build a Pydantic model dynamically for validation.
    model_name = f"{func.__name__.title().replace('_', '')}Params"
    param_model: type[BaseModel] = create_model(model_name, **fields)  # type: ignore[call-overload]

    schema = param_model.model_json_schema()
    # Strip Pydantic-generated cosmetic keys the model doesn't need to see.
    schema.pop("title", None)
    # Some Pydantic versions include a top-level "description"; leave it
    # to the caller to decide (tool description sits outside `parameters`).
    schema.pop("description", None)

    return ToolSpec(
        description=summary,
        param_model=param_model,
        schema=schema,
        injected_params=injected_params,
    )
