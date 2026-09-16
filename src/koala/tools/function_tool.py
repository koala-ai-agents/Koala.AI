"""FunctionTool + the @tool decorator.

Turn any Python function into a first-class Tool that satisfies L1's Runnable
protocol and speaks the OpenAI function-calling wire format.

Usage::

    from koala import tool

    @tool
    def add(a: int, b: int) -> int:
        \"\"\"Add two numbers.\"\"\"
        return a + b

    @tool("greet")
    async def greet_user(name: str) -> str:
        \"\"\"Greet the user by name.\"\"\"
        return f"Hello, {name}!"

    @tool(description="Custom description that overrides the docstring.")
    def custom(x: int) -> int:
        return x * 2

RunContext injection::

    @tool
    async def get_user(ctx: RunContext, user_id: str) -> dict:
        return await ctx.deps.db.fetch_user(user_id)

The model never sees the ``ctx`` parameter — it's absent from the JSON schema.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable, overload

from pydantic import ValidationError

from ..core.context import RunContext
from ..core.errors import ModelRetry
from .base import BaseTool
from .errors import ToolExecutionError, ToolValidationError
from .schema import ToolSpec, build_tool_spec


class FunctionTool(BaseTool):
    """A Tool backed by a plain Python function (sync or async).

    Args:
        func: The function to wrap.
        name: Public name. Defaults to ``func.__name__``.
        description: Description shown to the model. Defaults to the docstring
            summary; empty string if the function has no docstring.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        spec: ToolSpec = build_tool_spec(func)
        self.func = func
        self.name = name or func.__name__
        self.description = description if description is not None else spec.description
        self.schema = spec.schema
        self._param_model = spec.param_model
        self._injected_params = spec.injected_params
        self._is_async = inspect.iscoroutinefunction(func)

    async def run(self, ctx: RunContext, arguments: dict[str, Any]) -> Any:
        # 1. Validate + coerce arguments through the generated Pydantic model.
        try:
            validated_model = self._param_model.model_validate(arguments)
        except ValidationError as e:
            details = [
                f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
                for err in e.errors()
            ]
            raise ToolValidationError(self.name, details) from e

        # `.model_dump()` gives us a plain dict with coerced types (e.g.
        # strings for ints get converted, defaults filled in).
        validated: dict[str, Any] = validated_model.model_dump()

        # 2. Inject RunContext into any declared injection params.
        for injected_name in self._injected_params:
            validated[injected_name] = ctx

        # 3. Invoke — async natively, sync via thread pool so it doesn't
        # block the event loop.
        try:
            if self._is_async:
                return await self.func(**validated)
            return await asyncio.to_thread(self.func, **validated)
        except (ToolValidationError, ToolExecutionError, ModelRetry):
            raise  # already tagged; don't re-wrap
        except Exception as e:  # noqa: BLE001 — wrap ANY user error
            raise ToolExecutionError(self.name, e) from e


# ---------------------------------------------------------------------------
# @tool decorator
# ---------------------------------------------------------------------------


@overload
def tool(func: Callable[..., Any], /) -> FunctionTool: ...
@overload
def tool(
    name: str, /, *, description: str | None = None
) -> Callable[[Callable[..., Any]], FunctionTool]: ...
@overload
def tool(
    *, name: str | None = None, description: str | None = None
) -> Callable[[Callable[..., Any]], FunctionTool]: ...


def tool(
    name_or_func: str | Callable[..., Any] | None = None,
    /,
    *,
    name: str | None = None,
    description: str | None = None,
) -> FunctionTool | Callable[[Callable[..., Any]], FunctionTool]:
    """Decorate a function to make it a Tool.

    Three forms::

        @tool
        def foo(x: int) -> int: ...

        @tool("public_name")
        def foo(x: int) -> int: ...

        @tool(name="public_name", description="Does foo.")
        def foo(x: int) -> int: ...
    """
    # Form 1: @tool (no parens) — name_or_func is the function
    if callable(name_or_func):
        if name is not None or description is not None:
            raise TypeError(
                "@tool used without parentheses cannot take name/description "
                "kwargs. Use @tool(name=..., description=...) instead."
            )
        return FunctionTool(name_or_func)

    # Form 2: @tool("name") — name_or_func is a string
    if isinstance(name_or_func, str):
        resolved_name = name_or_func

        def decorator_str(func: Callable[..., Any]) -> FunctionTool:
            return FunctionTool(func, name=resolved_name, description=description)

        return decorator_str

    # Form 3: @tool() or @tool(name=..., description=...)
    if name_or_func is None:
        def decorator_kw(func: Callable[..., Any]) -> FunctionTool:
            return FunctionTool(func, name=name, description=description)

        return decorator_kw

    raise TypeError(
        "@tool expects to decorate a function, or to receive a name string, "
        f"or no positional args. Got {type(name_or_func).__name__}."
    )
