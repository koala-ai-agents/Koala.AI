"""Tools / Actions API for Kola.

Provides a ToolsRegistry for registering and discovering callable tools, a
`tool` decorator for convenience, and minimal input/output contract checking.

This is intentionally lightweight: schemas are simple mappings of parameter
names to primitive type names ("int", "str", "float", "bool", "any").
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from .auth import default_auth


class ToolsError(Exception):
    pass


_PRIMITIVE_MAP = {
    "int": int,
    "str": str,
    "float": float,
    "bool": bool,
    "any": object,
}


def _check_type(expected: str, value: Any) -> bool:
    if expected == "any":
        return True
    typ = _PRIMITIVE_MAP.get(expected)
    if typ is None:
        raise ToolsError(f"Unknown type in schema: {expected}")
    return isinstance(value, typ)


class ToolMeta:
    def __init__(
        self,
        name: str,
        func: Callable[..., Any],
        input_schema: Optional[Dict[str, str]] = None,
        output_schema: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
    ) -> None:
        self.name = name
        self.func = func
        self.input_schema = input_schema or {}
        self.output_schema = output_schema or {}
        self.description = description or ""


class ToolsRegistry:
    """Registry for tools/actions.

    Methods:
    - register(name, func, input_schema, output_schema)
    - unregister(name)
    - get(name) -> ToolMeta
    - list_tools() -> List[str]
    - call(name, **kwargs) -> Any (validates input against schema)
    """

    def __init__(self) -> None:
        self._tools: Dict[str, ToolMeta] = {}

    def register(
        self,
        name: str,
        func: Callable[..., Any],
        input_schema: Optional[Dict[str, str]] = None,
        output_schema: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
    ) -> None:
        if name in self._tools:
            # allow idempotent registration when the same callable is re-registered
            existing = self._tools[name]
            if existing.func is func:
                return
            # overwrite existing registration with new callable (tests and dev flows expect re-registration)
            self._tools[name] = ToolMeta(
                name=name,
                func=func,
                input_schema=input_schema,
                output_schema=output_schema,
                description=description,
            )
            return
        self._tools[name] = ToolMeta(
            name=name,
            func=func,
            input_schema=input_schema,
            output_schema=output_schema,
            description=description,
        )

    def unregister(self, name: str) -> None:
        if name in self._tools:
            del self._tools[name]
        else:
            raise ToolsError(f"Tool {name} not found")

    def get(self, name: str) -> ToolMeta:
        t = self._tools.get(name)
        if not t:
            raise ToolsError(f"Tool {name} not found")
        return t

    def list_tools(self) -> List[str]:
        return sorted(self._tools.keys())

    def call(self, name: str, principal: Optional[str] = None, **kwargs: Any) -> Any:
        """Call a registered tool.

        If `principal` (api_key) is provided, check `default_auth` to see if the
        principal is allowed to call `name`. Backwards-compatible callers can
        omit `principal`.
        """
        t = self.get(name)
        # access control check if principal provided
        if principal:
            if not default_auth.is_allowed(principal, name):
                raise ToolsError(f"Principal not authorized to call tool '{name}'")
        # validate input schema: required keys are those present in schema
        for param, ptype in t.input_schema.items():
            if param not in kwargs:
                raise ToolsError(
                    f"Missing required parameter '{param}' for tool '{name}'"
                )
            if not _check_type(ptype, kwargs[param]):
                raise ToolsError(
                    f"Parameter '{param}' for tool '{name}' expected type {ptype}, got {type(kwargs[param]).__name__}"
                )

        # call the function
        return t.func(**kwargs)


# module-level default registry
default_registry = ToolsRegistry()


def tool(
    name: Optional[str] = None,
    input_schema: Optional[Dict[str, str]] = None,
    output_schema: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    registry: ToolsRegistry = default_registry,
):
    """Decorator to register a function as a tool in the given registry.

    Usage:
        @tool('add', input_schema={'a':'int','b':'int'})
        def add(a,b):
            return a+b
    """

    def deco(func: Callable[..., Any]):
        nm = name or getattr(func, "__name__", None)
        if not nm:
            raise ToolsError("Tool must have a name")
        registry.register(
            nm,
            func,
            input_schema=input_schema,
            output_schema=output_schema,
            description=description,
        )
        return func

    return deco
