"""Minimal access control / auth helper for Kola tools.

This provides a tiny API-key based AuthManager intended for local development
and tests. It is intentionally simple: api_key -> allowed tool names.

Usage:
    from koala import auth
    auth.default_auth.register_api_key("key1", allowed_tools=["add"])
    registry.call("add", principal="key1", a=1, b=2)
"""

from __future__ import annotations

from typing import Dict, Iterable, Optional, Set


class AuthError(Exception):
    pass


class AuthManager:
    def __init__(self) -> None:
        # map api_key -> set of allowed tool names (empty set means allow none)
        self._keys: Dict[str, Set[str]] = {}

    def register_api_key(
        self, api_key: str, allowed_tools: Optional[Iterable[str]] = None
    ) -> None:
        self._keys[api_key] = set(allowed_tools or [])

    def unregister_api_key(self, api_key: str) -> None:
        if api_key in self._keys:
            del self._keys[api_key]

    def is_allowed(self, api_key: str, tool_name: str) -> bool:
        # if api_key not recognized -> deny
        allowed = self._keys.get(api_key)
        if allowed is None:
            return False
        # empty set means no permissions; '*' in allowed set means full access
        if "*" in allowed:
            return True
        return tool_name in allowed


# module-level default auth manager used by ToolsRegistry when principal is provided
default_auth = AuthManager()
