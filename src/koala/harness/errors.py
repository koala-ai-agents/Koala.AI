"""Harness / session errors."""

from __future__ import annotations


class SessionError(Exception):
    """Base for harness / session errors."""


class SessionClosedError(SessionError):
    """Operation attempted on a session that's been closed."""


class ResolverTimeoutError(SessionError):
    """A resolver (approval / user-input) did not receive a reply in time."""

    def __init__(self, kind: str, request_id: str, timeout: float) -> None:
        self.kind = kind
        self.request_id = request_id
        self.timeout = timeout
        super().__init__(
            f"{kind} resolver for request {request_id!r} timed out after "
            f"{timeout:.1f}s"
        )
