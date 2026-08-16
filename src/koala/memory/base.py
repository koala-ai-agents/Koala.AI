"""Abstract memory backend for per-session conversation history.

`BaseMemory` is a small async contract: append/get/clear/sessions. It stores
conversation history keyed by ``session_id`` — the shape agents need for
multi-turn interactions.

All methods are ``async`` (even for backends that could be sync) so higher
layers can await memory ops uniformly without special-casing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..core.messages import Message


class BaseMemory(ABC):
    """Store conversation history per session.

    Backends implement four operations:
        - ``append(session_id, messages)`` — extend a session's history
        - ``get(session_id, limit=?)``     — retrieve chronological history
        - ``clear(session_id)``            — remove a session's messages
        - ``sessions()``                   — list every known session
    """

    @abstractmethod
    async def append(
        self, session_id: str, messages: list[Message]
    ) -> None:
        """Append one or more messages to a session's history.

        Order is preserved. Callers should append in the order the messages
        appeared in the run (user, assistant, tool_result, ...).
        """
        raise NotImplementedError

    @abstractmethod
    async def get(
        self, session_id: str, *, limit: int | None = None
    ) -> list[Message]:
        """Return the session's history in chronological order.

        Args:
            session_id: Session key.
            limit: When set to a positive int, return only the last ``limit``
                messages. ``0`` returns an empty list. ``None`` returns all.

        Returns:
            A list of messages. Empty list for unknown sessions.
        """
        raise NotImplementedError

    @abstractmethod
    async def clear(self, session_id: str) -> None:
        """Remove all messages for a session. Idempotent."""
        raise NotImplementedError

    @abstractmethod
    async def sessions(self) -> list[str]:
        """Return every known session id, sorted."""
        raise NotImplementedError

    # Lifecycle -----------------------------------------------------------

    async def close(self) -> None:
        """Release backend resources. Default: no-op."""
        return None

    async def __aenter__(self) -> "BaseMemory":
        return self

    async def __aexit__(
        self, exc_type: object, exc: object, tb: object
    ) -> None:
        await self.close()
