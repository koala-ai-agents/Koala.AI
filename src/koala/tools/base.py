"""Abstract tool interface.

Every callable capability the model can invoke is a `BaseTool`. Concrete
implementations only need to provide `run(ctx, arguments)`; the L1 Runnable
interface (`astream`) is derived for free from `run`, and the OpenAI wire
schema is emitted from `self.name` / `self.description` / `self.schema`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

from ..core.context import RunContext
from ..core.events import Done, Error, Event, Output, Start


class BaseTool(ABC):
    """A callable capability the model can invoke.

    Subclasses must set ``name``, ``description``, ``schema`` before use, and
    implement ``run``.

    Attributes:
        name: Public identifier the model calls with.
        description: Human-readable description shown to the model.
        schema: JSON Schema (OpenAI-style `parameters` object) describing
            the tool's arguments.
    """

    name: str
    description: str
    schema: dict[str, Any]

    @abstractmethod
    async def run(self, ctx: RunContext, arguments: dict[str, Any]) -> Any:
        """Execute the tool.

        Args:
            ctx: Per-run context (deps, usage, cancellation).
            arguments: Arguments matching `self.schema`. Implementations may
                assume these have already been validated by the caller, but
                should still handle malformed input defensively.

        Returns:
            The tool's result. Must be JSON-serializable so it can round-trip
            through the LLM as a tool_result.

        Raises:
            ToolValidationError: If arguments fail validation.
            ToolExecutionError: If the underlying function raises.
        """
        raise NotImplementedError

    def to_openai_schema(self) -> dict[str, Any]:
        """Emit this tool in OpenAI's function-calling wire format."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.schema,
            },
        }

    # -- L1 Runnable[dict[str, Any], Any] --------------------------------

    async def astream(
        self,
        ctx: RunContext,
        input: dict[str, Any],
        /,
    ) -> AsyncIterator[Event]:
        """Runnable interface — emits Start / Output / Done (or Error / Done).

        Errors during `run` are emitted as a fatal `Error` event; they do
        NOT propagate as exceptions from the iterator. This makes tools
        composable with hooks and observability without try/except at every
        call site.
        """
        run_id = ctx.session_id
        yield Start(run_id=run_id, name=f"Tool({self.name})", input=input)
        try:
            result = await self.run(ctx, input)
        except Exception as e:  # noqa: BLE001 — we serialize every exception
            yield Error(error=str(e), exc_type=type(e).__name__, fatal=True)
            yield Done(run_id=run_id)
            return
        yield Output(value=result)
        yield Done(run_id=run_id)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r})"
