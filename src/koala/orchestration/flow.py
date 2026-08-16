"""Flow — DAG orchestration that accepts Runnable steps directly.

Unlike the legacy ``koala.flow.DAGFlow`` which addressed tools by string
name and looked them up in a registry, the new ``Flow`` accepts step actions
in any of these forms:

    - a string (resolved via ``LocalExecutor.registry`` — legacy path)
    - a plain Python callable (sync or async function)
    - a ``BaseTool`` (called with the step's args dict)
    - a ``BaseAgent`` (called with a ``prompt=`` / ``input=`` string)
    - a ``Model`` (called with a ``prompt=`` / ``messages=``)
    - anything that satisfies the L1 ``Runnable`` protocol

The `flow(...)` helper starts a fluent builder that produces a ``Flow``:

    from koala.orchestration import flow, LocalExecutor

    my_flow = (
        flow("research-and-write")
        .step("research", research_agent, prompt="$input.topic")
        .step("write", writer_agent, prompt="$result.research")
        .edge("research", "write")
        .build()
    )
    results = LocalExecutor().run(my_flow, input={"topic": "koalas"})
    print(results["write"])
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Union

from ..core.runnable import Runnable
from .errors import FlowError

# The action a step may perform. `Runnable` catches Agents, Tools, Models,
# and any user-defined Protocol-conforming type structurally.
StepAction = Union[str, Runnable, Callable[..., Any]]


@dataclass
class Step:
    """One node in a Flow.

    Args:
        id: Unique step id within the flow.
        action: What to invoke — see ``StepAction`` for the accepted forms.
        args: Keyword arguments passed to the action. Values may contain
            ``"$result.other_step"`` or ``"$input.some_key"`` references that
            the executor substitutes at run time.
        timeout: Optional per-step timeout in seconds. When exceeded the step
            is cancelled and the flow fails with ``StepExecutionError``.
        retries: Number of additional attempts after the first failure.
            ``0`` (default) means no retries.
    """

    id: str
    action: StepAction
    args: dict[str, Any] = field(default_factory=dict)
    timeout: float | None = None
    retries: int = 0


@dataclass
class Flow:
    """A directed acyclic graph of steps.

    Prefer building via ``flow(id).step(...).edge(...).build()`` rather than
    constructing ``Flow`` directly.
    """

    id: str
    steps: list[Step] = field(default_factory=list)
    edges: list[tuple[str, str]] = field(default_factory=list)
    version: str = "0.1.0"

    # ------------------------------------------------------------------
    # Convenience runners — one-call access to executors.
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        deps: Any = None,
        input: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run this Flow via a fresh :class:`LocalExecutor`.

        Equivalent to ``LocalExecutor().run(self, deps=deps, input=input)``
        but saves the executor construction line for one-shot scripts.
        """
        from .executor import LocalExecutor

        return LocalExecutor().run(self, deps=deps, input=input)


class FlowBuilder:
    """Fluent builder for a ``Flow``.

    Detects duplicate ids and unknown edge endpoints as they're added, and
    detects cycles on ``build()``.
    """

    def __init__(self, id: str, version: str | None = None) -> None:
        self._flow = Flow(id=id, version=version or "0.1.0")

    def step(
        self,
        id: str,
        action: StepAction,
        *,
        timeout: float | None = None,
        retries: int = 0,
        **kwargs: Any,
    ) -> "FlowBuilder":
        """Add a step. Any additional kwargs become the step's args."""
        if any(s.id == id for s in self._flow.steps):
            raise FlowError(f"Duplicate step id: {id!r}")
        self._flow.steps.append(
            Step(
                id=id,
                action=action,
                args=dict(kwargs),
                timeout=timeout,
                retries=retries,
            )
        )
        return self

    def edge(self, from_id: str, to_id: str) -> "FlowBuilder":
        """Add a directed dependency ``from_id -> to_id``."""
        step_ids = {s.id for s in self._flow.steps}
        if from_id not in step_ids:
            raise FlowError(f"Edge from unknown step: {from_id!r}")
        if to_id not in step_ids:
            raise FlowError(f"Edge to unknown step: {to_id!r}")
        if from_id == to_id:
            raise FlowError(f"Self-loop edge on step {from_id!r} is not allowed")
        self._flow.edges.append((from_id, to_id))
        return self

    def build(self) -> Flow:
        """Validate and return the flow. Raises ``FlowError`` on a cycle."""
        _toposort(self._flow)  # raises if cyclic
        return self._flow


def flow(id: str, version: str | None = None) -> FlowBuilder:
    """Start building a Flow. Alias for ``FlowBuilder(id, version)``."""
    return FlowBuilder(id=id, version=version)


# ---------------------------------------------------------------------------
# Toposort — public so executors can reuse it.
# ---------------------------------------------------------------------------


def _toposort(flow: Flow) -> list[str]:
    """Return step ids in topological order. Raise ``FlowError`` on a cycle."""
    nodes = {s.id: s for s in flow.steps}
    incoming: dict[str, int] = dict.fromkeys(nodes, 0)
    outgoing: dict[str, list[str]] = {nid: [] for nid in nodes}
    for f, t in flow.edges:
        if f not in nodes:
            raise FlowError(f"Edge from unknown step: {f!r}")
        if t not in nodes:
            raise FlowError(f"Edge to unknown step: {t!r}")
        outgoing[f].append(t)
        incoming[t] += 1

    order: list[str] = []
    queue: list[str] = [nid for nid, deg in incoming.items() if deg == 0]
    while queue:
        n = queue.pop(0)
        order.append(n)
        for m in outgoing[n]:
            incoming[m] -= 1
            if incoming[m] == 0:
                queue.append(m)

    if len(order) != len(nodes):
        raise FlowError(f"Flow {flow.id!r} contains a cycle")
    return order
