"""Guardrails and policy hooks for flows.

Provides a Guard interface for pre/post step checks and a GuardsRegistry to
register multiple guards. Executors will call guards before and after each
step, allowing validation or modification of inputs/outputs.
"""

from __future__ import annotations

from typing import Any, Dict, List


class GuardError(Exception):
    pass


class Guard:
    """Base class for guards.

    Subclasses may override `pre_step` to validate or modify args and
    `post_step` to validate or modify results. Both should raise GuardError
    to stop execution on violations.
    """

    def pre_step(
        self, step_id: str, action: str, args: Dict[str, Any]
    ) -> Dict[str, Any]:
        return args

    def post_step(
        self, step_id: str, action: str, args: Dict[str, Any], result: Any
    ) -> Any:
        return result


class GuardsRegistry:
    def __init__(self) -> None:
        self._guards: List[Guard] = []

    def register(self, guard: Guard) -> None:
        self._guards.append(guard)

    def unregister(self, guard: Guard) -> None:
        try:
            self._guards.remove(guard)
        except ValueError:
            pass

    def run_pre(
        self, step_id: str, action: str, args: Dict[str, Any]
    ) -> Dict[str, Any]:
        cur = args
        for g in self._guards:
            cur = g.pre_step(step_id, action, cur)
            if not isinstance(cur, dict):
                raise GuardError("pre_step must return a dict of args")
        return cur

    def run_post(
        self, step_id: str, action: str, args: Dict[str, Any], result: Any
    ) -> Any:
        cur = result
        for g in self._guards:
            cur = g.post_step(step_id, action, args, cur)
        return cur
