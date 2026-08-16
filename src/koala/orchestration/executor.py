"""Runnable-aware LocalExecutor for Koala flows.

Executes a ``Flow`` in-process with async concurrency for independent steps.
Dispatches on the step's action type so users can mix Agents, Tools, Models,
plain callables, and legacy string-registry actions in a single flow.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import replace
from typing import Any

from ..agents.agent import BaseAgent
from ..core.context import RunContext
from ..core.messages import Message
from ..core.runnable import Runnable, ainvoke
from ..models.model import Model
from ..tools.base import BaseTool
from .errors import FlowError, StepExecutionError
from .flow import Flow, Step


class LocalExecutor:
    """Run flows in-process with async concurrency.

    Args:
        registry: Optional legacy-style ``{action_name: callable}`` map used
            when a step's action is a plain string. Enables back-compat with
            the old ``koala.flow`` API.
    """

    def __init__(
        self,
        *,
        registry: dict[str, Any] | None = None,
    ) -> None:
        self.registry: dict[str, Any] = dict(registry or {})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        flow: Flow,
        *,
        deps: Any = None,
        input: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Sync wrapper around ``arun``. Uses ``asyncio.run``."""
        return asyncio.run(self.arun(flow, deps=deps, input=input))

    async def arun(
        self,
        flow: Flow,
        *,
        deps: Any = None,
        input: dict[str, Any] | None = None,
        ctx: RunContext | None = None,
    ) -> dict[str, Any]:
        """Execute the flow and return ``{step_id: result}``.

        Steps whose dependencies are all satisfied run concurrently.
        ``$result.other_step`` and ``$input.key`` references in step args are
        substituted before invocation.
        """
        if not flow.steps:
            return {}

        if ctx is None:
            ctx = RunContext(deps=deps)

        nodes: dict[str, Step] = {s.id: s for s in flow.steps}
        incoming: dict[str, int] = dict.fromkeys(nodes, 0)
        outgoing: dict[str, list[str]] = {nid: [] for nid in nodes}
        for f, t in flow.edges:
            if f not in nodes:
                raise FlowError(f"Edge from unknown step: {f!r}")
            if t not in nodes:
                raise FlowError(f"Edge to unknown step: {t!r}")
            outgoing[f].append(t)
            incoming[t] += 1

        results: dict[str, Any] = {}
        input_vars: dict[str, Any] = dict(input or {})
        running: dict[asyncio.Task[Any], str] = {}

        async def submit(nid: str) -> None:
            step = nodes[nid]
            try:
                resolved = self._resolve_args(step.args, results, input_vars)
            except FlowError as e:
                # Arg resolution failures are per-step problems — wrap so the
                # caller sees a StepExecutionError like it would for any
                # other step-level failure.
                raise StepExecutionError(nid, e) from e
            task = asyncio.create_task(
                self._invoke_step(step, resolved, ctx),
                name=f"Flow[{flow.id}].{nid}",
            )
            running[task] = nid

        # Submit initial roots.
        roots = [nid for nid, deg in incoming.items() if deg == 0]
        if not roots and nodes:
            raise FlowError(f"Flow {flow.id!r} has no roots — cycle detected")
        for nid in roots:
            await submit(nid)

        try:
            while running:
                done, _ = await asyncio.wait(
                    list(running.keys()),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    nid = running.pop(task)
                    if task.cancelled():
                        raise FlowError(f"Step {nid!r} was cancelled")
                    exc = task.exception()
                    if exc is not None:
                        raise StepExecutionError(nid, exc) from exc
                    results[nid] = task.result()
                    for m in outgoing.get(nid, []):
                        incoming[m] -= 1
                        if incoming[m] == 0:
                            await submit(m)
        except (StepExecutionError, FlowError):
            # Cancel remaining running steps so we don't leak background work.
            for pending in list(running.keys()):
                pending.cancel()
            # Await cancellations to clean up.
            await asyncio.gather(*running.keys(), return_exceptions=True)
            raise

        if len(results) != len(nodes):
            missing = sorted(set(nodes) - set(results))
            raise FlowError(
                f"Flow {flow.id!r}: not all steps executed. Missing: {missing}"
            )
        return results

    # ------------------------------------------------------------------
    # Step dispatch
    # ------------------------------------------------------------------

    async def _invoke_step(
        self,
        step: Step,
        args: dict[str, Any],
        ctx: RunContext,
    ) -> Any:
        """Dispatch a step to its appropriate execution path.

        Order matters: concrete koala types (Agent/Tool/Model) are checked
        before the generic ``Runnable`` and ``callable`` fallbacks because
        the concrete types also structurally satisfy those.
        """
        action = step.action

        # 1. String -> executor registry (legacy compat).
        if isinstance(action, str):
            target = self.registry.get(action)
            if target is None:
                available = sorted(self.registry.keys()) or "(none)"
                raise FlowError(
                    f"Step {step.id!r}: action {action!r} not in registry. "
                    f"Available: {available}"
                )
            resolved_step = replace(step, action=target)
            return await self._invoke_step(resolved_step, args, ctx)

        # 2. Agent -> arun with prompt/input.
        if isinstance(action, BaseAgent):
            prompt = self._extract_agent_prompt(step.id, args)
            result = await action.arun(prompt, ctx=ctx.child())
            return result.output

        # 3. Tool -> run with args dict.
        if isinstance(action, BaseTool):
            return await action.run(ctx, args)

        # 4. Model -> chat with a user prompt or explicit messages.
        if isinstance(action, Model):
            messages = args.get("messages")
            prompt = args.get("prompt")
            if messages is None:
                if prompt is None:
                    raise FlowError(
                        f"Step {step.id!r}: model steps need `prompt=` or "
                        "`messages=`"
                    )
                messages = [Message.user(prompt)]
            reply = await action.chat(messages)
            return reply.text

        # 5. Generic Runnable (custom user-defined types).
        if isinstance(action, Runnable):
            return await ainvoke(action, ctx, args)

        # 6. Plain callable — sync in a worker thread, async awaited directly.
        if callable(action):
            if inspect.iscoroutinefunction(action):
                return await action(**args)
            return await asyncio.to_thread(lambda: action(**args))

        raise FlowError(
            f"Step {step.id!r}: unsupported action type "
            f"{type(action).__name__}. Expected str, callable, Runnable, "
            "BaseTool, BaseAgent, or Model."
        )

    @staticmethod
    def _extract_agent_prompt(step_id: str, args: dict[str, Any]) -> Any:
        """Pull the prompt for an Agent step from the resolved args."""
        if "prompt" in args:
            return args["prompt"]
        if "input" in args:
            return args["input"]
        if len(args) == 1:
            return next(iter(args.values()))
        if not args:
            raise FlowError(
                f"Step {step_id!r}: agent step has no args; supply `prompt=` "
                "or `input=`."
            )
        raise FlowError(
            f"Step {step_id!r}: agent step has multiple args but no "
            "`prompt=` or `input=` key to disambiguate."
        )

    # ------------------------------------------------------------------
    # Argument resolution ($result / $input)
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_args(
        args: dict[str, Any],
        results: dict[str, Any],
        input_vars: dict[str, Any],
    ) -> dict[str, Any]:
        """Resolve ``$result.X`` and ``$input.X`` references in step args."""
        resolved: dict[str, Any] = {}
        for k, v in args.items():
            if isinstance(v, str) and v.startswith("$result."):
                resolved[k] = _lookup_dotted(results, v[len("$result.") :])
            elif isinstance(v, str) and v.startswith("$input."):
                resolved[k] = _lookup_dotted(input_vars, v[len("$input.") :])
            else:
                resolved[k] = v
        return resolved


def _lookup_dotted(source: dict[str, Any], path: str) -> Any:
    """Look up a dotted path in a dict, following ``.attr`` on nested values.

    ``result.foo.bar`` becomes ``source["foo"]["bar"]`` for dicts, or
    ``getattr(source["foo"], "bar")`` for objects (used with Pydantic
    structured outputs).
    """
    parts = path.split(".")
    if not parts or not parts[0]:
        raise FlowError(f"Invalid reference path: {path!r}")
    head = parts[0]
    if head not in source:
        raise FlowError(f"Reference to unknown key: {head!r}")
    val: Any = source[head]
    for p in parts[1:]:
        if isinstance(val, dict):
            val = val.get(p)
        else:
            val = getattr(val, p, None)
    return val
