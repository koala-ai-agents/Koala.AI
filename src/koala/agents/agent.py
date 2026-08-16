"""Agent — the tool-calling loop, structured output, handoffs.

An ``Agent`` composes a ``Model`` with a set of tools, instructions, and
optional constraints (approval rules, output type, handoffs, max iterations).
It runs the classical tool-calling loop:

    1. Send messages + tool schemas to the model.
    2. If the model returns tool_calls, execute each and append tool_result
       messages.
    3. Loop until the model returns a plain answer or ``max_iterations`` hits.
    4. Optionally validate the final text against a Pydantic ``output_type``.

An Agent satisfies L1's Runnable protocol via ``astream``. That means the
Flow/harness/observability layers can treat it uniformly with Models and Tools.
"""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from ..harness.session import AgentSession

from ..behaviors.base import AgentSpec, Behavior
from ..core.approval import ApprovalRule
from ..core.capabilities import Capability
from ..core.context import RunContext
from ..core.events import (
    AwaitingApproval,
    Done,
    Error,
    Event,
    ModelMessage,
    Output,
    Start,
    ToolResult,
    UsageEvent,
)
from ..core.messages import Message, TextBlock, ToolResultBlock
from ..memory.base import BaseMemory
from ..models.model import Model
from ..models.settings import ChatSettings
from ..observability.otel import agent_span, model_span, tool_span
from ..tools.approval import evaluate_approval_chain
from ..tools.base import BaseTool
from ..tools.errors import ToolExecutionError, ToolValidationError
from .errors import OutputParseError
from .output import build_prompt_schema_hint, build_response_format, parse_output
from .result import RunResult


class BaseAgent(ABC):
    """Abstract agent contract.

    Concrete agents must implement ``astream``. ``arun`` / ``run`` are
    provided by default and drive ``astream`` to collect a ``RunResult``.
    """

    name: str

    @abstractmethod
    def astream(
        self,
        ctx: RunContext,
        input: str | list[Message],
        /,
    ) -> AsyncIterator[Event]:
        raise NotImplementedError


class Agent(BaseAgent):
    """The standard tool-calling agent.

    Args:
        model: A ``Model`` instance or a ``provider/name`` string that is
            resolved via the built-in provider registry.
        instructions: Optional system prompt prepended to every run.
        tools: List of ``BaseTool`` objects the agent can call.
        output_type: A Pydantic ``BaseModel`` subclass. When set, forces
            structured output and validates the final response into an
            instance of this model.
        approval_rules: Ordered list of ``ApprovalRule`` consulted before
            every tool invocation. First non-``None`` decision wins.
        handoffs: Other agents exposed as ``transfer_to_*`` tools; falls
            out of the Rig "Agent is a Tool" pattern.
        max_iterations: Hard cap on tool-call loops. Default 20.
        name: Public name — used in Start events and as the default source
            for handoff tool names.
        settings: ``ChatSettings`` overrides layered over the Model's defaults.
    """

    def __init__(
        self,
        model: Model | str,
        *,
        instructions: str | None = None,
        tools: list[BaseTool] | None = None,
        output_type: type[BaseModel] | None = None,
        approval_rules: list[ApprovalRule] | None = None,
        handoffs: list["Agent"] | None = None,
        memory: BaseMemory | None = None,
        behaviors: list[Behavior] | None = None,
        max_iterations: int = 20,
        name: str = "agent",
        settings: ChatSettings | None = None,
    ) -> None:
        # 1. Model — coerce string shorthand.
        if isinstance(model, str):
            model = Model(model)
        elif not isinstance(model, Model):
            raise TypeError(
                "Agent expects `model` to be a Model instance or a "
                f"'provider/name' string, got {type(model).__name__}."
            )
        self.model: Model = model

        # 2. Build a spec from explicit args, then layer behaviors on top.
        # This lets behaviors extend instructions/tools/rules or override
        # output_type / settings composably.
        spec = AgentSpec(
            instructions_parts=[instructions] if instructions else [],
            tools=list(tools or []),
            approval_rules=list(approval_rules or []),
            settings=settings or ChatSettings(),
            output_type=output_type,
        )
        # Handoff agents contribute as tools too — before user behaviors so
        # a ToolPack behavior can still stack on top.
        for a in handoffs or []:
            spec.tools.append(a.as_tool())
        # Apply user-supplied behaviors in order.
        for b in behaviors or []:
            if not hasattr(b, "apply"):
                raise TypeError(
                    f"behavior {b!r} does not implement apply(spec) — "
                    "see koala.behaviors.Behavior."
                )
            b.apply(spec)

        # 3. Extract final agent state from the completed spec.
        self.instructions: str | None = (
            "\n\n".join(p for p in spec.instructions_parts if p)
            or None
        )
        self.tools: list[BaseTool] = spec.tools
        self._tool_map: dict[str, BaseTool] = {}
        for t in self.tools:
            if t.name in self._tool_map:
                raise ValueError(
                    f"Duplicate tool name {t.name!r} on agent {name!r}."
                )
            self._tool_map[t.name] = t

        # 4. Validate output_type — behaviors could have set an invalid one.
        if spec.output_type is not None and not (
            isinstance(spec.output_type, type)
            and issubclass(spec.output_type, BaseModel)
        ):
            raise TypeError(
                "output_type must be a subclass of pydantic.BaseModel."
            )
        self.output_type: type[BaseModel] | None = spec.output_type

        # 5. Approval + settings.
        self.approval_rules: list[ApprovalRule] = spec.approval_rules
        self.settings: ChatSettings = spec.settings

        # 6. Loop cap.
        if max_iterations < 1:
            raise ValueError("max_iterations must be >= 1")
        self.max_iterations = max_iterations

        # 7. Optional conversation memory (L4).
        self.memory: BaseMemory | None = memory

        self.name = name

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Agent(name={self.name!r}, model={self.model!r}, "
            f"tools={len(self.tools)})"
        )

    # ------------------------------------------------------------------
    # Sync + async run
    # ------------------------------------------------------------------

    def run(
        self,
        input: str | list[Message],
        *,
        deps: Any = None,
        session_id: str | None = None,
    ) -> RunResult:
        """Sync convenience — runs the agent to completion via ``asyncio.run``.

        Must not be called from within a running event loop; use ``arun``
        from async code.
        """
        return asyncio.run(
            self.arun(input, deps=deps, session_id=session_id)
        )

    async def arun(
        self,
        input: str | list[Message],
        *,
        deps: Any = None,
        ctx: RunContext | None = None,
        session_id: str | None = None,
    ) -> RunResult:
        """Run to completion and return a ``RunResult``.

        Iterates the tool-calling loop until the model produces a final
        message with no tool_calls, or ``max_iterations`` is exceeded.

        Args:
            input: The user's input — a plain string or a full message list.
            deps: User-provided dependencies for tools (stored on ``RunContext``).
            ctx: Explicit run context. Mutually enriched with ``session_id`` if
                both are provided (session_id overrides ctx.session_id).
            session_id: Session key for conversation memory. When ``memory`` is
                set, prior turns for this session are loaded and prepended,
                and the new turn is persisted on success.
        """
        # 1. Resolve run context + session id.
        if ctx is None:
            ctx = RunContext(deps=deps)
        if session_id is not None:
            ctx.session_id = session_id
        sid = ctx.session_id

        # 2. Load prior conversation history from memory (if configured).
        prior_history: list[Message] = []
        if self.memory is not None:
            prior_history = await self.memory.get(sid)

        # 3. Materialize the new-turn input as a list of messages so we can
        # track what to persist.
        if isinstance(input, str):
            new_turn_input: list[Message] = [Message.user(input)]
        else:
            new_turn_input = list(input)

        # 4. Effective input passed to astream = prior + new turn.
        effective_input: list[Message] = prior_history + new_turn_input

        # 5. Run the loop, collecting produced messages.
        output: Any = None
        turn_messages: list[Message] = []
        stop_reason: Any = "error"
        error_msg: str | None = None

        async for event in self.astream(ctx, effective_input):
            if isinstance(event, Output):
                output = event.value
                stop_reason = "final_output"
            elif isinstance(event, ModelMessage):
                turn_messages.append(event.message)
            elif isinstance(event, ToolResult):
                turn_messages.append(
                    Message.tool(
                        tool_call_id=event.result.tool_call_id,
                        content=event.result.content,
                    )
                )
            elif isinstance(event, Error):
                error_msg = event.error
                if "Max iterations" in event.error:
                    stop_reason = "max_iterations"
                elif "cancelled" in event.error.lower():
                    stop_reason = "cancelled"
                else:
                    stop_reason = "error"

        iterations = sum(1 for m in turn_messages if m.role == "assistant")

        # 6. Persist to memory on success. Only the NEW turn is written;
        # prior history was already stored.
        if self.memory is not None and stop_reason == "final_output":
            await self.memory.append(sid, new_turn_input + turn_messages)

        return RunResult(
            output=output,
            messages=(
                self._build_initial_messages(effective_input) + turn_messages
            ),
            usage=ctx.usage,
            iterations=iterations,
            stop_reason=stop_reason,
            error=error_msg,
            metadata={"session_id": sid},
        )

    # ------------------------------------------------------------------
    # L1 Runnable — the tool-calling loop as an event stream
    # ------------------------------------------------------------------

    async def astream(
        self,
        ctx: RunContext,
        input: str | list[Message],
        /,
    ) -> AsyncIterator[Event]:
        """The tool-calling loop, streamed as core Events.

        Emits, in order:
            Start(agent)
            [ (ModelDelta*, ModelMessage, (ToolCall, ToolResult)*) ]+
            Output(final_value)
            Done
        Or on failure:
            Start(agent), ..., Error(...), Done
        """
        run_id = ctx.session_id
        yield Start(run_id=run_id, name=f"Agent({self.name})", input=input)

        messages: list[Message] = self._build_initial_messages(input)

        tools_schema: list[dict[str, Any]] | None = (
            [t.to_openai_schema() for t in self.tools] if self.tools else None
        )
        response_format: dict[str, Any] | None = None
        if (
            self.output_type is not None
            and Capability.STRUCTURED_OUTPUT in self.model.capabilities
        ):
            response_format = build_response_format(self.output_type)

        merged_settings = self.model.settings.merge(self.settings)

        with agent_span(
            name=self.name,
            description=self.instructions,
            conversation_id=ctx.session_id,
        ) as ag_span:
            iterations_done = 0
            for _iteration in range(1, self.max_iterations + 1):
                iterations_done = _iteration
                if ctx.cancel.cancelled:
                    ag_span.record_completion(
                        iterations=iterations_done,
                        stop_reason="cancelled",
                        input_tokens=ctx.usage.input_tokens,
                        output_tokens=ctx.usage.output_tokens,
                    )
                    yield Error(error="Run cancelled", fatal=True)
                    yield Done(run_id=run_id)
                    return

                assistant_msg: Message | None = None
                turn_in_tokens = 0
                turn_out_tokens = 0
                with model_span(
                    system=self.model.provider_slug,
                    model=self.model.name,
                    settings=merged_settings,
                ) as m_span:
                    try:
                        async for m_event in self.model.provider.stream_chat(
                            self.model.name,
                            messages,
                            merged_settings,
                            tools=tools_schema,
                            response_format=response_format,
                        ):
                            yield m_event
                            if isinstance(m_event, ModelMessage):
                                assistant_msg = m_event.message
                            elif isinstance(m_event, UsageEvent):
                                ctx.usage.add(m_event.usage)
                                turn_in_tokens = m_event.usage.input_tokens
                                turn_out_tokens = m_event.usage.output_tokens
                    except Exception as e:  # noqa: BLE001 — surface as fatal
                        m_span.record_error(e)
                        ag_span.record_error(e)
                        ag_span.record_completion(
                            iterations=iterations_done,
                            stop_reason="error",
                            input_tokens=ctx.usage.input_tokens,
                            output_tokens=ctx.usage.output_tokens,
                        )
                        yield Error(
                            error=str(e), exc_type=type(e).__name__, fatal=True
                        )
                        yield Done(run_id=run_id)
                        return

                    finish_reason: str | None = None
                    if assistant_msg is not None and assistant_msg.tool_calls:
                        finish_reason = "tool_calls"
                    elif assistant_msg is not None:
                        finish_reason = "stop"
                    m_span.record_response(
                        input_tokens=turn_in_tokens or None,
                        output_tokens=turn_out_tokens or None,
                        finish_reason=finish_reason,
                        response_model=self.model.name,
                    )

                if assistant_msg is None:
                    ag_span.record_completion(
                        iterations=iterations_done, stop_reason="error"
                    )
                    yield Error(error="Model returned no message", fatal=True)
                    yield Done(run_id=run_id)
                    return

                messages.append(assistant_msg)

                # Terminal case: no tool calls -> we have a final answer.
                if not assistant_msg.tool_calls:
                    final_output: Any = assistant_msg.text
                    if self.output_type is not None:
                        try:
                            final_output = parse_output(
                                assistant_msg.text,
                                self.output_type,
                                agent_name=self.name,
                            )
                        except OutputParseError as e:
                            ag_span.record_error(e)
                            ag_span.record_completion(
                                iterations=iterations_done,
                                stop_reason="error",
                                input_tokens=ctx.usage.input_tokens,
                                output_tokens=ctx.usage.output_tokens,
                            )
                            yield Error(
                                error=str(e),
                                exc_type="OutputParseError",
                                fatal=True,
                            )
                            yield Done(run_id=run_id)
                            return
                    ag_span.record_completion(
                        iterations=iterations_done,
                        stop_reason="final_output",
                        input_tokens=ctx.usage.input_tokens,
                        output_tokens=ctx.usage.output_tokens,
                    )
                    yield Output(value=final_output)
                    yield Done(run_id=run_id)
                    return

                # Execute each tool call from the assistant message.
                # NOTE: we do NOT emit ToolCall here — the provider already
                # emitted it during stream_chat when it finalized the
                # accumulated args. Re-emitting would double-print in every
                # downstream renderer.
                for call in assistant_msg.tool_calls:
                    # Approval flow: chain -> (optional resolver) -> decision.
                    decision: str = "allow"
                    deny_reason = ""
                    rule_name: str | None = None
                    if self.approval_rules:
                        chain_result = evaluate_approval_chain(
                            self.approval_rules, ctx, call
                        )
                        decision = chain_result.decision
                        deny_reason = chain_result.reason
                        rule_name = chain_result.rule_name

                    if decision == "ask":
                        yield AwaitingApproval(
                            call=call,
                            request_id=call.id,
                            reason=deny_reason,
                        )
                        resolver = ctx.approval_resolver
                        if resolver is None:
                            decision = "deny"
                            deny_reason = (
                                "requires human approval which is not "
                                "available in this run context"
                            )
                        else:
                            try:
                                decision = await resolver(call, deny_reason)
                            except Exception as e:  # noqa: BLE001
                                decision = "deny"
                                deny_reason = (
                                    f"approval resolver raised "
                                    f"{type(e).__name__}: {e}"
                                )

                    if decision == "deny":
                        tr = ToolResultBlock(
                            tool_call_id=call.id,
                            content=(
                                "Tool call denied"
                                + (f" by {rule_name}" if rule_name else "")
                                + (f": {deny_reason}" if deny_reason else "")
                            ),
                            is_error=True,
                        )
                        yield ToolResult(result=tr)
                        messages.append(Message.tool(call.id, tr.content))
                        continue

                    # decision == "allow" — proceed to execute.

                    tool = self._tool_map.get(call.name)
                    if tool is None:
                        available = (
                            ", ".join(sorted(self._tool_map.keys())) or "(none)"
                        )
                        tr = ToolResultBlock(
                            tool_call_id=call.id,
                            content=(
                                f"Tool {call.name!r} not found. "
                                f"Available: {available}"
                            ),
                            is_error=True,
                        )
                        yield ToolResult(result=tr)
                        messages.append(Message.tool(call.id, tr.content))
                        continue

                    with tool_span(
                        name=call.name,
                        call_id=call.id,
                        arguments=call.arguments,
                        description=getattr(tool, "description", None),
                    ) as t_span:
                        try:
                            result_value = await tool.run(ctx, call.arguments)
                            content = self._serialize_tool_result(result_value)
                            tr = ToolResultBlock(
                                tool_call_id=call.id,
                                content=content,
                                is_error=False,
                            )
                            t_span.record_result(content, is_error=False)
                        except ToolValidationError as e:
                            tr = ToolResultBlock(
                                tool_call_id=call.id,
                                content=(
                                    "Argument validation failed: "
                                    + "; ".join(e.errors)
                                ),
                                is_error=True,
                            )
                            t_span.record_error(e)
                        except ToolExecutionError as e:
                            tr = ToolResultBlock(
                                tool_call_id=call.id,
                                content=(
                                    f"Tool raised "
                                    f"{type(e.original).__name__}: {e.original}"
                                ),
                                is_error=True,
                            )
                            t_span.record_error(e)
                        except Exception as e:  # noqa: BLE001
                            tr = ToolResultBlock(
                                tool_call_id=call.id,
                                content=f"Unexpected tool error: {e}",
                                is_error=True,
                            )
                            t_span.record_error(e)

                    yield ToolResult(result=tr)
                    messages.append(Message.tool(call.id, tr.content))

            # Fell out of the loop — exceeded max_iterations.
            ag_span.record_completion(
                iterations=iterations_done,
                stop_reason="max_iterations",
                input_tokens=ctx.usage.input_tokens,
                output_tokens=ctx.usage.output_tokens,
            )
        yield Error(
            error=f"Max iterations ({self.max_iterations}) exceeded",
            exc_type="MaxIterationsError",
            fatal=True,
        )
        yield Done(run_id=run_id)

    # ------------------------------------------------------------------
    # Handoff
    # ------------------------------------------------------------------

    def as_tool(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
    ) -> BaseTool:
        """Expose this Agent as a Tool that other agents can call.

        Used to implement handoffs: ``Agent(handoffs=[other])`` internally
        adds ``other.as_tool()`` to the agent's ``tools`` list.
        """
        return AgentTool(self, name=name, description=description)

    def show(
        self,
        input: str | list[Message],
        *,
        deps: Any = None,
        end: str = "\n",
    ) -> Any:
        """Sync convenience: run this agent and print output live."""
        from ..ui import show as _show

        return _show(self, input, deps=deps, end=end)

    async def ashow(
        self,
        input: str | list[Message],
        *,
        deps: Any = None,
        end: str = "\n",
    ) -> Any:
        """Async version of :meth:`show`."""
        from ..ui import ashow as _ashow

        return await _ashow(self, input, deps=deps, end=end)

    def session(
        self,
        *,
        session_id: str | None = None,
        deps: Any = None,
        approval_timeout: float = 300.0,
        checkpointer: "Any | None" = None,
    ) -> "AgentSession":
        """Open a bidirectional, streaming, HITL-capable session on this Agent.

        The returned object is an async context manager that implements the
        L1 ``Channel`` protocol: push new user input with ``send()``, consume
        events with ``events()``, and answer ``AwaitingApproval`` events with
        ``reply_approval(...)``.

        Args:
            session_id: Conversation key for memory persistence. Auto-generated
                if omitted.
            deps: Value stored on the run context for tool functions to reach.
            approval_timeout: Seconds to wait for a human approval reply
                before the resolver raises ``ResolverTimeoutError`` (which the
                agent loop converts to a deny with an informative reason).
            checkpointer: Optional durable backing store. When set, the
                session saves a checkpoint after every terminal event and
                the caller can later ``AgentSession.resume(...)`` from it.
        """
        # Local import so ``koala.agents`` doesn't require ``koala.harness``
        # at import time — L6 stays usable without L8.
        from ..harness.session import AgentSession

        return AgentSession(
            agent=self,
            session_id=session_id,
            deps=deps,
            approval_timeout=approval_timeout,
            checkpointer=checkpointer,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_initial_messages(
        self, input: str | list[Message]
    ) -> list[Message]:
        """Build the message list for the first model call.

        Rules:
          - str input -> [system(instructions?), user(input)]
          - list[Message] -> use as-is; if no system message and instructions
            are set, prepend one. If a system message is already present,
            our instructions merge in front of its text.
          - If output_type is set and native structured output is NOT
            supported by the model, append a JSON-schema hint to the
            system message.
        """
        if isinstance(input, str):
            base: list[Message] = [Message.user(input)]
        else:
            base = list(input)

        system_text = self.instructions or ""

        needs_prompt_hint = (
            self.output_type is not None
            and Capability.STRUCTURED_OUTPUT not in self.model.capabilities
        )
        if needs_prompt_hint:
            assert self.output_type is not None
            system_text = (
                system_text + build_prompt_schema_hint(self.output_type)
            ).strip()

        if not system_text:
            return base

        if base and base[0].role == "system":
            first = base[0]
            merged = Message(
                role="system",
                content=[TextBlock(text=system_text + "\n\n" + first.text)],
            )
            return [merged] + base[1:]

        return [Message.system(system_text)] + base

    @staticmethod
    def _serialize_tool_result(value: Any) -> str:
        """Convert a tool's return value into a string the model can read."""
        if isinstance(value, str):
            return value
        if isinstance(value, BaseModel):
            return value.model_dump_json()
        if value is None or isinstance(value, (dict, list, int, float, bool)):
            try:
                return json.dumps(value, default=str)
            except (TypeError, ValueError):
                return str(value)
        return str(value)


# ---------------------------------------------------------------------------
# AgentTool — Rig-style "Agent is a Tool" wrapper for handoffs
# ---------------------------------------------------------------------------


class AgentTool(BaseTool):
    """Wraps an ``Agent`` as a ``BaseTool`` so another agent can call it.

    The wrapped tool takes a single string ``input`` argument and passes it
    to the target agent's ``arun``. The target agent's final output is
    returned to the caller (Pydantic models are JSON-serialized).
    """

    def __init__(
        self,
        agent: Agent,
        *,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        self.agent = agent
        self.name = name or f"transfer_to_{agent.name}"
        self.description = (
            description
            or f"Delegate a task to the {agent.name!r} agent. "
            "The `input` string is passed as the user message."
        )
        self.schema = {
            "type": "object",
            "properties": {
                "input": {
                    "type": "string",
                    "description": (
                        "The task, question, or message to send to the "
                        f"{agent.name!r} agent."
                    ),
                }
            },
            "required": ["input"],
        }

    async def run(self, ctx: RunContext, arguments: dict[str, Any]) -> Any:
        text = arguments.get("input", "")
        if not isinstance(text, str):
            text = str(text)
        child_ctx = ctx.child()
        result = await self.agent.arun(text, ctx=child_ctx)
        if isinstance(result.output, BaseModel):
            return result.output.model_dump_json()
        return result.output
