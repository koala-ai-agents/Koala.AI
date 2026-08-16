# Handoffs

An agent can delegate to another agent. Koala uses the Rig-style
"Agent-as-Tool" pattern: each handoff agent is wrapped in an `AgentTool`
that exposes it as a `transfer_to_<name>` function the LLM can call.

## Basic

```python
from koala import Agent


math = Agent(
    "openai/gpt-4o-mini",
    name="math",
    instructions="You are a math specialist. Answer numerical questions concisely.",
)

writer = Agent(
    "openai/gpt-4o-mini",
    name="writer",
    instructions="You are a copywriter. Turn dry facts into engaging lines.",
)

coordinator = Agent(
    "openai/gpt-4o-mini",
    name="coordinator",
    instructions=(
        "Route the user's request to the right specialist. "
        "Use `transfer_to_math` for calculations, `transfer_to_writer` "
        "for phrasing. Combine their outputs into the final answer."
    ),
    handoffs=[math, writer],
)

result = coordinator.run("Compute 12 squared, then write a punchy line about it.")
```

## What `handoffs=` actually does

Under the hood:

```python
for handoff_agent in handoffs:
    spec.tools.append(handoff_agent.as_tool())
```

`agent.as_tool()` returns an `AgentTool` — a `BaseTool` whose:

- `name` = `f"transfer_to_{agent.name}"`
- `description` = auto-generated, or override via `agent.as_tool(name="...", description="...")`
- `schema` accepts one string argument `input`
- `run(ctx, args)` calls `child_agent.arun(args["input"], ctx=ctx.child())`

The child's final output is returned as the tool result (Pydantic outputs
are JSON-serialized).

## Manual `AgentTool` usage

You don't have to pass agents via `handoffs=` — expose them as tools
directly:

```python
from koala.agents.agent import AgentTool

specialist = Agent("...", name="specialist")

coordinator = Agent(
    "...",
    tools=[
        AgentTool(specialist, name="ask_specialist",
                  description="Ask the domain specialist a question."),
        other_normal_tool,
    ],
)
```

## Nested runs share context

`AgentTool.run` calls `ctx.child()` before invoking the sub-agent, which
forks a `RunContext` that shares:

- `session_id`
- `cancel` token — cancelling the outer run cancels the inner one
- `usage` — the sub-agent's tokens are accumulated into the parent's total
- `approval_resolver` — HITL flows through nested agents

but gets its own `metadata` dict copy so per-agent tags don't leak back.

## Passing structured data across a handoff

The tool schema is a single `input: str`. If you need to pass structured
data:

```python
import json

# Serialize before handing off
result = coordinator.run(f"Process this: {json.dumps(data)}")
```

Or add extra tools that the coordinator can call in sequence. Explicit
structured handoffs are on the roadmap; today the string interface is
what ships.

## Multi-hop handoffs

A handoff agent can itself have handoffs. Koala doesn't limit depth,
but each hop counts as a `tool.run` inside the parent's `max_iterations`
budget, so give the top-level agent enough iterations.

```python
level_a = Agent("...", name="level_a")
level_b = Agent("...", name="level_b", handoffs=[level_a])
top = Agent("...", name="top", handoffs=[level_b], max_iterations=30)
```

## Handoff error handling

If the sub-agent errors (any `stop_reason != "final_output"`):

- The `AgentTool.run` returns whatever the sub-agent produced in
  `result.output`, which may be `None`.
- The parent sees a `ToolResult` with the (possibly empty) content.
- The parent can then decide to try a different specialist or give up.

If you need stricter propagation (parent should fail when child fails),
wrap `AgentTool.run` with your own subclass.

## Reference

- `koala.agents.Agent.as_tool` — exposes an agent as a `BaseTool`.
- `koala.agents.agent.AgentTool` — the wrapper class.

See the [API reference for `koala.agents`](../reference/agents.md).
