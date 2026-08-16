# Behaviors

A `Behavior` is a small composable bundle that contributes to an agent's
configuration — instructions, tools, approval rules, output type, model
settings. Behaviors are applied in order when the `Agent` is constructed,
each getting one chance to mutate a shared `AgentSpec`.

The idea: compose reusable slices of agent config so you don't copy the
same tools/rules/persona across every agent that shares them.

## The Protocol

```python
@runtime_checkable
class Behavior(Protocol):
    name: str
    def apply(self, spec: AgentSpec) -> None: ...
```

Any class with a `name` attribute and an `apply(spec)` method is a
`Behavior`. No inheritance required.

## `AgentSpec`

The mutable builder each behavior touches:

```python
@dataclass
class AgentSpec:
    instructions_parts: list[str] = field(default_factory=list)
    tools: list[BaseTool]         = field(default_factory=list)
    approval_rules: list[ApprovalRule] = field(default_factory=list)
    settings: ChatSettings        = field(default_factory=ChatSettings)
    output_type: type | None      = None
    metadata: dict[str, Any]      = field(default_factory=dict)
```

## Built-in behaviors

### `Persona`

Append persona text to the agent's system prompt.

```python
from koala.behaviors import Persona

Persona("You are Sam, a concise support agent.")
```

Multiple `Persona` behaviors stack — each appends its text with a blank
line separator.

### `ToolPack`

A named collection of tools that plug into an agent as a unit. Constructor
takes tools as positional args.

```python
from koala.behaviors import ToolPack

acme_support = ToolPack(
    lookup_order,
    refund,
    log_ticket,
    name="acme-support",
)
```

### `ApprovalPolicy`

A named chain of approval rules. Constructor takes rules as positional args.

```python
from koala.behaviors import ApprovalPolicy
from koala.tools import DenyList, RequireApprovalFor

production = ApprovalPolicy(
    DenyList(frozenset({"delete_customer"})),
    RequireApprovalFor(prefixes=frozenset({"send_", "publish_"})),
    name="production-safety",
)
```

### `OutputSchema`

Force the agent to return a structured Pydantic model.

```python
from koala.behaviors import OutputSchema
from pydantic import BaseModel

class Ticket(BaseModel):
    id: str
    priority: str

OutputSchema(Ticket)
```

If the caller also passed `output_type=` to the Agent, the behavior wins
(behaviors apply after explicit args).

### `ModelSettings`

Layer chat settings onto the agent. Merges over any earlier settings.

```python
from koala.behaviors import ModelSettings
from koala.models.settings import ChatSettings

ModelSettings(ChatSettings(temperature=0.0, max_tokens=256))
```

## Composing

```python
from koala import Agent
from koala.behaviors import Persona, ToolPack, ApprovalPolicy, OutputSchema
from koala.tools import DenyList

# Reusable slices
acme_toolpack = ToolPack(lookup_order, refund, name="acme-support")
acme_safety = ApprovalPolicy(DenyList(frozenset({"refund"})), name="safe")

# Agent A — support
support = Agent(
    "openai/gpt-4o-mini",
    behaviors=[
        Persona("You are Sam, a friendly support agent."),
        acme_toolpack,
        acme_safety,
    ],
)

# Agent B — sales, reuses the toolpack + safety policy
sales = Agent(
    "openai/gpt-4o-mini",
    behaviors=[
        Persona("You are Alex, an enthusiastic sales agent."),
        acme_toolpack,
        acme_safety,
    ],
)
```

## Application order

Behaviors apply **after** explicit constructor args and **in list order**:

1. `Agent(instructions=..., tools=..., approval_rules=..., ...)` writes
   its args into a fresh `AgentSpec`.
2. Each behavior's `apply(spec)` runs in the order given.
3. The resulting `AgentSpec` is finalized on the Agent.

This means:

- Multiple `Persona`s stack (concatenate).
- Multiple `ToolPack`s stack (extend).
- Multiple `ApprovalPolicy`s stack (extend the rule chain).
- Multiple `ModelSettings`s merge (later fields win).
- Multiple `OutputSchema`s override — last one wins.

## Custom behaviors

Anything with `name` and `apply(spec)` works. Common patterns:

```python
from dataclasses import dataclass
from koala.behaviors import AgentSpec


@dataclass
class TimestampInstruction:
    """Prepend a timestamp to every agent invocation."""

    name: str = "timestamp"

    def apply(self, spec: AgentSpec) -> None:
        import datetime
        now = datetime.datetime.now().isoformat()
        spec.instructions_parts.append(f"Current time: {now}")


agent = Agent("openai/gpt-4o-mini", behaviors=[TimestampInstruction()])
```

## Introspection

The finalized state lives on the Agent:

```python
agent.instructions   # combined system prompt
agent.tools          # list of BaseTool
agent.approval_rules # list of ApprovalRule
agent.settings       # ChatSettings
agent.output_type    # None or Pydantic class
```

## Reference

See [`koala.behaviors` API reference](../reference/behaviors.md).
