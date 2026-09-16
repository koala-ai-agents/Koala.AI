"""Koala — a Python framework for building AI agents.

Top-level public API:

    from koala import Agent, Model, tool, AgentSession, show

See ``koala.agents``, ``koala.models``, ``koala.tools``, ``koala.memory``,
``koala.behaviors``, ``koala.orchestration``, ``koala.harness``, ``koala.ui``
for the full surface.
"""

from __future__ import annotations

# L6 — Agents
from .agents import Agent as Agent
from .agents import BaseAgent as BaseAgent
from .agents import ContextPolicy as ContextPolicy
from .agents import RunResult as RunResult

# L5 — Behaviors
from .behaviors import ApprovalPolicy as ApprovalPolicy
from .behaviors import Behavior as Behavior
from .behaviors import ModelSettings as ModelSettings
from .behaviors import OutputSchema as OutputSchema
from .behaviors import Persona as Persona
from .behaviors import ToolPack as ToolPack

# L1 — Core
from .core import ModelRetry as ModelRetry
from .core import RetryPolicy as RetryPolicy

# L8 — Harness
from .harness import AgentSession as AgentSession

# L2 — Models
from .models import Model as Model
from .models import register_provider as register_provider

# L3 — Tools
from .tools import BaseTool as BaseTool
from .tools import FunctionTool as FunctionTool
from .tools import tool as tool

# UI helpers
from .ui import ashow as ashow
from .ui import show as show

__version__ = "0.1.0"

__all__ = [
    "Agent",
    "BaseAgent",
    "RunResult",
    "ContextPolicy",
    "ModelRetry",
    "RetryPolicy",
    "AgentSession",
    "Model",
    "register_provider",
    "tool",
    "FunctionTool",
    "BaseTool",
    "Behavior",
    "Persona",
    "ToolPack",
    "ApprovalPolicy",
    "OutputSchema",
    "ModelSettings",
    "show",
    "ashow",
    "__version__",
]
