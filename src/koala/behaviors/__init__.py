"""Koala behaviors package (L5).

Composable bundles of agent configuration that layer onto an ``Agent`` via
its ``behaviors=[...]`` kwarg. Ship your own by implementing the
:class:`Behavior` protocol — one attribute (``name``) and one method
(``apply(spec)``).

Public surface:
    - :class:`Behavior` — Protocol every behavior satisfies
    - :class:`AgentSpec` — the mutable builder each behavior receives
    - Built-ins: :class:`Persona`, :class:`ToolPack`, :class:`ApprovalPolicy`,
      :class:`OutputSchema`, :class:`ModelSettings`
"""

from __future__ import annotations

from .base import AgentSpec, Behavior
from .builtins import (
    ApprovalPolicy,
    ModelSettings,
    OutputSchema,
    Persona,
    ToolPack,
)

__all__ = [
    "Behavior",
    "AgentSpec",
    "Persona",
    "ToolPack",
    "ApprovalPolicy",
    "OutputSchema",
    "ModelSettings",
]
