"""Streaming constraint abstractions.

A constraint is a read-only, shareable object.  Every active request owns a
mutable ``ConstraintState`` which consumes the stream one character at a time
and reports whether each character is extendable.  All state memory is bounded
by the service-level maximum output length.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass

from ..models import ErrorCode


@dataclass
class CharResult:
    """Outcome of feeding one character."""

    ok: bool
    code: ErrorCode | None = None
    message: str = ""
    final: bool = False  # the legal stream must end exactly here


class ConstraintState(abc.ABC):
    """Per-request mutable streaming state for one constraint."""

    @abc.abstractmethod
    def feed(self, ch: str) -> CharResult:
        """Consume a single character and update internal state."""

    @abc.abstractmethod
    def rollback(self) -> None:
        """Undo the most recent ``feed`` (for illegal characters)."""

    @abc.abstractmethod
    def finish(self) -> CharResult | None:
        """Validate stream end; return an invalid result when incomplete."""


class Constraint(abc.ABC):
    """Read-only constraint definition, safe to share across requests."""

    type_name: str = "base"

    def __init__(self, name: str, version: int, *, fatal: bool = True) -> None:
        self.name = name
        self.version = version
        self.fatal = fatal

    @abc.abstractmethod
    def new_state(self) -> ConstraintState:
        """Create an isolated per-request state."""

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """Serializable description."""
