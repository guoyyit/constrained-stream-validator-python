"""Versioned, immutable constraint registry.

Published versions are never mutated: registering a new spec under an existing
name appends a new :class:`ConstraintVersion`. In-flight sessions pin the
exact version object they received at creation, so concurrent version updates
never affect them.
"""

from __future__ import annotations

import threading

from .constraints import StreamingConstraint, build_constraint
from .models import ConstraintSpec, ErrorCode, ValidationError


class ConstraintVersion:
    """An immutable published version of a named constraint."""

    __slots__ = ("name", "spec", "version")

    def __init__(self, name: str, version: int, spec: ConstraintSpec) -> None:
        self.name = name
        self.version = version
        self.spec = spec

    def build_validator(self) -> StreamingConstraint:
        """Build a fresh per-session validator instance."""
        return build_constraint(self.spec)

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"ConstraintVersion(name={self.name!r}, version={self.version})"


class ConstraintRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._versions: dict[str, list[ConstraintVersion]] = {}

    def register(self, name: str, spec: ConstraintSpec) -> ConstraintVersion:
        if not isinstance(name, str) or not name:
            raise ValueError("constraint name must be a non-empty string")
        # Fail fast: validate spec and check the type is constructible.
        build_constraint(spec)
        with self._lock:
            history = self._versions.setdefault(name, [])
            version = len(history) + 1
            published = ConstraintVersion(name, version, spec)
            history.append(published)
            return published

    def get(self, name: str, version: int | None = None) -> ConstraintVersion:
        with self._lock:
            history = self._versions.get(name)
            if not history:
                raise ValidationError(
                    code=ErrorCode.UNKNOWN_CONSTRAINT,
                    message=f"no constraint registered under {name!r}",
                )
            if version is None:
                return history[-1]
            if version < 1 or version > len(history):
                raise ValidationError(
                    code=ErrorCode.UNKNOWN_CONSTRAINT,
                    message=f"constraint {name!r} has no version {version}",
                )
            return history[version - 1]

    def latest_version(self, name: str) -> int:
        with self._lock:
            history = self._versions.get(name)
            if not history:
                raise ValidationError(
                    code=ErrorCode.UNKNOWN_CONSTRAINT,
                    message=f"no constraint registered under {name!r}",
                )
            return len(history)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._versions)
