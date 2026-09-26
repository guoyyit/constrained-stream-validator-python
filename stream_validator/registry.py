"""Versioned, read-only constraint registry.

Constraints are referenced by name and resolved to a pinned immutable version
when a request starts.  Registering under an existing name appends a new
version; in-flight requests hold their own reference to the old object and
are unaffected.
"""

from __future__ import annotations

import threading

from .constraints.base import Constraint


class ConstraintNotFoundError(KeyError):
    """Raised when a referenced constraint name/version does not exist."""


class ConstraintRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._versions: dict[str, dict[int, Constraint]] = {}

    def register(self, name: str, constraint: Constraint) -> int:
        """Add ``constraint`` under ``name`` and return its assigned version.

        The version is auto-incremented per name starting at 1.  The stored
        object's ``name``/``version`` are normalized to the registry values.
        """
        with self._lock:
            history = self._versions.setdefault(name, {})
            version = (max(history, default=0)) + 1
            # Keep the object identity the caller passed but make sure it is
            # addressed correctly.
            constraint.name = name
            constraint.version = version
            history[version] = constraint
            return version

    def get(self, name: str, version: int | None = None) -> Constraint:
        with self._lock:
            history = self._versions.get(name)
            if not history:
                raise ConstraintNotFoundError(f"constraint {name!r} is not registered")
            if version is None:
                version = max(history)
            if version not in history:
                raise ConstraintNotFoundError(
                    f"constraint {name!r} version {version} does not exist")
            return history[version]

    def latest_version(self, name: str) -> int:
        with self._lock:
            history = self._versions.get(name)
            if not history:
                raise ConstraintNotFoundError(f"constraint {name!r} is not registered")
            return max(history)

    def list_versions(self, name: str) -> list[int]:
        with self._lock:
            history = self._versions.get(name)
            if not history:
                raise ConstraintNotFoundError(f"constraint {name!r} is not registered")
            return sorted(history)

    def describe(self) -> dict[str, dict]:
        with self._lock:
            return {
                name: {"latest": max(history),
                       "versions": {v: c.to_dict() for v, c in sorted(history.items())}}
                for name, history in sorted(self._versions.items())
            }

    def remove(self, name: str) -> None:
        """Drop all versions of a name (future lookups fail).

        In-flight sessions keep working: they already hold direct references.
        """
        with self._lock:
            if name not in self._versions:
                raise ConstraintNotFoundError(f"constraint {name!r} is not registered")
            del self._versions[name]
