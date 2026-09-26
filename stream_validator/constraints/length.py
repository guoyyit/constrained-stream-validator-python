"""Maximum-length constraint (character count, not byte count)."""

from __future__ import annotations

from ..models import ErrorCode
from .base import Constraint, ConstraintState, CharResult


class LengthState(ConstraintState):
    def __init__(self, max_length: int) -> None:
        if max_length < 0:
            raise ValueError("max_length must be non-negative")
        self._max = max_length
        self._count = 0

    def feed(self, ch: str) -> CharResult:
        # ``feed`` is atomic: a rejected character is never consumed.
        if self._count >= self._max:
            return CharResult(
                ok=False,
                code=ErrorCode.LENGTH_EXCEEDED,
                message=f"output exceeds maximum length of {self._max} characters",
            )
        self._count += 1
        return CharResult(ok=True)

    def rollback(self) -> None:
        # Kept for interface completeness; failed feeds never mutate.
        if self._count > 0:
            self._count -= 1

    def finish(self) -> CharResult | None:
        # A length cap alone cannot make a finished stream incomplete.
        return None

    @property
    def count(self) -> int:
        return self._count


class MaxLengthConstraint(Constraint):
    type_name = "max_length"

    def __init__(self, name: str, version: int, max_length: int, *, fatal: bool = True) -> None:
        super().__init__(name, version, fatal=fatal)
        if max_length < 0:
            raise ValueError("max_length must be non-negative")
        self.max_length = max_length

    def new_state(self) -> ConstraintState:
        return LengthState(self.max_length)

    def to_dict(self) -> dict:
        return {
            "type": self.type_name,
            "name": self.name,
            "version": self.version,
            "max_length": self.max_length,
            "fatal": self.fatal,
        }
