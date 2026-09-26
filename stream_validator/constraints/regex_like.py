"""Regular-expression constraint with streaming prefix compatibility.

A character is accepted while the consumed prefix remains a *partial* match of
the pattern.  The per-request state keeps only the consumed prefix, whose
length is bounded by the configured ``max_length`` (also the hard
service-level output cap), so memory stays bounded for long responses.
"""

from __future__ import annotations

import regex

from ..models import ErrorCode
from .base import Constraint, ConstraintState, CharResult


class RegexState(ConstraintState):
    def __init__(self, compiled: "regex.Pattern[str]", max_length: int) -> None:
        self._rx = compiled
        self._max = max_length
        self._buf: list[str] = []

    def _try(self, *, partial: bool) -> bool:
        # fullmatch so that every consumed character belongs to the match:
        # '1x' cannot extend '\\d+', while '12' can still extend to '\\d{3}'.
        return self._rx.fullmatch("".join(self._buf), partial=partial) is not None

    def feed(self, ch: str) -> CharResult:
        if len(self._buf) >= self._max:
            return CharResult(
                ok=False,
                code=ErrorCode.LENGTH_EXCEEDED,
                message=f"output exceeds maximum length of {self._max} characters",
            )
        self._buf.append(ch)
        if self._try(partial=True):
            return CharResult(ok=True)
        self._buf.pop()
        return CharResult(
            ok=False,
            code=ErrorCode.REGEX_MISMATCH,
            message="prefix can no longer be extended to match the pattern",
        )

    def rollback(self) -> None:
        if self._buf:
            self._buf.pop()

    def finish(self) -> CharResult | None:
        if self._try(partial=False):
            return None
        return CharResult(
            ok=False,
            code=ErrorCode.REGEX_MISMATCH,
            message="final output does not fully match the pattern",
        )

    @property
    def consumed(self) -> int:
        return len(self._buf)


class RegexConstraint(Constraint):
    type_name = "regex"

    def __init__(
        self,
        name: str,
        version: int,
        pattern: str,
        *,
        max_length: int = 1 << 20,
        fatal: bool = True,
    ) -> None:
        super().__init__(name, version, fatal=fatal)
        try:
            self._compiled = regex.compile(pattern)
        except regex.error as exc:
            raise ValueError(f"invalid regex pattern: {exc}") from exc
        self.pattern = pattern
        self.max_length = max_length

    def new_state(self) -> ConstraintState:
        return RegexState(self._compiled, self.max_length)

    def to_dict(self) -> dict:
        return {
            "type": self.type_name,
            "name": self.name,
            "version": self.version,
            "pattern": self.pattern,
            "max_length": self.max_length,
            "fatal": self.fatal,
        }
