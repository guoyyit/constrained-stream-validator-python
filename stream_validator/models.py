"""Core domain types: constraints, policies, requests, events, errors."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# ---------------------------------------------------------------------------
# Constraints
# ---------------------------------------------------------------------------

class ConstraintType(str, Enum):
    STRUCTURED = "structured"
    ENUM = "enum"
    REGEX = "regex"
    LENGTH = "length"


@dataclass(frozen=True)
class ConstraintSpec:
    """Immutable constraint definition (shared read-only across requests)."""

    type: ConstraintType
    params: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Violation handling policy
# ---------------------------------------------------------------------------

class ViolationPolicy(str, Enum):
    ABORT = "abort"
    TRUNCATE = "truncate"
    SKIP = "skip"


# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------

class ErrorCode(str, Enum):
    CONSTRAINT_VIOLATION = "constraint_violation"
    LENGTH_LIMIT_EXCEEDED = "length_limit_exceeded"
    TIME_LIMIT_EXCEEDED = "time_limit_exceeded"
    CANCELLED = "cancelled"
    CHUNK_OUT_OF_RANGE = "chunk_out_of_range"
    UNKNOWN_CONSTRAINT = "unknown_constraint"
    CONSTRAINT_TYPE_UNKNOWN = "constraint_type_unknown"
    DUPLICATE_SEQ = "duplicate_seq"
    INVALID_CHUNK = "invalid_chunk"
    SESSION_NOT_FOUND = "session_not_found"
    SESSION_FINISHED = "session_finished"


@dataclass
class ValidationError(Exception):
    """Raised for terminal validation failures.

    Not frozen: Python's exception machinery assigns ``__traceback__`` when the
    error is raised.
    """

    code: ErrorCode
    message: str
    position: int = 0
    consumed_prefix: str = ""

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.code.value}: {self.message} at {self.position}"


# ---------------------------------------------------------------------------
# Stream chunks / sessions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StreamChunk:
    """A possibly out-of-order or duplicated chunk.

    seq is the global character offset at which ``text`` starts.
    """

    seq: int
    text: str
    final: bool = False


@dataclass
class FeedbackEvent:
    """Feedback returned to the caller for an accepted/processed chunk."""

    seq: int
    accepted_text: str
    rejected_text: str = ""
    position: int = 0
    error: ValidationError | None = None


class SessionStatus(str, Enum):
    OPEN = "open"
    FINISHED = "finished"
    ABORTED = "aborted"
    CANCELLED = "cancelled"
    TRUNCATED = "truncated"
    TIMED_OUT = "timed_out"


@dataclass
class SessionResult:
    status: SessionStatus
    output: str
    error_code: str | None = None
    error_position: int | None = None
    consumed_prefix: str = ""


@dataclass
class SessionStats:
    chunks_received: int = 0
    chunks_applied: int = 0
    chunks_duplicate: int = 0
    chunks_out_of_range: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    violations: int = 0
