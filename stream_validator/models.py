"""Shared data types: policies, error codes, statuses, feedback and stats."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class Policy(str, enum.Enum):
    """Violation handling strategy."""

    ABORT = "abort"        # stop immediately on the first illegal character
    TRUNCATE = "truncate"  # cut to the last legal prefix and finalize
    DROP = "drop"          # discard illegal characters and keep validating


class ErrorCode(str, enum.Enum):
    CONSTRAINT_VIOLATION = "constraint_violation"
    LENGTH_EXCEEDED = "length_exceeded"
    REGEX_MISMATCH = "regex_mismatch"
    ENUM_MISMATCH = "enum_mismatch"
    INVALID_JSON = "invalid_json"
    INCOMPLETE_OUTPUT = "incomplete_output"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    SESSION_NOT_FOUND = "session_not_found"
    CONSTRAINT_NOT_FOUND = "constraint_not_found"
    CONCURRENCY_LIMIT = "concurrency_limit"
    REORDER_OVERFLOW = "reorder_overflow"
    SESSION_CLOSED = "session_closed"
    BAD_REQUEST = "bad_request"


class SessionStatus(str, enum.Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    ABORTED = "aborted"
    TRUNCATED = "truncated"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class ChunkStatus(str, enum.Enum):
    ACCEPTED = "accepted"    # at least part of the chunk was emitted
    WARNED = "warned"        # illegal chars skipped because constraint is non-fatal
    DROPPED = "dropped"      # illegal characters were skipped (DROP policy)
    DUPLICATE = "duplicate"  # seq already applied (idempotent no-op)
    STALE = "stale"          # reserved: explicit stale-delivery distinction
    BUFFERED = "buffered"    # out-of-order chunk held for later delivery
    VIOLATION = "violation"  # abort/truncate terminal violation
    REJECTED = "rejected"    # closed/unknown session or overflow
    FINALIZED = "finalized"  # stream completed normally


@dataclass(frozen=True)
class Violation:
    code: ErrorCode
    message: str
    offset: int
    consumed_prefix: str = ""


@dataclass(frozen=True)
class ChunkFeedback:
    seq: int
    status: ChunkStatus
    emitted: str = ""
    emitted_chars: int = 0
    dropped_chars: int = 0
    warning_count: int = 0
    buffered: bool = False
    violation: Violation | None = None


@dataclass(frozen=True)
class SessionSnapshot:
    session_id: str
    constraint_name: str
    constraint_version: int
    policy: Policy
    status: SessionStatus
    consumed_chars: int
    dropped_chars: int
    warning_count: int
    prefix_preview: str
    violation: Violation | None
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class ServiceStats:
    active_sessions: int
    total_created: int
    total_completed: int
    total_aborted: int
    total_truncated: int
    total_cancelled: int
    total_timeout: int
    total_chars_emitted: int
    total_chars_dropped: int
    total_warnings: int
    total_chunks: int
    rejected_chunks: int
