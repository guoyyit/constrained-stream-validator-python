"""Session manager: lifecycle, strict per-request isolation, bounded memory.

The registry is read-only for sessions; every session gets its own validator
instance and its own lock. The manager additionally enforces a cap on the
number of concurrently active sessions and supports reaping sessions that
have finished, been cancelled, or exceeded their duration.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from .engine import ValidationSession
from .models import (
    ErrorCode,
    FeedbackEvent,
    SessionResult,
    SessionStats,
    SessionStatus,
    StreamChunk,
    ValidationError,
    ViolationPolicy,
)
from .registry import ConstraintRegistry

_TERMINAL_STATUSES = frozenset(
    {
        SessionStatus.FINISHED,
        SessionStatus.ABORTED,
        SessionStatus.CANCELLED,
        SessionStatus.TRUNCATED,
        SessionStatus.TIMED_OUT,
    }
)


@dataclass(frozen=True)
class ManagerStats:
    active_sessions: int
    total_sessions_created: int
    total_sessions_finished: int
    total_violations: int
    total_chunks_received: int
    total_bytes_in: int
    total_bytes_out: int


class SessionManager:
    def __init__(
        self,
        registry: ConstraintRegistry,
        max_active_sessions: int = 10_000,
        default_max_length: int | None = None,
        default_max_duration_seconds: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.registry = registry
        self.max_active_sessions = max_active_sessions
        self.default_max_length = default_max_length
        self.default_max_duration_seconds = default_max_duration_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._sessions: dict[str, ValidationSession] = {}
        self._created = 0
        self._finished = 0

    # ------------------------------------------------------------------ create

    def create_session(
        self,
        constraint_name: str,
        policy: ViolationPolicy = ViolationPolicy.ABORT,
        constraint_version: int | None = None,
        max_length: int | None = None,
        max_duration_seconds: float | None = None,
        session_id: str | None = None,
    ) -> str:
        # Pin the exact constraint version BEFORE the manager lock: registry
        # lookups are independent and never block on session state.
        pinned = self.registry.get(constraint_name, constraint_version)

        with self._lock:
            active = sum(
                1 for s in self._sessions.values()
                if s.status not in _TERMINAL_STATUSES
            )
            if active >= self.max_active_sessions:
                raise ValidationError(
                    code=ErrorCode.LENGTH_LIMIT_EXCEEDED,
                    message=(
                        f"active session cap {self.max_active_sessions} reached"
                    ),
                )
            sid = session_id or uuid.uuid4().hex
            if sid in self._sessions:
                raise ValidationError(
                    code=ErrorCode.INVALID_CHUNK,
                    message=f"session id {sid!r} already exists",
                )
            session = ValidationSession(
                sid,
                pinned,
                policy=policy,
                max_length=max_length if max_length is not None else self.default_max_length,
                max_duration_seconds=(
                    max_duration_seconds
                    if max_duration_seconds is not None
                    else self.default_max_duration_seconds
                ),
                clock=self._clock,
            )
            self._sessions[sid] = session
            self._created += 1
            return sid

    # ------------------------------------------------------------------ lookup

    def _require(self, session_id: str) -> ValidationSession:
        session = self._sessions.get(session_id)
        if session is None:
            raise ValidationError(
                code=ErrorCode.SESSION_NOT_FOUND,
                message=f"no session {session_id!r}",
            )
        return session

    def ingest(self, session_id: str, chunk: StreamChunk) -> FeedbackEvent:
        session = self._require(session_id)
        was_open = session.status not in _TERMINAL_STATUSES
        event = session.ingest(chunk)
        if was_open and session.status in _TERMINAL_STATUSES:
            with self._lock:
                self._finished += 1
        return event

    def cancel(self, session_id: str) -> None:
        session = self._require(session_id)
        was_open = session.status not in _TERMINAL_STATUSES
        session.cancel()
        if was_open:
            with self._lock:
                self._finished += 1

    def finish(self, session_id: str) -> SessionResult:
        session = self._require(session_id)
        was_open = session.status not in _TERMINAL_STATUSES
        result = session.finish()
        if was_open and session.status in _TERMINAL_STATUSES:
            with self._lock:
                self._finished += 1
        return result

    def get_result(self, session_id: str) -> SessionResult:
        return self._require(session_id).result_snapshot()

    def get_session(self, session_id: str) -> ValidationSession:
        return self._require(session_id)

    def get_stats(self, session_id: str) -> SessionStats:
        return self._require(session_id).get_stats()

    # ------------------------------------------------------------------ global

    def manager_stats(self) -> ManagerStats:
        with self._lock:
            sessions = list(self._sessions.values())
        active = total_v = chunks_in = bytes_in = bytes_out = 0
        for s in sessions:
            st = s.get_stats()
            if s.status not in _TERMINAL_STATUSES:
                active += 1
            total_v += st.violations
            chunks_in += st.chunks_received
            bytes_in += st.bytes_in
            bytes_out += st.bytes_out
        with self._lock:
            return ManagerStats(
                active_sessions=active,
                total_sessions_created=self._created,
                total_sessions_finished=self._finished,
                total_violations=total_v,
                total_chunks_received=chunks_in,
                total_bytes_in=bytes_in,
                total_bytes_out=bytes_out,
            )

    # ------------------------------------------------------------------ reaper

    def reaper_sweep(self, force: bool = False) -> int:
        """Remove terminal sessions and sessions past their duration.

        ``force=True`` removes terminal sessions even if their state might be
        queried once more (normal sweeps keep them until they expire).
        Returns the number of sessions removed.
        """
        removed = 0
        with self._lock:
            for sid in list(self._sessions):
                session = self._sessions[sid]
                status = session.status
                if status in _TERMINAL_STATUSES:
                    del self._sessions[sid]
                    removed += 1
                    continue
                if (
                    session.max_duration_seconds is not None
                    and session.elapsed() > session.max_duration_seconds
                ):
                    session.cancel()
                    del self._sessions[sid]
                    removed += 1
                    self._finished += 1
        return removed

    def active_count(self) -> int:
        with self._lock:
            return sum(
                1 for s in self._sessions.values()
                if s.status not in _TERMINAL_STATUSES
            )
