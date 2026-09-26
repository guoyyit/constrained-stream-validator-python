"""Session manager: concurrency limits, lifecycle and aggregate statistics.

Thread-safety: a single registry-level lock guards the session table and
counters; each session additionally carries its own lock for its internal
state.  The manager lock is only held for short dict operations, never while
running user validation, so throughput is not serialized.
"""

from __future__ import annotations

import threading
import uuid

from .models import (
    ChunkFeedback,
    ChunkStatus,
    ErrorCode,
    Policy,
    ServiceStats,
    SessionSnapshot,
    SessionStatus,
    Violation,
)
from .registry import ConstraintRegistry, ConstraintNotFoundError
from .session import ValidationSession

# Closed sessions are retained this long so clients can still fetch the final
# status/violation, then reaped to keep memory bounded.
_DEFAULT_RETENTION = 60.0


class ConcurrencyLimitError(RuntimeError):
    """Raised when the active-session limit is reached."""


class SessionManager:
    def __init__(
        self,
        registry: ConstraintRegistry,
        *,
        max_active: int = 1000,
        default_timeout: float | None = None,
        reorder_window: int = 32,
        max_prefix_preview: int = 4096,
        retention: float = _DEFAULT_RETENTION,
        reap_interval: float | None = 1.0,
        clock=None,
    ) -> None:
        self.registry = registry
        self.max_active = max_active
        self.default_timeout = default_timeout
        self.reorder_window = reorder_window
        self.max_prefix_preview = max_prefix_preview
        self.retention = retention
        self._clock = clock

        self._lock = threading.RLock()
        self._sessions: dict[str, ValidationSession] = {}
        self._closed_at: dict[str, float] = {}
        self._stats = {
            "created": 0, "completed": 0, "aborted": 0, "truncated": 0,
            "cancelled": 0, "timeout": 0, "chars_emitted": 0,
            "chars_dropped": 0, "warnings": 0, "chunks": 0,
            "rejected_chunks": 0,
        }
        self._stop = threading.Event()
        self._reaper: threading.Thread | None = None
        if reap_interval is not None and reap_interval > 0:
            self._reaper = threading.Thread(
                target=self._reap_loop, args=(reap_interval,), daemon=True)
            self._reaper.start()

    # -- helpers -----------------------------------------------------------
    def _now(self) -> float:
        if self._clock is not None:
            return self._clock()
        import time
        return time.time()

    def _active_count_locked(self) -> int:
        return sum(1 for s in self._sessions.values()
                   if s.status == SessionStatus.ACTIVE)

    # -- lifecycle ---------------------------------------------------------
    def create_session(
        self,
        constraint_name: str,
        policy: Policy,
        *,
        version: int | None = None,
        timeout: float | None = None,
        session_id: str | None = None,
    ) -> SessionSnapshot:
        try:
            constraint = self.registry.get(constraint_name, version)
        except ConstraintNotFoundError:
            raise

        effective_timeout = timeout if timeout is not None else self.default_timeout
        deadline = (self._now() + effective_timeout) if effective_timeout else None

        with self._lock:
            if self._active_count_locked() >= self.max_active:
                raise ConcurrencyLimitError(
                    f"active session limit {self.max_active} reached")
            sid = session_id or uuid.uuid4().hex
            if sid in self._sessions:
                raise ValueError(f"session id {sid!r} already exists")
            session = ValidationSession(
                sid, constraint, policy,
                deadline=deadline,
                reorder_window=self.reorder_window,
                max_prefix_preview=self.max_prefix_preview,
                now=self._now())
            self._sessions[sid] = session
            self._closed_at.pop(sid, None)
            self._stats["created"] += 1
            return session.snapshot(self._now())

    def feed(self, session_id: str, seq: int, text: str, *,
             final: bool = False) -> ChunkFeedback:
        with self._lock:
            session = self._sessions.get(session_id)
        if session is None:
            fb = ChunkFeedback(
                seq=seq, status=_rejected_status(),
                violation=Violation(ErrorCode.SESSION_NOT_FOUND,
                                    f"unknown session {session_id!r}",
                                    offset=0, consumed_prefix=""))
            with self._lock:
                self._stats["chunks"] += 1
                self._stats["rejected_chunks"] += 1
            return fb
        before = session.status
        fb = session.process_chunk(seq, text, final=final, now=self._now())
        self._record_feedback(session, before, fb)
        return fb

    def cancel(self, session_id: str) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
        if session is None:
            return False
        before = session.status
        ok = session.cancel(self._now())
        if ok:
            with self._lock:
                self._stats["cancelled"] += 1
                self._closed_at[session_id] = self._now()
        return ok

    def get(self, session_id: str) -> SessionSnapshot:
        with self._lock:
            session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(session_id)
        return session.snapshot(self._now())

    def remove(self, session_id: str) -> bool:
        """Immediately reclaim a session's state."""
        with self._lock:
            existed = self._sessions.pop(session_id, None) is not None
            self._closed_at.pop(session_id, None)
        return existed

    def stats(self) -> ServiceStats:
        with self._lock:
            s = dict(self._stats)
            active = self._active_count_locked()
        return ServiceStats(
            active_sessions=active,
            total_created=s["created"],
            total_completed=s["completed"],
            total_aborted=s["aborted"],
            total_truncated=s["truncated"],
            total_cancelled=s["cancelled"],
            total_timeout=s["timeout"],
            total_chars_emitted=s["chars_emitted"],
            total_chars_dropped=s["chars_dropped"],
            total_warnings=s["warnings"],
            total_chunks=s["chunks"],
            rejected_chunks=s["rejected_chunks"],
        )

    def list_sessions(self, *, include_closed: bool = False) -> list[SessionSnapshot]:
        with self._lock:
            sessions = list(self._sessions.values())
        snapshots = [s.snapshot(self._now()) for s in sessions]
        if include_closed:
            return snapshots
        return [s for s in snapshots if s.status == SessionStatus.ACTIVE]

    def shutdown(self) -> None:
        self._stop.set()
        if self._reaper is not None:
            self._reaper.join(timeout=2.0)
        with self._lock:
            self._sessions.clear()
            self._closed_at.clear()

    # -- internals ---------------------------------------------------------
    def _record_feedback(self, session: ValidationSession,
                         before: SessionStatus, fb: ChunkFeedback) -> None:
        with self._lock:
            self._stats["chunks"] += 1
            # Chunks that never applied (duplicates/stale/buffered) contribute
            # no output statistics.
            if fb.status.name in ("REJECTED",):
                self._stats["rejected_chunks"] += 1
            self._stats["chars_emitted"] += fb.emitted_chars
            self._stats["chars_dropped"] += fb.dropped_chars
            self._stats["warnings"] += fb.warning_count

            if before == SessionStatus.ACTIVE and session.status != before:
                mapping = {
                    SessionStatus.COMPLETED: "completed",
                    SessionStatus.ABORTED: "aborted",
                    SessionStatus.TRUNCATED: "truncated",
                    SessionStatus.TIMEOUT: "timeout",
                    SessionStatus.CANCELLED: "cancelled",
                }
                key = mapping.get(session.status)
                if key:
                    self._stats[key] += 1
                if session.status != SessionStatus.ACTIVE:
                    self._closed_at[session.id] = self._now()

    def _reap_loop(self, interval: float) -> None:
        import time
        while not self._stop.wait(interval):
            try:
                self.reap_once()
            except Exception:  # pragma: no cover - reaper must never die
                pass

    def reap_once(self) -> int:
        """Expire timed-out active sessions and evict old closed ones."""
        now = self._now()
        expired = 0
        evicted = 0
        with self._lock:
            sessions = list(self._sessions.items())
        for _sid, session in sessions:
            if session.status == SessionStatus.ACTIVE and session.check_timeout(now):
                with self._lock:
                    if self._sessions.get(session.id) is session \
                            and session.status == SessionStatus.TIMEOUT:
                        self._stats["timeout"] += 1
                        self._closed_at[session.id] = now
                        expired += 1
        with self._lock:
            for sid, closed_ts in list(self._closed_at.items()):
                if now - closed_ts > self.retention:
                    self._sessions.pop(sid, None)
                    self._closed_at.pop(sid, None)
                    evicted += 1
        return expired + evicted


def _rejected_status():
    return ChunkStatus.REJECTED
