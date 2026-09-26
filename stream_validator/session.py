"""Single streaming validation session.

The session pins one immutable constraint version and owns all mutable state:
the per-request constraint state, a bounded reorder buffer, a bounded emitted
prefix and counters.  Only read-only constraint objects are shared between
sessions.

Memory bounds:
* emitted text kept for feedback previews is capped at ``max_prefix_preview``;
* out-of-order buffering is capped at ``reorder_window`` chunks;
* the constraint state itself is bounded by the constraint's own length cap.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass

from .constraints.base import Constraint
from .models import (
    ChunkFeedback,
    ChunkStatus,
    ErrorCode,
    Policy,
    SessionStatus,
    Violation,
)

_TERMINAL_STATUSES = {
    SessionStatus.COMPLETED,
    SessionStatus.ABORTED,
    SessionStatus.TRUNCATED,
    SessionStatus.CANCELLED,
    SessionStatus.TIMEOUT,
}


@dataclass
class _Counters:
    emitted_chars: int = 0
    dropped_chars: int = 0
    warning_count: int = 0
    chunks_seen: int = 0
    chunks_rejected: int = 0


class ValidationSession:
    def __init__(
        self,
        session_id: str,
        constraint: Constraint,
        policy: Policy,
        *,
        deadline: float | None = None,
        reorder_window: int = 32,
        max_prefix_preview: int = 4096,
        now: float | None = None,
    ) -> None:
        import time

        self.id = session_id
        self.constraint = constraint  # pinned version, never swapped
        self.version = constraint.version
        self.policy = policy
        self._state = constraint.new_state()
        self.deadline = deadline
        self.reorder_window = reorder_window
        self._max_preview = max_prefix_preview

        self.status = SessionStatus.ACTIVE
        self.violation: Violation | None = None
        self.last_warning: Violation | None = None
        self.counters = _Counters()

        # Stream order tracking.  Every seq < _next_seq has already been
        # applied, so duplicate/late delivery needs no ever-growing set.
        self._next_seq = 0
        self._buffer: dict[int, tuple[str, bool]] = {}

        self._prefix = deque(maxlen=max_prefix_preview)
        self._prefix_truncated = False

        ts = time.time() if now is None else now
        self.created_at = ts
        self.updated_at = ts
        self.lock = threading.RLock()

    # -- public API --------------------------------------------------------
    def process_chunk(self, seq: int, text: str, *, final: bool = False,
                      now: float | None = None) -> ChunkFeedback:
        with self.lock:
            ts = self._now(now)
            self.updated_at = ts
            self.counters.chunks_seen += 1

            if self.status == SessionStatus.CANCELLED:
                return self._reject(seq, ErrorCode.CANCELLED, "session was cancelled")
            if self.status == SessionStatus.TIMEOUT:
                return self._reject(seq, ErrorCode.TIMEOUT, "session timed out")
            if self.status in _TERMINAL_STATUSES:
                return self._reject(seq, ErrorCode.SESSION_CLOSED,
                                    f"session already {self.status.value}")
            if self.deadline is not None and ts > self.deadline:
                self._expire()
                return self._reject(seq, ErrorCode.TIMEOUT, "session deadline exceeded")
            if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
                return self._reject(seq, ErrorCode.BAD_REQUEST, "seq must be >= 0")

            if seq < self._next_seq:
                # Duplicate or late delivery: idempotent no-op, nothing is
                # written to the result or to statistics.
                return ChunkFeedback(seq=seq, status=ChunkStatus.DUPLICATE)
            if seq != self._next_seq:
                if seq in self._buffer:
                    # Re-delivery of an already buffered future chunk is an
                    # idempotent no-op: keep the first payload, write nothing.
                    return ChunkFeedback(seq=seq, status=ChunkStatus.DUPLICATE)
                if len(self._buffer) >= self.reorder_window:
                    return self._reject(
                        seq, ErrorCode.REORDER_OVERFLOW,
                        f"gap exceeds reorder window of {self.reorder_window}")
                self._buffer[seq] = (text, final)
                return ChunkFeedback(seq=seq, status=ChunkStatus.BUFFERED, buffered=True)

            return self._apply_in_order(seq, text, final)

    def cancel(self, now: float | None = None) -> bool:
        with self.lock:
            if self.status in _TERMINAL_STATUSES:
                return False
            self.status = SessionStatus.CANCELLED
            self.violation = Violation(
                ErrorCode.CANCELLED, "cancelled by client",
                offset=self.counters.emitted_chars,
                consumed_prefix=self.prefix_preview())
            self._release()
            self.updated_at = self._now(now)
            return True

    def check_timeout(self, now: float | None = None) -> bool:
        with self.lock:
            if self.status != SessionStatus.ACTIVE or self.deadline is None:
                return False
            ts = self._now(now)
            if ts > self.deadline:
                self._expire(ts)
                return True
            return False

    def prefix_preview(self) -> str:
        return "".join(self._prefix)

    def snapshot(self, now: float | None = None):
        from .models import SessionSnapshot

        with self.lock:
            return SessionSnapshot(
                session_id=self.id,
                constraint_name=self.constraint.name,
                constraint_version=self.version,
                policy=self.policy,
                status=self.status,
                consumed_chars=self.counters.emitted_chars,
                dropped_chars=self.counters.dropped_chars,
                warning_count=self.counters.warning_count,
                prefix_preview=self.prefix_preview(),
                violation=self.violation,
                created_at=self.created_at,
                updated_at=self.updated_at,
            )

    # -- internals ---------------------------------------------------------
    def _now(self, now: float | None) -> float:
        import time
        return time.time() if now is None else now

    def _expire(self, ts: float | None = None) -> None:
        self.status = SessionStatus.TIMEOUT
        self.violation = Violation(
            ErrorCode.TIMEOUT, "session deadline exceeded",
            offset=self.counters.emitted_chars,
            consumed_prefix=self.prefix_preview())
        self._release()
        self.updated_at = self._now(ts)

    def _release(self) -> None:
        # Drop buffered future chunks so a cancelled/timed-out session retains
        # only the bounded preview, not the pending stream.
        self._buffer.clear()

    def _reject(self, seq: int, code: ErrorCode, message: str) -> ChunkFeedback:
        return ChunkFeedback(
            seq=seq, status=ChunkStatus.REJECTED,
            violation=Violation(code, message,
                                offset=self.counters.emitted_chars,
                                consumed_prefix=self.prefix_preview()))

    def _append_emitted(self, text: str) -> None:
        self.counters.emitted_chars += len(text)
        self._prefix.extend(text)
        if self.counters.emitted_chars > self._max_preview:
            self._prefix_truncated = True

    def _prefix_with(self, extra: str) -> str:
        """Bounded preview consisting of stored prefix plus this chunk's text."""
        tail = self.prefix_preview() + extra
        return tail[-self._max_preview:]

    def _apply_text(
        self, text: str
    ) -> tuple[str, int, list[Violation], Violation | None]:
        """Apply chunk text under the configured policy.

        A *non-fatal* constraint never terminates the stream: illegal
        characters are skipped as warnings.  For a *fatal* constraint the
        policy decides.

        Returns ``(emitted, dropped, warnings, violation)`` where ``violation``
        is the terminal violation for abort/truncate, otherwise ``None``.
        """
        emitted: list[str] = []
        dropped = 0
        warnings: list[Violation] = []

        for ch in text:
            res = self._state.feed(ch)
            if res.ok:
                emitted.append(ch)
                continue

            # Position of the rejected character in the output coordinate
            # space (emitted chars plus chars dropped earlier in this chunk).
            fail_offset = self.counters.emitted_chars + len(emitted) + dropped
            prefix = self._prefix_with("".join(emitted))
            violation = Violation(
                res.code or ErrorCode.CONSTRAINT_VIOLATION,
                res.message, offset=fail_offset, consumed_prefix=prefix)

            if not self.constraint.fatal:
                # Observability only: skip and continue.
                warnings.append(violation)
                dropped += 1
                continue

            if self.policy is Policy.ABORT:
                return "".join(emitted), dropped, warnings, violation

            if self.policy is Policy.TRUNCATE:
                # Failed feed is atomic: state stays at the last legal prefix.
                fin = self._state.finish()
                suffix = ("(truncated; prefix already complete)" if fin is None
                          else "(truncated at last legal prefix)")
                terminal = Violation(
                    violation.code, f"{violation.message} {suffix}",
                    offset=fail_offset, consumed_prefix=prefix)
                return "".join(emitted), dropped, warnings, terminal

            # DROP: skip the illegal character and keep going.
            dropped += 1

        return "".join(emitted), dropped, warnings, None

    def _finalize_stream(self) -> Violation | None:
        res = self._state.finish()
        if res is None:
            return None
        return Violation(
            res.code or ErrorCode.CONSTRAINT_VIOLATION,
            res.message, offset=self.counters.emitted_chars,
            consumed_prefix=self.prefix_preview())

    def _apply_in_order(self, seq: int, text: str, final: bool) -> ChunkFeedback:
        # Apply this chunk plus every buffered chunk whose gap has now closed.
        # Aggregate emitted/dropped text and propagate the final terminal state
        # so the triggering call observes the whole drained run atomically.
        work: list[tuple[int, str, bool]] = [(seq, text, final)]
        self._next_seq = seq + 1

        emitted_parts: list[str] = []
        dropped_total = 0
        warnings_total = 0
        terminal_status: ChunkStatus | None = None

        while work:
            cur_seq, cur_text, cur_final = work.pop(0)
            if self.status in _TERMINAL_STATUSES:
                break

            emitted, dropped, warnings, violation = self._apply_text(cur_text)
            if emitted:
                self._append_emitted(emitted)
                emitted_parts.append(emitted)
            if dropped:
                self.counters.dropped_chars += dropped
                dropped_total += dropped
            if warnings:
                self.counters.warning_count += len(warnings)
                warnings_total += len(warnings)
                self.last_warning = warnings[-1]

            if violation is not None:
                self.status = (SessionStatus.ABORTED if self.policy is Policy.ABORT
                               else SessionStatus.TRUNCATED)
                self.violation = violation
                terminal_status = ChunkStatus.VIOLATION
                self._release()
                break

            if cur_final:
                fin_violation = self._finalize_stream()
                if fin_violation is not None and self.constraint.fatal:
                    self.status = SessionStatus.ABORTED
                    self.violation = fin_violation
                    terminal_status = ChunkStatus.VIOLATION
                    self._release()
                    break
                # A non-fatal incomplete output completes with a warning.
                if fin_violation is not None:
                    self.counters.warning_count += 1
                    warnings_total += 1
                    self.last_warning = fin_violation
                self.status = SessionStatus.COMPLETED
                terminal_status = ChunkStatus.FINALIZED
                break

            # Enqueue the next consecutive buffered chunk, if any.
            if self._next_seq in self._buffer:
                nxt_seq = self._next_seq
                nxt_text, nxt_final = self._buffer.pop(nxt_seq)
                work.append((nxt_seq, nxt_text, nxt_final))
                self._next_seq = nxt_seq + 1

        emitted_all = "".join(emitted_parts)
        if terminal_status is None:
            status = (ChunkStatus.WARNED if warnings_total
                      else ChunkStatus.DROPPED if dropped_total
                      else ChunkStatus.ACCEPTED)
        elif terminal_status is ChunkStatus.FINALIZED and warnings_total:
            status = ChunkStatus.FINALIZED
        else:
            status = terminal_status
        return ChunkFeedback(
            seq=seq,
            status=status,
            emitted=emitted_all,
            emitted_chars=len(emitted_all),
            dropped_chars=dropped_total,
            warning_count=warnings_total,
            violation=self.violation if status == ChunkStatus.VIOLATION else None)

    # keep reference for tests/introspection
    @property
    def buffered_count(self) -> int:
        with self.lock:
            return len(self._buffer)
