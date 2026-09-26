"""Per-session streaming validation engine.

Responsibilities:
 * reassemble out-of-order / duplicate chunks keyed by character offset
 * feed accepted text into a per-session, independent constraint validator
 * apply the configured violation policy deterministically
 * enforce max-length and max-duration resource limits
 * support cancellation with prompt state reclamation

Concurrency: a session is guarded by its own lock. Sessions never share
mutable state — each builds its own validator instance from the immutable
constraint version it was pinned to at creation.

Coordinate systems (kept strictly separate):
 * ``seq``       – offsets in the *upstream* byte/character stream
 * ``position``  – length of the validated *output* produced so far
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from .constraints import StreamingConstraint, build_constraint
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
from .registry import ConstraintVersion

_TERMINAL_STATUSES = frozenset(
    {
        SessionStatus.FINISHED,
        SessionStatus.ABORTED,
        SessionStatus.CANCELLED,
        SessionStatus.TRUNCATED,
        SessionStatus.TIMED_OUT,
    }
)


class ValidationSession:
    def __init__(
        self,
        session_id: str,
        constraint_version: ConstraintVersion,
        policy: ViolationPolicy = ViolationPolicy.ABORT,
        max_length: int | None = None,
        max_duration_seconds: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.session_id = session_id
        self.constraint_name = constraint_version.name
        self.constraint_version = constraint_version.version
        self.policy = policy
        self.max_length = max_length
        self.max_duration_seconds = max_duration_seconds

        self._clock = clock
        self._started_at = clock()
        self._lock = threading.Lock()

        self._spec = constraint_version.spec
        self._validator: StreamingConstraint = constraint_version.build_validator()

        # reassembly buffers (bounded: contiguous fragments are drained)
        self._pending: dict[int, str] = {}
        self._eof = False

        # progress
        self._next_seq = 0              # contiguous upstream offset consumed
        self._output = ""               # validated output
        self._status = SessionStatus.OPEN
        self._error: ValidationError | None = None

        self.stats = SessionStats()

    # ------------------------------------------------------------------ views

    @property
    def status(self) -> SessionStatus:
        with self._lock:
            return self._status

    def get_stats(self) -> SessionStats:
        with self._lock:
            return SessionStats(**self.stats.__dict__)

    def result_snapshot(self) -> SessionResult:
        with self._lock:
            return self._build_result()

    def elapsed(self) -> float:
        return self._clock() - self._started_at

    # ------------------------------------------------------------------ ingest

    def ingest(self, chunk: StreamChunk) -> FeedbackEvent:
        with self._lock:
            # A cancelled session drops later chunks completely: no result
            # mutation and no statistics.
            if self._status == SessionStatus.CANCELLED:
                return self._error_event(ErrorCode.CANCELLED, "session cancelled")
            if self._status in _TERMINAL_STATUSES:
                return self._stored_error_event()
            if self._eof:
                return self._error_event(
                    ErrorCode.SESSION_FINISHED, "stream already finished"
                )

            if self.max_duration_seconds is not None and self.elapsed() > self.max_duration_seconds:
                return self._terminalize(
                    SessionStatus.TIMED_OUT,
                    ErrorCode.TIME_LIMIT_EXCEEDED,
                    "session exceeded maximum duration",
                )

            if not isinstance(chunk, StreamChunk) or not isinstance(chunk.text, str):
                return self._error_event(
                    ErrorCode.INVALID_CHUNK, "chunk must contain string text"
                )
            if chunk.seq < 0:
                return self._error_event(
                    ErrorCode.CHUNK_OUT_OF_RANGE,
                    f"negative seq {chunk.seq}",
                    position=0,
                )
            # Resource cap on *upstream* memory too: the engine must not buffer
            # arbitrarily far-ahead fragments.
            if (
                self.max_length is not None
                and chunk.seq - self._next_seq > self.max_length
            ):
                return self._error_event(
                    ErrorCode.CHUNK_OUT_OF_RANGE,
                    f"gap from {self._next_seq} to {chunk.seq} exceeds max_length",
                    position=self._next_seq,
                )

            self.stats.chunks_received += 1
            self.stats.bytes_in += len(chunk.text.encode("utf-8"))

            # Duplicate / overlap classification ----------------------------
            end = chunk.seq + len(chunk.text)
            if end <= self._next_seq:
                self.stats.chunks_duplicate += 1
                return FeedbackEvent(
                    seq=chunk.seq,
                    accepted_text=chunk.text,
                    position=len(self._output),
                )
            if chunk.seq < self._next_seq:
                self.stats.chunks_out_of_range += 1
                return self._error_event(
                    ErrorCode.CHUNK_OUT_OF_RANGE,
                    f"chunk at {chunk.seq} partially overlaps consumed boundary "
                    f"{self._next_seq}",
                    position=self._next_seq,
                )
            if chunk.seq in self._pending:
                self.stats.chunks_duplicate += 1
                if self._pending[chunk.seq] != chunk.text:
                    return self._error_event(
                        ErrorCode.DUPLICATE_SEQ,
                        f"conflicting retransmission at seq {chunk.seq}",
                        position=chunk.seq,
                    )
                return FeedbackEvent(
                    seq=chunk.seq,
                    accepted_text=chunk.text,
                    position=len(self._output),
                )

            self._pending[chunk.seq] = chunk.text
            if chunk.final:
                self._eof = True

            return self._pump()

    # -- reassembly + incremental validation --------------------------------

    def _pump(self) -> FeedbackEvent:
        """Drain every contiguous pending fragment and return one aggregated
        feedback event for this ingest call."""
        seq_start = self._next_seq
        aggregated = ""
        rejected_total = ""
        terminal_seq = seq_start
        event = FeedbackEvent(
            seq=seq_start, accepted_text="", position=len(self._output)
        )
        while self._next_seq in self._pending:
            seq = self._next_seq
            text = self._pending.pop(seq)
            self.stats.chunks_applied += 1
            out_before = len(self._output)
            event = self._validate_fragment(seq, text)
            # Net output contributed by this single fragment.
            aggregated += self._output[out_before:]
            rejected_total += event.rejected_text
            if self._status in _TERMINAL_STATUSES:
                terminal_seq = seq
                break
        if self._status not in _TERMINAL_STATUSES:
            event = FeedbackEvent(
                seq=seq_start,
                accepted_text=aggregated,
                rejected_text=rejected_total,
                position=len(self._output),
            )
        else:
            # Preserve the accepted legal prefix in the terminal event too.
            event = FeedbackEvent(
                seq=terminal_seq,
                accepted_text=aggregated,
                rejected_text=rejected_total or event.rejected_text,
                position=event.position,
                error=event.error,
            )
        return event

    def _validate_fragment(self, seq: int, text: str) -> FeedbackEvent:
        if self.max_duration_seconds is not None and self.elapsed() > self.max_duration_seconds:
            return self._terminalize(
                SessionStatus.TIMED_OUT,
                ErrorCode.TIME_LIMIT_EXCEEDED,
                "session exceeded maximum duration",
            )

        accepted, error = self._validator.feed(text)
        self._next_seq = seq + len(text)
        self.stats.bytes_out += len(accepted.encode("utf-8"))

        if error is None:
            self._output += accepted
            return FeedbackEvent(
                seq=seq,
                accepted_text=accepted,
                position=len(self._output),
            )

        # Accepted legal prefix counts even on a violation.
        self._output += accepted
        self.stats.violations += 1
        rejected = text[len(accepted):]

        if error.code is ErrorCode.LENGTH_LIMIT_EXCEEDED:
            self._terminalize(
                SessionStatus.ABORTED,
                ErrorCode.LENGTH_LIMIT_EXCEEDED,
                error.message,
            )
            return self._fragment_event(seq, accepted, rejected)

        if self.policy is ViolationPolicy.ABORT:
            self._terminalize(
                SessionStatus.ABORTED,
                ErrorCode.CONSTRAINT_VIOLATION,
                error.message,
            )
            return self._fragment_event(seq, accepted, rejected)
        if self.policy is ViolationPolicy.TRUNCATE:
            self._terminalize(
                SessionStatus.TRUNCATED,
                ErrorCode.CONSTRAINT_VIOLATION,
                error.message,
            )
            return self._fragment_event(seq, accepted, rejected)

        # SKIP: discard the rejected run, resync at earliest viable cut.
        return self._skip_resync(seq + len(accepted), rejected)

    def _fragment_event(self, seq: int, accepted: str, rejected: str) -> FeedbackEvent:
        assert self._error is not None
        return FeedbackEvent(
            seq=seq,
            accepted_text=accepted,
            rejected_text=rejected,
            position=len(self._output),
            error=self._error,
        )

    def _skip_resync(self, start_seq: int, rejected: str) -> FeedbackEvent:
        base = self._output
        viable_cut = None
        trial = None
        for cut in range(1, len(rejected) + 1):
            tail = rejected[cut:]
            if not tail:
                continue
            candidate = build_constraint(self._spec)
            got, err = candidate.feed(base + tail)
            if err is None and got == base + tail:
                viable_cut = cut
                trial = candidate
                break

        if viable_cut is None or trial is None:
            # Nothing in the remainder is currently viable; retain the legal
            # prefix and stay open (later chunks may complete the token).
            return FeedbackEvent(
                seq=start_seq,
                accepted_text="",
                rejected_text=rejected,
                position=len(self._output),
            )

        discarded = rejected[:viable_cut]
        kept = rejected[viable_cut:]
        self._validator = trial
        self._output = base + kept
        self._next_seq = start_seq + len(rejected)
        self.stats.bytes_out += len(kept.encode("utf-8"))
        return FeedbackEvent(
            seq=start_seq,
            accepted_text=kept,
            rejected_text=discarded,
            position=len(self._output),
        )

    # -- terminal helpers ---------------------------------------------------

    def _terminalize(
        self,
        status: SessionStatus,
        code: ErrorCode,
        message: str,
    ) -> FeedbackEvent:
        self._status = status
        self._error = ValidationError(
            code=code,
            message=message,
            position=len(self._output),
            consumed_prefix=self._output,
        )
        self._pending.clear()
        return self._stored_error_event()

    def _stored_error_event(self) -> FeedbackEvent:
        assert self._error is not None
        return FeedbackEvent(
            seq=self._next_seq,
            accepted_text="",
            position=len(self._output),
            error=self._error,
        )

    def _error_event(
        self, code: ErrorCode, message: str, position: int | None = None
    ) -> FeedbackEvent:
        # Non-terminal protocol error (session state is not modified).
        return FeedbackEvent(
            seq=self._next_seq,
            accepted_text="",
            position=self._next_seq if position is None else position,
            error=ValidationError(
                code=code,
                message=message,
                position=self._next_seq if position is None else position,
                consumed_prefix=self._output,
            ),
        )

    # ------------------------------------------------------------------ cancel

    def cancel(self) -> None:
        with self._lock:
            if self._status == SessionStatus.CANCELLED:
                return
            self._status = SessionStatus.CANCELLED
            self._error = ValidationError(
                code=ErrorCode.CANCELLED,
                message="session cancelled",
                position=self._next_seq,
                consumed_prefix="",
            )
            # Reclaim all per-request state promptly.
            self._pending.clear()
            self._output = ""

    # ------------------------------------------------------------------ finish

    def finish(self) -> SessionResult:
        with self._lock:
            if self._status in _TERMINAL_STATUSES:
                return self._build_result()

            if self._pending:
                self._terminalize(
                    SessionStatus.ABORTED,
                    ErrorCode.CHUNK_OUT_OF_RANGE,
                    "finish called with missing chunks in the stream",
                )
                return self._build_result()

            if self.max_duration_seconds is not None and self.elapsed() > self.max_duration_seconds:
                self._terminalize(
                    SessionStatus.TIMED_OUT,
                    ErrorCode.TIME_LIMIT_EXCEEDED,
                    "session exceeded maximum duration",
                )
                return self._build_result()

            error = self._validator.finish()
            if error is not None:
                self.stats.violations += 1
                if self.policy is ViolationPolicy.SKIP:
                    # The trailing incomplete token cannot be repaired; keep the
                    # already validated prefix as the delivered output.
                    self._status = SessionStatus.FINISHED
                    return self._build_result()
                status = (
                    SessionStatus.TRUNCATED
                    if self.policy is ViolationPolicy.TRUNCATE
                    else SessionStatus.ABORTED
                )
                self._status = status
                self._error = ValidationError(
                    code=error.code,
                    message=error.message,
                    position=error.position,
                    consumed_prefix=self._output,
                )
                return self._build_result()

            self._status = SessionStatus.FINISHED
            return self._build_result()

    def _build_result(self) -> SessionResult:
        if self._status == SessionStatus.CANCELLED:
            return SessionResult(
                status=SessionStatus.CANCELLED,
                output="",
                error_code=ErrorCode.CANCELLED.value,
                error_position=self._next_seq,
                consumed_prefix="",
            )
        return SessionResult(
            status=self._status,
            output=self._output,
            error_code=self._error.code.value if self._error else None,
            error_position=self._error.position if self._error else None,
            consumed_prefix=self._output,
        )
