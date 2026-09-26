"""Tests for the per-session validation engine: policies, reordering, cancel."""

import pytest

from stream_validator.engine import ValidationSession
from stream_validator.models import (
    ErrorCode,
    SessionStatus,
    StreamChunk,
    ViolationPolicy,
)
from stream_validator.registry import ConstraintRegistry


@pytest.fixture()
def registry():
    return ConstraintRegistry()


def make_session(reg, name="c", spec=None, policy=ViolationPolicy.ABORT,
                 version=None, max_length=None, max_duration=None, clock=None,
                 register_first=True):
    if spec is None:
        from stream_validator.models import ConstraintSpec
        from stream_validator.models import ConstraintType as T
        spec = ConstraintSpec(T.REGEX, {"pattern": "[a-z]+"})
    cv = reg.register(name, spec) if register_first else reg.get(name, version)
    kwargs = {
        "policy": policy,
        "max_length": max_length,
        "max_duration_seconds": max_duration,
    }
    if clock:
        kwargs["clock"] = clock
    return ValidationSession("s1", cv, **kwargs)


# -- happy path -------------------------------------------------------------

def test_in_order_legal_stream_finishes(registry):
    from stream_validator.models import ConstraintSpec
    from stream_validator.models import ConstraintType as T
    s = make_session(registry, spec=ConstraintSpec(T.ENUM, {"values": ["DONE"]}))
    e1 = s.ingest(StreamChunk(0, "DO"))
    assert e1.accepted_text == "DO" and e1.error is None
    e2 = s.ingest(StreamChunk(2, "NE", final=True))
    assert e2.accepted_text == "NE"
    result = s.finish()
    assert result.status is SessionStatus.FINISHED
    assert result.output == "DONE"
    assert result.error_code is None
    assert s.status is SessionStatus.FINISHED


# -- out-of-order / duplicates ---------------------------------------------

def test_out_of_order_chunks_are_reassembled(registry):
    from stream_validator.models import ConstraintSpec
    from stream_validator.models import ConstraintType as T
    s = make_session(registry, spec=ConstraintSpec(T.REGEX, {"pattern": "abcdef"}))
    # arrives: [3..5], [0..2]
    e = s.ingest(StreamChunk(3, "def"))
    assert e.accepted_text == ""  # gap, buffered
    e = s.ingest(StreamChunk(0, "abc"))
    assert e.accepted_text == "abcdef"
    assert s.result_snapshot().output == "abcdef"


def test_exact_duplicate_is_idempotent(registry):
    s = make_session(registry)
    s.ingest(StreamChunk(0, "abc"))
    dup = s.ingest(StreamChunk(0, "abc"))
    assert dup.error is None
    stats = s.get_stats()
    assert stats.chunks_duplicate == 1
    assert stats.chunks_received == 2
    assert s.result_snapshot().output == "abc"


def test_duplicate_with_conflicting_text_errors(registry):
    s = make_session(registry)
    s.ingest(StreamChunk(5, "abc"))  # buffered pending
    e = s.ingest(StreamChunk(5, "xyz"))
    assert e.error is not None
    assert e.error.code is ErrorCode.DUPLICATE_SEQ
    # session stays open and original text is preserved
    s.ingest(StreamChunk(0, "abcdefghijklmnop"[:5]))
    assert s.result_snapshot().output.startswith("abc")


def test_overlapping_partial_chunk_rejected(registry):
    s = make_session(registry)
    s.ingest(StreamChunk(0, "abc"))
    e = s.ingest(StreamChunk(2, "cx"))
    assert e.error.code is ErrorCode.CHUNK_OUT_OF_RANGE


# -- policies ---------------------------------------------------------------

def test_abort_policy_stops_immediately(registry):
    s = make_session(registry, policy=ViolationPolicy.ABORT)
    e = s.ingest(StreamChunk(0, "ab12"))
    assert e.accepted_text == "ab"
    assert e.error.code is ErrorCode.CONSTRAINT_VIOLATION
    result = s.finish()
    assert result.status is SessionStatus.ABORTED
    assert result.output == "ab"
    assert result.error_position == 2
    # later chunks have no effect and carry the stored error
    later = s.ingest(StreamChunk(4, "z"))
    assert later.error.code is ErrorCode.CONSTRAINT_VIOLATION


def test_truncate_policy_keeps_last_legal_prefix(registry):
    s = make_session(registry, policy=ViolationPolicy.TRUNCATE)
    e = s.ingest(StreamChunk(0, "hello9"))
    assert e.accepted_text == "hello"
    result = s.finish()
    assert result.status is SessionStatus.TRUNCATED
    assert result.output == "hello"
    assert result.error_code == ErrorCode.CONSTRAINT_VIOLATION.value


def test_skip_policy_discards_illegal_fragment_and_continues(registry):
    s = make_session(registry, policy=ViolationPolicy.SKIP)
    e = s.ingest(StreamChunk(0, "ab1cd"))
    # net text added to the validated output in this ingest call
    assert e.accepted_text == "abcd"
    assert e.rejected_text == "1"
    e2 = s.ingest(StreamChunk(5, "ef", final=True))
    assert e2.accepted_text == "ef"
    result = s.finish()
    assert result.status is SessionStatus.FINISHED
    assert result.output == "abcdef"


def test_policy_results_are_deterministic(registry):
    from stream_validator.models import ConstraintSpec
    from stream_validator.models import ConstraintType as T
    outcomes = set()
    for _ in range(5):
        cv = registry.register(
            f"r{_}", ConstraintSpec(T.REGEX, {"pattern": "[a-z]+"})
        )
        s = ValidationSession("x", cv, policy=ViolationPolicy.SKIP)
        events = [
            s.ingest(StreamChunk(0, "ab1cd")),
            s.ingest(StreamChunk(5, "e9f")),
        ]
        r = s.finish()
        outcomes.add(
            (tuple((ev.accepted_text, ev.rejected_text) for ev in events), r.output)
        )
    assert len(outcomes) == 1


def test_truncate_with_length_limit(registry):
    from stream_validator.models import ConstraintSpec
    from stream_validator.models import ConstraintType as T
    s = make_session(
        registry,
        spec=ConstraintSpec(T.LENGTH, {"max": 4}),
        policy=ViolationPolicy.TRUNCATE,
    )
    e = s.ingest(StreamChunk(0, "abcdef", final=True))
    assert e.accepted_text == "abcd"
    assert e.error.code is ErrorCode.LENGTH_LIMIT_EXCEEDED
    result = s.finish()
    assert result.status is SessionStatus.ABORTED  # length cap is a hard abort
    assert result.output == "abcd"


# -- cancellation -----------------------------------------------------------

def test_cancelled_session_drops_later_chunks_and_stats(registry):
    s = make_session(registry)
    s.ingest(StreamChunk(0, "ab"))
    s.cancel()
    assert s.status is SessionStatus.CANCELLED
    received_before = s.get_stats().chunks_received
    e = s.ingest(StreamChunk(2, "cd"))
    assert e.error.code is ErrorCode.CANCELLED
    stats = s.get_stats()
    assert stats.chunks_received == received_before
    result = s.finish()
    assert result.status is SessionStatus.CANCELLED
    assert result.output == ""
    assert result.consumed_prefix == ""


def test_cancel_clears_pending_buffers(registry):
    s = make_session(registry)
    s.ingest(StreamChunk(10, "zzz"))  # gap, buffered
    s.cancel()
    result = s.finish()
    assert result.status is SessionStatus.CANCELLED


# -- resource limits --------------------------------------------------------

def test_max_length_rejects_far_ahead_gap(registry):
    s = make_session(registry, max_length=4)
    e = s.ingest(StreamChunk(100, "a"))
    assert e.error.code is ErrorCode.CHUNK_OUT_OF_RANGE


def test_time_limit_terminalizes(registry):
    t = [100.0]
    s = make_session(
        registry, max_duration=10.0, clock=lambda: t[0]
    )
    t[0] = 111.0
    e = s.ingest(StreamChunk(0, "ab"))
    assert e.error.code is ErrorCode.TIME_LIMIT_EXCEEDED
    assert s.finish().status is SessionStatus.TIMED_OUT


def test_finish_with_gap_errors(registry):
    s = make_session(registry)
    s.ingest(StreamChunk(5, "f"))
    result = s.finish()
    assert result.status is SessionStatus.ABORTED
    assert result.error_code == ErrorCode.CHUNK_OUT_OF_RANGE.value


def test_finish_incomplete_document_truncates(registry):
    from stream_validator.models import ConstraintSpec
    from stream_validator.models import ConstraintType as T
    s = make_session(
        registry,
        spec=ConstraintSpec(T.STRUCTURED, {}),
        policy=ViolationPolicy.TRUNCATE,
    )
    s.ingest(StreamChunk(0, '{"a":'))
    result = s.finish()
    assert result.status is SessionStatus.TRUNCATED
    assert result.error_code == ErrorCode.CONSTRAINT_VIOLATION.value


# -- stats ------------------------------------------------------------------

def test_stats_track_flow(registry):
    s = make_session(registry)
    s.ingest(StreamChunk(3, "def"))
    s.ingest(StreamChunk(0, "abc"))
    stats = s.get_stats()
    assert stats.chunks_received == 2
    assert stats.chunks_applied == 2
    assert stats.bytes_in == 6
    assert stats.bytes_out == 6
