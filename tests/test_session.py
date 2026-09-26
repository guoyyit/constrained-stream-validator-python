"""Streaming session: policies, ordering, cancellation, bounded memory."""

import pytest

from stream_validator.constraints.enum_like import EnumConstraint
from stream_validator.constraints.json_like import JsonConstraint
from stream_validator.constraints.length import MaxLengthConstraint
from stream_validator.constraints.regex_like import RegexConstraint
from stream_validator.models import ChunkStatus, ErrorCode, Policy, SessionStatus
from stream_validator.session import ValidationSession


def make(constraint, policy=Policy.ABORT, **kw):
    return ValidationSession("sid", constraint, policy, **kw)


class TestAbort:
    def test_stops_at_first_illegal_char(self):
        s = make(MaxLengthConstraint("c", 1, 3), Policy.ABORT)
        s.process_chunk(0, "ab")
        fb = s.process_chunk(1, "cde")
        assert fb.status is ChunkStatus.VIOLATION
        assert fb.emitted == "c"
        assert s.status is SessionStatus.ABORTED
        assert s.violation.code is ErrorCode.LENGTH_EXCEEDED

    def test_violation_reports_offset_and_prefix(self):
        s = make(MaxLengthConstraint("c", 1, 2), Policy.ABORT)
        s.process_chunk(0, "ab")
        fb = s.process_chunk(1, "ZZ")
        assert fb.violation.offset == 2
        assert fb.violation.consumed_prefix == "ab"

    def test_later_chunks_rejected_and_not_counted(self):
        s = make(MaxLengthConstraint("c", 1, 1), Policy.ABORT)
        s.process_chunk(0, "a")
        s.process_chunk(1, "b")  # aborts
        emitted = s.counters.emitted_chars
        fb = s.process_chunk(2, "cccc")
        assert fb.status is ChunkStatus.REJECTED
        assert s.counters.emitted_chars == emitted
        assert s.prefix_preview() == "a"


class TestTruncate:
    def test_keeps_last_legal_prefix(self):
        s = make(EnumConstraint("c", 1, ["ok", "open"]), Policy.TRUNCATE)
        fb = s.process_chunk(0, "okXYZ")
        assert fb.status is ChunkStatus.VIOLATION
        assert s.status is SessionStatus.TRUNCATED
        assert s.prefix_preview() == "ok"
        assert fb.violation.consumed_prefix == "ok"

    def test_truncated_json_prefix(self):
        s = make(JsonConstraint("c", 1), Policy.TRUNCATE)
        fb = s.process_chunk(0, '{"a":1}X')
        assert fb.status is ChunkStatus.VIOLATION
        assert s.status is SessionStatus.TRUNCATED
        assert s.prefix_preview() == '{"a":1}'
        assert fb.violation.consumed_prefix == '{"a":1}'


class TestDrop:
    def test_skips_illegal_characters(self):
        s = make(EnumConstraint("c", 1, ["yes"]), Policy.DROP)
        fb = s.process_chunk(0, "yQeXs")
        assert fb.status is ChunkStatus.DROPPED
        assert fb.emitted == "yes"
        assert fb.dropped_chars == 2
        assert s.counters.dropped_chars == 2

    def test_drop_then_complete_normally(self):
        s = make(EnumConstraint("c", 1, ["ok"]), Policy.DROP)
        s.process_chunk(0, "oXk")
        fb = s.process_chunk(1, "", final=True)
        assert fb.status is ChunkStatus.FINALIZED
        assert s.status is SessionStatus.COMPLETED

    def test_drop_unrepairable_chars_keeps_going(self):
        s = make(MaxLengthConstraint("c", 1, 2), Policy.DROP)
        s.process_chunk(0, "abcdef")
        assert s.prefix_preview() == "ab"
        assert s.counters.dropped_chars == 4


class TestDeterminism:
    @pytest.mark.parametrize("policy", list(Policy))
    def test_same_input_same_result(self, policy):
        text = '{"a": 1}??'

        def run():
            s = make(JsonConstraint("c", 1), policy)
            s.process_chunk(0, text)
            return (s.status.value, s.prefix_preview(),
                    s.counters.dropped_chars,
                    s.violation.code.value if s.violation else None)

        assert run() == run()


class TestOrdering:
    def test_duplicate_seq_is_idempotent(self):
        s = make(MaxLengthConstraint("c", 1, 10), Policy.ABORT)
        first = s.process_chunk(0, "abc")
        again = s.process_chunk(0, "abc")
        assert first.status is ChunkStatus.ACCEPTED
        assert again.status is ChunkStatus.DUPLICATE
        assert s.prefix_preview() == "abc"
        assert s.counters.emitted_chars == 3

    def test_late_chunk_after_progress_ignored(self):
        s = make(MaxLengthConstraint("c", 1, 10), Policy.ABORT)
        s.process_chunk(0, "a")
        s.process_chunk(1, "b")
        fb = s.process_chunk(0, "zzz")
        assert fb.status is ChunkStatus.DUPLICATE
        assert s.prefix_preview() == "ab"

    def test_out_of_order_buffered_and_drained_in_order(self):
        s = make(MaxLengthConstraint("c", 1, 20), Policy.ABORT)
        fb = s.process_chunk(2, "C")
        assert fb.status is ChunkStatus.BUFFERED and fb.buffered
        assert s.process_chunk(1, "B").status is ChunkStatus.BUFFERED
        fb = s.process_chunk(0, "A")
        assert fb.status is ChunkStatus.ACCEPTED
        assert s.prefix_preview() == "ABC"

    def test_reorder_window_overflow_rejected(self):
        s = make(MaxLengthConstraint("c", 1, 20), Policy.ABORT, reorder_window=1)
        assert s.process_chunk(1, "B").status is ChunkStatus.BUFFERED
        fb = s.process_chunk(2, "C")
        assert fb.status is ChunkStatus.REJECTED
        assert fb.violation.code is ErrorCode.REORDER_OVERFLOW

    def test_final_flag_on_buffered_chunk_applies_on_drain(self):
        s = make(EnumConstraint("c", 1, ["ok"]), Policy.ABORT)
        s.process_chunk(1, "k", final=True)  # buffered, final held
        s.process_chunk(0, "o")             # drains 0 then 1(final)
        assert s.status is SessionStatus.COMPLETED

    def test_redelivery_of_buffered_chunk_is_idempotent(self):
        s = make(MaxLengthConstraint("c", 1, 20), Policy.ABORT)
        assert s.process_chunk(1, "B").status is ChunkStatus.BUFFERED
        # different payload on re-delivery must be ignored
        again = s.process_chunk(1, "TAMPERED")
        assert again.status is ChunkStatus.DUPLICATE
        s.process_chunk(0, "A")
        assert s.prefix_preview() == "AB"  # first buffered payload wins


class TestCancellation:
    def test_cancel_rejects_following_chunks(self):
        s = make(MaxLengthConstraint("c", 1, 20), Policy.ABORT)
        s.process_chunk(2, "future")  # buffered
        assert s.cancel()
        fb = s.process_chunk(0, "x")
        assert fb.status is ChunkStatus.REJECTED
        assert fb.violation.code is ErrorCode.CANCELLED
        assert s.buffered_count == 0
        assert s.status is SessionStatus.CANCELLED

    def test_double_cancel_returns_false(self):
        s = make(MaxLengthConstraint("c", 1, 1), Policy.ABORT)
        assert s.cancel()
        assert s.cancel() is False

    def test_cancel_after_completion_is_false(self):
        s = make(MaxLengthConstraint("c", 1, 3), Policy.ABORT)
        s.process_chunk(0, "ok", final=True)
        assert s.cancel() is False


class TestTimeout:
    def test_deadline_expires_session(self):
        s = make(MaxLengthConstraint("c", 1, 3), Policy.ABORT, deadline=10.0)
        assert s.check_timeout(now=9.0) is False
        assert s.check_timeout(now=11.0) is True
        assert s.status is SessionStatus.TIMEOUT
        fb = s.process_chunk(0, "a", now=11.0)
        assert fb.status is ChunkStatus.REJECTED
        assert fb.violation.code is ErrorCode.TIMEOUT

    def test_chunk_after_deadline_expires_lazily(self):
        s = make(MaxLengthConstraint("c", 1, 3), Policy.ABORT, deadline=10.0)
        fb = s.process_chunk(0, "a", now=20.0)
        assert fb.status is ChunkStatus.REJECTED
        assert s.status is SessionStatus.TIMEOUT


class TestBoundedPreview:
    def test_preview_is_capped(self):
        s = make(MaxLengthConstraint("c", 1, 10_000), Policy.ABORT,
                 max_prefix_preview=8)
        s.process_chunk(0, "0123456789")
        assert s.counters.emitted_chars == 10
        assert s.prefix_preview() == "23456789"

    def test_violation_prefix_respects_cap(self):
        s = make(MaxLengthConstraint("c", 1, 2), Policy.ABORT,
                 max_prefix_preview=2)
        s.process_chunk(0, "ab")
        fb = s.process_chunk(1, "c")
        assert fb.violation is not None
        assert fb.violation.consumed_prefix == "ab"


class TestSnapshot:
    def test_snapshot_reflects_state(self):
        s = make(RegexConstraint("c", 1, r"[a-z]+"), Policy.DROP)
        s.process_chunk(0, "ab1")
        snap = s.snapshot()
        assert snap.constraint_name == "c"
        assert snap.constraint_version == 1
        assert snap.policy is Policy.DROP
        assert snap.status is SessionStatus.ACTIVE
        assert snap.consumed_chars == 2
        assert snap.dropped_chars == 1
