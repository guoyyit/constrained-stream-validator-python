"""Non-fatal constraints: violations become warnings, stream continues."""

from stream_validator.constraints.length import MaxLengthConstraint
from stream_validator.factory import build_constraint
from stream_validator.manager import SessionManager
from stream_validator.models import ChunkStatus, Policy, SessionStatus
from stream_validator.registry import ConstraintRegistry
from stream_validator.session import ValidationSession


def test_nonfatal_length_warns_and_continues():
    c = MaxLengthConstraint("c", 1, 2, fatal=False)
    s = ValidationSession("s", c, Policy.ABORT)  # policy irrelevant when non-fatal
    fb = s.process_chunk(0, "abcde")
    assert fb.status is ChunkStatus.WARNED
    assert fb.emitted == "ab"
    assert fb.dropped_chars == 3
    assert fb.warning_count == 3
    assert s.status is SessionStatus.ACTIVE
    assert s.violation is None
    snap = s.snapshot()
    assert snap.warning_count == 3


def test_nonfatal_final_completes_even_if_incomplete():
    # enum non-fatal; an output that never matches completes anyway, warned
    from stream_validator.constraints.enum_like import EnumConstraint
    c = EnumConstraint("c", 1, ["ok"], fatal=False)
    s = ValidationSession("s", c, Policy.ABORT)
    s.process_chunk(0, "oX")  # 'o' legal, 'X' warned+skipped
    fb = s.process_chunk(1, "", final=True)  # prefix 'o' incomplete
    assert fb.status is ChunkStatus.FINALIZED
    assert s.status is SessionStatus.COMPLETED
    assert fb.warning_count == 1  # the incomplete-output warning
    assert s.last_warning is not None


def test_fatal_default_unchanged():
    c = MaxLengthConstraint("c", 1, 1, fatal=True)
    s = ValidationSession("s", c, Policy.ABORT)
    fb = s.process_chunk(0, "ab")
    assert fb.status is ChunkStatus.VIOLATION
    assert s.status is SessionStatus.ABORTED
    assert fb.warning_count == 0


def test_nonfatal_over_manager_and_stats():
    reg = ConstraintRegistry()
    mgr = SessionManager(reg, reap_interval=None)
    c = build_constraint("soft", 1, {"type": "max_length", "max_length": 2,
                                     "fatal": False})
    reg.register("soft", c)
    snap = mgr.create_session("soft", Policy.ABORT)
    fb = mgr.feed(snap.session_id, 0, "abcd")
    assert fb.status is ChunkStatus.WARNED
    assert fb.warning_count == 2
    assert fb.emitted == "ab"
    assert mgr.stats().total_warnings == 2
    mgr.shutdown()
