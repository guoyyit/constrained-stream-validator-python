"""SessionManager: limits, stats, lifecycle, retention, concurrency."""

import threading

import pytest

from stream_validator.constraints.enum_like import EnumConstraint
from stream_validator.constraints.length import MaxLengthConstraint
from stream_validator.manager import ConcurrencyLimitError, SessionManager
from stream_validator.models import ChunkStatus, ErrorCode, Policy, SessionStatus
from stream_validator.registry import ConstraintRegistry


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def manager():
    reg = ConstraintRegistry()
    reg.register("len", MaxLengthConstraint("len", 0, 5))
    mgr = SessionManager(reg, max_active=3, reap_interval=None, clock=FakeClock())
    yield mgr, reg
    mgr.shutdown()


def test_create_and_feed(manager):
    mgr, _ = manager
    snap = mgr.create_session("len", Policy.ABORT)
    assert snap.status is SessionStatus.ACTIVE
    fb = mgr.feed(snap.session_id, 0, "abc", final=True)
    assert fb.status is ChunkStatus.FINALIZED
    assert mgr.get(snap.session_id).status is SessionStatus.COMPLETED


def test_version_pin_new_requests_see_new_version(manager):
    mgr, reg = manager
    old = mgr.create_session("len", Policy.ABORT)  # v1, cap 5
    reg.register("len", MaxLengthConstraint("len", 0, 50))
    new = mgr.create_session("len", Policy.ABORT)  # latest v2, cap 50

    # old session still capped at 5
    assert mgr.feed(old.session_id, 0, "abcdef").status is ChunkStatus.VIOLATION
    # new session tolerates 6 chars
    fb = mgr.feed(new.session_id, 0, "abcdef")
    assert fb.status is ChunkStatus.ACCEPTED
    # explicit pinning
    pinned = mgr.create_session("len", Policy.ABORT, version=1)
    assert mgr.feed(pinned.session_id, 0, "abcdef").status is ChunkStatus.VIOLATION


def test_concurrency_limit(manager):
    mgr, _ = manager
    mgr.create_session("len", Policy.ABORT)
    mgr.create_session("len", Policy.ABORT)
    mgr.create_session("len", Policy.ABORT)
    with pytest.raises(ConcurrencyLimitError):
        mgr.create_session("len", Policy.ABORT)
    # a completed session frees a slot
    mgr.shutdown()


def test_slot_freed_after_completion():
    reg = ConstraintRegistry()
    reg.register("len", MaxLengthConstraint("len", 0, 5))
    mgr = SessionManager(reg, max_active=1, reap_interval=None)
    s = mgr.create_session("len", Policy.ABORT)
    with pytest.raises(ConcurrencyLimitError):
        mgr.create_session("len", Policy.ABORT)
    mgr.feed(s.session_id, 0, "ok", final=True)
    s2 = mgr.create_session("len", Policy.ABORT)  # slot freed
    assert s2.status is SessionStatus.ACTIVE
    mgr.shutdown()


def test_unknown_session_feed_is_rejected_and_counted(manager):
    mgr, _ = manager
    fb = mgr.feed("ghost", 0, "x")
    assert fb.status is ChunkStatus.REJECTED
    assert fb.violation.code is ErrorCode.SESSION_NOT_FOUND
    assert mgr.stats().rejected_chunks == 1


def test_cancel_lifecycle_and_stats(manager):
    mgr, _ = manager
    s = mgr.create_session("len", Policy.ABORT)
    assert mgr.cancel(s.session_id) is True
    assert mgr.get(s.session_id).status is SessionStatus.CANCELLED
    assert mgr.stats().total_cancelled == 1
    assert mgr.cancel(s.session_id) is False  # idempotent
    fb = mgr.feed(s.session_id, 0, "x")
    assert fb.violation.code is ErrorCode.CANCELLED


def test_timeout_expiry_and_retention():
    clock = FakeClock()
    reg = ConstraintRegistry()
    reg.register("len", MaxLengthConstraint("len", 0, 5))
    mgr = SessionManager(reg, max_active=5, reap_interval=None,
                         retention=10.0, clock=clock)
    s = mgr.create_session("len", Policy.ABORT, timeout=5.0)
    clock.t += 6  # 1006 -> deadline exceeded
    assert mgr.reap_once() >= 1
    assert mgr.get(s.session_id).status is SessionStatus.TIMEOUT
    assert mgr.stats().total_timeout == 1
    # still retrievable within retention (1006 + 10 = 1016)
    clock.t = 1015
    mgr.reap_once()
    assert mgr.get(s.session_id).status is SessionStatus.TIMEOUT
    # after retention, evicted
    clock.t = 1017
    mgr.reap_once()
    with pytest.raises(KeyError):
        mgr.get(s.session_id)
    mgr.shutdown()


def test_aggregate_stats_isolation(manager):
    mgr, reg = manager
    reg.register("enum", EnumConstraint("e", 0, ["ok"]))
    a = mgr.create_session("len", Policy.ABORT)
    b = mgr.create_session("enum", Policy.DROP)
    mgr.feed(a.session_id, 0, "abcdef")        # 5 emitted, aborted
    mgr.feed(b.session_id, 0, "oXk", final=True)  # 2 emitted, 1 dropped
    st = mgr.stats()
    assert st.total_chars_emitted == 7
    assert st.total_chars_dropped == 1
    assert st.total_aborted == 1
    assert st.total_completed == 1
    # sessions do not share prefixes/counters
    assert mgr.get(a.session_id).prefix_preview == "abcde"
    assert mgr.get(b.session_id).prefix_preview == "ok"


def test_concurrent_sessions_are_isolated():
    reg = ConstraintRegistry()
    reg.register("len", MaxLengthConstraint("len", 0, 100))
    mgr = SessionManager(reg, max_active=200, reap_interval=None)
    errors: list[BaseException] = []

    def worker(i):
        try:
            snap = mgr.create_session("len", Policy.ABORT)
            token = f"s{i}-"
            mgr.feed(snap.session_id, 0, token)
            mgr.feed(snap.session_id, 1, "data")
            got = mgr.get(snap.session_id).prefix_preview
            assert got == token + "data", got
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert mgr.stats().total_created == 50
    mgr.shutdown()


def test_concurrent_feeds_one_session_serialized():
    reg = ConstraintRegistry()
    reg.register("len", MaxLengthConstraint("len", 0, 1000))
    mgr = SessionManager(reg, max_active=5, reap_interval=None)
    snap = mgr.create_session("len", Policy.ABORT)
    errors: list[BaseException] = []

    def feed(seq, text):
        try:
            mgr.feed(snap.session_id, seq, text)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    # deliberately out of order; result must be deterministic after seq 0..9
    pairs = [(i, f"chunk{i};") for i in range(10)]
    threads = [threading.Thread(target=feed, args=p)
               for p in reversed(pairs)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    preview = mgr.get(snap.session_id).prefix_preview
    assert preview == "".join(f"chunk{i};" for i in range(10))
    mgr.shutdown()
