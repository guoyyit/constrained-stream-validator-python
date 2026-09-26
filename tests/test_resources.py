"""Resource bounds: length caps, reorder caps, preview caps and stable
throughput/memory under concurrency."""

import threading

from stream_validator.constraints.enum_like import EnumConstraint
from stream_validator.constraints.length import MaxLengthConstraint
from stream_validator.constraints.regex_like import RegexConstraint
from stream_validator.manager import ConcurrencyLimitError, SessionManager
from stream_validator.models import ChunkStatus, Policy
from stream_validator.registry import ConstraintRegistry


def test_length_constraint_bounds_long_stream():
    c = MaxLengthConstraint("c", 1, 100)
    s = c.new_state()
    for i in range(100):
        assert s.feed("x").ok
    r = s.feed("x")
    assert not r.ok
    # per-request state does not retain the stream text
    assert s.count == 100


def test_regex_state_memory_is_bounded():
    c = RegexConstraint("c", 1, r"a*b", max_length=64)
    s = c.new_state()
    for _ in range(64):
        assert s.feed("a").ok
    r = s.feed("a")
    assert not r.ok and r.code.name == "LENGTH_EXCEEDED"


def test_preview_cap_on_long_valid_stream():
    reg = ConstraintRegistry()
    reg.register("len", MaxLengthConstraint("len", 0, 1_000_000))
    mgr = SessionManager(reg, max_active=10, max_prefix_preview=128,
                         reap_interval=None)
    snap = mgr.create_session("len", Policy.ABORT)
    big = "x" * 50_000
    mgr.feed(snap.session_id, 0, big)
    got = mgr.get(snap.session_id)
    assert got.consumed_chars == 50_000
    assert len(got.prefix_preview) == 128
    mgr.shutdown()


def test_reorder_buffer_is_bounded():
    c = MaxLengthConstraint("c", 1, 1000)
    from stream_validator.session import ValidationSession
    s = ValidationSession("s", c, Policy.ABORT, reorder_window=4)
    for seq in range(1, 5):
        assert s.process_chunk(seq, "x").status is ChunkStatus.BUFFERED
    fb = s.process_chunk(5, "x")
    assert fb.status is ChunkStatus.REJECTED
    assert fb.violation.code.name == "REORDER_OVERFLOW"


def test_cancelled_session_drops_buffered_chunks():
    c = MaxLengthConstraint("c", 1, 1000)
    from stream_validator.session import ValidationSession
    s = ValidationSession("s", c, Policy.ABORT, reorder_window=10)
    for seq in range(1, 8):
        s.process_chunk(seq, "x" * 1000)
    assert s.buffered_count == 7
    s.cancel()
    assert s.buffered_count == 0


def test_high_concurrency_bounded_active_sessions():
    reg = ConstraintRegistry()
    # Cap equals exactly the legal payload per session so the extra chars in
    # each stream are dropped under the DROP policy.
    reg.register("len", MaxLengthConstraint("len", 0, 400))
    mgr = SessionManager(reg, max_active=8, reorder_window=4,
                         reap_interval=None)
    # Phase 1: 64 threads only create sessions and hold them until the gate
    # opens, so the active cap cannot be dodged by early release.
    start = threading.Event()
    created: list[str] = []
    limited: list[bool] = []
    lock = threading.Lock()

    def try_create():
        start.wait()
        try:
            snap = mgr.create_session("len", Policy.DROP)
            with lock:
                created.append(snap.session_id)
        except ConcurrencyLimitError:
            with lock:
                limited.append(True)

    threads = [threading.Thread(target=try_create) for _ in range(64)]
    for t in threads:
        t.start()
    start.set()
    for t in threads:
        t.join()
    assert len(created) == 8
    assert len(limited) == 56
    assert mgr.stats().active_sessions == 8

    # Phase 2: streams on the admitted sessions are isolated; then release.
    def use(sid, i):
        mgr.feed(sid, 0, "a" * 200 + "!" * 50)
        mgr.feed(sid, 1, "b" * 200 + "?" * 50, final=True)

    workers = [threading.Thread(target=use, args=(sid, i))
               for i, sid in enumerate(created)]
    for t in workers:
        t.start()
    for t in workers:
        t.join()
    st = mgr.stats()
    assert st.total_chars_emitted == 8 * 400
    assert st.total_chars_dropped == 8 * 100
    for sid in created:
        mgr.remove(sid)
    assert len(mgr._sessions) == 0
    mgr.shutdown()


def test_throughput_stable_across_rounds():
    reg = ConstraintRegistry()
    reg.register("e", EnumConstraint("e", 0, ["accepted", "available"]))
    mgr = SessionManager(reg, max_active=200, max_prefix_preview=64,
                         reap_interval=None)
    for r in range(5):
        done = []
        lock = threading.Lock()

        def worker(i):
            snap = mgr.create_session("e", Policy.DROP)
            text = "acXcepZte?d"
            fb = mgr.feed(snap.session_id, 0, text, final=True)
            with lock:
                done.append(fb.emitted_chars)
            mgr.remove(snap.session_id)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(done) == 100
        assert set(done) == {8}  # 'accepted'
        assert mgr.stats().active_sessions == 0
    mgr.shutdown()
