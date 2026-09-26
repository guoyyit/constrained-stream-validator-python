"""High-concurrency isolation and bounded-resource tests."""

import threading

from stream_validator.manager import SessionManager
from stream_validator.models import (
    ConstraintSpec,
    StreamChunk,
    ViolationPolicy,
)
from stream_validator.models import (
    ConstraintType as T,
)
from stream_validator.registry import ConstraintRegistry


def test_mixed_policies_under_high_concurrency():
    reg = ConstraintRegistry()
    reg.register("word", ConstraintSpec(T.REGEX, {"pattern": "[a-z]+"}))
    reg.register("json", ConstraintSpec(T.STRUCTURED, {"kind": "json"}))
    mgr = SessionManager(reg, max_active_sessions=10_000)

    n_per_policy = 120
    barrier = threading.Barrier(n_per_policy * 2)
    failures: list[str] = []

    def worker_skip(i: int):
        barrier.wait()
        try:
            sid = mgr.create_session("word", policy=ViolationPolicy.SKIP)
            # same deterministic input for every worker
            mgr.ingest(sid, StreamChunk(0, "ab1cd"))
            mgr.ingest(sid, StreamChunk(5, "ef2gh", final=True))
            r = mgr.finish(sid)
            assert r.status.value == "finished", r
            assert r.output == "abcdefgh", r.output
        except AssertionError as exc:
            failures.append(str(exc))

    def worker_json(i: int):
        barrier.wait()
        try:
            sid = mgr.create_session("json")
            parts = ['{"n": ', '12, "ok": ', 'true}']
            seq = 0
            for p in parts:
                mgr.ingest(sid, StreamChunk(seq, p, final=(p == parts[-1])))
                seq += len(p)
            r = mgr.finish(sid)
            assert r.output == '{"n": 12, "ok": true}', r.output
        except AssertionError as exc:
            failures.append(str(exc))

    threads = [threading.Thread(target=worker_skip, args=(i,)) for i in range(n_per_policy)]
    threads += [threading.Thread(target=worker_json, args=(i,)) for i in range(n_per_policy)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert failures == []
    stats = mgr.manager_stats()
    assert stats.total_sessions_created == 2 * n_per_policy
    assert stats.active_sessions == 0
    assert stats.total_bytes_out > 0


def test_reaper_keeps_active_set_bounded():
    reg = ConstraintRegistry()
    reg.register("word", ConstraintSpec(T.LENGTH, {"max": 100}))
    mgr = SessionManager(reg, max_active_sessions=5)

    import pytest

    from stream_validator.models import ValidationError

    open_ids = []
    for i in range(5):
        sid = mgr.create_session("word")
        open_ids.append(sid)
    # cap reached
    with pytest.raises(ValidationError):
        mgr.create_session("word")
    # finishing a session frees a slot even before reaping
    mgr.ingest(open_ids[0], StreamChunk(0, "x", final=True))
    mgr.finish(open_ids[0])
    assert mgr.active_count() == 4
    new_sid = mgr.create_session("word")
    assert mgr.active_count() == 5
    # reaper removes the terminal one; open sessions remain untouched
    removed = mgr.reaper_sweep()
    assert removed == 1
    assert mgr.get_result(new_sid).status.value == "open"
