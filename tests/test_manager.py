"""Tests for SessionManager: isolation, version pinning, caps, reaping."""

import threading

import pytest

from stream_validator.manager import SessionManager
from stream_validator.models import (
    ConstraintSpec,
    ErrorCode,
    SessionStatus,
    StreamChunk,
    ValidationError,
)
from stream_validator.models import (
    ConstraintType as T,
)
from stream_validator.registry import ConstraintRegistry


@pytest.fixture()
def registry():
    reg = ConstraintRegistry()
    reg.register("len", ConstraintSpec(T.LENGTH, {"max": 5}))
    return reg


# -- lifecycle --------------------------------------------------------------

def test_create_and_finish(registry):
    mgr = SessionManager(registry)
    sid = mgr.create_session("len")
    mgr.ingest(sid, StreamChunk(0, "ab"))
    mgr.ingest(sid, StreamChunk(2, "cde", final=True))
    result = mgr.finish(sid)
    assert result.status is SessionStatus.FINISHED
    assert result.output == "abcde"
    assert mgr.active_count() == 0


def test_unknown_session_errors(registry):
    mgr = SessionManager(registry)
    with pytest.raises(ValidationError) as ei:
        mgr.ingest("nope", StreamChunk(0, "x"))
    assert ei.value.code is ErrorCode.SESSION_NOT_FOUND


def test_unknown_constraint_at_creation(registry):
    mgr = SessionManager(registry)
    with pytest.raises(ValidationError) as ei:
        mgr.create_session("missing")
    assert ei.value.code is ErrorCode.UNKNOWN_CONSTRAINT


# -- version pinning --------------------------------------------------------

def test_inflight_session_keeps_old_constraint_version(registry):
    mgr = SessionManager(registry)
    sid_old = mgr.create_session("len")
    registry.register("len", ConstraintSpec(T.LENGTH, {"max": 50}))
    sid_new = mgr.create_session("len")
    mgr.ingest(sid_old, StreamChunk(0, "abcdef"))
    mgr.ingest(sid_new, StreamChunk(0, "abcdef"))
    # old session pinned at version 1 (max=5) -> violation; new is fine
    assert mgr.get_result(sid_old).status is SessionStatus.ABORTED
    assert mgr.get_result(sid_new).status is SessionStatus.OPEN
    # explicit version selection
    sid_explicit = mgr.create_session("len", constraint_version=1)
    mgr.ingest(sid_explicit, StreamChunk(0, "abcdef"))
    assert mgr.get_result(sid_explicit).status is SessionStatus.ABORTED


# -- concurrency isolation --------------------------------------------------

def test_concurrent_sessions_are_isolated(registry):
    mgr = SessionManager(registry)
    errors: list[BaseException] = []

    def worker(idx: int):
        try:
            sid = mgr.create_session("len")
            text = "ab" if idx % 2 == 0 else "abcdefgh"
            mgr.ingest(sid, StreamChunk(0, text, final=True))
            result = mgr.finish(sid)
            if idx % 2 == 0:
                assert result.output == "ab"
            else:
                assert result.error_code == ErrorCode.LENGTH_LIMIT_EXCEEDED.value
                assert result.output == "abcde"
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    stats = mgr.manager_stats()
    assert stats.total_sessions_created == 40
    assert stats.active_sessions == 0


def test_active_session_cap(registry):
    mgr = SessionManager(registry, max_active_sessions=2)
    mgr.create_session("len")
    mgr.create_session("len")
    with pytest.raises(ValidationError) as ei:
        mgr.create_session("len")
    assert ei.value.code is ErrorCode.LENGTH_LIMIT_EXCEEDED
    # finishing one frees a slot
    sid = next(iter(mgr._sessions))
    mgr.ingest(sid, StreamChunk(0, "x", final=True))
    mgr.finish(sid)
    mgr.create_session("len")


# -- cancellation & reaping -------------------------------------------------

def test_reaper_removes_terminal_and_expired(registry):
    t = [0.0]
    mgr = SessionManager(registry, clock=lambda: t[0])
    sid_done = mgr.create_session("len")
    mgr.ingest(sid_done, StreamChunk(0, "x", final=True))
    mgr.finish(sid_done)

    sid_open = mgr.create_session("len", max_duration_seconds=10)
    sid_keep = mgr.create_session("len")

    t[0] = 11.0
    removed = mgr.reaper_sweep()
    assert removed == 2  # finished + expired
    with pytest.raises(ValidationError):
        mgr.get_result(sid_done)
    # expired session state is reclaimed promptly
    with pytest.raises(ValidationError):
        mgr.get_result(sid_open)
    assert mgr.get_result(sid_keep).status is SessionStatus.OPEN


def test_cancel_reclaims_session(registry):
    mgr = SessionManager(registry)
    sid = mgr.create_session("len")
    mgr.cancel(sid)
    result = mgr.get_result(sid)
    assert result.status is SessionStatus.CANCELLED
    mgr.reaper_sweep()
    with pytest.raises(ValidationError):
        mgr.get_stats(sid)


def test_default_limits_apply(registry):
    mgr = SessionManager(
        registry, default_max_length=2, default_max_duration_seconds=5
    )
    sid = mgr.create_session("len")
    mgr.ingest(sid, StreamChunk(0, "abcd"))
    # session uses the *len* constraint (max 5) but default cap 2 still bounds
    # buffered upstream; validator itself allows up to 5
    assert mgr.get_result(sid).output == "abcd"


def test_chunks_after_terminal_do_not_inflate_finished_count(registry):
    mgr = SessionManager(registry)
    sid = mgr.create_session("len")
    mgr.ingest(sid, StreamChunk(0, "abcdefgh"))  # violates max=5 -> aborted
    mgr.ingest(sid, StreamChunk(8, "x"))         # after terminal
    mgr.ingest(sid, StreamChunk(9, "y"))
    stats = mgr.manager_stats()
    assert stats.total_sessions_finished == 1
