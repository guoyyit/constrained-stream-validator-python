"""FastAPI HTTP layer for the streaming validation service."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .factory import build_constraint
from .manager import ConcurrencyLimitError, SessionManager
from .models import ChunkStatus, ErrorCode, Policy
from .registry import ConstraintRegistry, ConstraintNotFoundError

# Stable HTTP error mapping; application error codes remain distinguishable.
_HTTP_STATUS = {
    ErrorCode.CONSTRAINT_VIOLATION: 422,
    ErrorCode.LENGTH_EXCEEDED: 422,
    ErrorCode.REGEX_MISMATCH: 422,
    ErrorCode.ENUM_MISMATCH: 422,
    ErrorCode.INVALID_JSON: 422,
    ErrorCode.INCOMPLETE_OUTPUT: 422,
    ErrorCode.CANCELLED: 409,
    ErrorCode.TIMEOUT: 408,
    ErrorCode.SESSION_NOT_FOUND: 404,
    ErrorCode.CONSTRAINT_NOT_FOUND: 404,
    ErrorCode.CONCURRENCY_LIMIT: 503,
    ErrorCode.REORDER_OVERFLOW: 422,
    ErrorCode.SESSION_CLOSED: 409,
    ErrorCode.BAD_REQUEST: 400,
}


# -- request/response schemas ------------------------------------------------
class ConstraintIn(BaseModel):
    type: str
    fatal: bool = True
    config: dict[str, Any] = Field(default_factory=dict)


class FeedIn(BaseModel):
    seq: int = Field(ge=0)
    text: str = ""
    final: bool = False


class CreateSessionIn(BaseModel):
    constraint: str
    policy: Policy = Policy.ABORT
    version: int | None = None
    timeout: float | None = Field(default=None, gt=0)


def _enum_value(obj: Any) -> Any:
    if hasattr(obj, "value"):
        return obj.value
    return obj


def _feedback_payload(fb) -> dict:
    data = asdict(fb)
    data["status"] = fb.status.value
    if fb.violation is not None:
        data["violation"]["code"] = fb.violation.code.value
    return data


def _snapshot_payload(snap) -> dict:
    data = asdict(snap)
    data["policy"] = snap.policy.value
    data["status"] = snap.status.value
    if snap.violation is not None:
        data["violation"]["code"] = snap.violation.code.value
    return data


def create_app(
    manager: SessionManager | None = None,
    registry: ConstraintRegistry | None = None,
) -> FastAPI:
    registry = registry or ConstraintRegistry()
    manager = manager or SessionManager(registry)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        manager.shutdown()

    app = FastAPI(
        title="Constrained Stream Validator",
        version="0.1.0",
        description="Online validation and repair for model output streams.",
        lifespan=lifespan,
    )

    @app.exception_handler(ConstraintNotFoundError)
    async def _not_found(_request, exc):  # noqa: ANN001
        return JSONResponse(
            status_code=404,
            content={"error": {"code": ErrorCode.CONSTRAINT_NOT_FOUND.value,
                               "message": str(exc)}},
        )

    @app.exception_handler(ConcurrencyLimitError)
    async def _busy(_request, exc):  # noqa: ANN001
        return JSONResponse(
            status_code=503,
            content={"error": {"code": ErrorCode.CONCURRENCY_LIMIT.value,
                               "message": str(exc)}},
        )

    @app.exception_handler(ValueError)
    async def _bad(_request, exc):  # noqa: ANN001
        return JSONResponse(
            status_code=400,
            content={"error": {"code": ErrorCode.BAD_REQUEST.value,
                               "message": str(exc)}},
        )

    @app.get("/health")
    async def health():
        return {"ok": True}

    # -- constraints --------------------------------------------------------
    @app.put("/constraints/{name}")
    async def put_constraint(name: str, body: ConstraintIn):
        spec = {"type": body.type, "fatal": body.fatal, **body.config}
        # version is assigned by the registry; build with 0 placeholder first.
        constraint = build_constraint(name, 0, spec)
        version = registry.register(name, constraint)
        return {"name": name, "version": version,
                "constraint": constraint.to_dict()}

    @app.get("/constraints")
    async def list_constraints():
        return registry.describe()

    @app.get("/constraints/{name}")
    async def get_constraint(name: str, version: int | None = None):
        c = registry.get(name, version)
        return c.to_dict()

    @app.delete("/constraints/{name}")
    async def delete_constraint(name: str):
        registry.remove(name)
        return {"deleted": name}

    # -- sessions -----------------------------------------------------------
    @app.post("/sessions", status_code=201)
    async def create_session(body: CreateSessionIn):
        snap = manager.create_session(
            body.constraint, body.policy,
            version=body.version, timeout=body.timeout)
        return _snapshot_payload(snap)

    @app.post("/sessions/{session_id}/chunks")
    async def feed_chunk(session_id: str, body: FeedIn):
        fb = manager.feed(session_id, body.seq, body.text, final=body.final)
        code = 200
        if fb.violation is not None:
            code = _HTTP_STATUS.get(fb.violation.code, 422)
        return JSONResponse(status_code=code, content=_feedback_payload(fb))

    @app.post("/sessions/{session_id}/cancel")
    async def cancel_session(session_id: str):
        ok = manager.cancel(session_id)
        if not ok:
            return JSONResponse(
                status_code=404,
                content={"error": {"code": ErrorCode.SESSION_NOT_FOUND.value,
                                   "message": f"unknown or closed session {session_id!r}"}})
        return _snapshot_payload(manager.get(session_id))

    @app.get("/sessions/{session_id}")
    async def get_session(session_id: str):
        try:
            snap = manager.get(session_id)
        except KeyError:
            return JSONResponse(
                status_code=404,
                content={"error": {"code": ErrorCode.SESSION_NOT_FOUND.value,
                                   "message": f"unknown session {session_id!r}"}})
        return _snapshot_payload(snap)

    @app.get("/sessions")
    async def list_sessions(include_closed: bool = False):
        return {"sessions": [_snapshot_payload(s)
                             for s in manager.list_sessions(include_closed=include_closed)]}

    @app.get("/stats")
    async def stats():
        return asdict(manager.stats())

    # expose for tests / lifecycle hooks
    app.state.manager = manager
    app.state.registry = registry
    return app
