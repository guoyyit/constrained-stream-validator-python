"""FastAPI HTTP service for streaming validation.

Endpoints
---------
POST /constraints/{name}
    Register (publish a new immutable version of) a named constraint.
GET  /constraints
    List registered constraints and their latest versions.
POST /sessions
    Open a validation session; pins the named constraint at a chosen version.
POST /sessions/{id}/chunks
    Submit a (possibly out-of-order / duplicated) stream chunk.
POST /sessions/{id}/cancel
    Cancel; later chunks are discarded.
POST /sessions/{id}/finish
    Close the stream and obtain the final result.
GET  /sessions/{id}
    Current result snapshot.
GET  /sessions/{id}/stats
    Per-session statistics.
GET  /stats
    Global manager statistics.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .manager import SessionManager
from .models import (
    ConstraintSpec,
    ConstraintType,
    ErrorCode,
    SessionStatus,
    StreamChunk,
    ValidationError,
    ViolationPolicy,
)
from .registry import ConstraintRegistry

_CONFLICT_CODES = frozenset(
    {
        ErrorCode.CONSTRAINT_VIOLATION,
        ErrorCode.LENGTH_LIMIT_EXCEEDED,
        ErrorCode.TIME_LIMIT_EXCEEDED,
        ErrorCode.CANCELLED,
    }
)


def _error_response(exc: ValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=409 if exc.code in _CONFLICT_CODES else 400,
        content={
            "error_code": exc.code.value,
            "message": exc.message,
            "position": exc.position,
            "consumed_prefix": exc.consumed_prefix,
        },
    )


def _constraint_spec_from_body(body: dict[str, Any]) -> ConstraintSpec:
    try:
        ctype = ConstraintType(body["type"])
    except (KeyError, ValueError) as exc:
        raise ValidationError(
            code=ErrorCode.CONSTRAINT_TYPE_UNKNOWN,
            message=f"unknown or missing constraint type: {body.get('type')!r}",
        ) from exc
    params = body.get("params", {})
    if not isinstance(params, dict):
        raise ValidationError(
            code=ErrorCode.INVALID_CHUNK,
            message="params must be an object",
        )
    return ConstraintSpec(type=ctype, params=params)


def create_app(
    manager: SessionManager | None = None,
    registry: ConstraintRegistry | None = None,
    default_max_length: int | None = None,
    default_max_duration_seconds: float | None = None,
    reaper_interval_seconds: float | None = None,
) -> FastAPI:
    registry = registry or ConstraintRegistry()
    manager = manager or SessionManager(
        registry,
        default_max_length=default_max_length,
        default_max_duration_seconds=default_max_duration_seconds,
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        task: asyncio.Task | None = None
        if reaper_interval_seconds is not None:

            async def sweep_loop() -> None:
                while True:
                    await asyncio.sleep(reaper_interval_seconds)
                    await asyncio.get_running_loop().run_in_executor(
                        None, manager.reaper_sweep
                    )

            task = asyncio.create_task(sweep_loop())
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    app = FastAPI(title="Constrained Stream Validator", version="0.1.0", lifespan=lifespan)
    app.state.registry = registry
    app.state.manager = manager

    async def validation_error_handler(request: Request, exc: ValidationError):
        return _error_response(exc)

    app.add_exception_handler(ValidationError, validation_error_handler)

    # -- constraints --------------------------------------------------------

    @app.post("/constraints/{name}")
    async def register_constraint(name: str, body: dict[str, Any]):
        spec = _constraint_spec_from_body(body)
        version = registry.register(name, spec)
        return {"name": name, "version": version.version}

    @app.get("/constraints")
    async def list_constraints():
        return {
            "constraints": [
                {"name": n, "latest_version": registry.latest_version(n)}
                for n in registry.names()
            ]
        }

    # -- sessions -----------------------------------------------------------

    @app.post("/sessions", status_code=201)
    async def create_session(body: dict[str, Any]):
        if not isinstance(body.get("constraint"), str):
            raise ValidationError(
                code=ErrorCode.INVALID_CHUNK,
                message="session body requires string 'constraint'",
            )
        try:
            policy = ViolationPolicy(body.get("policy", "abort"))
        except ValueError as exc:
            raise ValidationError(
                code=ErrorCode.INVALID_CHUNK,
                message=f"unknown policy {body.get('policy')!r}",
            ) from exc
        sid = manager.create_session(
            constraint_name=body["constraint"],
            policy=policy,
            constraint_version=body.get("constraint_version"),
            max_length=body.get("max_length"),
            max_duration_seconds=body.get("max_duration_seconds"),
        )
        session = manager.get_session(sid)
        return {
            "session_id": sid,
            "constraint": session.constraint_name,
            "constraint_version": session.constraint_version,
            "policy": policy.value,
        }

    @app.post("/sessions/{session_id}/chunks")
    async def ingest_chunk(session_id: str, body: dict[str, Any]):
        if "text" not in body or not isinstance(body["text"], str):
            raise ValidationError(
                code=ErrorCode.INVALID_CHUNK,
                message="chunk body requires string 'text'",
            )
        chunk = StreamChunk(
            seq=int(body.get("seq", 0)),
            text=body["text"],
            final=bool(body.get("final", False)),
        )
        event = manager.ingest(session_id, chunk)
        payload: dict[str, Any] = {
            "seq": event.seq,
            "accepted_text": event.accepted_text,
            "rejected_text": event.rejected_text,
            "position": event.position,
            "status": manager.get_session(session_id).status.value,
        }
        if event.error is not None:
            payload["error"] = {
                "code": event.error.code.value,
                "message": event.error.message,
                "position": event.error.position,
                "consumed_prefix": event.error.consumed_prefix,
            }
            status_code = 409 if event.error.code in _CONFLICT_CODES else 400
            return JSONResponse(status_code=status_code, content=payload)
        return payload

    @app.post("/sessions/{session_id}/cancel", status_code=200)
    async def cancel_session(session_id: str):
        manager.cancel(session_id)
        return {"status": SessionStatus.CANCELLED.value}

    @app.post("/sessions/{session_id}/finish")
    async def finish_session(session_id: str):
        result = manager.finish(session_id)
        return {
            "status": result.status.value,
            "output": result.output,
            "error_code": result.error_code,
            "error_position": result.error_position,
            "consumed_prefix": result.consumed_prefix,
        }

    @app.get("/sessions/{session_id}")
    async def get_session(session_id: str):
        result = manager.get_result(session_id)
        return {
            "status": result.status.value,
            "output": result.output,
            "error_code": result.error_code,
            "error_position": result.error_position,
        }

    @app.get("/sessions/{session_id}/stats")
    async def session_stats(session_id: str):
        stats = manager.get_stats(session_id)
        return stats.__dict__

    @app.get("/stats")
    async def global_stats():
        stats = manager.manager_stats()
        return {
            "active_sessions": stats.active_sessions,
            "total_sessions_created": stats.total_sessions_created,
            "total_sessions_finished": stats.total_sessions_finished,
            "total_violations": stats.total_violations,
            "total_chunks_received": stats.total_chunks_received,
            "total_bytes_in": stats.total_bytes_in,
            "total_bytes_out": stats.total_bytes_out,
        }

    # -- background reaper configured via lifespan above --------------------

    return app


app = create_app()
