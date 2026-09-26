"""Service entry point: ``uvicorn main:app`` / ``python main.py``."""

import os

from stream_validator.manager import SessionManager
from stream_validator.registry import ConstraintRegistry
from stream_validator.service import create_app

registry = ConstraintRegistry()
manager = SessionManager(
    registry,
    max_active_sessions=int(os.getenv("CSV_MAX_ACTIVE_SESSIONS", "10000")),
    default_max_length=(
        int(os.getenv("CSV_MAX_LENGTH")) if os.getenv("CSV_MAX_LENGTH") else None
    ),
    default_max_duration_seconds=(
        float(os.getenv("CSV_MAX_DURATION_SECONDS"))
        if os.getenv("CSV_MAX_DURATION_SECONDS")
        else None
    ),
)

app = create_app(manager=manager, registry=registry)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
