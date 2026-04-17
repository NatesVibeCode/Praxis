"""Runtime helper for canonical system-event writes."""

from __future__ import annotations

from typing import Any

from storage.postgres.workflow_runtime_repository import record_system_event


def emit_system_event(
    conn: Any,
    *,
    event_type: str,
    source_id: str,
    source_type: str,
    payload: dict[str, Any],
) -> None:
    """Route runtime system-event writes through one storage authority."""
    record_system_event(
        conn,
        event_type=event_type,
        source_id=source_id,
        source_type=source_type,
        payload=payload,
    )


__all__ = ["emit_system_event"]
