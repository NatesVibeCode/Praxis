"""Runtime helper for canonical system-event writes.

`emit_system_event` is the single runtime wrapper around the storage
authority. Production call sites are expected to pass the canonical
keyword-only envelope fields: `event_type`, `source_id`, `source_type`,
and `payload`.
"""

from __future__ import annotations

from typing import Any

from storage.postgres.workflow_runtime_repository import record_system_event

SYSTEM_EVENT_ENVELOPE_FIELDS = (
    "event_type",
    "source_id",
    "source_type",
    "payload",
)
SYSTEM_EVENT_SIGNATURE_FIELDS = ("conn", *SYSTEM_EVENT_ENVELOPE_FIELDS)


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


__all__ = [
    "SYSTEM_EVENT_ENVELOPE_FIELDS",
    "SYSTEM_EVENT_SIGNATURE_FIELDS",
    "emit_system_event",
]
