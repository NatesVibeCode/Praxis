"""SSE helpers for /api/shell/state/stream.

Streams session-scoped shell-navigation events from ``authority_events`` to
the React shell. The client-side ``useShellState`` hook consumes the stream
to keep its local ``ShellState`` in sync with the projection.

Pattern mirrors ``atlas_graph_stream`` in ``rest.py``: outbox-style tail with
cursor + keepalive. The ``@app.get`` route registration lives in ``rest.py``
and calls ``stream_shell_state(request, session, ...)`` from this module.

Anchored to decision.shell_navigation_cqrs.20260426.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

from fastapi.encoders import jsonable_encoder


SHELL_NAVIGATION_EVENT_TYPES = (
    "session.bootstrapped",
    "surface.opened",
    "tab.closed",
    "history.popped",
    "draft.guard.consulted",
)

_KEEPALIVE_INTERVAL_S = 25.0
_POLL_SLEEP_S = 0.5


def _cursor_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _cursor_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _event_payload_dict(value: object) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = {}
    return dict(value) if isinstance(value, dict) else {}


def _row_to_envelope(row: Any) -> dict[str, Any]:
    get = (lambda key: row.get(key)) if isinstance(row, dict) else (lambda key: row[key])
    payload = _event_payload_dict(get("event_payload"))
    return {
        "event_id": str(get("event_id")) if get("event_id") is not None else None,
        "event_type": get("event_type"),
        "authority_domain_ref": get("authority_domain_ref"),
        "operation_ref": get("operation_ref"),
        "receipt_id": str(get("receipt_id")) if get("receipt_id") is not None else None,
        "emitted_at": _cursor_value(get("emitted_at")),
        "payload": payload,
    }


def _fetch_events(conn: Any, *, session: str, after: str | None, limit: int = 50) -> list[Any]:
    return conn.fetch(
        """
        SELECT
            event_id,
            event_type,
            authority_domain_ref,
            operation_ref,
            receipt_id,
            emitted_at,
            event_payload
          FROM authority_events
         WHERE event_type = ANY($1::text[])
           AND event_payload->>'session_aggregate_ref' = $2
           AND ($3::timestamptz IS NULL OR emitted_at > $3::timestamptz)
         ORDER BY emitted_at ASC, event_id ASC
         LIMIT $4
        """,
        list(SHELL_NAVIGATION_EVENT_TYPES),
        session,
        _cursor_datetime(after),
        limit,
    )


def _latest_cursor(conn: Any, *, session: str) -> str | None:
    return _cursor_value(
        conn.fetchval(
            """
            SELECT MAX(emitted_at)
              FROM authority_events
             WHERE event_type = ANY($1::text[])
               AND event_payload->>'session_aggregate_ref' = $2
            """,
            list(SHELL_NAVIGATION_EVENT_TYPES),
            session,
        )
    )


def stream_shell_state_events(subsystems: Any, *, session: str, after: str | None):
    """Generator yielding SSE-encoded shell-navigation events.

    Caller wraps in ``StreamingResponse(..., media_type='text/event-stream')``.
    Empty session string yields no events; the caller should validate input.
    """
    if not session:
        return

    conn = subsystems.get_pg_conn()
    cursor = _cursor_value(after)
    if cursor is None:
        cursor = _latest_cursor(conn, session=session)

    last_keepalive = time.monotonic()
    while True:
        rows = _fetch_events(conn, session=session, after=cursor)
        if rows:
            for row in rows:
                envelope = _row_to_envelope(row)
                cursor = envelope.get("emitted_at") or cursor
                event_id = envelope.get("event_id") or cursor or ""
                yield f"id: {event_id}\n"
                yield f"data: {json.dumps(jsonable_encoder(envelope), sort_keys=True)}\n\n"
            last_keepalive = time.monotonic()
            continue

        now = time.monotonic()
        if now - last_keepalive >= _KEEPALIVE_INTERVAL_S:
            yield ": keepalive\n\n"
            last_keepalive = now
        time.sleep(_POLL_SLEEP_S)


__all__ = ["SHELL_NAVIGATION_EVENT_TYPES", "stream_shell_state_events"]
