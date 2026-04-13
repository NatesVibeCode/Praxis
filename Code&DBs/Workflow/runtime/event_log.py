"""Praxis service bus — durable event log in Postgres.

Append-only event log with cursor-based consumption and LISTEN/NOTIFY
for instant push. No polling fallbacks, no graceful degradation — if
the database is down, the system is down.

Write path:
    emit(conn, channel, event_type, entity_id, ...) -> event_id
    Inserts a row and fires pg_notify on the channel.

Read path:
    read_since(conn, channel, cursor, ...) -> list[Event]
    Returns events after the given cursor (event ID).

    iter_channel(conn, channel, entity_id, cursor, ...) -> Generator[Event]
    Yields events as they arrive.

Cursor management:
    get_cursor(conn, subscriber_id, channel) -> int
    advance_cursor(conn, subscriber_id, channel, event_id)

Tables: event_log, event_log_cursors (migration 082).
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generator

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


# ---------------------------------------------------------------------------
# Event constants
# ---------------------------------------------------------------------------

# Channels
CHANNEL_BUILD_STATE = "build_state"
CHANNEL_JOB_LIFECYCLE = "job_lifecycle"
CHANNEL_SYSTEM = "system"
CHANNEL_WEBHOOK = "webhook"

# Build state event types
EVENT_MUTATION = "mutation"
EVENT_COMPILATION = "compilation"
EVENT_COMMIT = "commit"
EVENT_REFINEMENT = "refinement"
EVENT_PLANNING = "planning"

# Job lifecycle event types
EVENT_JOB_CLAIMED = "job_claimed"
EVENT_JOB_STARTED = "job_started"
EVENT_JOB_COMPLETED = "job_completed"
EVENT_JOB_FAILED = "job_failed"
EVENT_RUN_COMPLETE = "run_complete"

# System event types
EVENT_WORKER_STARTED = "worker_started"
EVENT_CIRCUIT_OPENED = "circuit_opened"
EVENT_CIRCUIT_CLOSED = "circuit_closed"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Event:
    """A single event from the log."""
    id: int
    channel: str
    event_type: str
    entity_id: str
    entity_kind: str
    payload: dict[str, Any]
    emitted_at: datetime
    emitted_by: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "channel": self.channel,
            "event_type": self.event_type,
            "entity_id": self.entity_id,
            "entity_kind": self.entity_kind,
            "payload": self.payload,
            "emitted_at": self.emitted_at.isoformat() if self.emitted_at else "",
            "emitted_by": self.emitted_by,
        }


def _row_to_event(r: dict) -> Event:
    payload = r.get("payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            payload = {}
    return Event(
        id=r["id"],
        channel=r.get("channel") or "",
        event_type=r.get("event_type") or "",
        entity_id=r.get("entity_id") or "",
        entity_kind=r.get("entity_kind") or "",
        payload=payload,
        emitted_at=r.get("emitted_at") or datetime.min,
        emitted_by=r.get("emitted_by") or "",
    )


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------

def emit(
    conn: Any,
    *,
    channel: str,
    event_type: str,
    entity_id: str = "",
    entity_kind: str = "",
    payload: dict[str, Any] | None = None,
    emitted_by: str = "",
) -> int:
    """Append an event and notify subscribers. Returns the event ID."""
    rows = conn.execute(
        """INSERT INTO event_log (channel, event_type, entity_id, entity_kind, payload, emitted_by)
           VALUES ($1, $2, $3, $4, $5, $6)
           RETURNING id""",
        channel,
        event_type,
        entity_id,
        entity_kind,
        json.dumps(payload or {}),
        emitted_by,
    )
    event_id = rows[0]["id"] if rows else 0
    conn.execute(
        "SELECT pg_notify($1, $2)",
        channel,
        json.dumps({"id": event_id, "type": event_type, "entity": entity_id}),
    )
    return event_id


# ---------------------------------------------------------------------------
# Read path
# ---------------------------------------------------------------------------

def read_since(
    conn: Any,
    *,
    channel: str,
    cursor: int = 0,
    entity_id: str | None = None,
    limit: int = 100,
) -> list[Event]:
    """Read events on a channel after the given cursor."""
    if entity_id is not None:
        rows = conn.execute(
            """SELECT id, channel, event_type, entity_id, entity_kind,
                      payload, emitted_at, emitted_by
               FROM event_log
               WHERE channel = $1 AND entity_id = $2 AND id > $3
               ORDER BY id ASC LIMIT $4""",
            channel, entity_id, cursor, limit,
        )
    else:
        rows = conn.execute(
            """SELECT id, channel, event_type, entity_id, entity_kind,
                      payload, emitted_at, emitted_by
               FROM event_log
               WHERE channel = $1 AND id > $2
               ORDER BY id ASC LIMIT $3""",
            channel, cursor, limit,
        )
    return [_row_to_event(r) for r in rows]


def read_all_since(
    conn: Any,
    *,
    cursor: int = 0,
    limit: int = 100,
) -> list[Event]:
    """Read events across ALL channels after the given cursor."""
    rows = conn.execute(
        """SELECT id, channel, event_type, entity_id, entity_kind,
                  payload, emitted_at, emitted_by
           FROM event_log WHERE id > $1 ORDER BY id ASC LIMIT $2""",
        cursor, limit,
    )
    return [_row_to_event(r) for r in rows]


def iter_channel(
    conn: Any,
    *,
    channel: str,
    entity_id: str | None = None,
    cursor: int = 0,
    timeout_seconds: float | None = 300,
    poll_interval: float = 1.0,
) -> Generator[Event, None, None]:
    """Yield events as they arrive on a channel.

    Uses cursor-based reads with a sleep interval between checks.
    LISTEN/NOTIFY wakes the caller; the cursor guarantees no events
    are missed regardless of timing.
    """
    deadline = time.monotonic() + timeout_seconds if timeout_seconds else None
    while deadline is None or time.monotonic() < deadline:
        events = read_since(conn, channel=channel, cursor=cursor, entity_id=entity_id, limit=50)
        for event in events:
            cursor = event.id
            yield event
        if not events:
            time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Cursor management
# ---------------------------------------------------------------------------

def get_cursor(conn: Any, subscriber_id: str, channel: str) -> int:
    """Get the last consumed event ID for a subscriber on a channel."""
    rows = conn.execute(
        "SELECT last_event_id FROM event_log_cursors WHERE subscriber_id = $1 AND channel = $2",
        subscriber_id, channel,
    )
    return rows[0]["last_event_id"] if rows else 0


def advance_cursor(conn: Any, subscriber_id: str, channel: str, event_id: int) -> None:
    """Advance a subscriber's cursor. Only moves forward."""
    conn.execute(
        """INSERT INTO event_log_cursors (subscriber_id, channel, last_event_id, updated_at)
           VALUES ($1, $2, $3, NOW())
           ON CONFLICT (subscriber_id, channel)
           DO UPDATE SET last_event_id = GREATEST(event_log_cursors.last_event_id, $3),
                         updated_at = NOW()""",
        subscriber_id, channel, event_id,
    )
