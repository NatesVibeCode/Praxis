"""Praxis service bus — durable event log in Postgres.

Append-only event log with cursor-based consumption and LISTEN/NOTIFY
for instant push. Cursor reads stay authoritative; LISTEN/NOTIFY is
the wakeup path that keeps stream consumers responsive without relying
on blind sleep intervals.

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
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generator

from ._workflow_database import resolve_runtime_database_url

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)


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
EVENT_REVIEW_DECISION = "review_decision"

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


class _ChannelWakeupListener:
    """Background LISTEN/NOTIFY consumer that wakes stream readers."""

    def __init__(
        self,
        *,
        database_url: str,
        channel: str,
        wakeup_event: threading.Event,
        reconnect_delay: float = 2.0,
    ) -> None:
        self._database_url = database_url
        self._channel = channel
        self._wakeup_event = wakeup_event
        self._reconnect_delay = reconnect_delay
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"event-log-listen-{channel}",
        )

    def start(self) -> None:
        import asyncpg  # noqa: F401

        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._wakeup_event.set()
        self._thread.join(timeout=3)

    def _on_notify(self, _connection, _pid, channel: str, payload: str) -> None:
        logger.debug("Event log notification received on %s: %s", channel, payload)
        self._wakeup_event.set()

    def _run(self) -> None:
        import asyncio
        import asyncpg

        async def _listen() -> None:
            while not self._stop_event.is_set():
                conn = None
                try:
                    conn = await asyncpg.connect(self._database_url, timeout=5.0)
                    await conn.add_listener(self._channel, self._on_notify)
                    while not self._stop_event.is_set():
                        await asyncio.sleep(1.0)
                except Exception as exc:
                    if not self._stop_event.is_set():
                        logger.warning(
                            "Event log LISTEN loop error for %s, reconnecting: %s",
                            self._channel,
                            exc,
                        )
                        await asyncio.sleep(self._reconnect_delay)
                finally:
                    if conn is not None:
                        await conn.close()

        asyncio.run(_listen())


def _start_channel_wakeup_listener(
    *,
    channel: str,
    wakeup_event: threading.Event,
) -> _ChannelWakeupListener | None:
    """Start a LISTEN wakeup helper for the requested channel when possible."""

    database_url = str(resolve_runtime_database_url(required=False) or "").strip()
    if not database_url:
        return None

    try:
        listener = _ChannelWakeupListener(
            database_url=database_url,
            channel=channel,
            wakeup_event=wakeup_event,
        )
        listener.start()
        return listener
    except Exception as exc:
        logger.debug(
            "Event log LISTEN wakeup unavailable for %s: %s",
            channel,
            exc,
        )
        return None


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

    Cursor reads remain the source of truth. When the workflow database
    authority is configured, a background LISTEN connection wakes the
    loop early so streams do not sit on fixed polling delays.
    """
    deadline = time.monotonic() + timeout_seconds if timeout_seconds else None
    wakeup_event = threading.Event()
    listener = _start_channel_wakeup_listener(
        channel=channel,
        wakeup_event=wakeup_event,
    )
    try:
        while deadline is None or time.monotonic() < deadline:
            events = read_since(conn, channel=channel, cursor=cursor, entity_id=entity_id, limit=50)
            for event in events:
                cursor = event.id
                yield event
            if not events:
                if listener is not None:
                    wakeup_event.wait(timeout=poll_interval)
                    wakeup_event.clear()
                else:
                    time.sleep(poll_interval)
    finally:
        if listener is not None:
            listener.stop()


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
