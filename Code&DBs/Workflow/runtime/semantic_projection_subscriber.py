"""Cursor-based semantic projection refresh over the event log.

This subscriber treats event_log cursors as the durable replay contract and
semantic_current_assertions as a rebuildable read model. Notifications are not
truth. The truth is the semantic_assertions write model plus explicit cursors.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from storage.postgres import PostgresSemanticAssertionRepository, connect_workflow_database

from .event_log import (
    CHANNEL_SEMANTIC_ASSERTION,
    aadvance_cursor,
    aemit,
    aget_cursor,
    aread_since,
)

_DEFAULT_SUBSCRIBER_ID = "semantic_projection_refresher"
_RELEVANT_EVENT_TYPES = frozenset(
    {
        "semantic_assertion_recorded",
        "semantic_assertion_retracted",
        "semantic_bridge_backfilled",
    }
)


class _Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str:
        """Execute one statement."""

    async def fetch(self, query: str, *args: object) -> list[Any]:
        """Fetch rows."""

    async def fetchrow(self, query: str, *args: object) -> Any:
        """Fetch one row."""

    def transaction(self) -> object:
        """Open a transaction context."""

    async def close(self) -> None:
        """Close the connection."""


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("as_of must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    return value.astimezone(timezone.utc)


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class SemanticProjectionRefreshResult:
    subscriber_id: str
    channel: str
    starting_cursor: int
    ending_cursor: int
    scanned_count: int
    relevant_count: int
    refreshed: bool
    projection_as_of: datetime | None
    row_count: int | None
    projection_event_id: int | None

    def to_json(self) -> dict[str, Any]:
        return {
            "subscriber_id": self.subscriber_id,
            "channel": self.channel,
            "starting_cursor": self.starting_cursor,
            "ending_cursor": self.ending_cursor,
            "scanned_count": self.scanned_count,
            "relevant_count": self.relevant_count,
            "refreshed": self.refreshed,
            "projection_as_of": (
                None if self.projection_as_of is None else self.projection_as_of.isoformat()
            ),
            "row_count": self.row_count,
            "projection_event_id": self.projection_event_id,
        }


@dataclass(slots=True)
class SemanticProjectionSubscriber:
    """Durable event-log subscriber that refreshes semantic_current_assertions."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )

    async def consume_available_async(
        self,
        *,
        limit: int = 100,
        subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
        as_of: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_limit = max(1, int(limit or 100))
        projection_as_of = _now() if as_of is None else _normalize_as_of(as_of)
        conn = await self.connect_database(env)
        try:
            async with conn.transaction():
                starting_cursor = await aget_cursor(
                    conn,
                    subscriber_id=subscriber_id,
                    channel=CHANNEL_SEMANTIC_ASSERTION,
                )
                events = await aread_since(
                    conn,
                    channel=CHANNEL_SEMANTIC_ASSERTION,
                    cursor=starting_cursor,
                    limit=normalized_limit,
                )
                relevant_events = tuple(
                    event for event in events if event.event_type in _RELEVANT_EVENT_TYPES
                )
                refreshed = False
                row_count: int | None = None
                projection_event_id: int | None = None
                if relevant_events:
                    repository = PostgresSemanticAssertionRepository(conn)  # type: ignore[arg-type]
                    row_count = await repository.rebuild_current_assertions(
                        as_of=projection_as_of,
                    )
                    projection_event_id = await aemit(
                        conn,
                        channel=CHANNEL_SEMANTIC_ASSERTION,
                        event_type="semantic_projection_rebuilt",
                        entity_id="semantic_current_assertions",
                        entity_kind="semantic_projection",
                        payload={
                            "projection_name": "semantic_current_assertions",
                            "as_of": projection_as_of.isoformat(),
                            "row_count": row_count,
                            "subscriber_id": subscriber_id,
                        },
                        emitted_by="semantic_projection_subscriber.consume",
                    )
                    refreshed = True
                ending_cursor = starting_cursor
                if events:
                    ending_cursor = events[-1].id
                    await aadvance_cursor(
                        conn,
                        subscriber_id=subscriber_id,
                        channel=CHANNEL_SEMANTIC_ASSERTION,
                        event_id=ending_cursor,
                    )
        finally:
            await conn.close()
        return SemanticProjectionRefreshResult(
            subscriber_id=subscriber_id,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            starting_cursor=starting_cursor,
            ending_cursor=ending_cursor,
            scanned_count=len(events),
            relevant_count=len(relevant_events),
            refreshed=refreshed,
            projection_as_of=projection_as_of if refreshed else None,
            row_count=row_count,
            projection_event_id=projection_event_id,
        ).to_json()

    def consume_available(
        self,
        *,
        limit: int = 100,
        subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
        as_of: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.consume_available_async(
                    limit=limit,
                    subscriber_id=subscriber_id,
                    as_of=as_of,
                    env=env,
                )
            )
        raise RuntimeError(
            "semantic projection sync subscriber requires a non-async call boundary"
        )


def consume_semantic_projection_events(
    *,
    limit: int = 100,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    as_of: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return SemanticProjectionSubscriber().consume_available(
        limit=limit,
        subscriber_id=subscriber_id,
        as_of=as_of,
        env=env,
    )


async def aconsume_semantic_projection_events(
    *,
    limit: int = 100,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    as_of: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return await SemanticProjectionSubscriber().consume_available_async(
        limit=limit,
        subscriber_id=subscriber_id,
        as_of=as_of,
        env=env,
    )


__all__ = [
    "SemanticProjectionRefreshResult",
    "SemanticProjectionSubscriber",
    "aconsume_semantic_projection_events",
    "consume_semantic_projection_events",
]
