"""Second subscriber on ``CHANNEL_SEMANTIC_ASSERTION`` for operator-decision bridge events.

Where :mod:`semantic_projection_subscriber` tracks the assertion read
model, this subscriber tracks operator decisions as a distinct domain.
It reads the same channel, filters for the ``operator_decisions`` bridge
source, and maintains an independent cursor. The two subscribers co-exist
without coordination — that is the point of the cursor-per-subscriber
pattern.

No new table, no new migration. The subscriber's output is a consumed
summary (count, scopes seen, latest decision_id per scope) committed as
an explicit ``operator_decision_projection_refreshed`` event so that
downstream tooling can replay subscriber activity from the log.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from storage.postgres import connect_workflow_database

from .event_log import (
    CHANNEL_SEMANTIC_ASSERTION,
    aadvance_cursor,
    aemit,
    aget_cursor,
    aread_since,
)


_DEFAULT_SUBSCRIBER_ID = "operator_decision_projection_refresher"
DEFAULT_SUBSCRIBER_ID = _DEFAULT_SUBSCRIBER_ID
OPERATOR_DECISION_PROJECTION_ID = "operator_decisions_current"
BRIDGE_SOURCE_OPERATOR_DECISIONS = "operator_decisions"
_RELEVANT_EVENT_TYPES = frozenset(
    {
        "semantic_assertion_recorded",
        "semantic_assertion_retracted",
    }
)


class _Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str: ...

    async def fetch(self, query: str, *args: object) -> list[Any]: ...

    async def fetchrow(self, query: str, *args: object) -> Any: ...

    def transaction(self) -> object: ...

    async def close(self) -> None: ...


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("as_of must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    return value.astimezone(timezone.utc)


def _is_operator_decision_bridge_event(event: Any) -> bool:
    if event.event_type not in _RELEVANT_EVENT_TYPES:
        return False
    payload = event.payload or {}
    return payload.get("bridge_source") == BRIDGE_SOURCE_OPERATOR_DECISIONS


def _scope_summary(events: tuple[Any, ...]) -> dict[str, Any]:
    latest_by_scope: dict[tuple[str, str], str] = {}
    for event in events:
        payload = event.payload or {}
        assertion = payload.get("semantic_assertion") or {}
        subject_kind = assertion.get("subject_kind")
        subject_ref = assertion.get("subject_ref")
        object_ref = assertion.get("object_ref")
        if not (subject_kind and subject_ref and object_ref):
            continue
        latest_by_scope[(str(subject_kind), str(subject_ref))] = str(object_ref)
    return {
        f"{scope_kind}:{scope_ref}": decision_id
        for (scope_kind, scope_ref), decision_id in latest_by_scope.items()
    }


@dataclass(frozen=True, slots=True)
class OperatorDecisionProjectionRefreshResult:
    subscriber_id: str
    channel: str
    starting_cursor: int
    ending_cursor: int
    scanned_count: int
    relevant_count: int
    refreshed: bool
    projection_as_of: datetime | None
    scope_summary: dict[str, str]
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
            "scope_summary": dict(self.scope_summary),
            "projection_event_id": self.projection_event_id,
        }


@dataclass(slots=True)
class OperatorDecisionProjectionSubscriber:
    """Durable event-log subscriber that tracks operator-decision bridge events."""

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
                    event for event in events if _is_operator_decision_bridge_event(event)
                )
                scope_summary = _scope_summary(relevant_events)
                projection_event_id: int | None = None
                refreshed = False
                if relevant_events:
                    projection_event_id = await aemit(
                        conn,
                        channel=CHANNEL_SEMANTIC_ASSERTION,
                        event_type="operator_decision_projection_refreshed",
                        entity_id=OPERATOR_DECISION_PROJECTION_ID,
                        entity_kind="operator_decision_projection",
                        payload={
                            "projection_name": OPERATOR_DECISION_PROJECTION_ID,
                            "as_of": projection_as_of.isoformat(),
                            "relevant_count": len(relevant_events),
                            "scope_summary": scope_summary,
                            "subscriber_id": subscriber_id,
                        },
                        emitted_by="operator_decision_projection_subscriber.consume",
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
        return OperatorDecisionProjectionRefreshResult(
            subscriber_id=subscriber_id,
            channel=CHANNEL_SEMANTIC_ASSERTION,
            starting_cursor=starting_cursor,
            ending_cursor=ending_cursor,
            scanned_count=len(events),
            relevant_count=len(relevant_events),
            refreshed=refreshed,
            projection_as_of=projection_as_of if refreshed else None,
            scope_summary=scope_summary,
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
            "operator decision projection sync subscriber requires a non-async call boundary"
        )


def consume_operator_decision_projection_events(
    *,
    limit: int = 100,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    as_of: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return OperatorDecisionProjectionSubscriber().consume_available(
        limit=limit,
        subscriber_id=subscriber_id,
        as_of=as_of,
        env=env,
    )


async def aconsume_operator_decision_projection_events(
    *,
    limit: int = 100,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    as_of: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return await OperatorDecisionProjectionSubscriber().consume_available_async(
        limit=limit,
        subscriber_id=subscriber_id,
        as_of=as_of,
        env=env,
    )


async def sample_operator_decision_projection_freshness(
    conn: Any,
    *,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    observed_at: datetime | None = None,
) -> Any:
    """Measure how far the operator_decisions_current subscriber trails."""

    from .projection_freshness import sample_event_log_cursor_freshness

    return await sample_event_log_cursor_freshness(
        conn,
        channel=CHANNEL_SEMANTIC_ASSERTION,
        subscriber_id=subscriber_id,
        projection_id=OPERATOR_DECISION_PROJECTION_ID,
        observed_at=observed_at,
    )


__all__ = [
    "BRIDGE_SOURCE_OPERATOR_DECISIONS",
    "DEFAULT_SUBSCRIBER_ID",
    "OPERATOR_DECISION_PROJECTION_ID",
    "OperatorDecisionProjectionRefreshResult",
    "OperatorDecisionProjectionSubscriber",
    "aconsume_operator_decision_projection_events",
    "consume_operator_decision_projection_events",
    "sample_operator_decision_projection_freshness",
]
