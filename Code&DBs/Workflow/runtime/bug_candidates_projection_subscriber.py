"""Cursor-consumed subscriber on ``CHANNEL_RECEIPT`` for failure candidates.

Receipts land on the event log via
:mod:`storage.postgres.receipt_repository`.  This subscriber tracks the
failure-receipt subset (``status == 'failed'`` or non-empty
``failure_code``), groups them by ``(failure_code, node_id)`` and emits a
``bug_candidates_projection_refreshed`` event summarising what was
observed in each batch.  A downstream bug-filing subscriber can consume
those summary events to decide whether to open a bug, without re-reading
the full receipt stream.

The subscriber carries its own cursor, independent of the receipt
repository.  Emitting the summary event back on ``CHANNEL_RECEIPT``
keeps all receipt-derived work on one channel for replay.
"""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from storage.postgres import connect_workflow_database

from .event_log import (
    CHANNEL_RECEIPT,
    aadvance_cursor,
    aemit,
    aget_cursor,
    aread_since,
)


_DEFAULT_SUBSCRIBER_ID = "bug_candidates_refresher"
DEFAULT_SUBSCRIBER_ID = _DEFAULT_SUBSCRIBER_ID
BUG_CANDIDATES_PROJECTION_ID = "bug_candidates_current"
_RELEVANT_EVENT_TYPE = "receipt_recorded"


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


def _is_failure_receipt_event(event: Any) -> bool:
    if event.event_type != _RELEVANT_EVENT_TYPE:
        return False
    payload = event.payload or {}
    status = str(payload.get("status") or "").strip().lower()
    failure_code = str(payload.get("failure_code") or "").strip()
    return status == "failed" or bool(failure_code)


def _candidate_summary(events: tuple[Any, ...]) -> dict[str, list[dict[str, Any]]]:
    by_signature: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for event in events:
        payload = event.payload or {}
        failure_code = str(payload.get("failure_code") or "").strip() or "unknown"
        node_id = str(payload.get("node_id") or "").strip() or "unknown"
        signature = (failure_code, node_id)
        candidate = {
            "receipt_id": str(payload.get("receipt_id") or ""),
            "run_id": str(payload.get("run_id") or ""),
            "evidence_seq": payload.get("evidence_seq"),
        }
        by_signature.setdefault(signature, []).append(candidate)
    return {
        f"{code}:{node}": candidates
        for (code, node), candidates in by_signature.items()
    }


@dataclass(frozen=True, slots=True)
class BugCandidatesRefreshResult:
    subscriber_id: str
    channel: str
    starting_cursor: int
    ending_cursor: int
    scanned_count: int
    relevant_count: int
    refreshed: bool
    projection_as_of: datetime | None
    candidate_summary: dict[str, list[dict[str, Any]]]
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
            "candidate_summary": {
                key: [dict(candidate) for candidate in candidates]
                for key, candidates in self.candidate_summary.items()
            },
            "projection_event_id": self.projection_event_id,
        }


@dataclass(slots=True)
class BugCandidatesProjectionSubscriber:
    """Event-log subscriber that groups failed receipts for downstream bug filing."""

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
                    channel=CHANNEL_RECEIPT,
                )
                events = await aread_since(
                    conn,
                    channel=CHANNEL_RECEIPT,
                    cursor=starting_cursor,
                    limit=normalized_limit,
                )
                relevant_events = tuple(
                    event for event in events if _is_failure_receipt_event(event)
                )
                candidate_summary = _candidate_summary(relevant_events)
                projection_event_id: int | None = None
                refreshed = False
                if relevant_events:
                    projection_event_id = await aemit(
                        conn,
                        channel=CHANNEL_RECEIPT,
                        event_type="bug_candidates_projection_refreshed",
                        entity_id=BUG_CANDIDATES_PROJECTION_ID,
                        entity_kind="bug_candidates_projection",
                        payload={
                            "projection_name": BUG_CANDIDATES_PROJECTION_ID,
                            "as_of": projection_as_of.isoformat(),
                            "relevant_count": len(relevant_events),
                            "candidate_summary": candidate_summary,
                            "subscriber_id": subscriber_id,
                        },
                        emitted_by="bug_candidates_projection_subscriber.consume",
                    )
                    refreshed = True
                ending_cursor = starting_cursor
                if events:
                    ending_cursor = events[-1].id
                    await aadvance_cursor(
                        conn,
                        subscriber_id=subscriber_id,
                        channel=CHANNEL_RECEIPT,
                        event_id=ending_cursor,
                    )
        finally:
            await conn.close()
        return BugCandidatesRefreshResult(
            subscriber_id=subscriber_id,
            channel=CHANNEL_RECEIPT,
            starting_cursor=starting_cursor,
            ending_cursor=ending_cursor,
            scanned_count=len(events),
            relevant_count=len(relevant_events),
            refreshed=refreshed,
            projection_as_of=projection_as_of if refreshed else None,
            candidate_summary=candidate_summary,
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
        return run_sync_safe(
            self.consume_available_async(
                limit=limit,
                subscriber_id=subscriber_id,
                as_of=as_of,
                env=env,
            )
        )


def consume_bug_candidates_projection_events(
    *,
    limit: int = 100,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    as_of: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return BugCandidatesProjectionSubscriber().consume_available(
        limit=limit,
        subscriber_id=subscriber_id,
        as_of=as_of,
        env=env,
    )


async def aconsume_bug_candidates_projection_events(
    *,
    limit: int = 100,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    as_of: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return await BugCandidatesProjectionSubscriber().consume_available_async(
        limit=limit,
        subscriber_id=subscriber_id,
        as_of=as_of,
        env=env,
    )


async def sample_bug_candidates_projection_freshness(
    conn: Any,
    *,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    observed_at: datetime | None = None,
) -> Any:
    """Measure how far the bug_candidates subscriber trails behind CHANNEL_RECEIPT."""

    from .projection_freshness import sample_event_log_cursor_freshness

    return await sample_event_log_cursor_freshness(
        conn,
        channel=CHANNEL_RECEIPT,
        subscriber_id=subscriber_id,
        projection_id=BUG_CANDIDATES_PROJECTION_ID,
        observed_at=observed_at,
    )


__all__ = [
    "BUG_CANDIDATES_PROJECTION_ID",
    "BugCandidatesProjectionSubscriber",
    "BugCandidatesRefreshResult",
    "DEFAULT_SUBSCRIBER_ID",
    "aconsume_bug_candidates_projection_events",
    "consume_bug_candidates_projection_events",
    "sample_bug_candidates_projection_freshness",
]
