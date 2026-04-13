"""Worker subscription seam over committed workflow outbox facts.

This module derives worker-consumable inbox facts from the workflow outbox.
Acknowledgements are explicit watermark updates only. They do not mutate
runtime lifecycle truth, compile queue state, or introduce transport concerns.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from .domain import RuntimeBoundaryError
from .outbox import WorkflowOutboxBatch
from .subscription_repository import (
    EventSubscriptionCheckpoint,
    EventSubscriptionDefinition,
    EventSubscriptionRepository,
    subscription_checkpoint_id,
)


def _require_text(value: str | None, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeBoundaryError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_non_negative_int(value: int | None, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeBoundaryError(f"{field_name} must be a non-negative integer")
    return value


def _require_positive_int(value: int, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeBoundaryError(f"{field_name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class WorkerSubscriptionCursor:
    """Explicit subscriber watermark for one worker-facing run feed."""

    subscription_id: str
    run_id: str
    last_acked_evidence_seq: int | None = None


@dataclass(frozen=True, slots=True)
class WorkerInboxFact:
    """One worker-consumable inbox fact copied from committed outbox authority."""

    inbox_fact_id: str
    subscription_id: str
    authority_table: str
    authority_id: str
    envelope_kind: str
    workflow_id: str
    run_id: str
    request_id: str
    evidence_seq: int
    transition_seq: int
    authority_recorded_at: object
    envelope: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class WorkerSubscriptionBatch:
    """One explicit worker inbox read over committed outbox rows."""

    cursor: WorkerSubscriptionCursor
    next_cursor: WorkerSubscriptionCursor
    facts: tuple[WorkerInboxFact, ...]
    has_more: bool


@dataclass(frozen=True, slots=True)
class WorkerSubscriptionAcknowledgement:
    """Explicit acknowledgement for one worker subscription batch."""

    subscription_id: str
    run_id: str
    through_evidence_seq: int | None
    cursor: WorkerSubscriptionCursor


class WorkflowOutboxReader(Protocol):
    """Minimal outbox reader contract required by the worker subscription seam."""

    def read_batch(
        self,
        *,
        run_id: str,
        after_evidence_seq: int | None = None,
        limit: int = 100,
    ) -> WorkflowOutboxBatch:
        ...


    async def load_batch(
        self,
        *,
        run_id: str,
        after_evidence_seq: int | None = None,
        limit: int = 100,
    ) -> WorkflowOutboxBatch:
        ...


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _next_cursor(
    cursor: WorkerSubscriptionCursor,
    *,
    last_acked_evidence_seq: int | None,
) -> WorkerSubscriptionCursor:
    return WorkerSubscriptionCursor(
        subscription_id=cursor.subscription_id,
        run_id=cursor.run_id,
        last_acked_evidence_seq=last_acked_evidence_seq,
    )


def _fact_id(*, subscription_id: str, evidence_seq: int) -> str:
    return f"inbox:{subscription_id}:{evidence_seq}"


def _fact_from_outbox_row(
    *,
    cursor: WorkerSubscriptionCursor,
    row,
) -> WorkerInboxFact:
    if row.run_id != cursor.run_id:
        raise RuntimeBoundaryError("outbox row run_id drifted from the subscription cursor")
    return WorkerInboxFact(
        inbox_fact_id=_fact_id(
            subscription_id=cursor.subscription_id,
            evidence_seq=row.evidence_seq,
        ),
        subscription_id=cursor.subscription_id,
        authority_table=row.authority_table,
        authority_id=row.authority_id,
        envelope_kind=row.envelope_kind,
        workflow_id=row.workflow_id,
        run_id=row.run_id,
        request_id=row.request_id,
        evidence_seq=row.evidence_seq,
        transition_seq=row.transition_seq,
        authority_recorded_at=row.authority_recorded_at,
        envelope=row.envelope,
    )


def _subscription_batch_from_outbox(
    *,
    cursor: WorkerSubscriptionCursor,
    outbox_batch: WorkflowOutboxBatch,
) -> WorkerSubscriptionBatch:
    if outbox_batch.cursor.run_id != cursor.run_id:
        raise RuntimeBoundaryError("outbox cursor run_id drifted from the subscription cursor")
    facts = tuple(
        _fact_from_outbox_row(cursor=cursor, row=row) for row in outbox_batch.rows
    )
    return WorkerSubscriptionBatch(
        cursor=cursor,
        next_cursor=_next_cursor(
            cursor,
            last_acked_evidence_seq=outbox_batch.cursor.last_evidence_seq,
        ),
        facts=facts,
        has_more=outbox_batch.has_more,
    )


def _subscription_definition_compatible(
    *,
    definition: EventSubscriptionDefinition | None,
    cursor: WorkerSubscriptionCursor,
) -> None:
    if definition is None:
        return
    if definition.run_id is not None and definition.run_id != cursor.run_id:
        raise RuntimeBoundaryError(
            "subscription definition run_id drifted from the requested cursor",
        )


def _fact_for_evidence_seq(
    *,
    batch: WorkerSubscriptionBatch,
    evidence_seq: int | None,
):
    if evidence_seq is None:
        return None
    for fact in batch.facts:
        if fact.evidence_seq == evidence_seq:
            return fact
    return None


def _checkpoint_from_acknowledgement(
    *,
    batch: WorkerSubscriptionBatch,
    through_evidence_seq: int | None,
    current_seq: int | None,
    existing_checkpoint: EventSubscriptionCheckpoint | None = None,
) -> EventSubscriptionCheckpoint:
    ack_fact = _fact_for_evidence_seq(
        batch=batch,
        evidence_seq=through_evidence_seq,
    )
    last_authority_id = (
        ack_fact.authority_id
        if ack_fact is not None
        else existing_checkpoint.last_authority_id
        if existing_checkpoint is not None
        and existing_checkpoint.last_evidence_seq == through_evidence_seq
        else None
    )
    return EventSubscriptionCheckpoint(
        checkpoint_id=subscription_checkpoint_id(
            subscription_id=batch.cursor.subscription_id,
            run_id=batch.cursor.run_id,
        ),
        subscription_id=batch.cursor.subscription_id,
        run_id=batch.cursor.run_id,
        last_evidence_seq=through_evidence_seq,
        last_authority_id=last_authority_id,
        checkpoint_status="committed",
        checkpointed_at=_now_utc(),
        metadata={
            "batch_size": len(batch.facts),
            "current_evidence_seq": current_seq,
            "has_more": batch.has_more,
            "through_evidence_seq": through_evidence_seq,
        },
    )


@dataclass(frozen=True, slots=True)
class WorkflowWorkerSubscription:
    """Thin worker inbox seam over the canonical workflow outbox and checkpoint store."""

    subscriber: WorkflowOutboxReader
    repository: EventSubscriptionRepository

    async def _load_subscription_context(
        self,
        *,
        cursor: WorkerSubscriptionCursor,
    ) -> tuple[EventSubscriptionDefinition, WorkerSubscriptionCursor]:
        definition = await self.repository.load_definition(
            subscription_id=cursor.subscription_id,
        )
        if definition is None:
            raise RuntimeBoundaryError(
                "subscription definition is missing from durable storage",
            )
        _subscription_definition_compatible(
            definition=definition,
            cursor=cursor,
        )
        effective_cursor = cursor
        checkpoint = await self.repository.load_checkpoint(
            subscription_id=cursor.subscription_id,
            run_id=cursor.run_id,
        )
        if checkpoint is not None:
            effective_cursor = _next_cursor(
                cursor,
                last_acked_evidence_seq=checkpoint.last_evidence_seq,
            )
        return definition, effective_cursor

    def read_batch(
        self,
        *,
        cursor: WorkerSubscriptionCursor,
        limit: int = 100,
    ) -> WorkerSubscriptionBatch:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            normalized_cursor = WorkerSubscriptionCursor(
                subscription_id=_require_text(
                    cursor.subscription_id,
                    field_name="cursor.subscription_id",
                ),
                run_id=_require_text(cursor.run_id, field_name="cursor.run_id"),
                last_acked_evidence_seq=_optional_non_negative_int(
                    cursor.last_acked_evidence_seq,
                    field_name="cursor.last_acked_evidence_seq",
                ),
            )
            return asyncio.run(
                self.load_batch(
                    cursor=normalized_cursor,
                    limit=limit,
                )
            )
        raise RuntimeBoundaryError(
            "sync read_batch() requires an explicit non-async call boundary"
        )

    async def load_batch(
        self,
        *,
        cursor: WorkerSubscriptionCursor,
        limit: int = 100,
    ) -> WorkerSubscriptionBatch:
        normalized_cursor = WorkerSubscriptionCursor(
            subscription_id=_require_text(
                cursor.subscription_id,
                field_name="cursor.subscription_id",
            ),
            run_id=_require_text(cursor.run_id, field_name="cursor.run_id"),
            last_acked_evidence_seq=_optional_non_negative_int(
                cursor.last_acked_evidence_seq,
                field_name="cursor.last_acked_evidence_seq",
            ),
        )
        _, effective_cursor = await self._load_subscription_context(
            cursor=normalized_cursor,
        )
        outbox_batch = await self.subscriber.load_batch(
            run_id=effective_cursor.run_id,
            after_evidence_seq=effective_cursor.last_acked_evidence_seq,
            limit=_require_positive_int(limit, field_name="limit"),
        )
        return _subscription_batch_from_outbox(
            cursor=effective_cursor,
            outbox_batch=outbox_batch,
        )

    def acknowledge(
        self,
        *,
        batch: WorkerSubscriptionBatch,
        through_evidence_seq: int | None = None,
    ) -> WorkerSubscriptionAcknowledgement:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.acknowledge_batch(
                    batch=batch,
                    through_evidence_seq=through_evidence_seq,
                )
            )
        raise RuntimeBoundaryError(
            "sync acknowledge() requires an explicit non-async call boundary",
        )

    async def acknowledge_batch(
        self,
        *,
        batch: WorkerSubscriptionBatch,
        through_evidence_seq: int | None = None,
    ) -> WorkerSubscriptionAcknowledgement:
        current_seq = _optional_non_negative_int(
            batch.cursor.last_acked_evidence_seq,
            field_name="batch.cursor.last_acked_evidence_seq",
        )
        next_seq = _optional_non_negative_int(
            batch.next_cursor.last_acked_evidence_seq,
            field_name="batch.next_cursor.last_acked_evidence_seq",
        )
        requested_seq = _optional_non_negative_int(
            through_evidence_seq,
            field_name="through_evidence_seq",
        )

        if requested_seq is None:
            ack_seq = next_seq
        else:
            ack_seq = requested_seq

        if current_seq is not None and ack_seq is not None and ack_seq < current_seq:
            raise RuntimeBoundaryError(
                "subscription acknowledgements cannot move backwards",
            )

        if batch.facts:
            first_seq = batch.facts[0].evidence_seq
            last_seq = batch.facts[-1].evidence_seq
            if ack_seq is not None and ack_seq not in {current_seq, last_seq}:
                if not first_seq <= ack_seq <= last_seq:
                    raise RuntimeBoundaryError(
                        "acknowledgement must stay within the visible inbox batch",
                    )
        elif ack_seq != current_seq:
            raise RuntimeBoundaryError(
                "empty batches cannot acknowledge unseen evidence",
            )

        definition = await self.repository.load_definition(
            subscription_id=batch.cursor.subscription_id,
        )
        if definition is None:
            raise RuntimeBoundaryError(
                "subscription definition is missing from durable storage",
            )
        _subscription_definition_compatible(
            definition=definition,
            cursor=batch.cursor,
        )
        existing_checkpoint = await self.repository.load_checkpoint(
            subscription_id=batch.cursor.subscription_id,
            run_id=batch.cursor.run_id,
        )
        checkpoint = _checkpoint_from_acknowledgement(
            batch=batch,
            through_evidence_seq=ack_seq,
            current_seq=current_seq,
            existing_checkpoint=existing_checkpoint,
        )
        await self.repository.save_checkpoint(checkpoint=checkpoint)

        cursor = _next_cursor(
            batch.cursor,
            last_acked_evidence_seq=ack_seq,
        )
        return WorkerSubscriptionAcknowledgement(
            subscription_id=batch.cursor.subscription_id,
            run_id=batch.cursor.run_id,
            through_evidence_seq=ack_seq,
            cursor=cursor,
        )


__all__ = [
    "WorkerInboxFact",
    "WorkerSubscriptionAcknowledgement",
    "WorkerSubscriptionBatch",
    "WorkerSubscriptionCursor",
    "WorkflowOutboxReader",
    "WorkflowWorkerSubscription",
]
