from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from datetime import datetime, timezone

from runtime import RunState
from runtime.claims import ClaimLeaseProposalSnapshot
from runtime.outbox import WorkflowOutboxBatch, WorkflowOutboxCursor, WorkflowOutboxRecord
from runtime.subscription_repository import EventSubscriptionCheckpoint, EventSubscriptionDefinition
from runtime.subscriptions import WorkerSubscriptionCursor, WorkflowWorkerSubscription
from surfaces.workflow_bridge import WorkflowBridge


def test_dispatch_bridge_consumes_runtime_authority_without_creating_second_state_machine() -> None:
    run_id = "run:dispatch-bridge"
    route_snapshot = ClaimLeaseProposalSnapshot(
        run_id=run_id,
        workflow_id="workflow:dispatch-bridge",
        request_id="request:dispatch-bridge",
        current_state=RunState.CLAIM_ACCEPTED,
        claim_id="claim:dispatch-bridge",
        lease_id=None,
        proposal_id=None,
        attempt_no=1,
        transition_seq=1,
        sandbox_group_id=None,
        sandbox_session_id=None,
        share_mode="exclusive",
        reuse_reason_code=None,
        last_event_id="workflow_event:dispatch-bridge:1",
    )
    outbox_rows = (
        WorkflowOutboxRecord(
            authority_table="workflow_events",
            authority_id="workflow_event:dispatch-bridge:1",
            envelope_kind="workflow_event",
            workflow_id=route_snapshot.workflow_id,
            run_id=run_id,
            request_id=route_snapshot.request_id,
            evidence_seq=1,
            transition_seq=1,
            authority_recorded_at=datetime(2026, 4, 2, 18, 0, tzinfo=timezone.utc),
            captured_at=datetime(2026, 4, 2, 18, 0, tzinfo=timezone.utc),
            envelope={
                "event_id": "workflow_event:dispatch-bridge:1",
                "event_type": "claim_received",
                "transition_seq": 1,
            },
        ),
        WorkflowOutboxRecord(
            authority_table="receipts",
            authority_id="receipt:dispatch-bridge:2",
            envelope_kind="receipt",
            workflow_id=route_snapshot.workflow_id,
            run_id=run_id,
            request_id=route_snapshot.request_id,
            evidence_seq=2,
            transition_seq=1,
            authority_recorded_at=datetime(2026, 4, 2, 18, 0, tzinfo=timezone.utc),
            captured_at=datetime(2026, 4, 2, 18, 0, tzinfo=timezone.utc),
            envelope={
                "receipt_id": "receipt:dispatch-bridge:2",
                "receipt_type": "claim_received_receipt",
                "transition_seq": 1,
            },
        ),
        WorkflowOutboxRecord(
            authority_table="workflow_events",
            authority_id="workflow_event:dispatch-bridge:3",
            envelope_kind="workflow_event",
            workflow_id=route_snapshot.workflow_id,
            run_id=run_id,
            request_id=route_snapshot.request_id,
            evidence_seq=3,
            transition_seq=2,
            authority_recorded_at=datetime(2026, 4, 2, 18, 1, tzinfo=timezone.utc),
            captured_at=datetime(2026, 4, 2, 18, 1, tzinfo=timezone.utc),
            envelope={
                "event_id": "workflow_event:dispatch-bridge:3",
                "event_type": "claim_accepted",
                "transition_seq": 2,
            },
        ),
    )

    routes = _FakeRouteReader(snapshot=route_snapshot)
    subscription = WorkflowWorkerSubscription(
        subscriber=_FakeOutboxSubscriber(rows=outbox_rows),
        repository=_FakeSubscriptionRepository(),
    )
    bridge = WorkflowBridge(routes=routes, subscriptions=subscription)

    first_work = bridge.claimable_work(
        cursor=WorkerSubscriptionCursor(
            subscription_id="dispatch:worker:bridge",
            run_id=run_id,
        ),
        limit=2,
    )

    assert first_work.claimable is True
    assert first_work.lifecycle_authority == "runtime.claims"
    assert first_work.bus_authority == "runtime.outbox"
    assert first_work.route_snapshot == route_snapshot
    assert [fact.evidence_seq for fact in first_work.inbox_batch.facts] == [1, 2]
    assert [fact.inbox_fact_id for fact in first_work.inbox_batch.facts] == [
        "inbox:dispatch:worker:bridge:1",
        "inbox:dispatch:worker:bridge:2",
    ]
    assert first_work.inbox_batch.next_cursor.last_acked_evidence_seq == 2
    assert first_work.inbox_batch.has_more is True

    ack = bridge.acknowledge(work=first_work)

    assert ack.lifecycle_authority == "runtime.claims"
    assert ack.bus_authority == "runtime.outbox"
    assert ack.route_snapshot.current_state is RunState.CLAIM_ACCEPTED
    assert ack.acknowledgement.through_evidence_seq == 2
    assert ack.acknowledgement.cursor.last_acked_evidence_seq == 2

    second_work = bridge.claimable_work(
        cursor=ack.acknowledgement.cursor,
        limit=2,
    )

    assert [fact.evidence_seq for fact in second_work.inbox_batch.facts] == [3]
    assert second_work.inbox_batch.has_more is False
    assert second_work.inbox_batch.cursor.last_acked_evidence_seq == 2
    assert second_work.inbox_batch.next_cursor.last_acked_evidence_seq == 3
    assert routes.inspect_calls == 2
    assert routes.mutation_attempts == 0


@dataclass
class _FakeRouteReader:
    snapshot: ClaimLeaseProposalSnapshot
    inspect_calls: int = 0
    mutation_attempts: int = 0

    def inspect_route(self, *, run_id: str) -> ClaimLeaseProposalSnapshot:
        self.inspect_calls += 1
        assert run_id == self.snapshot.run_id
        return self.snapshot


@dataclass
class _FakeOutboxSubscriber:
    rows: tuple[WorkflowOutboxRecord, ...]

    def read_batch(
        self,
        *,
        run_id: str,
        after_evidence_seq: int | None = None,
        limit: int = 100,
    ) -> WorkflowOutboxBatch:
        visible_rows = tuple(
            row
            for row in self.rows
            if row.run_id == run_id
            and (after_evidence_seq is None or row.evidence_seq > after_evidence_seq)
        )
        batch_rows = visible_rows[:limit]
        return WorkflowOutboxBatch(
            cursor=WorkflowOutboxCursor(
                run_id=run_id,
                last_evidence_seq=(
                    batch_rows[-1].evidence_seq if batch_rows else after_evidence_seq
                ),
            ),
            rows=batch_rows,
            has_more=len(visible_rows) > limit,
        )

    async def load_batch(
        self,
        *,
        run_id: str,
        after_evidence_seq: int | None = None,
        limit: int = 100,
    ) -> WorkflowOutboxBatch:
        return self.read_batch(
            run_id=run_id,
            after_evidence_seq=after_evidence_seq,
            limit=limit,
        )


@dataclass
class _FakeSubscriptionRepository:
    """Minimal in-memory subscription repository for dispatch bridge tests."""

    async def load_definition(
        self, *, subscription_id: str
    ) -> EventSubscriptionDefinition | None:
        return EventSubscriptionDefinition(
            subscription_id=subscription_id,
            subscription_name=f"test:{subscription_id}",
            consumer_kind="worker",
            envelope_kind="workflow_event",
            workflow_id=None,
            run_id=None,
            cursor_scope="run",
            status="active",
            delivery_policy={},
            filter_policy={},
            created_at=datetime(2026, 4, 2, 18, 0, tzinfo=timezone.utc),
        )

    async def load_checkpoint(
        self, *, subscription_id: str, run_id: str
    ) -> EventSubscriptionCheckpoint | None:
        return None

    async def save_definition(
        self, *, definition: EventSubscriptionDefinition
    ) -> EventSubscriptionDefinition:
        return definition

    async def save_checkpoint(
        self, *, checkpoint: EventSubscriptionCheckpoint
    ) -> EventSubscriptionCheckpoint:
        return checkpoint
