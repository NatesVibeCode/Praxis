from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from runtime import RunState
from runtime.outbox import (
    PostgresWorkflowOutboxSubscriber,
    bootstrap_workflow_outbox_schema,
    clear_workflow_outbox_schema_cache,
)
from runtime.subscriptions import WorkerSubscriptionCursor, WorkflowWorkerSubscription
from runtime.subscription_repository import (
    EventSubscriptionDefinition,
    PostgresEventSubscriptionRepository,
    bootstrap_subscription_repository_schema,
    clear_subscription_repository_schema_cache,
    subscription_checkpoint_id,
)
from storage import migrations as workflow_migrations
from storage.postgres import (
    PostgresConfigurationError,
    WorkflowAdmissionDecisionWrite,
    WorkflowAdmissionSubmission,
    WorkflowRunWrite,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    persist_workflow_admission,
)


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _clear_workflow_migration_caches() -> None:
    workflow_migrations.workflow_migrations_root.cache_clear()
    workflow_migrations.workflow_migration_manifest.cache_clear()
    workflow_migrations.workflow_migration_sql_text.cache_clear()
    workflow_migrations.workflow_migration_statements.cache_clear()


def _submission(*, suffix: str) -> WorkflowAdmissionSubmission:
    requested_at = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    admitted_at = requested_at + timedelta(seconds=1)
    decided_at = requested_at + timedelta(milliseconds=250)
    workflow_definition_id = f"workflow_definition:{suffix}:v1"
    decision = WorkflowAdmissionDecisionWrite(
        admission_decision_id=f"admission:{suffix}",
        workflow_id=f"workflow:{suffix}",
        request_id=f"request:{suffix}",
        decision="admit",
        reason_code="policy.admission_allowed",
        decided_at=decided_at,
        decided_by="policy.intake",
        policy_snapshot_ref="policy_snapshot:workflow_intake_v1",
        validation_result_ref=f"validation:{suffix}",
        authority_context_ref=f"authority:bundle:{suffix}",
    )
    run = WorkflowRunWrite(
        run_id=f"run:{suffix}",
        workflow_id=decision.workflow_id,
        request_id=decision.request_id,
        request_digest=f"digest:{suffix}",
        authority_context_digest=f"authority-digest:{suffix}",
        workflow_definition_id=workflow_definition_id,
        admitted_definition_hash=f"sha256:{suffix}",
        run_idempotency_key=decision.request_id,
        schema_version=1,
        request_envelope={
            "schema_version": 1,
            "workflow_id": decision.workflow_id,
            "request_id": decision.request_id,
            "workflow_definition_id": workflow_definition_id,
            "definition_version": 1,
            "definition_hash": f"sha256:{suffix}",
            "workspace_ref": f"workspace:{suffix}",
            "runtime_profile_ref": f"runtime_profile:{suffix}",
            "nodes": [
                {
                    "workflow_definition_node_id": f"{workflow_definition_id}:node_0",
                    "workflow_definition_id": workflow_definition_id,
                    "node_id": "node_0",
                    "node_type": "task",
                    "schema_version": 1,
                    "adapter_type": "noop",
                    "display_name": "Node 0",
                    "inputs": {},
                    "expected_outputs": {},
                    "success_condition": {"kind": "always"},
                    "failure_behavior": {"kind": "stop"},
                    "authority_requirements": {},
                    "execution_boundary": {},
                    "position_index": 0,
                }
            ],
            "edges": [],
        },
        context_bundle_id=f"context_bundle:{suffix}",
        admission_decision_id=decision.admission_decision_id,
        current_state=RunState.CLAIM_ACCEPTED.value,
        requested_at=requested_at,
        admitted_at=admitted_at,
        terminal_reason_code=None,
        started_at=None,
        finished_at=None,
        last_event_id=None,
    )
    return WorkflowAdmissionSubmission(decision=decision, run=run)


async def _seed_workflow_definition(
    conn: asyncpg.Connection,
    *,
    submission: WorkflowAdmissionSubmission,
) -> None:
    request_envelope = submission.run.request_envelope
    await conn.execute(
        """
        INSERT INTO workflow_definitions (
            workflow_definition_id,
            workflow_id,
            schema_version,
            definition_version,
            definition_hash,
            status,
            request_envelope,
            normalized_definition,
            created_at,
            supersedes_workflow_definition_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10)
        ON CONFLICT (workflow_definition_id) DO NOTHING
        """,
        submission.run.workflow_definition_id,
        submission.run.workflow_id,
        submission.run.schema_version,
        1,
        submission.run.admitted_definition_hash,
        "admitted",
        json.dumps(request_envelope),
        json.dumps(request_envelope),
        submission.run.admitted_at,
        None,
    )
    for node in request_envelope["nodes"]:
        await conn.execute(
            """
            INSERT INTO workflow_definition_nodes (
                workflow_definition_node_id,
                workflow_definition_id,
                node_id,
                node_type,
                schema_version,
                adapter_type,
                display_name,
                inputs,
                expected_outputs,
                success_condition,
                failure_behavior,
                authority_requirements,
                execution_boundary,
                position_index
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14
            )
            ON CONFLICT (workflow_definition_node_id) DO NOTHING
            """,
            node["workflow_definition_node_id"],
            node["workflow_definition_id"],
            node["node_id"],
            node["node_type"],
            node["schema_version"],
            node["adapter_type"],
            node["display_name"],
            json.dumps(node["inputs"]),
            json.dumps(node["expected_outputs"]),
            json.dumps(node["success_condition"]),
            json.dumps(node["failure_behavior"]),
            json.dumps(node["authority_requirements"]),
            json.dumps(node["execution_boundary"]),
            node["position_index"],
        )


async def _insert_workflow_outbox_rows(
    conn: asyncpg.Connection,
    *,
    submission: WorkflowAdmissionSubmission,
    suffix: str,
) -> None:
    rows = (
        (
            "workflow_events",
            f"workflow_event:{suffix}:1",
            "workflow_event",
            1,
            1,
            datetime(2026, 4, 1, 12, 1, tzinfo=timezone.utc),
            {
                "event_id": f"workflow_event:{suffix}:1",
                "event_type": "claim_received",
                "transition_seq": 1,
            },
        ),
        (
            "receipts",
            f"receipt:{suffix}:2",
            "receipt",
            2,
            1,
            datetime(2026, 4, 1, 12, 2, tzinfo=timezone.utc),
            {
                "receipt_id": f"receipt:{suffix}:2",
                "receipt_type": "claim_received_receipt",
                "transition_seq": 1,
            },
        ),
        (
            "workflow_events",
            f"workflow_event:{suffix}:3",
            "workflow_event",
            3,
            2,
            datetime(2026, 4, 1, 12, 3, tzinfo=timezone.utc),
            {
                "event_id": f"workflow_event:{suffix}:3",
                "event_type": "claim_accepted",
                "transition_seq": 2,
            },
        ),
    )
    for authority_table, authority_id, envelope_kind, evidence_seq, transition_seq, authority_recorded_at, envelope in rows:
        await conn.execute(
            """
            INSERT INTO workflow_outbox (
                authority_table,
                authority_id,
                envelope_kind,
                workflow_id,
                run_id,
                request_id,
                evidence_seq,
                transition_seq,
                authority_recorded_at,
                envelope
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
            ON CONFLICT (authority_table, authority_id) DO NOTHING
            """,
            authority_table,
            authority_id,
            envelope_kind,
            submission.run.workflow_id,
            submission.run.run_id,
            submission.run.request_id,
            evidence_seq,
            transition_seq,
            authority_recorded_at,
            json.dumps(envelope, sort_keys=True, separators=(",", ":")),
        )


def test_subscription_repository_resume_survives_restart() -> None:
    _clear_workflow_migration_caches()
    clear_workflow_outbox_schema_cache()
    clear_subscription_repository_schema_cache()
    try:
        asyncio.run(_exercise_subscription_repository_path())
    finally:
        _clear_workflow_migration_caches()
        clear_workflow_outbox_schema_cache()
        clear_subscription_repository_schema_cache()


async def _exercise_subscription_repository_path() -> None:
    try:
        conn = await connect_workflow_database()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for subscription repository integration test: "
            f"{exc.reason_code}"
        )

    suffix = _unique_suffix()
    submission = _submission(suffix=suffix)
    subscription_id = f"subscription:{suffix}"
    subscription_definition = EventSubscriptionDefinition(
        subscription_id=subscription_id,
        subscription_name=f"worker subscription {suffix}",
        consumer_kind="worker",
        envelope_kind="workflow_event",
        workflow_id=submission.run.workflow_id,
        run_id=submission.run.run_id,
        cursor_scope="run",
        status="active",
        delivery_policy={
            "delivery": "at_least_once",
            "batch_limit": 2,
        },
        filter_policy={
            "envelope_kinds": ["workflow_event", "receipt"],
            "workflow_id": submission.run.workflow_id,
        },
        created_at=datetime(2026, 4, 1, 11, 59, tzinfo=timezone.utc),
    )
    repository = PostgresEventSubscriptionRepository()
    worker_subscription = WorkflowWorkerSubscription(
        subscriber=PostgresWorkflowOutboxSubscriber(),
        repository=repository,
    )

    try:
        await bootstrap_control_plane_schema(conn)
        await bootstrap_workflow_outbox_schema(conn)
        await bootstrap_subscription_repository_schema(conn)
        await _seed_workflow_definition(conn, submission=submission)
        await persist_workflow_admission(conn, submission=submission)
        await _insert_workflow_outbox_rows(
            conn,
            submission=submission,
            suffix=suffix,
        )

        persisted_definition = await repository.save_definition(
            definition=subscription_definition,
        )
        assert persisted_definition.subscription_id == subscription_id
        assert persisted_definition.run_id == submission.run.run_id

        first_batch = await worker_subscription.load_batch(
            cursor=WorkerSubscriptionCursor(
                subscription_id=subscription_id,
                run_id=submission.run.run_id,
            ),
            limit=2,
        )
        assert [fact.evidence_seq for fact in first_batch.facts] == [1, 2]
        assert first_batch.cursor.last_acked_evidence_seq is None
        assert first_batch.next_cursor.last_acked_evidence_seq == 2

        acknowledgement = await worker_subscription.acknowledge_batch(
            batch=first_batch,
        )
        assert acknowledgement.cursor.last_acked_evidence_seq == 2
        assert acknowledgement.through_evidence_seq == 2

        persisted_checkpoint = await repository.load_checkpoint(
            subscription_id=subscription_id,
            run_id=submission.run.run_id,
        )
        assert persisted_checkpoint is not None
        assert persisted_checkpoint.checkpoint_id == subscription_checkpoint_id(
            subscription_id=subscription_id,
            run_id=submission.run.run_id,
        )
        assert persisted_checkpoint.last_evidence_seq == 2
        assert persisted_checkpoint.last_authority_id == f"receipt:{suffix}:2"
        assert persisted_checkpoint.checkpoint_status == "committed"

        restarted_worker_subscription = WorkflowWorkerSubscription(
            subscriber=PostgresWorkflowOutboxSubscriber(),
            repository=PostgresEventSubscriptionRepository(),
        )
        resumed_batch = await restarted_worker_subscription.load_batch(
            cursor=WorkerSubscriptionCursor(
                subscription_id=subscription_id,
                run_id=submission.run.run_id,
                last_acked_evidence_seq=0,
            ),
            limit=2,
        )
        assert [fact.evidence_seq for fact in resumed_batch.facts] == [3]
        assert resumed_batch.cursor.last_acked_evidence_seq == 2
        assert resumed_batch.next_cursor.last_acked_evidence_seq == 3
        assert resumed_batch.has_more is False
    finally:
        await conn.close()
