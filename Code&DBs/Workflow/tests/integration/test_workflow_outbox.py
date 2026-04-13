from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from adapters import build_claim_received_proof, build_transition_proof
from runtime import RouteIdentity, RunState
from runtime.outbox import (
    PostgresWorkflowOutboxSubscriber,
    bootstrap_workflow_outbox_schema,
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


def _route_identity(*, submission: WorkflowAdmissionSubmission, suffix: str) -> RouteIdentity:
    return RouteIdentity(
        workflow_id=submission.run.workflow_id,
        run_id=submission.run.run_id,
        request_id=submission.run.request_id,
        authority_context_ref=submission.decision.authority_context_ref,
        authority_context_digest=submission.run.authority_context_digest,
        claim_id=f"claim:{suffix}",
        lease_id=None,
        proposal_id=None,
        promotion_decision_id=None,
        attempt_no=1,
        transition_seq=0,
    )


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


async def _insert_transition_bundle(
    conn: asyncpg.Connection,
    *,
    proof,
) -> None:
    await conn.execute(
        """
        INSERT INTO workflow_events (
            event_id,
            event_type,
            schema_version,
            workflow_id,
            run_id,
            request_id,
            causation_id,
            node_id,
            occurred_at,
            evidence_seq,
            actor_type,
            reason_code,
            payload
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb
        )
        """,
        proof.event.event_id,
        proof.event.event_type,
        proof.event.schema_version,
        proof.event.workflow_id,
        proof.event.run_id,
        proof.event.request_id,
        proof.event.causation_id,
        proof.event.node_id,
        proof.event.occurred_at,
        proof.event.evidence_seq,
        proof.event.actor_type,
        proof.event.reason_code,
        json.dumps(proof.event.payload, sort_keys=True, separators=(",", ":")),
    )
    await conn.execute(
        """
        INSERT INTO receipts (
            receipt_id,
            receipt_type,
            schema_version,
            workflow_id,
            run_id,
            request_id,
            causation_id,
            node_id,
            attempt_no,
            supersedes_receipt_id,
            started_at,
            finished_at,
            evidence_seq,
            executor_type,
            status,
            inputs,
            outputs,
            artifacts,
            failure_code,
            decision_refs
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
            $16::jsonb, $17::jsonb, $18::jsonb, $19, $20::jsonb
        )
        """,
        proof.receipt.receipt_id,
        proof.receipt.receipt_type,
        proof.receipt.schema_version,
        proof.receipt.workflow_id,
        proof.receipt.run_id,
        proof.receipt.request_id,
        proof.receipt.causation_id,
        proof.receipt.node_id,
        proof.receipt.attempt_no,
        proof.receipt.supersedes_receipt_id,
        proof.receipt.started_at,
        proof.receipt.finished_at,
        proof.receipt.evidence_seq,
        proof.receipt.executor_type,
        proof.receipt.status,
        json.dumps(proof.receipt.inputs, sort_keys=True, separators=(",", ":")),
        json.dumps(proof.receipt.outputs, sort_keys=True, separators=(",", ":")),
        json.dumps(proof.receipt.artifacts, sort_keys=True, separators=(",", ":")),
        proof.receipt.failure_code,
        json.dumps(proof.receipt.decision_refs, sort_keys=True, separators=(",", ":")),
    )


def test_workflow_outbox_subscriber_reads_committed_authority_rows_in_replay_order() -> None:
    workflow_migrations.clear_workflow_migration_caches()
    try:
        asyncio.run(_exercise_workflow_outbox_path())
    finally:
        workflow_migrations.clear_workflow_migration_caches()


async def _exercise_workflow_outbox_path() -> None:
    try:
        conn = await connect_workflow_database()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for workflow outbox integration test: "
            f"{exc.reason_code}"
        )

    suffix = _unique_suffix()
    submission = _submission(suffix=suffix)
    route = _route_identity(submission=submission, suffix=suffix)
    subscriber = PostgresWorkflowOutboxSubscriber()

    try:
        await bootstrap_control_plane_schema(conn)
        await bootstrap_workflow_outbox_schema(conn)
        await _seed_workflow_definition(conn, submission=submission)
        await persist_workflow_admission(conn, submission=submission)

        proof_1 = build_claim_received_proof(
            route_identity=route,
            event_id=f"workflow_event:{submission.run.run_id}:1",
            receipt_id=f"receipt:{submission.run.run_id}:2",
            evidence_seq=1,
            transition_seq=1,
            request_payload=submission.run.request_envelope,
            admitted_definition_ref=submission.run.workflow_definition_id,
            admitted_definition_hash=submission.run.admitted_definition_hash,
            occurred_at=datetime(2026, 4, 1, 12, 5, tzinfo=timezone.utc),
        )
        proof_2 = build_transition_proof(
            route_identity=replace(route, transition_seq=2, lease_id=f"lease:{suffix}:1"),
            transition_seq=2,
            event_id=f"workflow_event:{submission.run.run_id}:3",
            receipt_id=f"receipt:{submission.run.run_id}:4",
            event_type="claim_validated",
            receipt_type="claim_validation_receipt",
            reason_code="claim.validated",
            evidence_seq=3,
            occurred_at=datetime(2026, 4, 1, 12, 4, tzinfo=timezone.utc),
            causation_id=proof_1.receipt.receipt_id,
            status="claim_validating",
        )
        proof_3 = build_transition_proof(
            route_identity=replace(route, transition_seq=3, lease_id=f"lease:{suffix}:1"),
            transition_seq=3,
            event_id=f"workflow_event:{submission.run.run_id}:5",
            receipt_id=f"receipt:{submission.run.run_id}:6",
            event_type="claim_accepted",
            receipt_type="claim_acceptance_receipt",
            reason_code="claim.accepted",
            evidence_seq=5,
            occurred_at=datetime(2026, 4, 1, 12, 6, tzinfo=timezone.utc),
            causation_id=proof_2.receipt.receipt_id,
            status="claim_accepted",
        )

        for proof in (proof_1, proof_2, proof_3):
            async with conn.transaction():
                await _insert_transition_bundle(conn, proof=proof)

        first_batch = await subscriber.load_batch(
            run_id=submission.run.run_id,
            limit=2,
        )
        second_batch = await subscriber.load_batch(
            run_id=submission.run.run_id,
            after_evidence_seq=first_batch.cursor.last_evidence_seq,
            limit=2,
        )
        third_batch = await subscriber.load_batch(
            run_id=submission.run.run_id,
            after_evidence_seq=second_batch.cursor.last_evidence_seq,
            limit=2,
        )

        assert [row.evidence_seq for row in first_batch.rows] == [1, 2]
        assert [row.evidence_seq for row in second_batch.rows] == [3, 4]
        assert [row.evidence_seq for row in third_batch.rows] == [5, 6]
        assert first_batch.has_more is True
        assert second_batch.has_more is True
        assert third_batch.has_more is False
        assert [row.envelope_kind for row in first_batch.rows] == ["workflow_event", "receipt"]
        assert first_batch.rows[0].authority_id == proof_1.event.event_id
        assert first_batch.rows[0].envelope["event_id"] == proof_1.event.event_id
        assert first_batch.rows[1].authority_id == proof_1.receipt.receipt_id
        assert first_batch.rows[1].envelope["receipt_id"] == proof_1.receipt.receipt_id
        assert second_batch.rows[0].authority_recorded_at < first_batch.rows[0].authority_recorded_at
        assert second_batch.rows[0].evidence_seq > first_batch.rows[1].evidence_seq

        replay_batch = await subscriber.load_batch(
            run_id=submission.run.run_id,
            limit=10,
        )
        replayed_ids = [row.authority_id for row in replay_batch.rows]
        incrementally_replayed_ids = [
            *(row.authority_id for row in first_batch.rows),
            *(row.authority_id for row in second_batch.rows),
            *(row.authority_id for row in third_batch.rows),
        ]
        assert replay_batch.cursor.last_evidence_seq == 6
        assert replayed_ids == incrementally_replayed_ids

        uncommitted = build_transition_proof(
            route_identity=replace(route, transition_seq=4, lease_id=f"lease:{suffix}:2"),
            transition_seq=4,
            event_id=f"workflow_event:{submission.run.run_id}:7",
            receipt_id=f"receipt:{submission.run.run_id}:8",
            event_type="lease_requested",
            receipt_type="lease_request_receipt",
            reason_code="lease.requested",
            evidence_seq=7,
            occurred_at=datetime(2026, 4, 1, 12, 7, tzinfo=timezone.utc),
            causation_id=proof_3.receipt.receipt_id,
            status="lease_requested",
        )

        class _Rollback(RuntimeError):
            pass

        try:
            async with conn.transaction():
                await _insert_transition_bundle(conn, proof=uncommitted)
                invisible_batch = await subscriber.load_batch(
                    run_id=submission.run.run_id,
                    after_evidence_seq=6,
                    limit=2,
                )
                assert invisible_batch.rows == ()
                raise _Rollback()
        except _Rollback:
            pass

        after_rollback_batch = await subscriber.load_batch(
            run_id=submission.run.run_id,
            after_evidence_seq=6,
            limit=2,
        )
        assert after_rollback_batch.rows == ()
        assert after_rollback_batch.cursor.last_evidence_seq == 6
    finally:
        await conn.close()
