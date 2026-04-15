from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from io import StringIO
import json
import uuid

import pytest

from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from receipts import AppendOnlyWorkflowEvidenceWriter, ReceiptV1
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RuntimeBoundaryError, RuntimeOrchestrator, WorkflowIntakePlanner
from runtime.persistent_evidence import PostgresEvidenceWriter
from storage.postgres import (
    PostgresEvidenceReader,
    WorkflowAdmissionDecisionWrite,
    WorkflowAdmissionSubmission,
    WorkflowRunWrite,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    persist_workflow_admission,
)
from surfaces.cli.main import main as workflow_cli_main


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _request(*, suffix: str) -> WorkflowRequest:
    workflow_id = f"workflow.{suffix}"
    request_id = f"request.{suffix}"
    workflow_definition_id = f"workflow_definition.{suffix}.v1"
    workspace_ref = f"workspace.{suffix}"
    runtime_profile_ref = f"runtime_profile.{suffix}"
    return WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id=workflow_id,
        request_id=request_id,
        workflow_definition_id=workflow_definition_id,
        definition_hash=f"sha256:{suffix}",
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        nodes=(
            WorkflowNodeContract(
                node_id="node_0",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="prepare",
                inputs={
                    "task_name": "prepare",
                    "input_payload": {"step": 0},
                },
                expected_outputs={"result": "prepared"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": workspace_ref},
                position_index=0,
            ),
            WorkflowNodeContract(
                node_id="node_1",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="admit",
                inputs={
                    "task_name": "admit",
                    "input_payload": {"step": 1},
                },
                expected_outputs={"result": "admitted"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": workspace_ref},
                position_index=1,
            ),
        ),
        edges=(
            WorkflowEdgeContract(
                edge_id="edge_0",
                edge_type=MINIMAL_WORKFLOW_EDGE_TYPE,
                from_node_id="node_0",
                to_node_id="node_1",
                release_condition={"upstream_result": "success"},
                payload_mapping={"prepared_result": "result"},
                position_index=0,
            ),
        ),
    )


def _resolver(*, suffix: str) -> RegistryResolver:
    workspace_ref = f"workspace.{suffix}"
    runtime_profile_ref = f"runtime_profile.{suffix}"
    return RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root=f"/tmp/{workspace_ref}",
                    workdir=f"/tmp/{workspace_ref}/workdir",
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id=f"model.{suffix}",
                    provider_policy_id=f"provider_policy.{suffix}",
                    sandbox_profile_ref=runtime_profile_ref,
                ),
            ),
        },
    )


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _request_envelope(request: WorkflowRequest) -> dict[str, object]:
    workflow_definition_id = request.workflow_definition_id
    return {
        "schema_version": request.schema_version,
        "workflow_id": request.workflow_id,
        "request_id": request.request_id,
        "workflow_definition_id": workflow_definition_id,
        "definition_version": 1,
        "definition_hash": request.definition_hash,
        "workspace_ref": request.workspace_ref,
        "runtime_profile_ref": request.runtime_profile_ref,
        "nodes": [
            {
                "workflow_definition_node_id": f"{workflow_definition_id}:{node.node_id}",
                "workflow_definition_id": workflow_definition_id,
                "node_id": node.node_id,
                "node_type": node.node_type,
                "schema_version": request.schema_version,
                "adapter_type": node.adapter_type,
                "display_name": node.display_name,
                "inputs": dict(node.inputs),
                "expected_outputs": dict(node.expected_outputs),
                "success_condition": dict(node.success_condition),
                "failure_behavior": dict(node.failure_behavior),
                "authority_requirements": dict(node.authority_requirements),
                "execution_boundary": dict(node.execution_boundary),
                "position_index": node.position_index,
            }
            for node in request.nodes
        ],
        "edges": [
            {
                "workflow_definition_edge_id": f"{workflow_definition_id}:{edge.edge_id}",
                "workflow_definition_id": workflow_definition_id,
                "edge_id": edge.edge_id,
                "edge_type": edge.edge_type,
                "schema_version": request.schema_version,
                "from_node_id": edge.from_node_id,
                "to_node_id": edge.to_node_id,
                "release_condition": dict(edge.release_condition),
                "payload_mapping": dict(edge.payload_mapping),
                "position_index": edge.position_index,
            }
            for edge in request.edges
        ],
    }


def _submission_from_outcome(*, outcome, requested_at) -> WorkflowAdmissionSubmission:
    request = outcome.workflow_request
    decision = outcome.admission_decision
    decision_write = WorkflowAdmissionDecisionWrite(
        admission_decision_id=decision.admission_decision_id,
        workflow_id=request.workflow_id,
        request_id=request.request_id,
        decision=decision.decision.value,
        reason_code=decision.reason_code,
        decided_at=decision.decided_at,
        decided_by=decision.decided_by,
        policy_snapshot_ref=decision.policy_snapshot_ref,
        validation_result_ref=decision.validation_result_ref,
        authority_context_ref=decision.authority_context_ref,
    )
    run_write = WorkflowRunWrite(
        run_id=outcome.run_id,
        workflow_id=request.workflow_id,
        request_id=request.request_id,
        request_digest=outcome.request_digest,
        authority_context_digest=outcome.route_identity.authority_context_digest,
        workflow_definition_id=outcome.admitted_definition_ref or request.workflow_definition_id,
        admitted_definition_hash=outcome.admitted_definition_hash or request.definition_hash,
        run_idempotency_key=outcome.run_idempotency_key,
        schema_version=request.schema_version,
        request_envelope=_request_envelope(request),
        context_bundle_id=decision.authority_context_ref,
        admission_decision_id=decision.admission_decision_id,
        current_state=outcome.current_state.value,
        requested_at=requested_at,
        admitted_at=decision.decided_at,
        terminal_reason_code=None,
        started_at=None,
        finished_at=None,
        last_event_id=None,
    )
    return WorkflowAdmissionSubmission(decision=decision_write, run=run_write)




def _spoofed_request_boundary(request: WorkflowRequest) -> WorkflowRequest:
    return replace(
        request,
        nodes=tuple(
            replace(
                node,
                execution_boundary={"workspace_ref": "workspace.spoofed"},
            )
            for node in request.nodes
        ),
    )


def test_first_slice_is_runnable_inspectable_and_replayable_from_postgres() -> None:
    suffix = _unique_suffix()
    request = _request(suffix=suffix)
    planner = WorkflowIntakePlanner(registry=_resolver(suffix=suffix))
    outcome = planner.plan(request=request)
    requested_at = outcome.admission_decision.decided_at - timedelta(seconds=1)
    submission = _submission_from_outcome(
        outcome=outcome,
        requested_at=requested_at,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    conn = loop.run_until_complete(connect_workflow_database())
    writer = PostgresEvidenceWriter()
    try:
        loop.run_until_complete(bootstrap_control_plane_schema(conn))
        loop.run_until_complete(
            persist_workflow_admission(conn, submission=submission)
        )

        result = RuntimeOrchestrator().execute_deterministic_path(
            intake_outcome=replace(
                outcome,
                workflow_request=_spoofed_request_boundary(outcome.workflow_request),
            ),
            evidence_writer=writer,
        )

        run_row = loop.run_until_complete(
            conn.fetchrow(
                """
                SELECT
                    run_id,
                    workflow_definition_id,
                    admitted_definition_hash,
                    current_state
                FROM workflow_runs
                WHERE run_id = $1
                """,
                outcome.run_id,
            )
        )
        assert run_row is not None
        assert run_row["run_id"] == outcome.run_id
        assert run_row["workflow_definition_id"] == outcome.admitted_definition_ref
        assert run_row["admitted_definition_hash"] == outcome.admitted_definition_hash
        assert run_row["current_state"] == result.current_state.value
    finally:
        try:
            writer._bridge.run(writer.close())
        finally:
            writer._bridge.close()
        loop.run_until_complete(conn.close())
        loop.close()
        asyncio.set_event_loop(None)

    reader = PostgresEvidenceReader()
    persisted_runtime = RuntimeOrchestrator(evidence_reader=reader)
    canonical_evidence = reader.evidence_timeline(outcome.run_id)

    assert result.current_state.value == "succeeded"
    assert result.node_order == ("node_0", "node_1")
    assert len(canonical_evidence) == 18
    assert [row.evidence_seq for row in canonical_evidence] == list(range(1, 19))
    assert [
        row.record.event_type
        for row in canonical_evidence
        if row.kind == "workflow_event"
    ] == [
        "claim_received",
        "claim_validated",
        "workflow_queued",
        "workflow_started",
        "node_started",
        "node_succeeded",
        "node_started",
        "node_succeeded",
        "workflow_succeeded",
    ]

    downstream_receipt = next(
        row.record
        for row in canonical_evidence
        if row.kind == "receipt"
        and isinstance(row.record, ReceiptV1)
        and row.record.receipt_type == "node_execution_receipt"
        and row.record.node_id == "node_1"
    )
    assert downstream_receipt.inputs["dependency_inputs"] == {
        "prepared_result": "prepared"
    }
    assert downstream_receipt.inputs["execution_boundary_ref"] == request.workspace_ref

    inspection = persisted_runtime.inspect_run(run_id=outcome.run_id)
    replay = persisted_runtime.replay_run(run_id=outcome.run_id)

    assert inspection.completeness.is_complete is True
    assert inspection.watermark.evidence_seq == 18
    assert inspection.current_state == "succeeded"
    assert inspection.node_timeline == (
        "node_0:running",
        "node_0:succeeded",
        "node_1:running",
        "node_1:succeeded",
    )
    assert inspection.terminal_reason == "runtime.workflow_succeeded"

    assert replay.completeness.is_complete is True
    assert replay.watermark.evidence_seq == 18
    assert replay.admitted_definition_ref == outcome.admitted_definition_ref
    assert replay.dependency_order == ("node_0", "node_1")
    assert replay.node_outcomes == ("node_0:succeeded", "node_1:succeeded")
    assert replay.terminal_reason == "runtime.workflow_succeeded"

    inspect_stdout = StringIO()
    replay_stdout = StringIO()

    assert workflow_cli_main(
        ["inspect", outcome.run_id],
        runtime_orchestrator=persisted_runtime,
        stdout=inspect_stdout,
    ) == 0
    assert workflow_cli_main(
        ["replay", outcome.run_id],
        runtime_orchestrator=persisted_runtime,
        stdout=replay_stdout,
    ) == 0
    assert "current_state: succeeded" in inspect_stdout.getvalue()
    assert "watermark_seq: 18" in inspect_stdout.getvalue()
    assert f"admitted_definition_ref: {outcome.admitted_definition_ref}" in replay_stdout.getvalue()
    assert "dependency_order: node_0, node_1" in replay_stdout.getvalue()


def test_first_slice_duplicate_submit_reuses_stable_run_without_second_evidence_stream() -> None:
    suffix = _unique_suffix()
    request = _request(suffix=suffix)
    planner = WorkflowIntakePlanner(registry=_resolver(suffix=suffix))
    first_outcome = planner.plan(request=request)
    duplicate_outcome = planner.plan(request=request)
    requested_at = first_outcome.admission_decision.decided_at - timedelta(seconds=1)

    assert duplicate_outcome.run_id == first_outcome.run_id
    assert (
        duplicate_outcome.admission_decision.admission_decision_id
        == first_outcome.admission_decision.admission_decision_id
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    conn = loop.run_until_complete(connect_workflow_database())
    writer = PostgresEvidenceWriter()
    try:
        loop.run_until_complete(bootstrap_control_plane_schema(conn))
        first_result = loop.run_until_complete(
            persist_workflow_admission(
                conn,
                submission=_submission_from_outcome(
                    outcome=first_outcome,
                    requested_at=requested_at,
                ),
            )
        )
        duplicate_result = loop.run_until_complete(
            persist_workflow_admission(
                conn,
                submission=_submission_from_outcome(
                    outcome=duplicate_outcome,
                    requested_at=requested_at,
                ),
            )
        )
        assert first_result.run_id == first_outcome.run_id
        assert duplicate_result.run_id == first_outcome.run_id
        assert (
            loop.run_until_complete(
                conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM admission_decisions
                    WHERE admission_decision_id = $1
                    """,
                    first_outcome.admission_decision.admission_decision_id,
                )
            )
            == 1
        )
        assert (
            loop.run_until_complete(
                conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM workflow_runs
                    WHERE workflow_id = $1
                      AND run_idempotency_key = $2
                    """,
                    first_outcome.workflow_request.workflow_id,
                    first_outcome.run_idempotency_key,
                )
            )
            == 1
        )

        execution_result = RuntimeOrchestrator().execute_deterministic_path(
            intake_outcome=first_outcome,
            evidence_writer=writer,
        )
    finally:
        try:
            writer._bridge.run(writer.close())
        finally:
            writer._bridge.close()
        loop.run_until_complete(conn.close())
        loop.close()
        asyncio.set_event_loop(None)

    canonical_evidence = PostgresEvidenceReader().evidence_timeline(first_outcome.run_id)
    assert execution_result.current_state.value == "succeeded"
    assert len(canonical_evidence) == 18
    assert {row.route_identity.run_id for row in canonical_evidence} == {
        first_outcome.run_id,
    }


def test_first_slice_execution_boundary_fails_closed_on_authority_mismatch() -> None:
    suffix = _unique_suffix()
    outcome = WorkflowIntakePlanner(registry=_resolver(suffix=suffix)).plan(
        request=_request(suffix=suffix),
    )
    assert outcome.authority_context is not None

    writer = AppendOnlyWorkflowEvidenceWriter()
    with pytest.raises(RuntimeBoundaryError) as exc_info:
        RuntimeOrchestrator().execute_deterministic_path(
            intake_outcome=replace(
                outcome,
                authority_context=replace(
                    outcome.authority_context,
                    workspace_ref="workspace.spoofed",
                ),
            ),
            evidence_writer=writer,
        )

    assert str(exc_info.value) == "runtime.execution_boundary_authority_mismatch"
    assert [
        row.record.event_type
        for row in writer.evidence_timeline(outcome.run_id)
        if row.kind == "workflow_event"
    ] == [
        "claim_received",
        "claim_validated",
        "workflow_queued",
        "workflow_started",
    ]
    assert all(row.record.node_id is None for row in writer.evidence_timeline(outcome.run_id))
