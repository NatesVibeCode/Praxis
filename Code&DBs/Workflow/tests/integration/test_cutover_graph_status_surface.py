from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import replace
from datetime import datetime, timezone

import asyncpg
import pytest

from _pg_test_conn import ensure_test_database_ready
from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from observability.operator_topology import (
    NativeCutoverGraphStatusReadModel,
    cutover_graph_status_run,
    render_cutover_graph_status,
)
from authority.operator_control import load_operator_control_authority
from receipts import AppendOnlyWorkflowEvidenceWriter, EvidenceRow, WorkflowEventV1
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RuntimeOrchestrator, WorkflowIntakePlanner
from runtime.work_item_workflow_bindings import WorkItemWorkflowBindingRecord
from storage.migrations import workflow_migration_statements
from storage.postgres import connect_workflow_database

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001
_TEST_DATABASE_URL = ensure_test_database_ready()


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _request() -> WorkflowRequest:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:1111222233334444",
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


def _resolver() -> RegistryResolver:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root="/tmp/workspace.alpha",
                    workdir="/tmp/workspace.alpha/workdir",
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id="model.alpha",
                    provider_policy_id="provider_policy.alpha",
                    sandbox_profile_ref=runtime_profile_ref,
                ),
            ),
        },
    )


def _successful_run() -> tuple[str, tuple[EvidenceRow, ...]]:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()
    RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )
    return outcome.run_id, tuple(writer.evidence_timeline(outcome.run_id))


def _claim_received_row(canonical_evidence: tuple[EvidenceRow, ...]) -> EvidenceRow:
    for row in canonical_evidence:
        if row.kind == "workflow_event" and isinstance(row.record, WorkflowEventV1):
            if row.record.event_type == "claim_received":
                return row
    raise AssertionError("claim_received row not found")


def _with_malformed_claim_envelope(
    canonical_evidence: tuple[EvidenceRow, ...],
) -> tuple[EvidenceRow, ...]:
    claim_row = _claim_received_row(canonical_evidence)
    payload = dict(claim_row.record.payload)
    payload["claim_envelope"] = "broken-envelope"
    mutated_event = replace(claim_row.record, payload=payload)
    return tuple(
        replace(row, record=mutated_event) if row.row_id == claim_row.row_id else row
        for row in canonical_evidence
    )


def _binding_from_row(row: asyncpg.Record) -> WorkItemWorkflowBindingRecord:
    payload = dict(row)
    return WorkItemWorkflowBindingRecord(
        work_item_workflow_binding_id=str(payload["work_item_workflow_binding_id"]),
        binding_kind=str(payload["binding_kind"]),
        binding_status=str(payload["binding_status"]),
        issue_id=None if payload.get("issue_id") is None else str(payload["issue_id"]),
        roadmap_item_id=(
            None if payload.get("roadmap_item_id") is None else str(payload["roadmap_item_id"])
        ),
        bug_id=None if payload.get("bug_id") is None else str(payload["bug_id"]),
        cutover_gate_id=(
            None if payload.get("cutover_gate_id") is None else str(payload["cutover_gate_id"])
        ),
        workflow_class_id=(
            None if payload.get("workflow_class_id") is None else str(payload["workflow_class_id"])
        ),
        schedule_definition_id=(
            None
            if payload.get("schedule_definition_id") is None
            else str(payload["schedule_definition_id"])
        ),
        workflow_run_id=(
            None if payload.get("workflow_run_id") is None else str(payload["workflow_run_id"])
        ),
        bound_by_decision_id=(
            None
            if payload.get("bound_by_decision_id") is None
            else str(payload["bound_by_decision_id"])
        ),
        created_at=payload["created_at"],
        updated_at=payload["updated_at"],
    )


async def _bootstrap_workflow_migration(conn, filename: str) -> None:
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in workflow_migration_statements(filename):
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if getattr(exc, "sqlstate", None) in {"42P07", "42701", "42710"}:
                    continue
                raise


def test_cutover_graph_status_surface_is_deterministic_and_surface_only() -> None:
    asyncio.run(_exercise_cutover_graph_status_surface_is_deterministic_and_surface_only())


async def _exercise_cutover_graph_status_surface_is_deterministic_and_surface_only() -> None:
    run_id, canonical_evidence = _successful_run()
    as_of = datetime(2026, 4, 2, 20, 0, tzinfo=timezone.utc)
    conn = await connect_workflow_database(
        env={"WORKFLOW_DATABASE_URL": _TEST_DATABASE_URL}
    )

    transaction = conn.transaction()
    await transaction.start()
    try:
        for filename in (
            "001_v1_control_plane.sql",
            "006_platform_authority_schema.sql",
            "008_workflow_class_and_schedule_schema.sql",
            "009_bug_and_roadmap_authority.sql",
            "010_operator_control_authority.sql",
            "132_issue_backlog_authority.sql",
            "124_operator_decision_scope_authority.sql",
            "126_operator_decision_scope_policy.sql",
        ):
            await _bootstrap_workflow_migration(conn, filename)

        suffix = _unique_suffix()
        roadmap_item_id = f"roadmap_item.{suffix}.cutover"
        decision_id = f"operator_decision.{suffix}.open"
        gate_id = f"cutover_gate.{suffix}.roadmap"
        workflow_lane_id = f"workflow_lane.{suffix}.cutover"
        workflow_class_id = f"workflow_class.{suffix}.cutover"

        await conn.execute(
            """
            INSERT INTO workflow_lanes (
                workflow_lane_id,
                lane_name,
                lane_kind,
                status,
                concurrency_cap,
                default_route_kind,
                review_required,
                retry_policy,
                effective_from,
                effective_to,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11
            )
            """,
            workflow_lane_id,
            "cutover",
            "review",
            "active",
            1,
            "manual",
            True,
            '{"max_attempts":1}',
            as_of,
            None,
            as_of,
        )
        await conn.execute(
            """
            INSERT INTO workflow_classes (
                workflow_class_id,
                class_name,
                class_kind,
                workflow_lane_id,
                status,
                queue_shape,
                throttle_policy,
                review_required,
                effective_from,
                effective_to,
                decision_ref,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12
            )
            """,
            workflow_class_id,
            "cutover",
            "cutover",
            workflow_lane_id,
            "active",
            '{"shape":"single-run"}',
            '{"max_attempts":1}',
            False,
            as_of,
            None,
            f"decision:workflow-class:{suffix}",
            as_of,
        )
        await conn.execute(
            """
            INSERT INTO roadmap_items (
                roadmap_item_id,
                roadmap_key,
                title,
                item_kind,
                status,
                priority,
                parent_roadmap_item_id,
                source_bug_id,
                summary,
                acceptance_criteria,
                decision_ref,
                target_start_at,
                target_end_at,
                completed_at,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13, $14, $15, $16
            )
            """,
            roadmap_item_id,
            f"roadmap.{suffix}.cutover",
            f"Cutover roadmap {suffix}",
            "initiative",
            "active",
            "p1",
            None,
            None,
            "Operator-controlled cutover target",
            json.dumps(
                {
                    "required_state": "ready",
                    "evidence": [
                        "roadmap.approved",
                        "binding.coverage",
                    ],
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            f"decision.{suffix}.roadmap",
            as_of,
            None,
            None,
            as_of,
            as_of,
        )
        await conn.execute(
            """
            INSERT INTO operator_decisions (
                operator_decision_id,
                decision_key,
                decision_kind,
                decision_status,
                title,
                rationale,
                decided_by,
                decision_source,
                effective_from,
                effective_to,
                decided_at,
                created_at,
                updated_at,
                decision_scope_kind,
                decision_scope_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
            )
            """,
            decision_id,
            f"decision.{suffix}.cutover-open",
            "cutover_gate",
            "decided",
            f"Open cutover gate {suffix}",
            "The gate is open because the roadmap row is the canonical target.",
            "operator.console",
            "operator.review",
            as_of,
            None,
            as_of,
            as_of,
            as_of,
            "roadmap_item",
            roadmap_item_id,
        )
        await conn.execute(
            """
            INSERT INTO cutover_gates (
                cutover_gate_id,
                gate_key,
                gate_name,
                gate_kind,
                gate_status,
                roadmap_item_id,
                workflow_class_id,
                schedule_definition_id,
                gate_policy,
                required_evidence,
                opened_by_decision_id,
                closed_by_decision_id,
                opened_at,
                closed_at,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13, $14, $15, $16
            )
            """,
            gate_id,
            f"gate.{suffix}.roadmap",
            f"Roadmap gate {suffix}",
            "cutover",
            "open",
            roadmap_item_id,
            None,
            None,
            json.dumps(
                {
                    "mode": "manual_review",
                    "owner": "operator",
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            json.dumps(
                {
                    "must_have": [
                        "roadmap.approved",
                        "binding.coverage",
                    ],
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            decision_id,
            None,
            as_of,
            None,
            as_of,
            as_of,
        )
        await conn.execute(
            """
            INSERT INTO work_item_workflow_bindings (
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                roadmap_item_id,
                bug_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
            )
            """,
            f"work_item_workflow_binding.{suffix}.roadmap",
            "governed_by",
            "active",
            roadmap_item_id,
            None,
            None,
            workflow_class_id,
            None,
            None,
            decision_id,
            as_of,
            as_of,
        )
        await conn.execute(
            """
            INSERT INTO work_item_workflow_bindings (
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                roadmap_item_id,
                bug_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
            )
            """,
            f"work_item_workflow_binding.{suffix}.gate",
            "governed_by",
            "active",
            None,
            None,
            gate_id,
            workflow_class_id,
            None,
            None,
            decision_id,
            as_of,
            as_of,
        )

        authority = await load_operator_control_authority(conn, as_of=as_of)
        binding_rows = await conn.fetch(
            """
            SELECT
                work_item_workflow_binding_id,
                binding_kind,
                binding_status,
                roadmap_item_id,
                bug_id,
                cutover_gate_id,
                workflow_class_id,
                schedule_definition_id,
                workflow_run_id,
                bound_by_decision_id,
                created_at,
                updated_at
            FROM work_item_workflow_bindings
            WHERE cutover_gate_id = $1 OR roadmap_item_id = $2
            ORDER BY created_at, work_item_workflow_binding_id
            """,
            gate_id,
            roadmap_item_id,
        )
        work_bindings = tuple(_binding_from_row(row) for row in binding_rows)

        baseline_view = cutover_graph_status_run(
            run_id=run_id,
            canonical_evidence=canonical_evidence,
            operator_control=authority,
            work_bindings=work_bindings,
        )
        reordered_view = cutover_graph_status_run(
            run_id=run_id,
            canonical_evidence=tuple(reversed(canonical_evidence)),
            operator_control=authority,
            work_bindings=tuple(reversed(work_bindings)),
        )

        rendered = render_cutover_graph_status(baseline_view)
        reordered_rendered = render_cutover_graph_status(reordered_view)

        assert rendered == reordered_rendered
        assert baseline_view.status_state == "fresh"
        assert baseline_view.completeness.is_complete is True
        assert "status.state: fresh" in rendered
        assert "cutover_gates.kind: native_cutover_gate_status" in rendered
        assert "cutover_gates[0].coverage_state: covered" in rendered
        assert "work_bindings.count: 2" in rendered
        assert "work_bindings[0].linkage_state: linked" in rendered
        assert "work_bindings[1].linkage_state: linked" in rendered

        stale_view = cutover_graph_status_run(
            run_id=run_id,
            canonical_evidence=canonical_evidence,
            operator_control=authority,
            work_bindings=(),
        )
        stale_rendered = render_cutover_graph_status(stale_view)

        assert stale_view.status_state == "stale"
        assert stale_view.completeness.is_complete is False
        assert "cutover_gate:" in stale_rendered
        assert "binding_missing" in stale_rendered
        assert "status.state: stale" in stale_rendered

        blocked_evidence = _with_malformed_claim_envelope(canonical_evidence)
        blocked_view = cutover_graph_status_run(
            run_id=run_id,
            canonical_evidence=blocked_evidence,
            operator_control=authority,
            work_bindings=work_bindings,
        )
        blocked_rendered = render_cutover_graph_status(blocked_view)

        assert blocked_view.status_state == "blocked"
        assert blocked_view.completeness.is_complete is False
        assert "graph_topology.completeness.is_complete: false" in blocked_rendered
        assert "graph:claim_envelope" in blocked_rendered
        assert "status.state: blocked" in blocked_rendered
    finally:
        await transaction.rollback()
        await conn.close()
