from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from runtime import RouteIdentity, RunState, RuntimeBoundaryError
from runtime import claims as runtime_claims
from runtime.claims import (
    ClaimLeaseProposalRuntime,
    ClaimLeaseProposalTransitionRequest,
    SandboxSessionRequest,
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


def _submission(
    *,
    suffix: str,
    run_index: int,
    authority_suffix: str | None = None,
) -> WorkflowAdmissionSubmission:
    requested_at = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc) + timedelta(minutes=run_index)
    admitted_at = requested_at + timedelta(seconds=1)
    decided_at = requested_at + timedelta(milliseconds=250)
    authority_token = authority_suffix or suffix
    workflow_definition_id = f"workflow_definition:{suffix}:{run_index}:v1"
    definition_hash = f"sha256:{suffix}:{run_index}"
    workspace_ref = f"workspace:{suffix}"
    runtime_profile_ref = f"runtime_profile:{suffix}"
    workflow_id = f"workflow:{suffix}:{run_index}"
    request_id = f"request:{suffix}:{run_index}"
    decision = WorkflowAdmissionDecisionWrite(
        admission_decision_id=f"admission:{suffix}:{run_index}",
        workflow_id=workflow_id,
        request_id=request_id,
        decision="admit",
        reason_code="policy.admission_allowed",
        decided_at=decided_at,
        decided_by="policy.intake",
        policy_snapshot_ref="policy_snapshot:workflow_intake_v1",
        validation_result_ref=f"validation:{suffix}:{run_index}",
        authority_context_ref=f"authority:bundle:{authority_token}",
    )
    run = WorkflowRunWrite(
        run_id=f"run:{suffix}:{run_index}",
        workflow_id=workflow_id,
        request_id=request_id,
        request_digest=f"digest:{suffix}:{run_index}",
        authority_context_digest=f"authority-digest:{authority_token}",
        workflow_definition_id=workflow_definition_id,
        admitted_definition_hash=definition_hash,
        run_idempotency_key=request_id,
        schema_version=1,
        request_envelope={
            "schema_version": 1,
            "workflow_id": workflow_id,
            "request_id": request_id,
            "workflow_definition_id": workflow_definition_id,
            "definition_version": 1,
            "definition_hash": definition_hash,
            "workspace_ref": workspace_ref,
            "runtime_profile_ref": runtime_profile_ref,
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


def _route_identity(
    *,
    submission: WorkflowAdmissionSubmission,
    suffix: str,
    route_index: int,
) -> RouteIdentity:
    return RouteIdentity(
        workflow_id=submission.run.workflow_id,
        run_id=submission.run.run_id,
        request_id=submission.run.request_id,
        authority_context_ref=submission.decision.authority_context_ref,
        authority_context_digest=submission.run.authority_context_digest,
        claim_id=f"claim:{suffix}:{route_index}",
        lease_id=None,
        proposal_id=None,
        promotion_decision_id=None,
        attempt_no=1,
        transition_seq=0,
    )


async def _seed_workflow_definition(conn, *, submission: WorkflowAdmissionSubmission) -> None:
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


def test_claim_lease_proposal_runtime_shared_sandbox_reuse_is_concurrent_canonical_and_fail_closed() -> None:
    asyncio.run(_exercise_runtime_path())


def test_claim_lease_proposal_runtime_schema_resolution_has_no_fallback_to_retired_storage_sql_root(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_root = tmp_path / "Databases" / "migrations" / "workflow"
    retired_root = tmp_path / "Workflow" / "storage" / "sql"
    retired_root.mkdir(parents=True)
    (retired_root / "004_claim_lease_proposal_runtime.sql").write_text(
        "SELECT 1;\n",
        encoding="utf-8",
    )

    _clear_workflow_migration_caches()
    runtime_claims._schema_statements.cache_clear()
    monkeypatch.setattr(
        workflow_migrations,
        "_workflow_migrations_root_path",
        lambda: canonical_root,
    )

    try:
        with pytest.raises(
            RuntimeBoundaryError,
            match="canonical workflow migration root",
        ):
            runtime_claims._schema_statements()
    finally:
        _clear_workflow_migration_caches()
        runtime_claims._schema_statements.cache_clear()

    assert retired_root.exists()
    assert not canonical_root.exists()


async def _exercise_runtime_path() -> None:
    runtime = ClaimLeaseProposalRuntime()
    try:
        primary = await connect_workflow_database()
        rival = await connect_workflow_database()
        auxiliary = await connect_workflow_database()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for claim/lease/proposal runtime integration test: "
            f"{exc.reason_code}"
        )
    try:
        await bootstrap_control_plane_schema(primary)
        await runtime.bootstrap_schema(primary)

        suffix = _unique_suffix()
        first_submission = _submission(suffix=suffix, run_index=1)
        second_submission = _submission(suffix=suffix, run_index=2)
        third_submission = _submission(suffix=suffix, run_index=3)
        fourth_submission = _submission(
            suffix=suffix,
            run_index=4,
            authority_suffix=f"{suffix}:other",
        )

        for submission in (
            first_submission,
            second_submission,
            third_submission,
            fourth_submission,
        ):
            await _seed_workflow_definition(primary, submission=submission)
            await persist_workflow_admission(primary, submission=submission)

        first_route = _route_identity(submission=first_submission, suffix=suffix, route_index=1)
        second_route = _route_identity(submission=second_submission, suffix=suffix, route_index=2)
        third_route = _route_identity(submission=third_submission, suffix=suffix, route_index=3)
        fourth_route = _route_identity(submission=fourth_submission, suffix=suffix, route_index=4)

        for route in (first_route, second_route, third_route, fourth_route):
            await runtime.register_route(primary, route_identity=route, current_state=RunState.CLAIM_ACCEPTED)

        occurred_at = datetime(2026, 4, 1, 12, 5, tzinfo=timezone.utc)

        async def _request_lease(
            conn,
            *,
            route: RouteIdentity,
            lease_id: str,
            offset_seconds: int,
        ):
            return await runtime.advance_transition(
                conn,
                transition=ClaimLeaseProposalTransitionRequest(
                    run_id=route.run_id,
                    from_state=RunState.CLAIM_ACCEPTED,
                    to_state=RunState.LEASE_REQUESTED,
                    reason_code="lease.requested",
                    occurred_at=occurred_at + timedelta(seconds=offset_seconds),
                    expected_transition_seq=0,
                    claim_id=route.claim_id,
                    lease_id=lease_id,
                    event_id=f"workflow_event:{route.run_id}:lease-requested",
                ),
            )

        async def _activate_lease(
            conn,
            *,
            route: RouteIdentity,
            lease_id: str,
            sandbox: SandboxSessionRequest,
            offset_seconds: int,
        ):
            return await runtime.advance_transition(
                conn,
                transition=ClaimLeaseProposalTransitionRequest(
                    run_id=route.run_id,
                    from_state=RunState.LEASE_REQUESTED,
                    to_state=RunState.LEASE_ACTIVE,
                    reason_code="lease.granted",
                    occurred_at=occurred_at + timedelta(seconds=offset_seconds),
                    expected_transition_seq=1,
                    claim_id=route.claim_id,
                    lease_id=lease_id,
                    sandbox=sandbox,
                    event_id=f"workflow_event:{route.run_id}:lease-active",
                ),
            )

        first_requested = await _request_lease(
            primary,
            route=first_route,
            lease_id=f"lease:{suffix}:1",
            offset_seconds=0,
        )
        second_requested = await _request_lease(
            rival,
            route=second_route,
            lease_id=f"lease:{suffix}:2",
            offset_seconds=1,
        )
        third_requested = await _request_lease(
            auxiliary,
            route=third_route,
            lease_id=f"lease:{suffix}:3",
            offset_seconds=2,
        )
        fourth_requested = await _request_lease(
            primary,
            route=fourth_route,
            lease_id=f"lease:{suffix}:4",
            offset_seconds=3,
        )

        assert first_requested.current_state is RunState.LEASE_REQUESTED
        assert second_requested.current_state is RunState.LEASE_REQUESTED
        assert third_requested.current_state is RunState.LEASE_REQUESTED
        assert fourth_requested.current_state is RunState.LEASE_REQUESTED

        shared_sandbox = SandboxSessionRequest(
            sandbox_group_id=f"sandbox_group:{suffix}",
            share_mode="shared",
            reuse_reason_code="packet.simple_compatible",
            base_ref="refs/heads/main",
            base_digest=f"sha256:{suffix}:base",
            sandbox_root=f"/tmp/sandbox/{suffix}",
            expires_at=occurred_at + timedelta(hours=1),
        )

        first_active, second_active = await asyncio.gather(
            _activate_lease(
                primary,
                route=first_route,
                lease_id=f"lease:{suffix}:1",
                sandbox=shared_sandbox,
                offset_seconds=4,
            ),
            _activate_lease(
                rival,
                route=second_route,
                lease_id=f"lease:{suffix}:2",
                sandbox=shared_sandbox,
                offset_seconds=4,
            ),
        )

        assert first_active.current_state is RunState.LEASE_ACTIVE
        assert second_active.current_state is RunState.LEASE_ACTIVE
        assert first_active.sandbox_group_id == shared_sandbox.sandbox_group_id
        assert second_active.sandbox_group_id == shared_sandbox.sandbox_group_id
        assert first_active.sandbox_session_id == second_active.sandbox_session_id
        assert first_active.share_mode == "shared"
        assert second_active.share_mode == "shared"
        assert first_active.claim_id != second_active.claim_id
        assert first_active.lease_id != second_active.lease_id

        session_rows = await primary.fetch(
            """
            SELECT
                sandbox_session_id,
                sandbox_group_id,
                authority_context_digest,
                shared_compatibility_key,
                owner_route_ref
            FROM sandbox_sessions
            WHERE sandbox_group_id = $1
              AND closed_at IS NULL
            ORDER BY opened_at ASC, sandbox_session_id ASC
            """,
            shared_sandbox.sandbox_group_id,
        )
        assert len(session_rows) == 1
        assert session_rows[0]["sandbox_session_id"] == first_active.sandbox_session_id
        assert session_rows[0]["sandbox_group_id"] == shared_sandbox.sandbox_group_id
        assert session_rows[0]["authority_context_digest"] == first_submission.run.authority_context_digest
        assert session_rows[0]["shared_compatibility_key"] is not None
        assert session_rows[0]["owner_route_ref"] in {first_route.run_id, second_route.run_id}

        with pytest.raises(
            RuntimeBoundaryError,
            match="shared sandbox reuse rejected: sandbox_group_id already carries a live incompatible session",
        ):
            await _activate_lease(
                auxiliary,
                route=third_route,
                lease_id=f"lease:{suffix}:3",
                sandbox=replace(
                    shared_sandbox,
                    base_digest=f"sha256:{suffix}:other-base",
                    sandbox_root=f"/tmp/sandbox/{suffix}/base-mismatch",
                ),
                offset_seconds=5,
            )

        with pytest.raises(
            RuntimeBoundaryError,
            match="shared sandbox reuse rejected: sandbox_group_id already carries a live incompatible session",
        ):
            await _activate_lease(
                primary,
                route=fourth_route,
                lease_id=f"lease:{suffix}:4",
                sandbox=replace(
                    shared_sandbox,
                    sandbox_root=f"/tmp/sandbox/{suffix}/authority-mismatch",
                ),
                offset_seconds=6,
            )

        route_rows = await primary.fetch(
            """
            SELECT
                run_id,
                claim_id,
                lease_id,
                proposal_id,
                sandbox_group_id,
                sandbox_session_id,
                share_mode,
                reuse_reason_code
            FROM workflow_claim_lease_proposal_runtime
            WHERE run_id = ANY($1::text[])
            ORDER BY run_id
            """,
            [
                first_route.run_id,
                second_route.run_id,
                third_route.run_id,
                fourth_route.run_id,
            ],
        )
        assert len(route_rows) == 4
        assert route_rows[0]["sandbox_session_id"] == route_rows[1]["sandbox_session_id"]
        assert route_rows[0]["claim_id"] != route_rows[1]["claim_id"]
        assert route_rows[0]["lease_id"] != route_rows[1]["lease_id"]
        assert route_rows[0]["share_mode"] == "shared"
        assert route_rows[1]["share_mode"] == "shared"
        assert route_rows[0]["reuse_reason_code"] == "packet.simple_compatible"
        assert route_rows[1]["reuse_reason_code"] == "packet.simple_compatible"
        assert route_rows[2]["sandbox_session_id"] is None
        assert route_rows[3]["sandbox_session_id"] is None
        assert route_rows[2]["share_mode"] == "exclusive"
        assert route_rows[3]["share_mode"] == "exclusive"
        assert route_rows[2]["reuse_reason_code"] is None
        assert route_rows[3]["reuse_reason_code"] is None

        binding_rows = await primary.fetch(
            """
            SELECT
                sandbox_session_id,
                run_id,
                claim_id,
                lease_id,
                proposal_id,
                binding_role,
                reuse_reason_code
            FROM sandbox_bindings
            WHERE sandbox_session_id = $1
            ORDER BY bound_at, binding_role, run_id
            """,
            first_active.sandbox_session_id,
        )
        assert [row["binding_role"] for row in binding_rows] == ["lease", "lease"]
        assert {row["run_id"] for row in binding_rows} == {first_route.run_id, second_route.run_id}
        assert {row["proposal_id"] for row in binding_rows} == {None}
        assert {row["reuse_reason_code"] for row in binding_rows} == {"packet.simple_compatible"}
    finally:
        await auxiliary.close()
        await rival.close()
        await primary.close()
