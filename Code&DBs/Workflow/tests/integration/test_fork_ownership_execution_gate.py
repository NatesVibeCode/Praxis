from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from runtime import RouteIdentity, RunState, RuntimeBoundaryError
from runtime import claims as runtime_claims
from runtime.claims import (
    ClaimLeaseProposalRuntime,
    ClaimLeaseProposalTransitionRequest,
    SandboxSessionRequest,
)
from storage.migrations import workflow_migration_statements
from storage.postgres import (
    PostgresConfigurationError,
    WorkflowAdmissionDecisionWrite,
    WorkflowAdmissionSubmission,
    WorkflowRunWrite,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    persist_workflow_admission,
)

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in {"42P07", "42710"}


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _jsonb(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 3, 18, 0, tzinfo=timezone.utc)


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
                if _is_duplicate_object_error(exc):
                    continue
                raise


def _submission(*, suffix: str) -> WorkflowAdmissionSubmission:
    requested_at = _fixed_clock()
    admitted_at = requested_at + timedelta(seconds=1)
    decided_at = requested_at + timedelta(milliseconds=250)
    workflow_definition_id = f"workflow_definition:{suffix}:v1"
    definition_hash = f"sha256:{suffix}"
    workflow_id = f"workflow:{suffix}"
    request_id = f"request:{suffix}"
    decision = WorkflowAdmissionDecisionWrite(
        admission_decision_id=f"admission:{suffix}",
        workflow_id=workflow_id,
        request_id=request_id,
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
        workflow_id=workflow_id,
        request_id=request_id,
        request_digest=f"digest:{suffix}",
        authority_context_digest=f"authority-digest:{suffix}",
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


async def _seed_shared_session(
    conn,
    *,
    submission: WorkflowAdmissionSubmission,
    route: RouteIdentity,
    sandbox: SandboxSessionRequest,
    sandbox_session_id: str,
    opened_at: datetime,
    sandbox_group_id: str | None = None,
) -> None:
    assert sandbox.sandbox_group_id is not None
    assert sandbox.expires_at is not None
    group_id = sandbox_group_id or sandbox.sandbox_group_id

    await conn.execute(
        """
        INSERT INTO sandbox_sessions (
            sandbox_session_id,
            sandbox_group_id,
            workspace_ref,
            runtime_profile_ref,
            base_ref,
            base_digest,
            authority_context_digest,
            shared_compatibility_key,
            sandbox_root,
            share_mode,
            opened_at,
            expires_at,
            closed_at,
            closed_reason_code,
            owner_route_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, 'shared', $10, $11, NULL, NULL, $12
        )
        """,
        sandbox_session_id,
        group_id,
        submission.run.request_envelope["workspace_ref"],
        submission.run.request_envelope["runtime_profile_ref"],
        sandbox.base_ref,
        sandbox.base_digest,
        submission.run.authority_context_digest,
        runtime_claims._shared_sandbox_compatibility_key(
            sandbox_group_id=group_id,
            workspace_ref=submission.run.request_envelope["workspace_ref"],
            runtime_profile_ref=submission.run.request_envelope["runtime_profile_ref"],
            authority_context_digest=submission.run.authority_context_digest,
            base_ref=sandbox.base_ref,
            base_digest=sandbox.base_digest,
        ),
        sandbox.sandbox_root,
        opened_at,
        sandbox.expires_at,
        route.run_id,
    )


async def _seed_fork_binding(
    conn,
    *,
    submission: WorkflowAdmissionSubmission,
    route: RouteIdentity,
    sandbox: SandboxSessionRequest,
    suffix: str,
    sandbox_session_id: str,
    created_at: datetime,
) -> None:
    assert sandbox.fork_ref is not None
    assert sandbox.worktree_ref is not None

    await conn.execute(
        """
        INSERT INTO fork_profiles (
            fork_profile_id,
            profile_name,
            orchestration_kind,
            status,
            fork_mode,
            worktree_strategy,
            sandbox_policy,
            retention_policy,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12
        )
        """,
        f"fork_profile:{suffix}",
        f"fork-ownership-execution-gate:{suffix}",
        "build",
        "active",
        "bounded_fork",
        "ephemeral_worktree",
        _jsonb({"share_mode": "shared"}),
        _jsonb({"retire_after": "proposal_or_reject"}),
        created_at - timedelta(hours=1),
        None,
        f"decision:fork-profile:{suffix}",
        created_at,
    )
    await conn.execute(
        """
        INSERT INTO fork_worktree_bindings (
            fork_worktree_binding_id,
            fork_profile_id,
            sandbox_session_id,
            workflow_run_id,
            binding_scope,
            binding_status,
            workspace_ref,
            runtime_profile_ref,
            base_ref,
            fork_ref,
            worktree_ref,
            created_at,
            retired_at,
            decision_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL, $13
        )
        """,
        f"fork_worktree_binding:{suffix}",
        f"fork_profile:{suffix}",
        sandbox_session_id,
        route.run_id,
        "proposal",
        "active",
        submission.run.request_envelope["workspace_ref"],
        submission.run.request_envelope["runtime_profile_ref"],
        sandbox.base_ref,
        sandbox.fork_ref,
        sandbox.worktree_ref,
        created_at,
        f"decision:fork-binding:{suffix}",
    )


def test_fork_ownership_execution_gate_rejects_inventory_fallback_until_the_authoritative_session_is_live() -> None:
    asyncio.run(
        _exercise_fork_ownership_execution_gate_rejects_inventory_fallback_until_the_authoritative_session_is_live()
    )


async def _exercise_fork_ownership_execution_gate_rejects_inventory_fallback_until_the_authoritative_session_is_live() -> None:
    runtime = ClaimLeaseProposalRuntime()
    suffix = _unique_suffix()
    schema_name = f"workflow_test_{suffix}"
    try:
        conn = await connect_workflow_database(
            env={
                "WORKFLOW_DATABASE_URL": os.environ.get(
                    "WORKFLOW_DATABASE_URL",
                    "postgresql://127.0.0.1/postgres",
                )
            }
        )
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for fork ownership execution gate integration test: "
            f"{exc.reason_code}"
        )

    try:
        await conn.execute(f'CREATE SCHEMA "{schema_name}"')
        await conn.execute(f'SET search_path TO "{schema_name}"')
        await bootstrap_control_plane_schema(conn)
        await runtime.bootstrap_schema(conn)
        await _bootstrap_workflow_migration(conn, "006_platform_authority_schema.sql")
        await _bootstrap_workflow_migration(conn, "011_runtime_breadth_authority.sql")

        submission = _submission(suffix=suffix)
        route = _route_identity(submission=submission, suffix=suffix)
        await _seed_workflow_definition(conn, submission=submission)
        await persist_workflow_admission(conn, submission=submission)
        await runtime.register_route(
            conn,
            route_identity=route,
            current_state=RunState.CLAIM_ACCEPTED,
            share_mode="shared",
            reuse_reason_code="packet.authoritative_fork",
        )

        requested_at = _fixed_clock() + timedelta(minutes=5)
        await runtime.advance_transition(
            conn,
            transition=ClaimLeaseProposalTransitionRequest(
                run_id=route.run_id,
                from_state=RunState.CLAIM_ACCEPTED,
                to_state=RunState.LEASE_REQUESTED,
                reason_code="lease.requested",
                occurred_at=requested_at,
                expected_transition_seq=0,
                claim_id=route.claim_id,
                lease_id=f"lease:{suffix}",
                event_id=f"workflow_event:{route.run_id}:lease-requested",
            ),
        )

        sandbox = SandboxSessionRequest(
            sandbox_group_id=f"sandbox_group:{suffix}",
            share_mode="shared",
            reuse_reason_code="packet.authoritative_fork",
            base_ref="refs/heads/main",
            base_digest=f"sha256:{suffix}:base",
            sandbox_root=f"/tmp/sandbox/{suffix}",
            expires_at=requested_at + timedelta(hours=1),
            fork_ref=f"refs/heads/{suffix}/bounded-fork",
            worktree_ref=f"worktree:{suffix}",
        )
        inventory_session_id = f"sandbox_session.inventory:{suffix}"
        authoritative_session_id = f"sandbox_session.authority:{suffix}"
        incompatible_group_id = f"{sandbox.sandbox_group_id}:incompatible"

        await _seed_shared_session(
            conn,
            submission=submission,
            route=route,
            sandbox=sandbox,
            sandbox_session_id=inventory_session_id,
            opened_at=requested_at + timedelta(seconds=5),
        )
        await _seed_shared_session(
            conn,
            submission=submission,
            route=route,
            sandbox=sandbox,
            sandbox_session_id=authoritative_session_id,
            opened_at=requested_at + timedelta(seconds=8),
            sandbox_group_id=incompatible_group_id,
        )
        await _seed_fork_binding(
            conn,
            submission=submission,
            route=route,
            sandbox=sandbox,
            suffix=suffix,
            sandbox_session_id=authoritative_session_id,
            created_at=requested_at + timedelta(seconds=10),
        )

        with pytest.raises(
            RuntimeBoundaryError,
            match="the active fork/worktree binding does not point to a live compatible shared sandbox session",
        ):
            await runtime.advance_transition(
                conn,
                transition=ClaimLeaseProposalTransitionRequest(
                    run_id=route.run_id,
                    from_state=RunState.LEASE_REQUESTED,
                    to_state=RunState.LEASE_ACTIVE,
                    reason_code="lease.granted",
                    occurred_at=requested_at + timedelta(seconds=20),
                    expected_transition_seq=1,
                    claim_id=route.claim_id,
                    lease_id=f"lease:{suffix}",
                    sandbox=sandbox,
                    event_id=f"workflow_event:{route.run_id}:lease-active-incompatible-authoritative-session",
                ),
            )

        blocked_snapshot = await runtime.inspect_route(conn, run_id=route.run_id)
        assert blocked_snapshot.current_state is RunState.LEASE_REQUESTED
        assert blocked_snapshot.sandbox_session_id is None

        await conn.execute(
            """
            UPDATE sandbox_sessions
            SET closed_at = expires_at,
                closed_reason_code = 'sandbox.superseded'
            WHERE sandbox_session_id = $1
            """,
            inventory_session_id,
        )
        await conn.execute(
            """
            UPDATE sandbox_sessions
            SET sandbox_group_id = $2,
                shared_compatibility_key = $3
            WHERE sandbox_session_id = $1
            """,
            authoritative_session_id,
            sandbox.sandbox_group_id,
            runtime_claims._shared_sandbox_compatibility_key(
                sandbox_group_id=sandbox.sandbox_group_id,
                workspace_ref=submission.run.request_envelope["workspace_ref"],
                runtime_profile_ref=submission.run.request_envelope["runtime_profile_ref"],
                authority_context_digest=submission.run.authority_context_digest,
                base_ref=sandbox.base_ref,
                base_digest=sandbox.base_digest,
            ),
        )

        active_snapshot = await runtime.advance_transition(
            conn,
            transition=ClaimLeaseProposalTransitionRequest(
                run_id=route.run_id,
                from_state=RunState.LEASE_REQUESTED,
                to_state=RunState.LEASE_ACTIVE,
                reason_code="lease.granted",
                occurred_at=requested_at + timedelta(seconds=40),
                expected_transition_seq=1,
                claim_id=route.claim_id,
                lease_id=f"lease:{suffix}",
                sandbox=sandbox,
                event_id=f"workflow_event:{route.run_id}:lease-active",
            ),
        )

        assert active_snapshot.current_state is RunState.LEASE_ACTIVE
        assert active_snapshot.sandbox_group_id == sandbox.sandbox_group_id
        assert active_snapshot.sandbox_session_id == authoritative_session_id
        assert active_snapshot.reuse_reason_code == "packet.authoritative_fork"

        binding_row = await conn.fetchrow(
            """
            SELECT sandbox_session_id, binding_role
            FROM sandbox_bindings
            WHERE run_id = $1
            """,
            route.run_id,
        )
        assert binding_row is not None
        assert binding_row["sandbox_session_id"] == authoritative_session_id
        assert binding_row["binding_role"] == "lease"
    finally:
        await conn.execute("SET search_path TO public")
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await conn.close()
