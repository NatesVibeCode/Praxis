from __future__ import annotations

from datetime import datetime, timedelta, timezone
import asyncio
import json
import uuid

import pytest

from runtime import RunState
from storage import migrations as workflow_migrations
from storage.migrations import WorkflowMigrationError
from storage.postgres import (
    PostgresConfigurationError,
    PostgresWriteError,
    WorkflowAdmissionDecisionWrite,
    WorkflowAdmissionSubmission,
    WorkflowRunWrite,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    persist_workflow_admission,
    resolve_workflow_database_url,
)


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


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
                },
                {
                    "workflow_definition_node_id": f"{workflow_definition_id}:node_1",
                    "workflow_definition_id": workflow_definition_id,
                    "node_id": "node_1",
                    "node_type": "task",
                    "schema_version": 1,
                    "adapter_type": "noop",
                    "display_name": "Node 1",
                    "inputs": {},
                    "expected_outputs": {},
                    "success_condition": {"kind": "always"},
                    "failure_behavior": {"kind": "stop"},
                    "authority_requirements": {},
                    "execution_boundary": {},
                    "position_index": 1,
                },
            ],
            "edges": [
                {
                    "workflow_definition_edge_id": f"{workflow_definition_id}:edge_0",
                    "workflow_definition_id": workflow_definition_id,
                    "edge_id": "edge_0",
                    "edge_type": "sequence",
                    "schema_version": 1,
                    "from_node_id": "node_0",
                    "to_node_id": "node_1",
                    "release_condition": {"kind": "always"},
                    "payload_mapping": {},
                    "position_index": 0,
                },
            ],
        },
        context_bundle_id=f"context_bundle:{suffix}",
        admission_decision_id=decision.admission_decision_id,
        current_state=RunState.CLAIM_ACCEPTED.value,
        requested_at=requested_at,
        admitted_at=admitted_at,
        terminal_reason_code=None,
        started_at=None,
        finished_at=None,
        last_event_id=f"workflow_event:{suffix}:1",
    )
    return WorkflowAdmissionSubmission(decision=decision, run=run)


def _clear_workflow_migration_caches() -> None:
    workflow_migrations.workflow_migrations_root.cache_clear()
    workflow_migrations.workflow_migration_manifest.cache_clear()
    workflow_migrations.workflow_migration_sql_text.cache_clear()
    workflow_migrations.workflow_migration_statements.cache_clear()


def test_workflow_database_url_resolution_fails_closed_when_missing() -> None:
    with pytest.raises(PostgresConfigurationError) as exc_info:
        resolve_workflow_database_url(env={})

    assert exc_info.value.reason_code == "postgres.config_missing"


def test_workflow_database_url_resolution_fails_closed_when_invalid() -> None:
    with pytest.raises(PostgresConfigurationError) as exc_info:
        resolve_workflow_database_url(
            env={"WORKFLOW_DATABASE_URL": "sqlite:///tmp/workflow.db"},
        )

    assert exc_info.value.reason_code == "postgres.config_invalid"


def test_workflow_migration_resolution_fails_closed_when_canonical_tree_is_incomplete(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_root = tmp_path / "workflow"
    canonical_root.mkdir()
    # Seed all canonical migrations except the two we want to detect as missing.
    _missing = {
        "003_gate_and_promotion_policy.sql",
        "004_claim_lease_proposal_runtime.sql",
    }
    for filename in workflow_migrations._WORKFLOW_MIGRATION_SEQUENCE:
        if filename not in _missing:
            (canonical_root / filename).write_text("SELECT 1;\n", encoding="utf-8")

    _clear_workflow_migration_caches()
    monkeypatch.setattr(
        workflow_migrations,
        "_workflow_migrations_root_path",
        lambda: canonical_root,
    )
    try:
        with pytest.raises(WorkflowMigrationError) as exc_info:
            workflow_migrations.workflow_migration_manifest()
    finally:
        _clear_workflow_migration_caches()

    assert exc_info.value.reason_code == "workflow.migration_manifest_incomplete"
    assert exc_info.value.details["missing_filenames"] == ",".join(sorted(_missing))


def test_workflow_migration_resolution_fails_closed_when_canonical_root_is_missing(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_root = tmp_path / "workflow"

    _clear_workflow_migration_caches()
    monkeypatch.setattr(
        workflow_migrations,
        "_workflow_migrations_root_path",
        lambda: missing_root,
    )
    try:
        with pytest.raises(WorkflowMigrationError) as exc_info:
            workflow_migrations.workflow_migrations_root()
    finally:
        _clear_workflow_migration_caches()

    assert exc_info.value.reason_code == "workflow.migration_root_missing"
    assert exc_info.value.details["path"] == str(missing_root)


def test_postgres_control_plane_path_writes_a_run_and_decision() -> None:
    asyncio.run(_exercise_postgres_control_plane_path())


def test_postgres_control_plane_path_rejects_conflicting_preseeded_definition_rows() -> None:
    asyncio.run(_exercise_conflicting_definition_rejection())


def test_postgres_control_plane_path_rejects_malformed_child_rows() -> None:
    asyncio.run(_exercise_malformed_child_rejection())


async def _exercise_postgres_control_plane_path() -> None:
    conn = await connect_workflow_database()
    try:
        await bootstrap_control_plane_schema(conn)
        suffix = _unique_suffix()
        submission = _submission(suffix=suffix)
        await _seed_workflow_definition(conn, submission=submission)

        result = await persist_workflow_admission(conn, submission=submission)

        decision_row = await conn.fetchrow(
            """
            SELECT
                admission_decision_id,
                workflow_id,
                request_id,
                decision,
                reason_code,
                decided_by
            FROM admission_decisions
            WHERE admission_decision_id = $1
            """,
            result.admission_decision_id,
        )
        run_row = await conn.fetchrow(
            """
            SELECT
                run_id,
                workflow_id,
                request_id,
                request_digest,
                authority_context_digest,
                workflow_definition_id,
                admitted_definition_hash,
                run_idempotency_key,
                schema_version,
                request_envelope,
                context_bundle_id,
                admission_decision_id,
                current_state,
                terminal_reason_code,
                requested_at,
                admitted_at,
                started_at,
                finished_at,
                last_event_id
            FROM workflow_runs
            WHERE run_id = $1
            """,
            result.run_id,
        )
        definition_row = await conn.fetchrow(
            """
            SELECT
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
            FROM workflow_definitions
            WHERE workflow_definition_id = $1
            """,
            submission.run.workflow_definition_id,
        )
        node_rows = await conn.fetch(
            """
            SELECT
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
            FROM workflow_definition_nodes
            WHERE workflow_definition_id = $1
            ORDER BY position_index
            """,
            submission.run.workflow_definition_id,
        )
        edge_row = await conn.fetchrow(
            """
            SELECT
                workflow_definition_edge_id,
                workflow_definition_id,
                edge_id,
                edge_type,
                schema_version,
                from_node_id,
                to_node_id,
                release_condition,
                payload_mapping,
                position_index
            FROM workflow_definition_edges
            WHERE workflow_definition_id = $1
            """,
            submission.run.workflow_definition_id,
        )

        assert decision_row is not None
        assert run_row is not None
        assert definition_row is not None
        assert len(node_rows) == 2
        assert edge_row is not None
        assert decision_row["decision"] == "admit"
        assert decision_row["reason_code"] == "policy.admission_allowed"
        assert decision_row["decided_by"] == "policy.intake"
        assert run_row["current_state"] == RunState.CLAIM_ACCEPTED.value
        assert run_row["admission_decision_id"] == result.admission_decision_id
        assert run_row["workflow_id"] == submission.run.workflow_id
        assert run_row["request_id"] == submission.run.request_id
        assert run_row["request_digest"] == submission.run.request_digest
        assert run_row["authority_context_digest"] == submission.run.authority_context_digest
        assert run_row["workflow_definition_id"] == submission.run.workflow_definition_id
        assert run_row["admitted_definition_hash"] == submission.run.admitted_definition_hash
        assert run_row["run_idempotency_key"] == submission.run.run_idempotency_key
        assert run_row["schema_version"] == submission.run.schema_version
        assert _json_value(run_row["request_envelope"]) == submission.run.request_envelope
        assert run_row["context_bundle_id"] == submission.run.context_bundle_id
        assert run_row["terminal_reason_code"] is None
        assert run_row["started_at"] is None
        assert run_row["finished_at"] is None
        assert run_row["last_event_id"] == submission.run.last_event_id
        assert definition_row["workflow_id"] == submission.run.workflow_id
        assert definition_row["schema_version"] == submission.run.schema_version
        assert definition_row["definition_version"] == 1
        assert definition_row["definition_hash"] == submission.run.admitted_definition_hash
        assert definition_row["status"] == "admitted"
        assert _json_value(definition_row["request_envelope"]) == submission.run.request_envelope
        assert _json_value(definition_row["normalized_definition"]) == submission.run.request_envelope
        assert definition_row["supersedes_workflow_definition_id"] is None
        assert node_rows[0]["workflow_definition_node_id"] == submission.run.request_envelope["nodes"][0]["workflow_definition_node_id"]
        assert node_rows[0]["workflow_definition_id"] == submission.run.workflow_definition_id
        assert node_rows[0]["node_id"] == "node_0"
        assert node_rows[0]["position_index"] == 0
        assert node_rows[1]["workflow_definition_node_id"] == submission.run.request_envelope["nodes"][1]["workflow_definition_node_id"]
        assert node_rows[1]["node_id"] == "node_1"
        assert node_rows[1]["position_index"] == 1
        assert edge_row["workflow_definition_edge_id"] == submission.run.request_envelope["edges"][0]["workflow_definition_edge_id"]
        assert edge_row["workflow_definition_id"] == submission.run.workflow_definition_id
        assert edge_row["edge_id"] == "edge_0"
        assert edge_row["from_node_id"] == "node_0"
        assert edge_row["to_node_id"] == "node_1"
        assert edge_row["position_index"] == 0
    finally:
        await conn.close()


async def _exercise_conflicting_definition_rejection() -> None:
    conn = await connect_workflow_database()
    try:
        await bootstrap_control_plane_schema(conn)
        suffix = _unique_suffix()
        submission = _submission(suffix=suffix)
        await _seed_workflow_definition(
            conn,
            submission=submission,
            node_overrides={
                1: {
                    "display_name": "Mutated Node 1",
                    "failure_behavior": {"kind": "retry"},
                },
            },
        )

        with pytest.raises(PostgresWriteError) as exc_info:
            await persist_workflow_admission(conn, submission=submission)

        assert exc_info.value.reason_code == "postgres.definition_conflict"
        assert (
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM admission_decisions
                WHERE admission_decision_id = $1
                """,
                submission.decision.admission_decision_id,
            )
        ) == 0
        assert (
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM workflow_runs
                WHERE run_id = $1
                """,
                submission.run.run_id,
            )
        ) == 0
        assert (
            await conn.fetchval(
                """
                SELECT display_name
                FROM workflow_definition_nodes
                WHERE workflow_definition_node_id = $1
                """,
                submission.run.request_envelope["nodes"][1]["workflow_definition_node_id"],
            )
        ) == "Mutated Node 1"
    finally:
        await conn.close()


async def _exercise_malformed_child_rejection() -> None:
    conn = await connect_workflow_database()
    try:
        await bootstrap_control_plane_schema(conn)
        suffix = _unique_suffix()
        submission = _submission(suffix=suffix)
        submission.run.request_envelope["nodes"][1].pop("display_name")

        with pytest.raises(PostgresWriteError) as exc_info:
            await persist_workflow_admission(conn, submission=submission)

        assert exc_info.value.reason_code == "postgres.invalid_submission"
        assert (
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM workflow_definitions
                WHERE workflow_definition_id = $1
                """,
                submission.run.workflow_definition_id,
            )
        ) == 0
        assert (
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM admission_decisions
                WHERE admission_decision_id = $1
                """,
                submission.decision.admission_decision_id,
            )
        ) == 0
        assert (
            await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM workflow_runs
                WHERE run_id = $1
                """,
                submission.run.run_id,
            )
        ) == 0
    finally:
        await conn.close()


async def _seed_workflow_definition(
    conn,
    *,
    submission: WorkflowAdmissionSubmission,
    node_overrides: dict[int, dict[str, object]] | None = None,
) -> None:
    request_envelope = submission.run.request_envelope
    normalized_definition = submission.run.request_envelope
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
        json.dumps(normalized_definition),
        submission.run.admitted_at,
        None,
    )

    for index, node in enumerate(request_envelope["nodes"]):
        node_row = dict(node)
        if node_overrides and index in node_overrides:
            node_row.update(node_overrides[index])
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
            node_row["workflow_definition_node_id"],
            node_row["workflow_definition_id"],
            node_row["node_id"],
            node_row["node_type"],
            node_row["schema_version"],
            node_row["adapter_type"],
            node_row["display_name"],
            json.dumps(node_row["inputs"]),
            json.dumps(node_row["expected_outputs"]),
            json.dumps(node_row["success_condition"]),
            json.dumps(node_row["failure_behavior"]),
            json.dumps(node_row["authority_requirements"]),
            json.dumps(node_row["execution_boundary"]),
            node_row["position_index"],
        )

    for edge in request_envelope["edges"]:
        await conn.execute(
            """
            INSERT INTO workflow_definition_edges (
                workflow_definition_edge_id,
                workflow_definition_id,
                edge_id,
                edge_type,
                schema_version,
                from_node_id,
                to_node_id,
                release_condition,
                payload_mapping,
                position_index
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10)
            ON CONFLICT (workflow_definition_edge_id) DO NOTHING
            """,
            edge["workflow_definition_edge_id"],
            edge["workflow_definition_id"],
            edge["edge_id"],
            edge["edge_type"],
            edge["schema_version"],
            edge["from_node_id"],
            edge["to_node_id"],
            json.dumps(edge["release_condition"]),
            json.dumps(edge["payload_mapping"]),
            edge["position_index"],
        )
