"""Integration test: inserting a bug fires db.bugs.insert system_event."""

from __future__ import annotations

import json
import os
import uuid

import pytest

from _pg_test_conn import ensure_test_database_ready
from storage.postgres import SyncPostgresConnection, get_workflow_pool


os.environ.setdefault("WORKFLOW_DATABASE_URL", ensure_test_database_ready())


@pytest.fixture
def conn() -> SyncPostgresConnection:
    try:
        return SyncPostgresConnection(get_workflow_pool())
    except Exception as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is configured but the bugs trigger integration test "
            f"cannot reach Postgres in this environment: {type(exc).__name__}: {exc}"
        )


def test_bug_insert_fires_system_event(conn: SyncPostgresConnection) -> None:
    bug_id = f"BUG-TEST-{uuid.uuid4().hex[:8].upper()}"
    bug_key = f"bug_test_{uuid.uuid4().hex[:8]}"

    try:
        conn.execute(
            """INSERT INTO bugs (
                   bug_id,
                   bug_key,
                   title,
                   status,
                   severity,
                   priority,
                   summary,
                   source_kind,
                   decision_ref,
                   category,
                   description,
                   filed_by,
                   opened_at,
                   created_at,
                   updated_at
               ) VALUES (
                   $1, $2, $3, 'OPEN', 'P3', 'low', 'test', 'manual', 'none',
                   'other', '', 'test_suite', NOW(), NOW(), NOW()
               )""",
            bug_id,
            bug_key,
            "Trigger e2e test bug",
        )

        rows = conn.execute(
            """SELECT event_type, source_id, source_type
               FROM system_events
               WHERE source_id = $1
                 AND source_type = 'bugs'
               ORDER BY created_at DESC
               LIMIT 1""",
            bug_id,
        )

        assert rows, "Expected a system_event for bug insert but got none"
        assert rows[0]["event_type"] == "db.bugs.insert"
        assert rows[0]["source_id"] == bug_id
        assert rows[0]["source_type"] == "bugs"
    finally:
        conn.execute(
            "DELETE FROM system_events WHERE source_id = $1 AND source_type = 'bugs'",
            bug_id,
        )
        conn.execute("DELETE FROM bugs WHERE bug_id = $1", bug_id)


def _latest_system_event(
    conn: SyncPostgresConnection,
    *,
    source_id: str,
    source_type: str,
):
    rows = conn.execute(
        """SELECT event_type, source_id, source_type
           FROM system_events
           WHERE source_id = $1
             AND source_type = $2
           ORDER BY id DESC
           LIMIT 1""",
        source_id,
        source_type,
    )
    return rows[0] if rows else None


def _seed_workflow_run(conn: SyncPostgresConnection) -> dict[str, str]:
    suffix = uuid.uuid4().hex[:8]
    workflow_id = f"workflow.trigger-proof.{suffix}"
    run_id = f"run.trigger-proof.{suffix}"
    request_id = f"request.trigger-proof.{suffix}"
    workflow_definition_id = f"workflow_definition.trigger-proof.{suffix}"
    admission_decision_id = f"admission.trigger-proof.{suffix}"

    conn.execute(
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
            created_at
        ) VALUES (
            $1, $2, 1, 1, $3, 'active', $4::jsonb, $5::jsonb, NOW()
        )
        """,
        workflow_definition_id,
        workflow_id,
        f"sha256:defn:{suffix}",
        json.dumps({"kind": "test", "suffix": suffix}),
        json.dumps({"nodes": [], "edges": []}),
    )
    conn.execute(
        """
        INSERT INTO admission_decisions (
            admission_decision_id,
            workflow_id,
            request_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_result_ref,
            authority_context_ref
        ) VALUES (
            $1, $2, $3, 'admit', 'test.db_change_trigger', NOW(), 'test_suite',
            'policy:test', 'validation:test', 'authority:test'
        )
        """,
        admission_decision_id,
        workflow_id,
        request_id,
    )
    conn.execute(
        """
        INSERT INTO workflow_runs (
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
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 1, $9::jsonb, $10, $11, 'queued',
            NULL, NOW(), NOW(), NULL, NULL, NULL
        )
        """,
        run_id,
        workflow_id,
        request_id,
        f"sha256:req:{suffix}",
        f"sha256:auth:{suffix}",
        workflow_definition_id,
        f"sha256:defn:{suffix}",
        request_id,
        json.dumps({"kind": "test", "suffix": suffix}),
        f"context_bundle.{suffix}",
        admission_decision_id,
    )
    return {
        "workflow_definition_id": workflow_definition_id,
        "admission_decision_id": admission_decision_id,
        "run_id": run_id,
    }


def test_workflow_trigger_expansion_surfaces_fire_system_events(
    conn: SyncPostgresConnection,
) -> None:
    seeded = _seed_workflow_run(conn)
    run_id = seeded["run_id"]
    job_id: int | None = None

    try:
        run_insert_event = _latest_system_event(
            conn,
            source_id=run_id,
            source_type="workflow_runs",
        )

        assert run_insert_event is not None
        assert run_insert_event["event_type"] == "db.workflow_runs.insert"

        conn.execute(
            "UPDATE workflow_runs SET current_state = 'running', started_at = NOW() WHERE run_id = $1",
            run_id,
        )

        run_update_event = _latest_system_event(
            conn,
            source_id=run_id,
            source_type="workflow_runs",
        )

        assert run_update_event is not None
        assert run_update_event["event_type"] == "db.workflow_runs.update"

        job_rows = conn.execute(
            """
            INSERT INTO workflow_jobs (
                run_id,
                label,
                agent_slug,
                prompt,
                prompt_hash,
                status
            ) VALUES (
                $1, $2, $3, $4, $5, 'pending'
            )
            RETURNING id
            """,
            run_id,
            "trigger-proof-job",
            "openai/gpt-5.4-mini",
            "prove db trigger expansion",
            f"prompt:{uuid.uuid4().hex[:8]}",
        )
        job_id = int(job_rows[0]["id"])

        job_insert_event = _latest_system_event(
            conn,
            source_id=str(job_id),
            source_type="workflow_jobs",
        )

        assert job_insert_event is not None
        assert job_insert_event["event_type"] == "db.workflow_jobs.insert"

        conn.execute(
            "UPDATE workflow_jobs SET status = 'running', started_at = NOW() WHERE id = $1",
            job_id,
        )

        job_update_event = _latest_system_event(
            conn,
            source_id=str(job_id),
            source_type="workflow_jobs",
        )

        assert job_update_event is not None
        assert job_update_event["event_type"] == "db.workflow_jobs.update"
    finally:
        if job_id is not None:
            conn.execute(
                "DELETE FROM system_events WHERE source_id = $1 AND source_type = 'workflow_jobs'",
                str(job_id),
            )
        conn.execute(
            "DELETE FROM system_events WHERE source_id = $1 AND source_type = 'workflow_runs'",
            run_id,
        )
        conn.execute("DELETE FROM workflow_runs WHERE run_id = $1", run_id)
        conn.execute(
            "DELETE FROM admission_decisions WHERE admission_decision_id = $1",
            seeded["admission_decision_id"],
        )
        conn.execute(
            "DELETE FROM workflow_definitions WHERE workflow_definition_id = $1",
            seeded["workflow_definition_id"],
        )
