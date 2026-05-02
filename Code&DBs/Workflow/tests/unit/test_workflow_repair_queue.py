from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from runtime.operations.commands.workflow_repair_queue import (
    WorkflowRepairQueueCommand,
    handle_workflow_repair_queue_command,
)
from runtime.operations.queries.workflow_repair_queue import (
    WorkflowRepairQueueStatusQuery,
    handle_query_workflow_repair_queue_status,
)
from runtime.workflow import repair_queue
from storage.migrations import workflow_migration_expected_objects


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
_MIGRATION_PATH = (
    _WORKFLOW_ROOT
    / ".."
    / "Databases"
    / "migrations"
    / "workflow"
    / "410_workflow_repair_queue.sql"
).resolve()


def _repair_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "repair_id": UUID("11111111-1111-4111-8111-111111111111"),
        "repair_scope": "job",
        "queue_status": "queued",
        "auto_repair": True,
        "priority": 100,
        "solution_id": "workflow_chain_abc",
        "wave_id": "wave-1",
        "workflow_id": "workflow.test",
        "run_id": "workflow_run_abc",
        "job_id": 17,
        "job_label": "build_a",
        "workflow_phase": "build",
        "spec_path": "artifacts/workflow/test.queue.json",
        "command_id": None,
        "reason_code": "provider_quota",
        "failure_code": "provider_quota",
        "failure_category": "external_quota",
        "failure_zone": "provider",
        "is_transient": True,
        "repair_strategy": "diagnose_job_then_retry",
        "retry_delta_required": True,
        "source_kind": "workflow_job",
        "source_ref": "17",
        "evidence_kind": "workflow_run",
        "evidence_ref": "workflow_run_abc",
        "repair_dedupe_key": "job:abc",
        "payload": {"attempt": 3},
        "claimed_by": None,
        "claim_expires_at": None,
        "result_ref": None,
        "repair_note": None,
        "created_by_ref": "workflow_repair_auto_enqueue",
        "created_at": datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        "claimed_at": None,
        "started_at": None,
        "completed_at": None,
    }
    row.update(overrides)
    return row


class _RepairConn:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((" ".join(query.split()), args))
        return self.rows


class _Subsystems:
    def __init__(self, conn: _RepairConn) -> None:
        self.conn = conn

    def get_pg_conn(self) -> _RepairConn:
        return self.conn


def test_repair_queue_migration_declares_db_authority_for_all_scopes() -> None:
    objects = {
        (obj.object_type, obj.object_name)
        for obj in workflow_migration_expected_objects("410_workflow_repair_queue.sql")
    }

    assert ("table", "workflow_repair_queue") in objects
    assert ("trigger", "workflow_repair_enqueue_workflow_run_terminal") in objects
    assert ("trigger", "workflow_repair_enqueue_solution_wave_terminal") in objects
    assert ("row", "data_dictionary_objects.workflow_repair_queue") in objects

    sql = _MIGRATION_PATH.read_text(encoding="utf-8")
    assert "repair_scope IN ('solution', 'workflow', 'job')" in sql
    assert "workflow_repair_enqueue_from_workflow_run" in sql
    assert "workflow_repair_enqueue_from_solution_wave" in sql
    assert "diagnose_solution_wave_then_repair_or_resubmit" in sql
    assert "diagnose_workflow_then_retry_or_resubmit" in sql
    assert "diagnose_job_then_retry" in sql


def test_repair_queue_operations_migration_declares_cqrs_authority() -> None:
    objects = {
        (obj.object_type, obj.object_name)
        for obj in workflow_migration_expected_objects(
            "411_register_workflow_repair_queue_operations.sql"
        )
    }

    assert ("row", "operation_catalog_registry.workflow_repair_queue.status") in objects
    assert ("row", "operation_catalog_registry.workflow_repair_queue.command") in objects


def test_list_repair_queue_filters_and_serializes_rows() -> None:
    conn = _RepairConn([_repair_row()])

    payload = repair_queue.list_repair_queue(
        conn,
        queue_status="queued",
        repair_scope="job",
        run_id="workflow_run_abc",
        solution_id="workflow_chain_abc",
        limit=5,
    )

    query, args = conn.calls[0]
    assert "FROM workflow_repair_queue" in query
    assert "queue_status = $1" in query
    assert "repair_scope = $2" in query
    assert "run_id = $3" in query
    assert "solution_id = $4" in query
    assert args == ("queued", "job", "workflow_run_abc", "workflow_chain_abc", 5)
    assert payload["status"] == "ok"
    assert payload["count"] == 1
    assert payload["items"][0]["repair_id"] == "11111111-1111-4111-8111-111111111111"
    assert payload["items"][0]["created_at"] == "2026-05-01T12:00:00+00:00"


def test_claim_repair_uses_skip_locked_and_returns_claimed_item() -> None:
    conn = _RepairConn([_repair_row(queue_status="claimed", claimed_by="agent.neo")])

    payload = repair_queue.claim_repair(
        conn,
        repair_scope="solution",
        claimed_by="agent.neo",
        claim_ttl_minutes=45,
    )

    query, args = conn.calls[0]
    assert "FOR UPDATE SKIP LOCKED" in query
    assert "repair_scope = $1" in query
    assert "claimed_by = $2" in query
    assert "make_interval(mins => $3::int)" in query
    assert args == ("solution", "agent.neo", 45)
    assert payload["status"] == "claimed"
    assert payload["item"]["claimed_by"] == "agent.neo"


def test_complete_repair_rejects_non_terminal_status() -> None:
    with pytest.raises(ValueError, match="completed, failed, cancelled, or superseded"):
        repair_queue.complete_repair(
            _RepairConn(),
            repair_id="11111111-1111-4111-8111-111111111111",
            queue_status="queued",
        )


def test_complete_repair_moves_open_item_to_terminal_status() -> None:
    conn = _RepairConn(
        [
            _repair_row(
                queue_status="completed",
                result_ref="workflow_run:workflow_run_abc",
                repair_note="retried with quota restored",
            )
        ]
    )

    payload = repair_queue.complete_repair(
        conn,
        repair_id="11111111-1111-4111-8111-111111111111",
        queue_status="completed",
        result_ref="workflow_run:workflow_run_abc",
        repair_note="retried with quota restored",
    )

    query, args = conn.calls[0]
    assert "UPDATE workflow_repair_queue" in query
    assert "queue_status IN ('queued', 'claimed', 'repairing')" in query
    assert args == (
        "11111111-1111-4111-8111-111111111111",
        "completed",
        "workflow_run:workflow_run_abc",
        "retried with quota restored",
    )
    assert payload["status"] == "updated"
    assert payload["item"]["queue_status"] == "completed"


def test_release_repair_returns_claimed_item_to_queue() -> None:
    conn = _RepairConn(
        [
            _repair_row(
                queue_status="queued",
                claimed_by=None,
                repair_note="dry-run claim released",
            )
        ]
    )

    payload = repair_queue.release_repair(
        conn,
        repair_id="11111111-1111-4111-8111-111111111111",
        repair_note="dry-run claim released",
    )

    query, args = conn.calls[0]
    assert "UPDATE workflow_repair_queue" in query
    assert "queue_status IN ('claimed', 'repairing')" in query
    assert "claimed_by = NULL" in query
    assert args == (
        "11111111-1111-4111-8111-111111111111",
        "dry-run claim released",
    )
    assert payload["status"] == "released"
    assert payload["item"]["queue_status"] == "queued"


def test_status_operation_reads_queue_with_authority_payload() -> None:
    conn = _RepairConn([_repair_row()])

    payload = handle_query_workflow_repair_queue_status(
        WorkflowRepairQueueStatusQuery(
            action="list",
            queue_status="queued",
            repair_scope="job",
            run_id="workflow_run_abc",
            limit=5,
        ),
        _Subsystems(conn),
    )

    assert payload["ok"] is True
    assert payload["view"] == "workflow_repair_queue"
    assert payload["authority"] == "workflow_repair_queue"
    assert payload["count"] == 1


def test_command_operation_claims_and_emits_event_payload() -> None:
    conn = _RepairConn([_repair_row(queue_status="claimed", claimed_by="agent.neo")])

    payload = handle_workflow_repair_queue_command(
        WorkflowRepairQueueCommand(
            action="claim",
            repair_scope="job",
            claimed_by="agent.neo",
            claim_ttl_minutes=30,
        ),
        _Subsystems(conn),
    )

    assert payload["ok"] is True
    assert payload["status"] == "claimed"
    assert payload["authority"] == "workflow_repair_queue"
    assert payload["event_payload"]["action"] == "claim"
    assert payload["event_payload"]["repair_id"] == "11111111-1111-4111-8111-111111111111"
