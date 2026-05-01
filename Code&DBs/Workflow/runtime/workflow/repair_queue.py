"""Durable repair queue helpers for failed Solutions, Workflows, and Jobs."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from runtime._helpers import _json_compatible

if False:  # pragma: no cover - typing only
    from storage.postgres.connection import SyncPostgresConnection

_OPEN_STATUSES = frozenset({"queued", "claimed", "repairing"})
_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "superseded"})
_VALID_SCOPES = frozenset({"solution", "workflow", "job"})
_VALID_STATUSES = _OPEN_STATUSES | _TERMINAL_STATUSES


def _normalize_optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _normalize_scope(value: object | None) -> str | None:
    scope = _normalize_optional_text(value, field_name="repair_scope")
    if scope is None:
        return None
    if scope not in _VALID_SCOPES:
        raise ValueError("repair_scope must be one of: job, solution, workflow")
    return scope


def _normalize_status(value: object | None) -> str | None:
    status = _normalize_optional_text(value, field_name="queue_status")
    if status is None:
        return None
    if status not in _VALID_STATUSES:
        raise ValueError(
            "queue_status must be one of: queued, claimed, repairing, "
            "completed, failed, cancelled, superseded"
        )
    return status


def _coerce_limit(value: object | None, *, default: int = 50) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError("limit must be an integer")
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    if limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")
    return limit


def _row_to_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
        elif key == "repair_id" and value is not None:
            payload[key] = str(value)
    return _json_compatible(payload)


def _where_clause(
    *,
    queue_status: str | None = None,
    repair_scope: str | None = None,
    run_id: str | None = None,
    solution_id: str | None = None,
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []

    if queue_status is not None:
        params.append(queue_status)
        clauses.append(f"queue_status = ${len(params)}")
    if repair_scope is not None:
        params.append(repair_scope)
        clauses.append(f"repair_scope = ${len(params)}")
    if run_id is not None:
        params.append(run_id)
        clauses.append(f"run_id = ${len(params)}")
    if solution_id is not None:
        params.append(solution_id)
        clauses.append(f"solution_id = ${len(params)}")

    if not clauses:
        return "", params
    return "WHERE " + " AND ".join(clauses), params


def list_repair_queue(
    conn: "SyncPostgresConnection",
    *,
    queue_status: object | None = "queued",
    repair_scope: object | None = None,
    run_id: object | None = None,
    solution_id: object | None = None,
    limit: object | None = 50,
) -> dict[str, Any]:
    """List durable repair queue items in priority order."""

    normalized_status = _normalize_status(queue_status)
    normalized_scope = _normalize_scope(repair_scope)
    normalized_run_id = _normalize_optional_text(run_id, field_name="run_id")
    normalized_solution_id = _normalize_optional_text(solution_id, field_name="solution_id")
    normalized_limit = _coerce_limit(limit)
    where_sql, params = _where_clause(
        queue_status=normalized_status,
        repair_scope=normalized_scope,
        run_id=normalized_run_id,
        solution_id=normalized_solution_id,
    )
    params.append(normalized_limit)
    rows = conn.execute(
        f"""SELECT repair_id, repair_scope, queue_status, auto_repair, priority,
                  solution_id, wave_id, workflow_id, run_id, job_id, job_label,
                  workflow_phase, spec_path, command_id, reason_code, failure_code,
                  failure_category, failure_zone, is_transient, repair_strategy,
                  retry_delta_required, source_kind, source_ref, evidence_kind,
                  evidence_ref, repair_dedupe_key, payload, claimed_by,
                  claim_expires_at, result_ref, repair_note, created_by_ref,
                  created_at, updated_at, claimed_at, started_at, completed_at
           FROM workflow_repair_queue
           {where_sql}
           ORDER BY priority ASC, created_at ASC, repair_id ASC
           LIMIT ${len(params)}""",
        *params,
    )
    items = [_row_to_payload(row) for row in (rows or [])]
    return {
        "status": "ok",
        "queue_status": normalized_status,
        "repair_scope": normalized_scope,
        "run_id": normalized_run_id,
        "solution_id": normalized_solution_id,
        "count": len(items),
        "items": items,
    }


def claim_repair(
    conn: "SyncPostgresConnection",
    *,
    claimed_by: object,
    repair_scope: object | None = None,
    claim_ttl_minutes: object | None = 30,
) -> dict[str, Any]:
    """Claim the next queued repair intent for one repair worker/agent."""

    normalized_claimed_by = _normalize_optional_text(claimed_by, field_name="claimed_by")
    if normalized_claimed_by is None:
        raise ValueError("claimed_by is required")
    normalized_scope = _normalize_scope(repair_scope)
    try:
        ttl_minutes = int(claim_ttl_minutes or 30)
    except (TypeError, ValueError) as exc:
        raise ValueError("claim_ttl_minutes must be an integer") from exc
    if ttl_minutes < 1 or ttl_minutes > 24 * 60:
        raise ValueError("claim_ttl_minutes must be between 1 and 1440")

    params: list[object] = []
    scope_clause = ""
    if normalized_scope is not None:
        params.append(normalized_scope)
        scope_clause = f"AND repair_scope = ${len(params)}"
    params.extend([normalized_claimed_by, ttl_minutes])
    claimed_by_index = len(params) - 1
    ttl_index = len(params)
    rows = conn.execute(
        f"""WITH candidate AS (
               SELECT repair_id
               FROM workflow_repair_queue
               WHERE queue_status = 'queued'
                 {scope_clause}
               ORDER BY priority ASC, created_at ASC, repair_id ASC
               LIMIT 1
               FOR UPDATE SKIP LOCKED
           )
           UPDATE workflow_repair_queue queue
           SET queue_status = 'claimed',
               claimed_by = ${claimed_by_index},
               claimed_at = now(),
               claim_expires_at = now() + make_interval(mins => ${ttl_index}::int)
           FROM candidate
           WHERE queue.repair_id = candidate.repair_id
           RETURNING queue.repair_id, queue.repair_scope, queue.queue_status,
                     queue.auto_repair, queue.priority, queue.solution_id, queue.wave_id,
                     queue.workflow_id, queue.run_id, queue.job_id, queue.job_label,
                     queue.workflow_phase, queue.spec_path, queue.command_id,
                     queue.reason_code, queue.failure_code, queue.failure_category,
                     queue.failure_zone, queue.is_transient, queue.repair_strategy,
                     queue.retry_delta_required, queue.source_kind, queue.source_ref,
                     queue.evidence_kind, queue.evidence_ref, queue.repair_dedupe_key,
                     queue.payload, queue.claimed_by, queue.claim_expires_at,
                     queue.result_ref, queue.repair_note, queue.created_by_ref,
                     queue.created_at, queue.updated_at, queue.claimed_at,
                     queue.started_at, queue.completed_at""",
        *params,
    )
    if not rows:
        return {
            "status": "empty",
            "repair_scope": normalized_scope,
            "claimed_by": normalized_claimed_by,
            "item": None,
        }
    return {
        "status": "claimed",
        "repair_scope": normalized_scope,
        "claimed_by": normalized_claimed_by,
        "item": _row_to_payload(rows[0]),
    }


def complete_repair(
    conn: "SyncPostgresConnection",
    *,
    repair_id: object,
    queue_status: object = "completed",
    result_ref: object | None = None,
    repair_note: object | None = None,
) -> dict[str, Any]:
    """Move a claimed/repairing repair intent to a terminal queue status."""

    normalized_repair_id = _normalize_optional_text(repair_id, field_name="repair_id")
    if normalized_repair_id is None:
        raise ValueError("repair_id is required")
    normalized_status = _normalize_status(queue_status)
    if normalized_status not in _TERMINAL_STATUSES:
        raise ValueError("queue_status must be completed, failed, cancelled, or superseded")
    normalized_result_ref = _normalize_optional_text(result_ref, field_name="result_ref")
    normalized_note = _normalize_optional_text(repair_note, field_name="repair_note")

    rows = conn.execute(
        """UPDATE workflow_repair_queue
           SET queue_status = $2,
               result_ref = COALESCE($3, result_ref),
               repair_note = COALESCE($4, repair_note),
               completed_at = now()
           WHERE repair_id = $1::uuid
             AND queue_status IN ('queued', 'claimed', 'repairing')
           RETURNING repair_id, repair_scope, queue_status, auto_repair, priority,
                     solution_id, wave_id, workflow_id, run_id, job_id, job_label,
                     workflow_phase, spec_path, command_id, reason_code, failure_code,
                     failure_category, failure_zone, is_transient, repair_strategy,
                     retry_delta_required, source_kind, source_ref, evidence_kind,
                     evidence_ref, repair_dedupe_key, payload, claimed_by,
                     claim_expires_at, result_ref, repair_note, created_by_ref,
                     created_at, updated_at, claimed_at, started_at, completed_at""",
        normalized_repair_id,
        normalized_status,
        normalized_result_ref,
        normalized_note,
    )
    if not rows:
        return {
            "status": "not_updated",
            "repair_id": normalized_repair_id,
            "queue_status": normalized_status,
        }
    return {
        "status": "updated",
        "repair_id": normalized_repair_id,
        "queue_status": normalized_status,
        "item": _row_to_payload(rows[0]),
    }


def release_repair(
    conn: "SyncPostgresConnection",
    *,
    repair_id: object,
    repair_note: object | None = None,
) -> dict[str, Any]:
    """Release a claimed/repairing repair intent back to the queued state."""

    normalized_repair_id = _normalize_optional_text(repair_id, field_name="repair_id")
    if normalized_repair_id is None:
        raise ValueError("repair_id is required")
    normalized_note = _normalize_optional_text(repair_note, field_name="repair_note")

    rows = conn.execute(
        """UPDATE workflow_repair_queue
           SET queue_status = 'queued',
               claimed_by = NULL,
               claimed_at = NULL,
               claim_expires_at = NULL,
               repair_note = COALESCE($2, repair_note)
           WHERE repair_id = $1::uuid
             AND queue_status IN ('claimed', 'repairing')
           RETURNING repair_id, repair_scope, queue_status, auto_repair, priority,
                     solution_id, wave_id, workflow_id, run_id, job_id, job_label,
                     workflow_phase, spec_path, command_id, reason_code, failure_code,
                     failure_category, failure_zone, is_transient, repair_strategy,
                     retry_delta_required, source_kind, source_ref, evidence_kind,
                     evidence_ref, repair_dedupe_key, payload, claimed_by,
                     claim_expires_at, result_ref, repair_note, created_by_ref,
                     created_at, updated_at, claimed_at, started_at, completed_at""",
        normalized_repair_id,
        normalized_note,
    )
    if not rows:
        return {
            "status": "not_updated",
            "repair_id": normalized_repair_id,
            "queue_status": "queued",
        }
    return {
        "status": "released",
        "repair_id": normalized_repair_id,
        "queue_status": "queued",
        "item": _row_to_payload(rows[0]),
    }


def repair_queue_summary(conn: "SyncPostgresConnection") -> dict[str, Any]:
    rows = conn.execute(
        """SELECT repair_scope, queue_status, COUNT(*) AS count
           FROM workflow_repair_queue
           GROUP BY repair_scope, queue_status
           ORDER BY repair_scope, queue_status"""
    )
    summary: dict[str, dict[str, int]] = {}
    total = 0
    for row in rows or []:
        scope = str(row["repair_scope"])
        status = str(row["queue_status"])
        count = int(row["count"] or 0)
        summary.setdefault(scope, {})[status] = count
        total += count
    return {"status": "ok", "total": total, "summary": summary}
