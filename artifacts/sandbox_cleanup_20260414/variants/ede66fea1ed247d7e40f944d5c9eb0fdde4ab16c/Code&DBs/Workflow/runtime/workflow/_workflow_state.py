"""Workflow authority management and dependency state for workflow runs."""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ._shared import (
    _ACTIVE_JOB_STATUSES,
    _BLOCKING_PARENT_STATUSES,
    _TERMINAL_JOB_STATUSES,
    _WORKFLOW_TERMINAL_STATES,
    _definition_version_for_hash,
    _json_loads_maybe,
    _workflow_id_for_spec,
    _workflow_run_envelope,
)
from ._routing import _build_request_envelope

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

__all__ = [
    "_workflow_row_reuse_authority",
    "_ensure_workflow_authority",
    "_workflow_run_envelope",
    "_release_ready_children",
    "_block_descendants",
    "_recompute_workflow_run_state",
    "_reset_blocked_descendants_for_retry",
]


def _workflow_row_reuse_authority(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    allowed = {}
    for field_name in ("id", "name"):
        if field_name in value:
            allowed[field_name] = value[field_name]
    return allowed




def _ensure_workflow_authority(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
    spec,
    raw_snapshot: dict,
    now: datetime,
    parent_run_id: str | None,
    trigger_depth: int,
) -> dict[str, str]:
    workflow_id = _workflow_id_for_spec(spec)
    request_envelope = _build_request_envelope(
        spec,
        raw_snapshot=raw_snapshot,
        workflow_id=workflow_id,
        total_jobs=len(spec.jobs),
        parent_run_id=parent_run_id,
        trigger_depth=trigger_depth,
    )
    definition_hash = hashlib.sha256(
        json.dumps(raw_snapshot, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    workflow_definition_id = f"workflow_def:{definition_hash[:24]}"
    definition_version = _definition_version_for_hash(definition_hash)
    request_id = f"req_{run_id}"
    admission_decision_id = f"workflow_admission:{run_id}"
    request_digest = definition_hash[:32]
    authority_context_digest = hashlib.sha256(f"{run_id}:{definition_hash}".encode("utf-8")).hexdigest()[:32]

    conn.execute(
        """INSERT INTO workflow_definitions (
               workflow_definition_id, workflow_id, schema_version, definition_version,
               definition_hash, status, request_envelope, normalized_definition, created_at
           ) VALUES ($1, $2, 1, $3, $4, 'active', $5::jsonb, $6::jsonb, $7)
           ON CONFLICT (workflow_definition_id) DO NOTHING""",
        workflow_definition_id,
        workflow_id,
        definition_version,
        definition_hash,
        json.dumps(request_envelope),
        json.dumps({"jobs": len(spec.jobs), "phase": getattr(spec, "phase", ""), "type": "workflow"}),
        now,
    )
    conn.execute(
        """INSERT INTO admission_decisions (
               admission_decision_id, workflow_id, request_id, decision, reason_code,
               decided_at, decided_by, policy_snapshot_ref, validation_result_ref, authority_context_ref
           ) VALUES ($1, $2, $3, 'admit', 'workflow_submit', $4, 'workflow_runtime',
                     'workflow_runtime', 'workflow_runtime', 'workflow_runtime')
           ON CONFLICT (admission_decision_id) DO NOTHING""",
        admission_decision_id,
        workflow_id,
        request_id,
        now,
    )
    conn.execute(
        """INSERT INTO workflow_runs (
               run_id, workflow_id, request_id, request_digest, authority_context_digest,
               workflow_definition_id, admitted_definition_hash, run_idempotency_key,
               schema_version, request_envelope, context_bundle_id, admission_decision_id,
               current_state, requested_at, admitted_at
           ) VALUES (
               $1, $2, $3, $4, $5, $6, $7, $8,
               1, $9::jsonb, $10, $11, 'queued', $12, $12
           )
           ON CONFLICT (run_id) DO UPDATE
           SET workflow_id = EXCLUDED.workflow_id,
               request_envelope = EXCLUDED.request_envelope""",
        run_id,
        workflow_id,
        request_id,
        request_digest,
        authority_context_digest,
        workflow_definition_id,
        definition_hash,
        run_id,
        json.dumps(request_envelope),
        f"context:{run_id}",
        admission_decision_id,
        now,
    )
    return {"workflow_id": workflow_id, "request_id": request_id}


def _release_ready_children(conn: SyncPostgresConnection, parent_job_id: int) -> None:
    child_rows = conn.execute(
        """SELECT child.id, child.dependency_threshold
           FROM workflow_job_edges edge
           JOIN workflow_jobs child ON child.id = edge.child_id
           WHERE edge.parent_id = $1""",
        parent_job_id,
    )
    for row in child_rows or []:
        child_id = int(row["id"])
        threshold = row.get("dependency_threshold")
        parent_states = conn.execute(
            """SELECT parent.status
               FROM workflow_job_edges edge
               JOIN workflow_jobs parent ON parent.id = edge.parent_id
               WHERE edge.child_id = $1""",
            child_id,
        )
        statuses = [str(parent["status"]) for parent in parent_states or []]
        if not statuses:
            continue

        if threshold is not None:
            # Threshold mode: release when enough parents have succeeded
            succeeded = sum(1 for s in statuses if s == "succeeded")
            if succeeded >= int(threshold):
                conn.execute(
                    """UPDATE workflow_jobs
                       SET status = 'ready', ready_at = now()
                       WHERE id = $1 AND status = 'pending'""",
                    child_id,
                )
        else:
            # Default: all parents must succeed
            if set(statuses) <= {"succeeded"}:
                conn.execute(
                    """UPDATE workflow_jobs
                       SET status = 'ready', ready_at = now()
                       WHERE id = $1 AND status = 'pending'""",
                    child_id,
                )


def _block_descendants(conn: SyncPostgresConnection, parent_job_id: int, error_code: str) -> None:
    child_rows = conn.execute(
        """SELECT child.id, child.dependency_threshold
           FROM workflow_job_edges edge
           JOIN workflow_jobs child ON child.id = edge.child_id
           WHERE edge.parent_id = $1""",
        parent_job_id,
    )
    for row in child_rows or []:
        child_id = int(row["id"])
        threshold = row.get("dependency_threshold")

        if threshold is not None:
            # Threshold mode: only block if it's impossible to reach the threshold.
            # Count how many parents can still succeed (pending/ready/claimed/running/succeeded).
            parent_states = conn.execute(
                """SELECT parent.status
                   FROM workflow_job_edges edge
                   JOIN workflow_jobs parent ON parent.id = edge.parent_id
                   WHERE edge.child_id = $1""",
                child_id,
            )
            statuses = [str(p["status"]) for p in parent_states or []]
            can_still_succeed = sum(
                1 for s in statuses
                if s in ("pending", "ready", "claimed", "running", "succeeded")
            )
            if can_still_succeed >= int(threshold):
                continue  # Still possible to reach threshold — don't block yet

        updated = conn.execute(
            """UPDATE workflow_jobs
               SET status = 'blocked',
                   finished_at = now(),
                   last_error_code = COALESCE(last_error_code, $2)
               WHERE id = $1
                 AND status NOT IN ('succeeded', 'failed', 'dead_letter', 'cancelled', 'blocked')
               RETURNING id""",
            child_id,
            error_code or "dependency_blocked",
        )
        if updated:
            _block_descendants(conn, child_id, error_code or "dependency_blocked")


def _recompute_workflow_run_state(conn: SyncPostgresConnection, run_id: str) -> str:
    rows = conn.execute(
        """SELECT status, COUNT(*) AS count
           FROM workflow_jobs
           WHERE run_id = $1
           GROUP BY status""",
        run_id,
    )
    counts = {str(row["status"]): int(row["count"]) for row in rows or []}
    total_jobs = sum(counts.values())
    active_jobs = sum(counts.get(status, 0) for status in _ACTIVE_JOB_STATUSES)
    pending_jobs = counts.get("pending", 0) + counts.get("ready", 0)

    run_rows = conn.execute(
        """SELECT current_state, workflow_id, request_envelope,
                  started_at, admitted_at, requested_at
           FROM workflow_runs
           WHERE run_id = $1""",
        run_id,
    )
    run_row = run_rows[0] if run_rows else {}
    prior_state = str(run_row.get("current_state") or "").strip()
    started_at = run_row.get("started_at") if run_row else None

    failed_jobs = counts.get("failed", 0)
    blocked_jobs = counts.get("blocked", 0)
    succeeded_jobs = counts.get("succeeded", 0)
    dead_letter_jobs = counts.get("dead_letter", 0)
    cancelled_jobs = counts.get("cancelled", 0)

    if total_jobs == 0:
        new_state = "failed"
        terminal_reason = "no_jobs"
    elif dead_letter_jobs > 0:
        new_state = "dead_letter"
        terminal_reason = "job_dead_lettered"
    elif failed_jobs > 0:
        # Only hard failures count — blocked jobs from unselected branches are not failures
        new_state = "failed"
        terminal_reason = "job_failed"
    elif cancelled_jobs > 0 and active_jobs == 0 and pending_jobs == 0:
        new_state = "cancelled"
        terminal_reason = "workflow_cancelled"
    elif succeeded_jobs > 0 and active_jobs == 0 and pending_jobs == 0:
        # All active-path jobs succeeded; blocked jobs are branch-skipped, not failures
        new_state = "succeeded"
        terminal_reason = "all_jobs_succeeded"
    elif active_jobs > 0:
        new_state = "running"
        terminal_reason = None
    elif pending_jobs > 0:
        new_state = "queued"
        terminal_reason = None
    else:
        new_state = "queued"
        terminal_reason = None

    conn.execute(
        """UPDATE workflow_runs
           SET current_state = $2,
               started_at = CASE
                   WHEN $2 IN ('running', 'succeeded', 'failed', 'dead_letter', 'cancelled')
                        AND started_at IS NULL
                   THEN COALESCE(admitted_at, requested_at, now())
                   ELSE started_at
               END,
               finished_at = CASE
                   WHEN $2 IN ('succeeded', 'failed', 'dead_letter', 'cancelled')
                   THEN GREATEST(COALESCE(started_at, admitted_at, requested_at, now()), now())
                   ELSE NULL
               END,
               terminal_reason_code = CASE
                   WHEN $2 IN ('succeeded', 'failed', 'dead_letter', 'cancelled') THEN $3
                   ELSE NULL
               END
           WHERE run_id = $1""",
        run_id,
        new_state,
        terminal_reason,
    )

    if prior_state != new_state and new_state in _WORKFLOW_TERMINAL_STATES:
        workflow_id = str(run_row.get("workflow_id") or "")
        request_envelope = _json_loads_maybe(run_row.get("request_envelope"), {}) or {}
        payload = {
            "run_id": run_id,
            "workflow_id": workflow_id,
            "status": new_state,
            "reason_code": terminal_reason,
            "total_jobs": total_jobs,
            "succeeded": counts.get("succeeded", 0),
            "failed": counts.get("failed", 0) + counts.get("dead_letter", 0),
            "blocked": counts.get("blocked", 0),
            "cancelled": counts.get("cancelled", 0),
            "parent_run_id": request_envelope.get("parent_run_id"),
            "trigger_depth": request_envelope.get("trigger_depth", 0),
        }
        lifecycle_event_type = "workflow.completed" if new_state == "succeeded" else "workflow.failed"
        compatibility_event_type = "run.succeeded" if new_state == "succeeded" else "run.failed"
        event_payload = json.dumps(payload, default=str)
        conn.execute(
            """INSERT INTO system_events (event_type, source_id, source_type, payload)
               VALUES ($1, $2, 'workflow_run', $3::jsonb)""",
            lifecycle_event_type,
            run_id,
            event_payload,
        )
        conn.execute(
            """INSERT INTO system_events (event_type, source_id, source_type, payload)
               VALUES ($1, $2, 'workflow_run', $3::jsonb)""",
            compatibility_event_type,
            run_id,
            event_payload,
        )
        conn.execute("SELECT pg_notify('run_complete', $1)", run_id)
    return new_state


def _reset_blocked_descendants_for_retry(conn: SyncPostgresConnection, parent_job_id: int) -> None:
    """Reset downstream jobs so they re-run when the retried parent succeeds.

    Handles both 'blocked' (never started) and 'cancelled' (killed by
    parent_failed propagation) — BUG-9A8B3651.
    """
    child_rows = conn.execute(
        """SELECT child.id
           FROM workflow_job_edges edge
           JOIN workflow_jobs child ON child.id = edge.child_id
           WHERE edge.parent_id = $1""",
        parent_job_id,
    )
    for row in child_rows or []:
        child_id = int(row["id"])
        updated = conn.execute(
            """UPDATE workflow_jobs
               SET status = 'pending',
                   finished_at = NULL,
                   ready_at = NULL,
                   last_error_code = NULL,
                   failure_category = '',
                   failure_zone = '',
                   is_transient = false
               WHERE id = $1 AND status IN ('blocked', 'cancelled')
               RETURNING id""",
            child_id,
        )
        if updated:
            _reset_blocked_descendants_for_retry(conn, child_id)
