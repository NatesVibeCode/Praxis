"""Run-level status queries, health summaries, and recovery recommendations."""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from runtime.failure_projection import project_failure_classification
from runtime.idempotency import canonical_hash, check_idempotency, record_idempotency

from ._shared import (
    _ACTIVE_JOB_STATUSES,
    _TERMINAL_JOB_STATUSES,
    _WORKFLOW_TERMINAL_STATES,
    _json_loads_maybe,
    _workflow_run_envelope,
)
from ._workflow_state import (
    _block_descendants,
    _recompute_workflow_run_state,
    _reset_blocked_descendants_for_retry,
)
from ._claiming import _submission_state_by_job_label
from ._context_building import (
    _shadow_packet_inspection_from_rows,
    _terminal_failure_classification,
)

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)
_POLICY_REASON_CODES = frozenset({"provider_disabled", "route_disabled", "policy_blocked"})

__all__ = [
    "get_run_status",
    "summarize_run_health",
    "summarize_run_recovery",
    "inspect_job",
    "cancel_run",
    "retry_job",
    "wait_for_run",
]


# ── Status / Health / Recovery ──────────────────────────────────────────


_RUN_STATUS_QUERY = "SELECT * FROM workflow_runs WHERE run_id = $1"
_RUN_JOBS_QUERY = """SELECT id, label, agent_slug, resolved_agent, status, attempt, max_attempts,
              last_error_code, failure_category, failure_zone, is_transient,
              duration_ms, cost_usd, token_input, token_output, stdout_preview,
              created_at, ready_at, claimed_at, started_at, finished_at, heartbeat_at,
              next_retry_at, claimed_by
       FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at"""
_RUN_TOTALS_QUERY = (
    "SELECT COALESCE(SUM(cost_usd),0) as total_cost, "
    "COALESCE(SUM(token_input),0) as total_tokens_in, "
    "COALESCE(SUM(token_output),0) as total_tokens_out, "
    "COALESCE(SUM(duration_ms),0) as total_duration_ms "
    "FROM workflow_jobs WHERE run_id = $1"
)
_RUN_CANCELLATION_QUERY = """SELECT command_id, requested_by_ref, requested_at, payload
               FROM control_commands
               WHERE command_type = 'workflow.cancel'
                 AND payload::jsonb->>'run_id' = $1
               ORDER BY requested_at DESC LIMIT 1"""
_RUN_ACTIVITY_TIMESTAMP_KEYS = (
    "heartbeat_at",
    "started_at",
    "claimed_at",
    "ready_at",
    "finished_at",
)


def _load_run_row(conn: SyncPostgresConnection, run_id: str) -> dict[str, Any] | None:
    rows = conn.execute(_RUN_STATUS_QUERY, run_id)
    if not rows:
        return None
    return dict(rows[0])


def _load_workflow_jobs(conn: SyncPostgresConnection, run_id: str) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(_RUN_JOBS_QUERY, run_id)]


def _normalize_run_row_metadata(run_row: dict[str, Any], *, job_count: int) -> str:
    envelope = _workflow_run_envelope(run_row)
    status_value = run_row.get("current_state") or run_row.get("status") or "unknown"
    run_row["status"] = status_value
    run_row["spec_name"] = (
        envelope.get("name")
        or envelope.get("spec_name")
        or run_row.get("workflow_id", "")
    )
    run_row["phase"] = envelope.get("phase", "")
    run_row["total_jobs"] = int(
        envelope.get("total_jobs", run_row.get("total_jobs", job_count)) or job_count
    )
    if "requested_at" in run_row and "created_at" not in run_row:
        run_row["created_at"] = run_row["requested_at"]
    run_row["terminal_reason"] = run_row.get("terminal_reason_code")
    lineage = envelope.get("lineage")
    if not isinstance(lineage, dict):
        lineage = {}
    if not lineage.get("child_run_id"):
        lineage["child_run_id"] = run_row.get("run_id")
    if not lineage.get("child_workflow_id"):
        lineage["child_workflow_id"] = run_row.get("workflow_id")
    if "parent_run_id" not in lineage and envelope.get("parent_run_id") is not None:
        lineage["parent_run_id"] = envelope.get("parent_run_id")
    if "parent_job_label" not in lineage and envelope.get("parent_job_label") is not None:
        lineage["parent_job_label"] = envelope.get("parent_job_label")
    if "dispatch_reason" not in lineage and envelope.get("dispatch_reason") is not None:
        lineage["dispatch_reason"] = envelope.get("dispatch_reason")
    if "lineage_depth" not in lineage:
        lineage["lineage_depth"] = int(
            envelope.get(
                "lineage_depth",
                (int(envelope.get("trigger_depth", 0) or 0) + (1 if envelope.get("parent_run_id") else 0)),
            )
            or 0
        )
    run_row["parent_run_id"] = lineage.get("parent_run_id")
    run_row["parent_job_label"] = lineage.get("parent_job_label")
    run_row["dispatch_reason"] = lineage.get("dispatch_reason")
    run_row["lineage_depth"] = int(lineage.get("lineage_depth") or 0)
    run_row["lineage"] = lineage
    return status_value


def _attach_submission_state(
    job_rows: list[dict[str, Any]],
    submission_by_label: Mapping[str, dict[str, Any]],
) -> None:
    for job_row in job_rows:
        submission_state = submission_by_label.get(str(job_row.get("label") or "").strip())
        if not submission_state:
            continue
        job_row["submission"] = submission_state
        job_row["submission_id"] = submission_state.get("submission_id")
        job_row["submission_comparison_status"] = submission_state.get("comparison_status")
        job_row["submission_acceptance_status"] = submission_state.get("acceptance_status")
        latest_review = submission_state.get("latest_review")
        if isinstance(latest_review, dict):
            job_row["latest_submission_review_decision"] = latest_review.get("decision")


def _load_status_job_rows(
    conn: SyncPostgresConnection,
    *,
    run_row: dict[str, Any],
    run_id: str,
) -> tuple[list[dict[str, Any]], bool]:
    workflow_jobs = _load_workflow_jobs(conn, run_id)
    if workflow_jobs:
        submission_by_label = _submission_state_by_job_label(conn, run_id=run_id)
        _attach_submission_state(workflow_jobs, submission_by_label)
        return workflow_jobs, True
    return _graph_job_rows_from_evidence(run_row=run_row, run_id=run_id), False


def _apply_run_totals(
    conn: SyncPostgresConnection,
    *,
    run_row: dict[str, Any],
    run_id: str,
    has_workflow_jobs: bool,
    job_rows: list[dict[str, Any]],
) -> None:
    if has_workflow_jobs:
        agg = conn.execute(_RUN_TOTALS_QUERY, run_id)
        if agg:
            run_row["total_cost_usd"] = float(agg[0]["total_cost"])
            run_row["total_tokens_in"] = int(agg[0]["total_tokens_in"])
            run_row["total_tokens_out"] = int(agg[0]["total_tokens_out"])
            run_row["total_duration_ms"] = int(agg[0]["total_duration_ms"])
            return
    run_row["total_cost_usd"] = 0.0
    run_row["total_tokens_in"] = 0
    run_row["total_tokens_out"] = 0
    run_row["total_duration_ms"] = sum(int(job.get("duration_ms") or 0) for job in job_rows)


def _attach_packet_inspection(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
    run_row: dict[str, Any],
) -> None:
    packet_inspection = _shadow_packet_inspection_from_rows(
        conn,
        run_id=run_id,
        run_row=run_row,
    )
    if packet_inspection is not None:
        run_row["packet_inspection"] = packet_inspection


def _attach_cancellation_provenance(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
    run_row: dict[str, Any],
    status_value: str,
) -> None:
    if status_value != "cancelled":
        return
    try:
        cancel_rows = conn.execute(_RUN_CANCELLATION_QUERY, run_id)
        if cancel_rows:
            cancellation = cancel_rows[0]
            run_row["cancellation"] = {
                "command_id": cancellation["command_id"],
                "requested_by": cancellation["requested_by_ref"],
                "requested_at": (
                    str(cancellation["requested_at"])
                    if cancellation["requested_at"]
                    else None
                ),
            }
    except Exception:
        pass  # control_commands table may not exist in all environments


def get_run_status(conn: SyncPostgresConnection, run_id: str) -> dict | None:
    """Get run-level status with per-job breakdown."""
    run_row = _load_run_row(conn, run_id)
    if run_row is None:
        return None

    job_rows, has_workflow_jobs = _load_status_job_rows(conn, run_row=run_row, run_id=run_id)
    status_value = _normalize_run_row_metadata(run_row, job_count=len(job_rows))
    run_row["jobs"] = job_rows
    if not int(run_row.get("total_jobs", 0) or 0) and job_rows:
        run_row["total_jobs"] = len(job_rows)
    run_row["completed_jobs"] = sum(
        1 for j in job_rows
        if j["status"] in _TERMINAL_JOB_STATUSES
    )
    _apply_run_totals(
        conn,
        run_row=run_row,
        run_id=run_id,
        has_workflow_jobs=has_workflow_jobs,
        job_rows=job_rows,
    )
    _attach_packet_inspection(conn, run_id=run_id, run_row=run_row)
    _attach_cancellation_provenance(
        conn,
        run_id=run_id,
        run_row=run_row,
        status_value=status_value,
    )

    return run_row


def _graph_job_rows_from_evidence(*, run_row: dict, run_id: str) -> list[dict]:
    envelope = _workflow_run_envelope(run_row)
    raw_nodes = envelope.get("nodes")
    if not isinstance(raw_nodes, list):
        return []

    nodes = [
        node
        for node in raw_nodes
        if isinstance(node, dict) and not node.get("template_owner_node_id")
    ]
    if not nodes:
        return []

    latest_receipt_by_node: dict[str, object] = {}
    try:
        from receipts import ReceiptV1
        from runtime._workflow_database import resolve_runtime_database_url
        from storage.postgres import PostgresEvidenceReader

        reader = PostgresEvidenceReader(
            database_url=resolve_runtime_database_url(required=True),
        )
        for evidence_row in reader.evidence_timeline(run_id):
            record = evidence_row.record
            if (
                evidence_row.kind == "receipt"
                and isinstance(record, ReceiptV1)
                and isinstance(record.node_id, str)
                and record.node_id
            ):
                latest_receipt_by_node[record.node_id] = record
    except Exception:
        latest_receipt_by_node = {}

    ordered_nodes = sorted(nodes, key=lambda node: int(node.get("position_index") or 0))
    job_rows: list[dict] = []
    for index, node in enumerate(ordered_nodes, start=1):
        node_id = str(node.get("node_id") or "").strip()
        if not node_id:
            continue
        receipt = latest_receipt_by_node.get(node_id)
        if receipt is not None:
            started_at = receipt.started_at
            finished_at = receipt.finished_at
            duration_ms = 0
            if isinstance(started_at, datetime) and isinstance(finished_at, datetime):
                duration_ms = int((finished_at - started_at).total_seconds() * 1000)
            outputs = receipt.outputs if isinstance(receipt.outputs, dict) else {}
            stdout_preview = json.dumps(outputs, default=str)[:1000] if outputs else ""
            status = receipt.status
            attempt = int(receipt.attempt_no or 1)
            failure_code = receipt.failure_code or ""
        else:
            started_at = None
            finished_at = None
            duration_ms = 0
            stdout_preview = ""
            status = "pending"
            attempt = 0
            failure_code = ""
        job_rows.append(
            {
                "id": index,
                "label": node_id,
                "agent_slug": str(node.get("adapter_type") or ""),
                "resolved_agent": str(node.get("adapter_type") or ""),
                "status": status,
                "attempt": attempt,
                "max_attempts": 1,
                "last_error_code": failure_code,
                "failure_category": "",
                "failure_zone": "",
                "is_transient": False,
                "duration_ms": duration_ms,
                "cost_usd": 0.0,
                "token_input": 0,
                "token_output": 0,
                "stdout_preview": stdout_preview,
                "created_at": run_row.get("requested_at") or run_row.get("created_at"),
                "ready_at": None,
                "claimed_at": None,
                "started_at": started_at,
                "finished_at": finished_at,
                "heartbeat_at": None,
                "next_retry_at": None,
                "claimed_by": None,
                "display_name": str(node.get("display_name") or node_id),
                "node_type": str(node.get("node_type") or ""),
            }
        )
    return job_rows


def _seconds_since(value, now: datetime) -> float | None:
    if not isinstance(value, datetime):
        return None
    return (now - value).total_seconds()


def _classify_run_job_failure(job: dict) -> dict | None:
    raw_error_code = str(job.get("last_error_code") or "").strip()
    if raw_error_code in _POLICY_REASON_CODES:
        classification = _terminal_failure_classification(
            error_code=raw_error_code,
            stderr=str(job.get("stdout_preview") or ""),
            exit_code=job.get("exit_code"),
        )
        if classification is not None and hasattr(classification, "to_dict"):
            return classification.to_dict()
    failure_category = str(job.get("failure_category") or "").strip()
    if failure_category:
        return project_failure_classification(
            failure_category=failure_category,
            is_transient=bool(job.get("is_transient", False)),
            stdout_preview=str(job.get("stdout_preview") or ""),
        )
    classification = _terminal_failure_classification(
        error_code=raw_error_code,
        stderr=str(job.get("stdout_preview") or ""),
        exit_code=job.get("exit_code"),
    )
    if classification is not None and hasattr(classification, "to_dict"):
        return classification.to_dict()
    return None


def _healing_retry_candidates(run_data: dict[str, Any]) -> list[dict[str, Any]]:
    from runtime.self_healing import RecoveryAction, SelfHealingOrchestrator

    healer = SelfHealingOrchestrator()
    allowed_actions = {
        RecoveryAction.RETRY_SAME,
        RecoveryAction.RETRY_ESCALATED,
        RecoveryAction.FIX_AND_RETRY,
    }
    candidates: list[dict[str, Any]] = []
    for job in run_data.get("jobs", []):
        job_status = str(job.get("status") or "")
        if job_status not in {"failed", "dead_letter"}:
            continue
        label = str(job.get("label") or "").strip()
        if not label:
            continue
        stderr = str(job.get("stdout_preview") or "")
        failure_code = (
            str(job.get("last_error_code") or "").strip()
            or str(job.get("failure_category") or "").strip()
        )
        recommendation = healer.diagnose(label, failure_code, stderr)
        if recommendation.action not in allowed_actions or recommendation.confidence < 0.6:
            continue
        candidates.append(
            {
                "label": label,
                "status": job_status,
                "action": recommendation.action.value,
                "reason": recommendation.reason,
                "confidence": round(recommendation.confidence, 3),
                "resolved_failure_code": healer.resolve_failure_code(failure_code, stderr),
            }
        )
    return candidates


def _scan_jobs_for_health(
    jobs: list[dict[str, Any]],
    *,
    now: datetime,
    created_at: datetime | None,
) -> dict[str, Any]:
    completed_jobs = 0
    running_or_claimed = 0
    terminal_jobs = 0
    stalled_claim: list[str | None] = []
    stalled_wait: list[str | None] = []
    non_retryable_failed: list[str | None] = []
    last_activity_ts = created_at
    active_heartbeat_count = 0
    stale_heartbeat_count = 0

    for job in jobs:
        job_status = job.get("status")
        if job_status in ("succeeded", "failed", "dead_letter", "cancelled"):
            completed_jobs += 1
            terminal_jobs += 1
        if job_status in ("claimed", "running"):
            running_or_claimed += 1
            heartbeat_age = _seconds_since(job.get("heartbeat_at"), now)
            if heartbeat_age is not None:
                active_heartbeat_count += 1
            if heartbeat_age is None or heartbeat_age > 180:
                stalled_claim.append(job.get("label"))
                stale_heartbeat_count += 1

        if job_status in ("pending", "ready"):
            reference_time = job.get("ready_at") or job.get("created_at")
            reference_age = _seconds_since(reference_time, now)
            threshold = 900 if job_status == "pending" else 600
            if reference_age is not None and reference_age > threshold:
                stalled_wait.append(job.get("label"))

        if job_status in ("failed", "dead_letter"):
            classification = _classify_run_job_failure(job)
            if classification and not classification.get("is_retryable", True):
                non_retryable_failed.append(job.get("label"))

        for ts_key in _RUN_ACTIVITY_TIMESTAMP_KEYS:
            ts = job.get(ts_key)
            if ts is not None and isinstance(ts, datetime):
                if last_activity_ts is None or ts > last_activity_ts:
                    last_activity_ts = ts

    return {
        "completed_jobs": completed_jobs,
        "running_or_claimed": running_or_claimed,
        "terminal_jobs": terminal_jobs,
        "stalled_claim": stalled_claim,
        "stalled_wait": stalled_wait,
        "non_retryable_failed": non_retryable_failed,
        "last_activity_ts": last_activity_ts,
        "active_heartbeat_count": active_heartbeat_count,
        "stale_heartbeat_count": stale_heartbeat_count,
    }


# --------------------------------------------------------------------------
# First-failure summary
# --------------------------------------------------------------------------
# Receipts store the full completion JSON inside job.stdout_preview. For a
# failed run, the OPERATOR almost always wants a one-line "what actually went
# wrong and where" before diving into receipts. The signal below pulls that
# out so `run-status` shows it inline instead of requiring a psql excursion.

# Ordered by specificity: earlier keys win when multiple are present in the
# same JSON envelope. Paired: (json_key, trim_length). `result` is claude's
# human-readable message; `error` and `message` are generic fallbacks.
_FAILURE_HINT_FIELDS: tuple[tuple[str, int], ...] = (
    ("result", 300),
    ("error", 300),
    ("message", 300),
    ("stderr", 300),
)


def _extract_failure_hint(stdout_preview: str) -> str | None:
    """Pull a short human-readable failure reason from a job's stdout_preview.

    Operators see this in `run-status`. Keep it a single line, trimmed.
    Returns None when no hint could be extracted.
    """
    preview = (stdout_preview or "").strip()
    if not preview:
        return None
    # Most adapters stringify JSON into stdout_preview; try to parse first.
    try:
        parsed = json.loads(preview)
    except (json.JSONDecodeError, TypeError, ValueError):
        parsed = None
    if isinstance(parsed, Mapping):
        for key, limit in _FAILURE_HINT_FIELDS:
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                snippet = value.strip().splitlines()[0]
                return snippet[:limit] + ("…" if len(snippet) > limit else "")
        # Nested: some adapters wrap the completion JSON inside a "stdout" key
        nested = parsed.get("stdout")
        if isinstance(nested, str) and nested.strip():
            return _extract_failure_hint(nested)
        return None
    # Non-JSON: take the first non-empty line, trimmed.
    for line in preview.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:300] + ("…" if len(stripped) > 300 else "")
    return None


def _append_first_failure_signal(
    signals: list[dict[str, object]],
    *,
    status: str,
    jobs: list[dict[str, Any]] | None,
) -> None:
    """Add an inline summary of the first failed job so operators don't have
    to dig through receipts to learn what broke.

    Activates only on terminal-failure run status; the per-job stall/running
    signals already cover in-flight cases.
    """
    if status not in {"failed", "dead_letter"}:
        return
    if not jobs:
        return
    for job in jobs:
        job_status = str(job.get("status") or "")
        if job_status != "failed":
            continue
        label = str(job.get("label") or "").strip() or "<unknown>"
        failure_code = str(job.get("last_error_code") or "").strip()
        hint = _extract_failure_hint(str(job.get("stdout_preview") or ""))
        parts = [f"{label} failed"]
        if failure_code:
            parts.append(f"({failure_code})")
        message = " ".join(parts)
        if hint:
            message = f"{message}: {hint}"
        entry: dict[str, object] = {
            "type": "first_failed_node",
            "severity": "high",
            "message": message,
            "node_id": label,
        }
        if failure_code:
            entry["failure_code"] = failure_code
        if hint:
            entry["hint"] = hint
        signals.append(entry)
        return


def _append_terminal_status_signals(
    signals: list[dict[str, object]],
    *,
    status: str,
    run_data: dict[str, Any],
) -> None:
    if status == "failed":
        signals.append({
            "type": "terminal_failed",
            "severity": "critical",
            "message": "Run status is failed.",
        })
    if status == "dead_letter":
        signals.append({
            "type": "terminal_dead_letter",
            "severity": "critical",
            "message": "Run status is dead_letter.",
        })
    if status == "cancelled":
        cancel_info = run_data.get("cancellation")
        if cancel_info:
            message = (
                f"Run was cancelled by {cancel_info['requested_by']}"
                f" at {cancel_info['requested_at']}"
                f" (command {cancel_info['command_id']})"
            )
        else:
            message = "Run was cancelled."
        signals.append({
            "type": "terminal_cancelled",
            "severity": "medium",
            "message": message,
        })


def _append_job_stall_signals(
    signals: list[dict[str, object]],
    *,
    stalled_claim: list[str | None],
    stalled_wait: list[str | None],
) -> None:
    if stalled_claim:
        signals.append({
            "type": "stale_claimed_jobs",
            "severity": "high",
            "message": "Claimed/running jobs have stale or missing heartbeat.",
            "jobs": stalled_claim,
        })
    if stalled_wait:
        signals.append({
            "type": "stalled_dependency_wait",
            "severity": "medium",
            "message": "Jobs have been pending/ready without progress for an extended period.",
            "jobs": stalled_wait,
        })


def _append_running_state_signals(
    signals: list[dict[str, object]],
    *,
    status: str,
    total_jobs: int,
    elapsed_seconds: float | None,
    seconds_since_activity: float | None,
    completed_jobs: int,
    running_or_claimed: int,
    stale_heartbeat_count: int,
    active_heartbeat_count: int,
    stalled_claim: list[str | None],
    token_delta: float,
) -> None:
    if status != "running":
        return
    if (
        running_or_claimed == 0
        and completed_jobs == 0
        and total_jobs > 0
        and elapsed_seconds is not None
        and elapsed_seconds > 300
    ):
        signals.append({
            "type": "no_progress",
            "severity": "high",
            "message": "No jobs have completed and no jobs are actively running.",
        })
    if (
        stale_heartbeat_count > 0
        and active_heartbeat_count == 0
        and elapsed_seconds is not None
        and elapsed_seconds > 180
    ):
        signals.append({
            "type": "stalled_running_jobs",
            "severity": "high",
            "message": "Running/claimed jobs have stale heartbeat and no fresh activity.",
            "jobs": stalled_claim,
        })
    if (
        seconds_since_activity is not None
        and seconds_since_activity > 1800
        and completed_jobs < total_jobs
    ):
        signals.append({
            "type": "low_activity_window",
            "severity": "high",
            "message": "Run hasn't shown meaningful activity within expected window.",
            "seconds_since_last_activity": round(seconds_since_activity, 1),
        })
    if (
        token_delta == 0
        and elapsed_seconds is not None
        and elapsed_seconds > 900
        and running_or_claimed == 0
    ):
        signals.append({
            "type": "idle_token_progress",
            "severity": "medium",
            "message": "No token burn detected while active and no jobs are currently running.",
        })


def _severity_present(signals: list[dict[str, object]], severity: str) -> bool:
    return any(signal.get("severity") == severity for signal in signals)


def _health_state_from_signals(signals: list[dict[str, object]]) -> str:
    if _severity_present(signals, "critical"):
        return "critical"
    if _severity_present(signals, "high"):
        return "degraded"
    if signals:
        return "elevated"
    return "healthy"


def _likely_failed_from_signals(
    *,
    status: str,
    signals: list[dict[str, object]],
    total_jobs: int,
    completed_jobs: int,
) -> bool:
    if status in _WORKFLOW_TERMINAL_STATES and status != "succeeded":
        return True
    if _severity_present(signals, "critical"):
        return True
    if _severity_present(signals, "high"):
        return True
    if total_jobs > 0 and completed_jobs >= total_jobs:
        return False
    return False


def _build_health_telemetry(
    *,
    jobs: list[dict[str, Any]],
    token_delta: float,
    token_rate_per_min: float,
    duration_rate_ms_per_job: float,
    stale_heartbeat_count: int,
    seconds_since_activity: float | None,
) -> dict[str, object]:
    telemetry: dict[str, object] = {}
    if token_delta:
        telemetry["tokens_total"] = token_delta
    if token_rate_per_min:
        telemetry["tokens_per_minute"] = token_rate_per_min
    if duration_rate_ms_per_job:
        telemetry["avg_job_duration_ms"] = duration_rate_ms_per_job
    if stale_heartbeat_count:
        telemetry["stale_heartbeat_jobs"] = stale_heartbeat_count
    if jobs:
        telemetry["heartbeat_freshness"] = "degraded" if stale_heartbeat_count else "fresh"
    if seconds_since_activity is not None:
        telemetry["seconds_since_last_activity"] = round(seconds_since_activity, 1)
    return telemetry


def summarize_run_health(run_data: dict, now: datetime) -> dict:
    """Derive a durable health snapshot from workflow run rows."""
    created_at = run_data.get("created_at")
    elapsed_seconds = (
        _seconds_since(created_at, now)
        if isinstance(created_at, datetime)
        else None
    )
    total_jobs = int(run_data.get("total_jobs", 0) or 0)
    status = run_data.get("status", "unknown")
    jobs = run_data.get("jobs", [])

    token_delta = float(run_data.get("total_tokens_in", 0) or 0) + float(run_data.get("total_tokens_out", 0) or 0)
    duration_total_ms = int(run_data.get("total_duration_ms", 0) or 0)
    scan = _scan_jobs_for_health(
        jobs,
        now=now,
        created_at=created_at if isinstance(created_at, datetime) else None,
    )

    signals: list[dict[str, object]] = []

    token_rate_per_min = 0.0
    duration_rate_ms_per_job = 0.0
    if elapsed_seconds:
        token_rate_per_min = round(token_delta / max(elapsed_seconds, 1) * 60, 2)
    if int(scan["completed_jobs"]) > 0:
        duration_rate_ms_per_job = round(duration_total_ms / int(scan["completed_jobs"]), 1)

    last_activity_ts = scan["last_activity_ts"]
    seconds_since_activity = (
        _seconds_since(last_activity_ts, now)
        if isinstance(last_activity_ts, datetime)
        else None
    )

    _append_terminal_status_signals(signals, status=str(status), run_data=run_data)
    _append_first_failure_signal(signals, status=str(status), jobs=jobs)
    _append_job_stall_signals(
        signals,
        stalled_claim=list(scan["stalled_claim"]),
        stalled_wait=list(scan["stalled_wait"]),
    )
    _append_running_state_signals(
        signals,
        status=str(status),
        total_jobs=total_jobs,
        elapsed_seconds=elapsed_seconds,
        seconds_since_activity=seconds_since_activity,
        completed_jobs=int(scan["completed_jobs"]),
        running_or_claimed=int(scan["running_or_claimed"]),
        stale_heartbeat_count=int(scan["stale_heartbeat_count"]),
        active_heartbeat_count=int(scan["active_heartbeat_count"]),
        stalled_claim=list(scan["stalled_claim"]),
        token_delta=token_delta,
    )

    if scan["non_retryable_failed"]:
        signals.append({
            "type": "non_retryable_failures",
            "severity": "high",
            "message": "One or more jobs have non-retryable failures.",
            "jobs": scan["non_retryable_failed"],
        })

    likely_failed = _likely_failed_from_signals(
        status=str(status),
        signals=signals,
        total_jobs=total_jobs,
        completed_jobs=int(scan["completed_jobs"]),
    )
    state = _health_state_from_signals(signals)

    result = {
        "state": state,
        "likely_failed": bool(likely_failed),
        "signals": signals,
        "elapsed_seconds": round(elapsed_seconds, 1) if elapsed_seconds is not None else None,
        "completed_jobs": int(scan["completed_jobs"]),
        "running_or_claimed": int(scan["running_or_claimed"]),
        "terminal_jobs": int(scan["terminal_jobs"]),
    }

    telemetry = _build_health_telemetry(
        jobs=jobs,
        token_delta=token_delta,
        token_rate_per_min=token_rate_per_min,
        duration_rate_ms_per_job=duration_rate_ms_per_job,
        stale_heartbeat_count=int(scan["stale_heartbeat_count"]),
        seconds_since_activity=seconds_since_activity,
    )
    if telemetry:
        result["resource_telemetry"] = telemetry

    if scan["stalled_claim"]:
        result.setdefault("stalled_jobs", {})["claimed"] = list(scan["stalled_claim"])
    if scan["stalled_wait"]:
        result.setdefault("stalled_jobs", {})["pending_ready"] = list(scan["stalled_wait"])
    result["non_retryable_failed_jobs"] = list(scan["non_retryable_failed"])

    return result


def summarize_run_recovery(
    run_data: dict,
    health: dict,
    now: datetime,
    *,
    idle_threshold_seconds: int | None = None,
) -> dict:
    """Describe the explicit next action for a run status snapshot."""
    run_id = str(run_data.get("run_id") or "")
    status = str(run_data.get("status") or "unknown")
    threshold = idle_threshold_seconds if isinstance(idle_threshold_seconds, int) and idle_threshold_seconds > 0 else 900

    if status == "succeeded":
        return {
            "mode": "done",
            "reason": "Run already succeeded.",
            "recommended_tool": None,
        }

    if status in {"failed", "dead_letter", "cancelled"}:
        retry_candidates = _healing_retry_candidates(run_data)
        if retry_candidates and not health.get("non_retryable_failed_jobs"):
            if len(retry_candidates) == 1:
                candidate = retry_candidates[0]
                return {
                    "mode": "retry_failed_job",
                    "reason": candidate["reason"],
                    "heal_action": candidate["action"],
                    "resolved_failure_code": candidate["resolved_failure_code"],
                    "recommended_tool": {
                        "name": "praxis_workflow",
                        "arguments": {
                            "action": "retry",
                            "run_id": run_id,
                            "label": candidate["label"],
                        },
                    },
                }
            return {
                "mode": "repair_then_retry",
                "reason": (
                    "Run failed on healable frontier jobs. Repair the orchestration boundary "
                    "if needed, then retry the failed frontier jobs so cancelled descendants "
                    "can resume without discarding completed work."
                ),
                "retry_labels": [candidate["label"] for candidate in retry_candidates],
                "retry_candidates": retry_candidates,
                "recommended_tool": {
                    "name": "praxis_workflow",
                    "arguments": {
                        "action": "inspect",
                        "run_id": run_id,
                    },
                },
            }
        return {
            "mode": "inspect",
            "reason": f"Run is terminal with status '{status}'. Inspect before retrying.",
            "recommended_tool": {
                "name": "praxis_workflow",
                "arguments": {
                    "action": "inspect",
                    "run_id": run_id,
                },
            },
        }

    if status == "running" and health.get("state") in {"degraded", "critical"}:
        signal_reason = next(
            (
                str(signal.get("message") or "")
                for signal in health.get("signals", [])
                if isinstance(signal, dict) and signal.get("message")
            ),
            "",
        )
        elapsed_seconds = health.get("elapsed_seconds")
        if (
            int(health.get("running_or_claimed", 0) or 0) == 0
            and elapsed_seconds is not None
            and float(elapsed_seconds) >= threshold
        ):
            return {
                "mode": "kill_if_idle",
                "reason": signal_reason or f"Run has been idle for {int(float(elapsed_seconds))}s and is unhealthy.",
                "idle_threshold_seconds": threshold,
                "recommended_tool": {
                    "name": "praxis_workflow",
                    "arguments": {
                        "action": "status",
                        "run_id": run_id,
                        "kill_if_idle": True,
                        "idle_threshold_seconds": threshold,
                    },
                },
            }

        return {
            "mode": "monitor",
            "reason": signal_reason or "Run is unhealthy but still has active work; keep polling or inspect the live jobs.",
            "recommended_tool": {
                "name": "praxis_workflow",
                "arguments": {
                    "action": "status",
                    "run_id": run_id,
                },
            },
        }

    if status == "queued":
        if health.get("state") in {"degraded", "critical"}:
            return {
                "mode": "inspect",
                "reason": "Queued run already shows stalled dependency waits or failures; inspect before retrying.",
                "recommended_tool": {
                    "name": "praxis_workflow",
                    "arguments": {
                        "action": "inspect",
                        "run_id": run_id,
                    },
                },
            }
        return {
            "mode": "monitor",
            "reason": "Run is queued and waiting for a claim.",
            "recommended_tool": {
                "name": "praxis_workflow",
                "arguments": {
                    "action": "status",
                    "run_id": run_id,
                },
            },
        }

    if status == "running":
        return {
            "mode": "monitor",
            "reason": "Run is active and healthy.",
            "recommended_tool": {
                "name": "praxis_workflow",
                "arguments": {
                    "action": "status",
                    "run_id": run_id,
                },
            },
        }

    return {
        "mode": "monitor",
        "reason": f"Run is in status '{status}'.",
        "recommended_tool": {
            "name": "praxis_workflow",
            "arguments": {
                "action": "status",
                "run_id": run_id,
            },
        },
    }


def inspect_job(conn: SyncPostgresConnection, run_id: str, label: str | None = None) -> dict:
    if label:
        rows = conn.execute(
            'SELECT * FROM workflow_jobs WHERE run_id = $1 AND label = $2', run_id, label)
    else:
        rows = conn.execute(
            'SELECT * FROM workflow_jobs WHERE run_id = $1 ORDER BY created_at', run_id)
    if not rows:
        run_rows = conn.execute('SELECT * FROM workflow_runs WHERE run_id = $1', run_id)
        if not run_rows:
            return {'error': 'not_found'}
        graph_jobs = _graph_job_rows_from_evidence(run_row=dict(run_rows[0]), run_id=run_id)
        if label:
            graph_jobs = [job for job in graph_jobs if str(job.get("label") or "").strip() == label]
        if not graph_jobs:
            return {'error': 'not_found'}
        return {'run_id': run_id, 'jobs': graph_jobs}
    submission_by_label = _submission_state_by_job_label(conn, run_id=run_id)
    jobs = []
    for r in rows:
        j = dict(r)
        submission_state = submission_by_label.get(str(j.get("label") or "").strip())
        if submission_state:
            j["submission"] = submission_state
        # Add computed heartbeat freshness
        if j.get('heartbeat_at'):
            from datetime import datetime, timezone
            age = (datetime.now(timezone.utc) - j['heartbeat_at']).total_seconds()
            j['heartbeat_age_seconds'] = round(age, 1)
            j['heartbeat_fresh'] = age < 60
        # Serialize timestamps
        for k in ('created_at','ready_at','claimed_at','started_at','finished_at','heartbeat_at','next_retry_at'):
            if j.get(k):
                j[k] = j[k].isoformat()
        jobs.append(j)
    return {'run_id': run_id, 'jobs': jobs}


def cancel_run(
    conn: SyncPostgresConnection,
    run_id: str,
    *,
    include_running: bool = False,
) -> dict:
    """Cancel all non-terminal jobs in a run and update run status."""
    job_statuses = ("pending", "ready", "claimed")
    if include_running:
        job_statuses = (*job_statuses, "running")
    status_sql = ", ".join(f"'{status}'" for status in job_statuses)

    rows = conn.execute(
        f"""UPDATE workflow_jobs SET status = 'cancelled', finished_at = now()
           WHERE run_id = $1 AND status IN ({status_sql})
           RETURNING id, label""",
        run_id,
    )
    cancelled = [dict(r) for r in (rows or [])]
    run_status = _recompute_workflow_run_state(conn, run_id)
    return {
        "run_id": run_id,
        "cancelled_jobs": len(cancelled),
        "labels": [r["label"] for r in cancelled],
        "run_status": run_status,
    }


def retry_job(conn: SyncPostgresConnection, run_id: str, label: str) -> dict:
    """Re-queue a single failed job and resume the run."""
    from ._admission import (
        IdempotencyConflict,
        _enforce_queue_admission,
        _retry_packet_reuse_provenance,
    )

    from runtime.compile_artifacts import CompileArtifactStore

    idempotency_key = f"{run_id}:{label}:retry"
    payload_hash = canonical_hash({"run_id": run_id, "label": label})
    result = check_idempotency(conn, "workflow.retry", idempotency_key, payload_hash)
    if result.is_replay:
        logger.info("Idempotent replay: returning existing run_id=%s", result.existing_run_id)
        return {'run_id': run_id, 'label': label, 'status': 'replayed', 'replayed': True}
    if result.is_conflict:
        logger.warning("Idempotency conflict: key=%s exists with different payload", idempotency_key)
        raise IdempotencyConflict(idempotency_key, result.existing_run_id, result.created_at)
    packet_reuse_provenance = _retry_packet_reuse_provenance(conn, run_id=run_id)
    _enforce_queue_admission(conn, job_count=1)

    rows = conn.execute(
        """UPDATE workflow_jobs
           SET status = 'ready', ready_at = now(),
               last_error_code = NULL, claimed_by = NULL, claimed_at = NULL,
               finished_at = NULL, started_at = NULL, heartbeat_at = NULL, next_retry_at = NULL,
               exit_code = NULL, failure_category = '', failure_zone = '', is_transient = false,
               stdout_preview = NULL, receipt_id = NULL, output_path = NULL,
               duration_ms = 0, token_input = 0, token_output = 0, cost_usd = 0
           WHERE run_id = $1 AND label = $2 AND status IN ('failed', 'dead_letter', 'cancelled')
           RETURNING id, label, attempt""",
        run_id, label,
    )
    if not rows:
        return {'error': f'No retryable job found: {run_id}/{label}'}
    job = dict(rows[0])
    _reset_blocked_descendants_for_retry(conn, int(job["id"]))
    _recompute_workflow_run_state(conn, run_id)
    record_idempotency(conn, "workflow.retry", idempotency_key, payload_hash, run_id=run_id)
    return {
        'run_id': run_id,
        'label': label,
        'status': 'requeued',
        'attempt': job['attempt'],
        'packet_reuse_provenance': packet_reuse_provenance,
    }


def wait_for_run(
    conn: SyncPostgresConnection,
    run_id: str,
    timeout: float = 600,
    poll_interval: float = 3.0,
) -> dict:
    """Block until all jobs in a run are terminal or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = get_run_status(conn, run_id)
        if status is None:
            return {"error": f"Run {run_id} not found"}
        if status["status"] in _WORKFLOW_TERMINAL_STATES:
            return status
        time.sleep(poll_interval)
    return {"error": f"Timeout waiting for run {run_id}", "partial": get_run_status(conn, run_id)}
