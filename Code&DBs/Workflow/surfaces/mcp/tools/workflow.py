"""Tools: praxis_workflow, praxis_workflow_validate."""
from __future__ import annotations

import json
from datetime import datetime, timezone
import threading
import uuid
from typing import Any

from storage.postgres.connection import resolve_workflow_database_url
from storage.postgres.validators import PostgresConfigurationError

from runtime.failure_projection import project_failure_classification
from runtime.claims import ClaimLeaseProposalSnapshot
from runtime.domain import RunState
from runtime.subscriptions import (
    WorkerInboxFact,
    WorkerSubscriptionBatch,
    WorkerSubscriptionCursor,
)
from surfaces.workflow_bridge import WorkflowBridge, WorkflowClaimableWork, build_live_workflow_bridge
from ..subsystems import _subs, REPO_ROOT
from ..helpers import _serialize


_TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "dead_letter"}
_RUN_STREAM_PATH = "/api/workflow-runs/{run_id}/stream"
_RUN_STATUS_PATH = "/api/workflow-runs/{run_id}/status"
_STALE_HEARTBEAT_SECONDS = 180
_STALE_READY_SECONDS = 600
_STALE_PENDING_SECONDS = 900
_NO_PROGRESS_SECONDS = 300
_IDLE_PROGRESS_SECONDS = 900
_RUN_ACTIVITY_LOOKBACK_SECONDS = 1800
_POLL_INTERVAL_MIN_SECONDS = 1.0
_POLL_INTERVAL_MAX_SECONDS = 12.0


def _workflow_spec_mod():
    import runtime.workflow_spec as spec_mod

    return spec_mod


def _workflow_database_url() -> str:
    postgres_env = getattr(_subs, "_postgres_env", None)
    env: dict[str, str] = {}
    if callable(postgres_env):
        try:
            env = dict(postgres_env() or {})
        except Exception:
            env = {}
    try:
        if env.get("WORKFLOW_DATABASE_URL"):
            return resolve_workflow_database_url(env=env)
        return resolve_workflow_database_url()
    except PostgresConfigurationError as exc:
        raise RuntimeError("WORKFLOW_DATABASE_URL is required to inspect workflow bridge state") from exc


def _build_workflow_bridge() -> WorkflowBridge:
    """Build the real workflow bridge over live Postgres-backed authorities."""

    return build_live_workflow_bridge(_workflow_database_url())


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc


def _datetime_or_none(value: Any, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO datetime string") from exc


def _run_state_from_value(value: Any) -> RunState:
    text = str(value or "").strip()
    if text.startswith("RunState."):
        text = text.split(".", 1)[1].lower()
    return RunState(text)


def _deserialize_claimable_work(payload: dict[str, Any]) -> WorkflowClaimableWork:
    route_snapshot_data = dict(payload.get("route_snapshot") or {})
    inbox_batch_data = dict(payload.get("inbox_batch") or {})

    route_snapshot = ClaimLeaseProposalSnapshot(
        run_id=str(route_snapshot_data.get("run_id") or ""),
        workflow_id=str(route_snapshot_data.get("workflow_id") or ""),
        request_id=str(route_snapshot_data.get("request_id") or ""),
        current_state=_run_state_from_value(route_snapshot_data.get("current_state")),
        claim_id=str(route_snapshot_data.get("claim_id") or ""),
        lease_id=_text_or_none(route_snapshot_data.get("lease_id")),
        proposal_id=_text_or_none(route_snapshot_data.get("proposal_id")),
        attempt_no=int(route_snapshot_data.get("attempt_no") or 0),
        transition_seq=int(route_snapshot_data.get("transition_seq") or 0),
        sandbox_group_id=_text_or_none(route_snapshot_data.get("sandbox_group_id")),
        sandbox_session_id=_text_or_none(route_snapshot_data.get("sandbox_session_id")),
        share_mode=str(route_snapshot_data.get("share_mode") or ""),
        reuse_reason_code=_text_or_none(route_snapshot_data.get("reuse_reason_code")),
        last_event_id=_text_or_none(route_snapshot_data.get("last_event_id")),
    )

    cursor_data = dict(inbox_batch_data.get("cursor") or {})
    next_cursor_data = dict(inbox_batch_data.get("next_cursor") or {})
    cursor = WorkerSubscriptionCursor(
        subscription_id=str(cursor_data.get("subscription_id") or ""),
        run_id=str(cursor_data.get("run_id") or ""),
        last_acked_evidence_seq=_int_or_none(
            cursor_data.get("last_acked_evidence_seq"),
            field_name="work.inbox_batch.cursor.last_acked_evidence_seq",
        ),
    )
    next_cursor = WorkerSubscriptionCursor(
        subscription_id=str(next_cursor_data.get("subscription_id") or cursor.subscription_id),
        run_id=str(next_cursor_data.get("run_id") or cursor.run_id),
        last_acked_evidence_seq=_int_or_none(
            next_cursor_data.get("last_acked_evidence_seq"),
            field_name="work.inbox_batch.next_cursor.last_acked_evidence_seq",
        ),
    )

    facts = []
    for fact_data in inbox_batch_data.get("facts") or []:
        if not isinstance(fact_data, dict):
            continue
        facts.append(
            WorkerInboxFact(
                inbox_fact_id=str(fact_data.get("inbox_fact_id") or ""),
                subscription_id=str(fact_data.get("subscription_id") or cursor.subscription_id),
                authority_table=str(fact_data.get("authority_table") or ""),
                authority_id=str(fact_data.get("authority_id") or ""),
                envelope_kind=str(fact_data.get("envelope_kind") or ""),
                workflow_id=str(fact_data.get("workflow_id") or ""),
                run_id=str(fact_data.get("run_id") or cursor.run_id),
                request_id=str(fact_data.get("request_id") or ""),
                evidence_seq=int(fact_data.get("evidence_seq") or 0),
                transition_seq=int(fact_data.get("transition_seq") or 0),
                authority_recorded_at=_datetime_or_none(
                    fact_data.get("authority_recorded_at"),
                    field_name="work.inbox_batch.facts[].authority_recorded_at",
                )
                or datetime.now(timezone.utc),
                envelope=dict(fact_data.get("envelope") or {}),
            )
        )

    return WorkflowClaimableWork(
        route_snapshot=route_snapshot,
        inbox_batch=WorkerSubscriptionBatch(
            cursor=cursor,
            next_cursor=next_cursor,
            facts=tuple(facts),
            has_more=bool(inbox_batch_data.get("has_more", False)),
        ),
        claimable=bool(payload.get("claimable", False)),
    )


def _normalize_run_state_strings(value: Any) -> Any:
    if isinstance(value, dict):
        normalized = {key: _normalize_run_state_strings(item) for key, item in value.items()}
        route_snapshot = normalized.get("route_snapshot")
        if isinstance(route_snapshot, dict):
            current_state = route_snapshot.get("current_state")
            if isinstance(current_state, str) and current_state.startswith("RunState."):
                route_snapshot["current_state"] = current_state.split(".", 1)[1].lower()
        return normalized
    if isinstance(value, list):
        return [_normalize_run_state_strings(item) for item in value]
    return value


def _structured_runtime_error(exc: Exception, *, action: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error": str(exc),
        "error_code": getattr(exc, "reason_code", f"workflow.{action}.failed"),
    }
    details = getattr(exc, "details", None)
    if isinstance(details, dict) and details:
        payload["details"] = details
    return payload


def _load_pg_conn(*, action: str) -> tuple[Any | None, dict[str, Any] | None]:
    """Return the workflow Postgres connection or a structured runtime error."""
    try:
        pg = _subs.get_pg_conn()
    except Exception as exc:
        return None, _structured_runtime_error(exc, action=action)
    if pg is None:
        return None, {
            "error": "Database connection not available",
            "error_code": f"workflow.{action}.unavailable",
        }
    return pg, None


# ---------------------------------------------------------------------------
# Streaming helpers
# ---------------------------------------------------------------------------

def _run_links(run_id: str) -> dict[str, str]:
    """Return MCP-friendly links for run status and streaming."""
    return {
        "stream_url": _RUN_STREAM_PATH.format(run_id=run_id),
        "status_url": _RUN_STATUS_PATH.format(run_id=run_id),
    }


def _delivery_metadata(*, emitter: Any | None = None, wait_requested: bool | None = None) -> dict[str, Any]:
    """Describe how dashboard/progress updates are being delivered."""
    progress_requested = bool(getattr(emitter, "enabled", False))
    message_channel = emitter is not None
    if progress_requested:
        live_channel = "notifications.message+notifications.progress"
    elif message_channel:
        live_channel = "notifications.message"
    else:
        live_channel = "none"

    payload: dict[str, Any] = {
        "dashboard_in_payload": True,
        "live_channel": live_channel,
        "message_notifications": message_channel,
        "progress_notifications": progress_requested,
    }
    if wait_requested is not None:
        payload["wait_requested"] = wait_requested
        payload["inline_polling"] = bool(wait_requested and progress_requested)
    return payload


def _classify_job_failure(job: dict) -> dict[str, Any] | None:
    """Classify a failed job and return machine-readable failure metadata."""
    job_status = str(job.get("status") or "").strip().lower()
    if job_status not in {"failed", "dead_letter"}:
        return None
    failure_category = str(job.get("failure_category") or "").strip()
    if failure_category:
        return project_failure_classification(
            failure_category=failure_category,
            is_transient=bool(job.get("is_transient", False)),
            stdout_preview=str(job.get("stdout_preview") or ""),
        )
    try:
        from runtime.workflow.unified import _terminal_failure_classification
    except Exception:
        return None
    classification = _terminal_failure_classification(
        error_code=str(job.get("last_error_code") or "").strip(),
        stderr=str(job.get("stdout_preview") or ""),
        exit_code=job.get("exit_code"),
    )
    if classification is not None and hasattr(classification, "to_dict"):
        return classification.to_dict()
    return None


def _seconds_since(value: Any, now: datetime) -> float | None:
    """Return age in seconds for a timestamp-like value."""
    if not isinstance(value, datetime):
        return None
    return (now - value).total_seconds()


def _run_health(run_data: dict, now: datetime) -> dict[str, Any]:
    """Compute richer health signals for one workflow run."""
    from runtime.workflow.unified import summarize_run_health

    return summarize_run_health(run_data, now)


def _should_kill_idle_run(
    run_data: dict[str, Any],
    health: dict[str, Any],
    now: datetime,
    idle_threshold_seconds: int | None,
) -> tuple[bool, str]:
    """Detect an idle run that likely needs operator intervention."""
    from runtime.workflow.unified import summarize_run_recovery

    recovery = summarize_run_recovery(
        run_data,
        health,
        now,
        idle_threshold_seconds=idle_threshold_seconds,
    )
    return recovery.get("mode") == "kill_if_idle", str(recovery.get("reason") or "")


def _run_status_payload(
    pg,
    run_id: str,
    *,
    status_data: dict[str, Any] | None = None,
    kill_if_idle: bool = False,
    idle_threshold_seconds: int | None = None,
    include_dashboard: bool = True,
    delivery: dict[str, Any] | None = None,
) -> dict:
    """Build a status payload with richer health and failure diagnostics."""
    from runtime.workflow.unified import get_run_status, summarize_run_health, summarize_run_recovery

    if status_data is None:
        status_data = get_run_status(pg, run_id)
    if status_data is None:
        return {"run_id": run_id, "status": "not_found"}

    now = datetime.now(timezone.utc)
    dashboard_view = _dashboard_view_from_status_data(status_data, now=now)
    jobs_summary = []

    for j in status_data.get("jobs", []):
        heartbeat_at = j.get("heartbeat_at")
        heartbeat_age = _seconds_since(heartbeat_at, now)
        failure_classification = _classify_job_failure(j)
        entry = {
            "job_label": j["label"],
            "status": j["status"],
        }
        agent = j.get("resolved_agent") or j.get("agent_slug")
        if agent:
            entry["agent_slug"] = agent
        attempt = j.get("attempt", 0)
        if attempt:
            entry["attempt"] = attempt
        error_code = j.get("failure_category") or j.get("last_error_code")
        if error_code:
            entry["error_code"] = error_code
        duration = j.get("duration_ms", 0)
        if duration:
            entry["duration_ms"] = duration
        if failure_classification:
            entry["failure_classification"] = failure_classification
        if j.get("failure_zone"):
            entry["failure_zone"] = j["failure_zone"]
        if j.get("is_transient"):
            entry["is_transient"] = True
        if heartbeat_age is not None and j["status"] in ("claimed", "running"):
            entry["heartbeat_age_seconds"] = round(heartbeat_age, 1)
        stdout = j.get("stdout_preview")
        if stdout:
            entry["stdout_preview"] = stdout

        submission = j.get("submission")
        if isinstance(submission, dict):
            sub_entry = {}
            for sk in (
                "submission_id",
                "result_kind",
                "summary",
                "measured_summary",
                "comparison_status",
                "acceptance_status",
            ):
                sv = submission.get(sk)
                if sv is not None:
                    sub_entry[sk] = sv
            if sub_entry.get("comparison_status") is not None:
                sub_entry["integrity_status"] = sub_entry["comparison_status"]
            review = submission.get("latest_review")
            if isinstance(review, dict) and review.get("decision"):
                sub_entry["latest_review_decision"] = review["decision"]
            if sub_entry:
                entry["submission"] = sub_entry

        # Only include start/finish timestamps — heartbeat staleness is covered by heartbeat_age_seconds
        for ts_key in ("started_at", "finished_at"):
            value = j.get(ts_key)
            if value is not None and isinstance(value, datetime):
                entry[ts_key] = value.isoformat()

        if j.get("status") == "running":
            claimed_by = j.get("claimed_by")
            if claimed_by:
                entry["claimed_by"] = claimed_by

        jobs_summary.append(entry)

    elapsed = None
    if status_data.get("created_at"):
        end = status_data.get("finished_at") or now
        elapsed = round((end - status_data["created_at"]).total_seconds(), 1)

    payload = {
        "run_id": run_id,
        "status": status_data["status"],
        "spec_name": status_data.get("spec_name", ""),
        "total_jobs": status_data.get("total_jobs", 0),
        "completed": status_data.get("completed_jobs", 0),
        "jobs": jobs_summary,
        "elapsed_seconds": elapsed,
    }
    lineage = status_data.get("lineage")
    if isinstance(lineage, dict) and lineage:
        payload["lineage"] = lineage
    cost = status_data.get("total_cost_usd", 0.0)
    if cost:
        payload["total_cost_usd"] = cost
    tokens_in = status_data.get("total_tokens_in", 0)
    tokens_out = status_data.get("total_tokens_out", 0)
    if tokens_in or tokens_out:
        payload["total_tokens_in"] = tokens_in
        payload["total_tokens_out"] = tokens_out
    duration = status_data.get("total_duration_ms", 0)
    if duration:
        payload["total_duration_ms"] = duration
    payload.update(_run_links(run_id))
    if include_dashboard:
        payload["dashboard"] = _render_dashboard_panel_from_view(
            dashboard_view,
            run_id=run_id,
        )
    if delivery is not None:
        payload["delivery"] = delivery
    payload["health"] = summarize_run_health(status_data, now)
    payload["recovery"] = summarize_run_recovery(
        status_data,
        payload["health"],
        now,
        idle_threshold_seconds=idle_threshold_seconds,
    )
    packet_inspection = status_data.get("packet_inspection")
    if packet_inspection is not None:
        payload["packet_inspection"] = packet_inspection
    if kill_if_idle:
        kill_health = payload["health"]
        should_kill, reason = _should_kill_idle_run(status_data, kill_health, now, idle_threshold_seconds)
        payload["kill_action"] = {"performed": False, "reason": None}
        if should_kill:
            try:
                from runtime.control_commands import ControlCommandType

                kill_result = _execute_workflow_command(
                    pg,
                    action="kill_if_idle",
                    command_type=ControlCommandType.WORKFLOW_CANCEL,
                    payload={"run_id": run_id, "include_running": True},
                    run_id=run_id,
                )
                if kill_result.get("error"):
                    payload["kill_action"] = {
                        "performed": False,
                        "reason": reason,
                        "error": kill_result["error"],
                    }
                else:
                    payload["kill_action"] = {
                        "performed": kill_result.get("command_status") == "succeeded",
                        "reason": reason,
                        "command": kill_result,
                    }
            except Exception as exc:
                payload["kill_action"] = {
                    "performed": False,
                    "reason": reason,
                    "error": str(exc),
                }
    return payload


def _run_submit_result_payload(
    result: dict[str, Any],
    include_warning: bool = False,
    *,
    pg: Any | None = None,
    status_data: dict[str, Any] | None = None,
    delivery: dict[str, Any] | None = None,
) -> dict:
    """Format the async submission payload for a workflow run."""
    payload = {
        "run_id": result["run_id"],
        "status": result.get("status", "queued"),
        "spec_name": result["spec_name"],
        "total_jobs": result["total_jobs"],
    }
    for key in ("command_id", "command_status", "approval_required", "result_ref"):
        if key in result:
            payload[key] = result[key]
    if "command_id" in payload and "command_status" not in payload:
        payload["command_status"] = "succeeded"
    if "stream_url" in result and "status_url" in result:
        payload["stream_url"] = result["stream_url"]
        payload["status_url"] = result["status_url"]
    else:
        payload.update(_run_links(result["run_id"]))
    if status_data is None and pg is not None:
        from runtime.workflow.unified import get_run_status

        try:
            status_data = get_run_status(pg, payload["run_id"])
        except Exception:
            status_data = None
    payload["dashboard"] = _render_dashboard_panel_from_view(
        _dashboard_view_from_status_data(
            status_data
            or {
                "spec_name": payload["spec_name"],
                "total_jobs": payload["total_jobs"],
                "completed_jobs": 0,
                "jobs": [],
            },
            spec_name=payload["spec_name"],
            total_jobs=payload["total_jobs"],
            elapsed_seconds=0.0,
        ),
        run_id=payload["run_id"],
    )
    if delivery is not None:
        payload["delivery"] = delivery
    if include_warning:
        payload["message"] = (
            "MCP workflow launch is always async. Launch returns immediately, while live status stays on the "
            "separate `stream_url` channel or via action='status' snapshots."
        )
    return payload


def _workflow_run_id_from_result_ref(result_ref: str | None) -> str | None:
    if not result_ref or not result_ref.startswith("workflow_run:"):
        return None
    return result_ref.split(":", 1)[1]


def _workflow_command_idempotency_key(action: str) -> str:
    return f"workflow.{action}.mcp.{uuid.uuid4().hex}"


def _render_workflow_command_result(
    conn,
    command: Any,
    *,
    action: str,
    run_id: str | None = None,
    label: str | None = None,
    spec_name: str | None = None,
    total_jobs: int | None = None,
) -> dict[str, Any]:
    from runtime.control_commands import render_control_command_response

    payload = render_control_command_response(
        conn,
        command,
        action=action,
        run_id=run_id,
        label=label,
        spec_name=spec_name,
        total_jobs=total_jobs,
    )
    if payload.get("run_id"):
        payload.update(_run_links(str(payload["run_id"])))
    return payload


def _request_workflow_command(
    pg,
    *,
    action: str,
    command_type: str,
    payload: dict[str, Any],
    requested_by_ref: str,
) -> Any:
    from runtime.control_commands import (
        ControlIntent,
        request_control_command,
    )

    return request_control_command(
        pg,
        ControlIntent(
            command_type=command_type,
            requested_by_kind="mcp",
            requested_by_ref=requested_by_ref,
            idempotency_key=_workflow_command_idempotency_key(action),
            payload=payload,
        ),
    )


def _execute_workflow_command(
    pg,
    *,
    action: str,
    command_type: str,
    payload: dict[str, Any],
    run_id: str | None = None,
    label: str | None = None,
) -> dict[str, Any]:
    from runtime.control_commands import (
        ControlIntent,
        execute_control_intent,
    )

    command = execute_control_intent(
        pg,
        ControlIntent(
            command_type=command_type,
            requested_by_kind="mcp",
            requested_by_ref=f"praxis_workflow.{action}",
            idempotency_key=_workflow_command_idempotency_key(action),
            payload=payload,
        ),
        approved_by=f"mcp.praxis_workflow.{action}",
    )
    return _render_workflow_command_result(
        pg,
        command,
        action=action,
        run_id=run_id,
        label=label,
    )


def _fmt_bytes(b: int | None) -> str:
    if b is None:
        return ""
    if b >= 1 << 30:
        return f"{b / (1 << 30):.1f}GB"
    if b >= 1 << 20:
        return f"{b / (1 << 20):.0f}MB"
    if b >= 1 << 10:
        return f"{b / (1 << 10):.0f}KB"
    return f"{b}B"


def _fmt_tokens(n: int | None) -> str:
    if n is None or n == 0:
        return ""
    if n >= 1000:
        return f"{n / 1000:.1f}K"
    return str(n)


def _run_elapsed_seconds(status_data: dict[str, Any], *, now: datetime | None = None) -> float:
    current = now or datetime.now(timezone.utc)
    created_at = status_data.get("created_at") or status_data.get("requested_at")
    if not isinstance(created_at, datetime):
        return 0.0
    end = status_data.get("finished_at")
    if not isinstance(end, datetime):
        end = current
    return max((end - created_at).total_seconds(), 0.0)


def _job_running_elapsed_seconds(job: dict[str, Any], *, now: datetime | None = None) -> float | None:
    started_at = job.get("started_at")
    if not isinstance(started_at, datetime):
        return None
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return max((current - started_at).total_seconds(), 0.0)


def _dashboard_view_from_status_data(
    status_data: dict[str, Any] | None,
    *,
    spec_name: str | None = None,
    total_jobs: int | None = None,
    elapsed_seconds: float | None = None,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    if status_data is None:
        return None

    current = now or datetime.now(timezone.utc)
    jobs = status_data.get("jobs")
    if not isinstance(jobs, list):
        jobs = []

    resolved_spec_name = spec_name if spec_name is not None else str(status_data.get("spec_name") or "")
    resolved_total_jobs = int(total_jobs or status_data.get("total_jobs") or len(jobs) or 0)
    resolved_elapsed = elapsed_seconds if elapsed_seconds is not None else _run_elapsed_seconds(
        status_data,
        now=current,
    )

    observed_completed = 0
    running_count = 0
    observed_total_cost = 0.0
    observed_tok_in = 0
    observed_tok_out = 0
    job_views: list[dict[str, Any]] = []

    for raw_job in jobs:
        if not isinstance(raw_job, dict):
            continue
        job = dict(raw_job)
        status = str(job.get("status") or "pending")
        label = str(job.get("label") or "?")
        observed_total_cost += float(job.get("cost_usd") or 0.0)
        observed_tok_in += int(job.get("token_input") or 0)
        observed_tok_out += int(job.get("token_output") or 0)

        if status in _TERMINAL_STATUSES:
            observed_completed += 1
            job_views.append(
                {
                    "kind": "terminal",
                    "status": status,
                    "label": label,
                    "duration_seconds": (int(job.get("duration_ms") or 0)) / 1000.0,
                    "cost_usd": float(job.get("cost_usd") or 0.0),
                    "token_input": int(job.get("token_input") or 0),
                    "token_output": int(job.get("token_output") or 0),
                    "error_code": str(job.get("failure_category") or job.get("last_error_code") or ""),
                    "cpu_percent": job.get("cpu_percent"),
                    "mem_bytes": job.get("mem_bytes"),
                }
            )
            continue

        if status in ("running", "executing", "claimed"):
            running_count += 1
            job_views.append(
                {
                    "kind": "running",
                    "status": status,
                    "label": label,
                    "elapsed_seconds": _job_running_elapsed_seconds(job, now=current),
                }
            )
            continue

    completed_count = int(status_data.get("completed_jobs") or observed_completed)
    total_cost = float(status_data.get("total_cost_usd") or observed_total_cost)
    total_tok_in = int(status_data.get("total_tokens_in") or observed_tok_in)
    total_tok_out = int(status_data.get("total_tokens_out") or observed_tok_out)
    pending_count = max(resolved_total_jobs - completed_count - running_count, 0)

    return {
        "spec_name": resolved_spec_name,
        "total_jobs": resolved_total_jobs,
        "completed_jobs": completed_count,
        "elapsed_seconds": max(float(resolved_elapsed or 0.0), 0.0),
        "total_cost_usd": total_cost,
        "total_tokens_in": total_tok_in,
        "total_tokens_out": total_tok_out,
        "pending_count": pending_count,
        "jobs": job_views,
    }


def _render_dashboard_panel_from_view(view: dict[str, Any] | None, *, run_id: str) -> str:
    if view is None:
        return f"[dashboard unavailable for {run_id}]"

    done_lines: list[str] = []
    running_lines: list[str] = []
    for job in view.get("jobs", []):
        kind = job.get("kind")
        if kind == "terminal":
            status = str(job.get("status") or "unknown")
            icon = "✓" if status == "succeeded" else "✗"
            parts = [
                f"{icon} {str(job.get('label') or '?'):<20}",
                f"{float(job.get('duration_seconds') or 0.0):>5.1f}s",
            ]
            cpu = job.get("cpu_percent")
            mem = job.get("mem_bytes")
            if cpu is not None:
                parts.append(f"{float(cpu):>5.1f}%cpu")
            if mem:
                parts.append(f"{_fmt_bytes(int(mem)):>6}")
            tok_in = int(job.get("token_input") or 0)
            tok_out = int(job.get("token_output") or 0)
            tok_str = f"{_fmt_tokens(tok_in)}→{_fmt_tokens(tok_out)}"
            if tok_str != "→":
                parts.append(f"{tok_str:>12}")
            cost = float(job.get("cost_usd") or 0.0)
            if cost:
                parts.append(f"${cost:.4f}")
            err = str(job.get("error_code") or "")
            if err and status != "succeeded":
                parts.append(f"[{err}]")
            done_lines.append("  " + "  ".join(parts))
            continue

        if kind == "running":
            elapsed = job.get("elapsed_seconds")
            elapsed_str = "?" if elapsed is None else f"{float(elapsed):.0f}s"
            running_lines.append(
                f"  ~ {str(job.get('label') or '?'):<20} {elapsed_str:>5}  (running)"
            )

    cost_total = float(view.get("total_cost_usd") or 0.0)
    cost_str = f"${cost_total:.4f}" if cost_total else "$0"
    lines = [
        f"━━━ {str(view.get('spec_name') or '')} | "
        f"{int(view.get('completed_jobs') or 0)}/{int(view.get('total_jobs') or 0)} | "
        f"{cost_str} | {float(view.get('elapsed_seconds') or 0.0):.0f}s ━━━"
    ]
    lines.extend(done_lines)
    lines.extend(running_lines)
    pending_count = int(view.get("pending_count") or 0)
    if pending_count:
        lines.append(f"  · {pending_count} pending")

    tok_summary = ""
    total_tok_in = int(view.get("total_tokens_in") or 0)
    total_tok_out = int(view.get("total_tokens_out") or 0)
    if total_tok_in or total_tok_out:
        tok_summary = f"  {_fmt_tokens(total_tok_in)} in / {_fmt_tokens(total_tok_out)} out"
    lines.append(f"  ─ {cost_str}{tok_summary}")
    return "\n".join(lines)


def _render_dashboard_panel(
    pg,
    run_id: str,
    spec_name: str,
    total_jobs: int,
    elapsed_seconds: float,
) -> str:
    """Build a compact ASCII dashboard panel for emitter.log()."""
    from runtime.workflow.unified import get_run_status

    try:
        status_data = get_run_status(pg, run_id)
    except Exception:
        return f"[dashboard unavailable for {run_id}]"
    view = _dashboard_view_from_status_data(
        status_data,
        spec_name=spec_name,
        total_jobs=total_jobs,
        elapsed_seconds=elapsed_seconds,
    )
    return _render_dashboard_panel_from_view(view, run_id=run_id)


def _poll_progress_signature(status_data: dict[str, Any] | None) -> tuple[Any, ...]:
    if status_data is None:
        return ("missing",)
    jobs = status_data.get("jobs")
    if not isinstance(jobs, list):
        jobs = []
    running_count = sum(
        1
        for job in jobs
        if isinstance(job, dict) and str(job.get("status") or "") in {"claimed", "running", "executing"}
    )
    return (
        str(status_data.get("status") or ""),
        int(status_data.get("completed_jobs") or 0),
        running_count,
    )


def _next_poll_interval(current_interval: float, *, progress_changed: bool) -> float:
    if progress_changed:
        return _POLL_INTERVAL_MIN_SECONDS
    return min(
        _POLL_INTERVAL_MAX_SECONDS,
        max(_POLL_INTERVAL_MIN_SECONDS, current_interval * 1.7),
    )


def _poll_run_to_completion(
    pg,
    run_id: str,
    *,
    spec_name: str,
    total_jobs: int,
    emitter: Any | None = None,
    poll_interval: float = _POLL_INTERVAL_MIN_SECONDS,
    max_poll_seconds: float = 3600.0,
) -> dict[str, Any]:
    """Track a workflow run until terminal, waking on workflow notifications when possible."""
    import time as _time
    from runtime.workflow.unified import get_run_status
    from runtime.workflow_notifications import start_run_wakeup_listener

    start_mono = _time.monotonic()

    if emitter is not None:
        emitter.log(f"Launched {spec_name} ({total_jobs} jobs) — run_id={run_id}")
        emitter.emit(progress=0, total=total_jobs, message=f"Submitted {spec_name}")

    current_interval = max(_POLL_INTERVAL_MIN_SECONDS, float(poll_interval))
    last_signature: tuple[Any, ...] | None = None
    latest_status_data: dict[str, Any] | None = None
    deadline = start_mono + max_poll_seconds
    wakeup_event = threading.Event()
    listener = start_run_wakeup_listener(
        run_id=run_id,
        wakeup_event=wakeup_event,
    )

    try:
        while _time.monotonic() < deadline:
            if listener is not None:
                wakeup_event.wait(timeout=current_interval)
                wakeup_event.clear()
            else:
                _time.sleep(current_interval)
            status_data = get_run_status(pg, run_id)
            if status_data is None:
                current_interval = _next_poll_interval(current_interval, progress_changed=False)
                continue
            latest_status_data = status_data

            signature = _poll_progress_signature(status_data)
            progress_changed = signature != last_signature
            last_signature = signature

            if progress_changed and emitter is not None:
                elapsed = _time.monotonic() - start_mono
                panel = _render_dashboard_panel_from_view(
                    _dashboard_view_from_status_data(
                        status_data,
                        spec_name=spec_name,
                        total_jobs=total_jobs,
                        elapsed_seconds=elapsed,
                    ),
                    run_id=run_id,
                )
                emitter.log(panel)
                emitter.emit(
                    progress=int(status_data.get("completed_jobs") or 0),
                    total=total_jobs,
                    message=f"{int(status_data.get('completed_jobs') or 0)}/{total_jobs} jobs complete",
                )

            run_status = status_data.get("status", "")
            if run_status in _TERMINAL_STATUSES:
                break
            current_interval = _next_poll_interval(
                current_interval,
                progress_changed=progress_changed,
            )
    finally:
        if listener is not None:
            listener.stop()

    # Final dashboard + summary
    elapsed = _time.monotonic() - start_mono
    if latest_status_data is None:
        latest_status_data = get_run_status(pg, run_id)
    payload = _run_status_payload(
        pg,
        run_id,
        status_data=latest_status_data,
        include_dashboard=False,
        delivery=_delivery_metadata(emitter=emitter, wait_requested=True),
    )
    panel = _render_dashboard_panel_from_view(
        _dashboard_view_from_status_data(
            latest_status_data,
            spec_name=spec_name,
            total_jobs=total_jobs,
            elapsed_seconds=elapsed,
        ),
        run_id=run_id,
    )
    payload["dashboard"] = panel
    if emitter is not None:
        emitter.log(panel)
        run_final_status = payload.get("status", "unknown")
        level = "info" if run_final_status == "succeeded" else "error"
        emitter.log(
            f"Done: {spec_name} — {run_final_status} ({elapsed:.0f}s)",
            level=level,
        )
        emitter.emit(
            progress=total_jobs,
            total=total_jobs,
            message=f"{spec_name} {run_final_status}",
        )
    return payload


def _submit_workflow_via_service_bus(
    pg,
    *,
    spec_path: str,
    spec_name: str,
    total_jobs: int,
    run_id: str | None = None,
    parent_run_id: str | None = None,
    parent_job_label: str | None = None,
    dispatch_reason: str | None = None,
    lineage_depth: int | None = None,
    force_fresh_run: bool = False,
) -> dict[str, Any]:
    from runtime.control_commands import (
        render_workflow_submit_response,
        request_workflow_submit_command,
    )

    command = request_workflow_submit_command(
        pg,
        requested_by_kind="mcp",
        requested_by_ref="praxis_workflow.run",
        spec_path=spec_path,
        repo_root=str(REPO_ROOT),
        run_id=run_id,
        parent_run_id=parent_run_id,
        parent_job_label=parent_job_label,
        dispatch_reason=dispatch_reason,
        lineage_depth=lineage_depth,
        force_fresh_run=force_fresh_run,
    )
    return render_workflow_submit_response(
        command,
        spec_name=spec_name,
        total_jobs=total_jobs,
    )


def _spawn_workflow_via_service_bus(
    pg,
    *,
    spec_path: str,
    spec_name: str,
    total_jobs: int,
    parent_run_id: str,
    parent_job_label: str | None = None,
    dispatch_reason: str,
    run_id: str | None = None,
    lineage_depth: int | None = None,
    force_fresh_run: bool = False,
) -> dict[str, Any]:
    from runtime.control_commands import (
        render_workflow_spawn_response,
        request_workflow_spawn_command,
    )

    command = request_workflow_spawn_command(
        pg,
        requested_by_kind="mcp",
        requested_by_ref="praxis_workflow.spawn",
        spec_path=spec_path,
        repo_root=str(REPO_ROOT),
        parent_run_id=parent_run_id,
        parent_job_label=parent_job_label,
        dispatch_reason=dispatch_reason,
        run_id=run_id,
        lineage_depth=lineage_depth,
        force_fresh_run=force_fresh_run,
    )
    return render_workflow_spawn_response(
        command,
        spec_name=spec_name,
        total_jobs=total_jobs,
    )


def _submit_workflow_chain_via_service_bus(
    pg,
    *,
    coordination_path: str,
    adopt_active: bool = True,
) -> dict[str, Any]:
    from runtime.control_commands import (
        render_workflow_chain_submit_response,
        request_workflow_chain_submit_command,
    )

    command = request_workflow_chain_submit_command(
        pg,
        requested_by_kind="mcp",
        requested_by_ref="praxis_workflow.chain",
        coordination_path=coordination_path,
        repo_root=str(REPO_ROOT),
        adopt_active=adopt_active,
    )
    return render_workflow_chain_submit_response(
        pg,
        command,
        coordination_path=coordination_path,
    )


# ---------------------------------------------------------------------------
# Workflow state — all in Postgres via the unified runtime
# ---------------------------------------------------------------------------

def tool_praxis_workflow(params: dict, _progress_emitter=None) -> dict:
    """Run or dry-run a workflow spec.

    The 'run' action submits a workflow and returns links to stream
    per-job progress and snapshot status. Poll action='status' for richer
    diagnostics while the workflow proceeds.
    """
    action = params.get("action", "run")

    # --- Poll for status of a workflow ---
    if action == "status":
        run_id = params.get("run_id", "")
        if not run_id:
            return {"error": "run_id is required for action='status'"}
        kill_if_idle = bool(params.get("kill_if_idle", False))
        idle_threshold_seconds = params.get("idle_threshold_seconds")
        try:
            if idle_threshold_seconds is not None:
                idle_threshold_seconds = int(idle_threshold_seconds)
        except (TypeError, ValueError):
            return {"error": "idle_threshold_seconds must be an integer when provided"}

        pg, error = _load_pg_conn(action="status")
        if error is not None:
            return error
        try:
            return _run_status_payload(
                pg,
                run_id,
                kill_if_idle=kill_if_idle,
                idle_threshold_seconds=idle_threshold_seconds,
                delivery=_delivery_metadata(emitter=_progress_emitter),
            )
        except Exception as exc:
            return _structured_runtime_error(exc, action="status")

    # --- Submit a durable multi-wave workflow chain ---
    if action == "chain":
        coordination_path = params.get("coordination_path", "")
        if not coordination_path:
            return {"error": "coordination_path is required for action='chain'"}

        adopt_active = params.get("adopt_active", True)
        if not isinstance(adopt_active, bool):
            if isinstance(adopt_active, str):
                adopt_active = adopt_active.strip().lower() in {"1", "true", "yes", "y", "on"}
            else:
                return {"error": "adopt_active must be a boolean"}

        pg, error = _load_pg_conn(action="chain")
        if error is not None:
            return error
        try:
            return _submit_workflow_chain_via_service_bus(
                pg,
                coordination_path=str(coordination_path),
                adopt_active=bool(adopt_active),
            )
        except Exception as exc:
            return _structured_runtime_error(exc, action="chain")

    # --- Deep job inspection ---
    if action == "inspect":
        run_id = params.get("run_id", "")
        if not run_id:
            return {"error": "run_id is required for action='inspect'"}

        label = params.get("label")
        pg, error = _load_pg_conn(action="inspect")
        if error is not None:
            return error

        try:
            from runtime.workflow.unified import inspect_job
            return inspect_job(pg, run_id, label)
        except Exception as exc:
            return _structured_runtime_error(exc, action="inspect")

    if action == "claim":
        subscription_id = str(params.get("subscription_id") or "").strip()
        run_id = str(params.get("run_id") or "").strip()
        if not subscription_id:
            return {"error": "subscription_id is required for action='claim'"}
        if not run_id:
            return {"error": "run_id is required for action='claim'"}

        limit = params.get("limit", 100)
        try:
            limit = max(1, int(limit))
        except (TypeError, ValueError):
            return {"error": "limit must be an integer for action='claim'"}

        last_acked_evidence_seq = params.get("last_acked_evidence_seq")
        try:
            if last_acked_evidence_seq is not None:
                last_acked_evidence_seq = int(last_acked_evidence_seq)
        except (TypeError, ValueError):
            return {"error": "last_acked_evidence_seq must be an integer when provided"}

        try:
            bridge = _build_workflow_bridge()
            work = bridge.claimable_work(
                cursor=WorkerSubscriptionCursor(
                    subscription_id=subscription_id,
                    run_id=run_id,
                    last_acked_evidence_seq=last_acked_evidence_seq,
                ),
                limit=limit,
            )
            claimable_work = _serialize(work)
            claimable_work = _normalize_run_state_strings(claimable_work)
            if isinstance(claimable_work, dict):
                claimable_work.setdefault("subscription_id", subscription_id)
                claimable_work.setdefault("run_id", run_id)
                claimable_work.setdefault("limit", limit)
            return {
                "routed_to": "workflow_bridge",
                "view": "claimable_work",
                "claimable_work": claimable_work,
            }
        except Exception as exc:
            return _structured_runtime_error(exc, action="claim")

    if action == "acknowledge":
        raw_work = params.get("work")
        if isinstance(raw_work, str):
            try:
                raw_work = json.loads(raw_work)
            except json.JSONDecodeError as exc:
                return {"error": f"work must be JSON if provided as a string: {exc}"}
        if not isinstance(raw_work, dict):
            return {"error": "work is required for action='acknowledge'"}

        through_evidence_seq = params.get("through_evidence_seq")
        try:
            if through_evidence_seq is not None:
                through_evidence_seq = int(through_evidence_seq)
        except (TypeError, ValueError):
            return {"error": "through_evidence_seq must be an integer when provided"}

        try:
            bridge = _build_workflow_bridge()
            work = _deserialize_claimable_work(raw_work)
            acknowledgement = bridge.acknowledge(
                work=work,
                through_evidence_seq=through_evidence_seq,
            )
            acknowledgement_payload = _normalize_run_state_strings(_serialize(acknowledgement))
            return {
                "routed_to": "workflow_bridge",
                "view": "acknowledge",
                "acknowledgement": acknowledgement_payload,
            }
        except Exception as exc:
            return _structured_runtime_error(exc, action="acknowledge")

    # --- Cancel a workflow run ---
    if action == "cancel":
        run_id = params.get("run_id", "")
        if not run_id:
            return {"error": "run_id is required for action='cancel'"}
        pg, error = _load_pg_conn(action="cancel")
        if error is not None:
            return error
        try:
            from runtime.control_commands import (
                ControlCommandType,
                render_control_command_failure,
            )

            return _execute_workflow_command(
                pg,
                action="cancel",
                command_type=ControlCommandType.WORKFLOW_CANCEL,
                payload={"run_id": run_id, "include_running": True},
                run_id=run_id,
            )
        except Exception as exc:
            details = getattr(exc, "details", None)
            return render_control_command_failure(
                error_code=getattr(exc, "reason_code", "control.command.execution_failed"),
                error_detail=str(exc),
                run_id=run_id,
                details=details if isinstance(details, dict) else None,
            )

    # --- List recent workflow runs ---
    # --- Drain pending notifications (platform-native, session-independent) ---
    if action == "notifications":
        notifications_text = _subs.drain_notifications()
        if notifications_text:
            return {"notifications": notifications_text}
        return {"notifications": "No pending workflow notifications."}

    # --- Retry a single failed job ---
    if action == "retry":
        run_id = params.get("run_id", "")
        label = params.get("label", "")
        if not run_id or not label:
            return {"error": "run_id and label are required for action='retry'"}

        pg, error = _load_pg_conn(action="retry")
        if error is not None:
            return error
        try:
            from runtime.control_commands import ControlCommandType

            return _execute_workflow_command(
                pg,
                action="retry",
                command_type=ControlCommandType.WORKFLOW_RETRY,
                payload={"run_id": run_id, "label": label},
                run_id=run_id,
                label=label,
            )
        except Exception as exc:
            return _structured_runtime_error(exc, action="retry")

    # --- Repair a degraded workflow sync state ---
    if action == "repair":
        run_id = params.get("run_id", "")
        if not run_id:
            return {"error": "run_id is required for action='repair'"}

        pg, error = _load_pg_conn(action="repair")
        if error is not None:
            return error
        try:
            from runtime.control_commands import ControlCommandType

            return _execute_workflow_command(
                pg,
                action="repair",
                command_type=ControlCommandType.SYNC_REPAIR,
                payload={"run_id": run_id},
                run_id=run_id,
            )
        except Exception as exc:
            return _structured_runtime_error(exc, action="repair")

    if action == "list":
        pg, error = _load_pg_conn(action="list")
        if error is not None:
            return error
        runs = []

        # Primary: list from workflow_runs with reconciled status
        try:
            rows = pg.execute(
                """SELECT wr.run_id, wr.current_state,
                    CASE
                        WHEN wr.current_state NOT IN ('queued', 'running') THEN wr.current_state
                        WHEN j.total = 0 THEN 'failed'
                        WHEN j.active = 0 AND j.failed > 0 THEN 'failed'
                        WHEN j.active = 0 AND j.cancelled > 0 THEN 'cancelled'
                        WHEN j.active = 0 AND j.succeeded > 0 THEN 'succeeded'
                        ELSE wr.current_state
                    END AS effective_state,
                    wr.request_envelope, wr.requested_at, wr.finished_at
                FROM workflow_runs wr
                LEFT JOIN LATERAL (
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE status IN ('ready', 'claimed', 'running', 'pending')) AS active,
                        COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                        COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
                        COUNT(*) FILTER (WHERE status = 'succeeded') AS succeeded
                    FROM workflow_jobs WHERE run_id = wr.run_id
                ) j ON true
                ORDER BY wr.requested_at DESC LIMIT 50""",
            )
            for row in rows:
                elapsed = None
                created_at = row["requested_at"]
                envelope = row["request_envelope"] if isinstance(row["request_envelope"], dict) else json.loads(row["request_envelope"])
                if created_at:
                    end = row["finished_at"] or datetime.now(timezone.utc)
                    elapsed = round((end - created_at).total_seconds(), 1)
                runs.append({
                    "run_id": row["run_id"],
                    "status": row["effective_state"],
                    "spec_name": envelope.get("name", ""),
                    "total_jobs": envelope.get("total_jobs", 0),
                    "elapsed_seconds": elapsed,
                })
        except Exception as exc:
            return _structured_runtime_error(exc, action="list")

        return {"runs": runs}

    # --- Run or dry-run ---
    spec_path = params.get("spec_path")
    if not spec_path:
        return {"error": "spec_path is required"}

    if action not in {"run", "spawn", "claim", "acknowledge"}:
        return {
            "error": (
                f"Unsupported action='{action}'. Expected one of: "
                "run, spawn, status, inspect, claim, acknowledge, cancel, list, notifications, retry, repair, chain."
            )
        }

    dry_run = params.get("dry_run", False)

    # Dry runs execute synchronously (fast)
    if dry_run:
        from runtime.workflow.dry_run import dry_run_workflow

        spec_mod = _workflow_spec_mod()
        spec = spec_mod.WorkflowSpec.load(spec_path)
        result = dry_run_workflow(spec)
        return {
            "spec_name": result.spec_name,
            "total_jobs": result.total_jobs,
            "succeeded": result.succeeded,
            "failed": result.failed,
            "skipped": result.skipped,
            "blocked": result.blocked,
            "duration_seconds": result.duration_seconds,
            "receipts_written": list(result.receipts_written),
            "job_results": [
                {
                    "job_label": jr.job_label,
                    "agent_slug": jr.agent_slug,
                    "status": jr.status,
                    "exit_code": jr.exit_code,
                    "duration_seconds": jr.duration_seconds,
                    "verify_passed": jr.verify_passed,
                    "retry_count": jr.retry_count,
                }
                for jr in result.job_results
            ],
        }

    # Unified workflow runtime: submit to queue, poll for completion,
    # and keep the call open only when the caller explicitly requested
    # progress-capable inline updates via _meta.progressToken.
    wait = params.get("wait", True)
    if isinstance(wait, str):
        wait = wait.lower() not in ("false", "0", "no")

    pg, error = _load_pg_conn(action="run")
    if error is not None:
        return error

    try:
        spec_mod = _workflow_spec_mod()
        spec = spec_mod.WorkflowSpec.load(spec_path)
        total_jobs = len(getattr(spec, "jobs", []))
        run_id_override = str(params.get("run_id") or "").strip() or None
        force_fresh_run = bool(params.get("force_fresh_run", False))
        if action == "spawn":
            parent_run_id = str(params.get("parent_run_id") or "").strip()
            if not parent_run_id:
                return {"error": "parent_run_id is required for action='spawn'"}
            parent_job_label = str(params.get("parent_job_label") or "").strip() or None
            dispatch_reason = str(params.get("dispatch_reason") or "").strip() or "manual.spawn"
            lineage_depth = params.get("lineage_depth")
            if lineage_depth is not None:
                try:
                    lineage_depth = max(int(lineage_depth), 0)
                except (TypeError, ValueError):
                    return {"error": "lineage_depth must be an integer when provided"}
            spawn_kwargs: dict[str, Any] = {
                "spec_path": spec_path,
                "spec_name": getattr(spec, "name", spec_path),
                "total_jobs": total_jobs,
                "parent_run_id": parent_run_id,
                "dispatch_reason": dispatch_reason,
            }
            if parent_job_label is not None:
                spawn_kwargs["parent_job_label"] = parent_job_label
            if run_id_override is not None:
                spawn_kwargs["run_id"] = run_id_override
            if lineage_depth is not None:
                spawn_kwargs["lineage_depth"] = lineage_depth
            if force_fresh_run:
                spawn_kwargs["force_fresh_run"] = True
            result = _spawn_workflow_via_service_bus(pg, **spawn_kwargs)
        else:
            submit_kwargs: dict[str, Any] = {
                "spec_path": spec_path,
                "spec_name": getattr(spec, "name", spec_path),
                "total_jobs": total_jobs,
            }
            if run_id_override is not None:
                submit_kwargs["run_id"] = run_id_override
            if force_fresh_run:
                submit_kwargs["force_fresh_run"] = True
            result = _submit_workflow_via_service_bus(pg, **submit_kwargs)
        if result.get("error"):
            return result

        run_id = result["run_id"]

        if not wait or not getattr(_progress_emitter, "enabled", False):
            return _run_submit_result_payload(
                result,
                pg=pg,
                delivery=_delivery_metadata(emitter=_progress_emitter, wait_requested=wait),
            )

        # Stream results: poll until terminal, emitting progress per job.
        return _poll_run_to_completion(
            pg, run_id,
            spec_name=getattr(spec, "name", spec_path),
            total_jobs=total_jobs,
            emitter=_progress_emitter,
        )
    except Exception as exc:
        return _structured_runtime_error(exc, action="run")


def tool_praxis_workflow_validate(params: dict) -> dict:
    """Validate a workflow spec without running it."""
    spec_path = params.get("spec_path")
    if not spec_path:
        return {"error": "spec_path is required"}

    spec_mod = _workflow_spec_mod()
    try:
        # Run authoring-level validation for new-format specs
        raw = spec_mod.load_raw(spec_path)
        if spec_mod._is_new_authoring_format(raw):
            ok, errors = spec_mod.validate_authoring_spec(raw)
            if not ok:
                return {"valid": False, "error": "Authoring schema errors:\n  " + "\n  ".join(errors)}

        spec = spec_mod.WorkflowSpec.load(spec_path)
        from runtime.workflow_validation import (
            _authority_error_result,
            validate_workflow_spec,
        )

        try:
            pg_conn = _subs.get_pg_conn()
        except Exception as exc:
            return _authority_error_result(spec, f"{type(exc).__name__}: {exc}")
        return validate_workflow_spec(spec, pg_conn=pg_conn)
    except spec_mod.WorkflowSpecError as e:
        return {
            "valid": False,
            "error": str(e),
        }


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_workflow": (
        tool_praxis_workflow,
        {
            "description": (
                "Execute work by launching a workflow for LLM agents. This is the primary way to run tasks — "
                "building code, running tests, writing reviews, refactoring, and debates.\n\n"
                "USE WHEN: you need to run a workflow spec, spawn a child workflow, check on a running workflow, "
                "retry a failed job, cancel a run, or repair a degraded post-run sync state.\n\n"
                "WORKFLOW CONTRACT:\n"
                "  - action='run' is the kickoff call. Treat run_id as the authority and follow with "
                "status or stream reads on separate channels.\n"
                "  - Kickoff and status payloads now always include dashboard.\n"
                "  - Direct callers without an MCP emitter receive kickoff-only payloads immediately.\n"
                "  - MCP callers with wait=true only keep the call open for inline polling when they send "
                "_meta.progressToken. In that mode they receive dashboard panels on notifications/message "
                "and progress counters on notifications/progress.\n"
                "  - Without _meta.progressToken, the kickoff payload returns immediately and the payload "
                "delivery metadata makes that explicit instead of hiding it.\n"
                "  - Use wait=false to force kickoff-only behavior even when a progress emitter exists.\n"
                "  - Use action='status' for a snapshot of a running or completed workflow, including "
                "dashboard.\n"
                "  - Use action='claim' to inspect claimable worker work for a subscription/run pair.\n"
                "  - Use action='acknowledge' to commit a checkpoint for a previously claimed worker batch.\n"
                "  - Read health.likely_failed, health.signals to detect stuck runs.\n"
                "  - Use kill_if_idle=true on a status call if the run is clearly idle and unhealthy.\n\n"
                "EXAMPLES:\n"
                "  Launch:          praxis_workflow(action='run', spec_path='artifacts/workflow/my_spec.queue.json')\n"
                "  Spawn child:     praxis_workflow(action='spawn', spec_path='...', parent_run_id='workflow_parent', dispatch_reason='phase.spawn')\n"
                "  Force kickoff:   praxis_workflow(action='run', spec_path='...', wait=false)\n"
                "  Check status:    praxis_workflow(action='status', run_id='workflow_abc123')\n"
                "  Retry a failure: praxis_workflow(action='retry', run_id='workflow_abc123', label='build_step')\n"
                "  Cancel a run:    praxis_workflow(action='cancel', run_id='workflow_abc123')\n"
                "  Repair sync:     praxis_workflow(action='repair', run_id='workflow_abc123')\n"
                "  List recent:     praxis_workflow(action='list')\n\n"
                "DO NOT USE: for asking questions about the system (use praxis_query), checking health "
                "(use praxis_health), or searching past results (use praxis_receipts)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "spec_path": {"type": "string", "description": "Path to a .queue.json spec file. Required for 'run' and 'spawn'."},
                    "wait": {
                        "type": "boolean",
                        "description": (
                            "If true and _meta.progressToken is present, the tool may keep polling inline "
                            "and emit dashboard panels on notifications/message plus counters on "
                            "notifications/progress. If false, or if no progress token is present, return "
                            "the kickoff payload immediately with run_id, dashboard, stream_url, "
                            "status_url, and delivery metadata."
                        ),
                        "default": True,
                    },
                    "dry_run": {"type": "boolean", "description": "If true, simulate without actual execution.", "default": False},
                    "action": {
                        "type": "string",
                        "description": (
                "Operation: 'run' (default — submits and returns run_id), "
                            "'spawn' (submits a child workflow with explicit lineage), "
                            "'status' (poll a running/completed workflow with health heuristics), "
                            "'inspect' (deep job inspection of all fields), "
                            "'claim' (inspect claimable worker work for a subscription/run pair), "
                            "'acknowledge' (commit a worker checkpoint for a previously claimed batch), "
                            "'cancel' (cancel a workflow run), "
                            "'repair' (repair the post-run sync state for a workflow run), "
                            "'chain' (submit a durable workflow chain from coordination JSON), "
                            "'list' (show recent workflows), "
                            "'notifications' (drain pending completion events), "
                            "'retry' (re-queue a failed job)."
                        ),
                        "enum": ["run", "spawn", "status", "inspect", "claim", "acknowledge", "cancel", "list", "notifications", "retry", "repair", "chain"],
                        "default": "run",
                    },
                    "parent_run_id": {
                        "type": "string",
                        "description": "Required for action='spawn'. Parent workflow run id.",
                    },
                    "parent_job_label": {
                        "type": "string",
                        "description": "Optional for action='spawn'. Parent job label responsible for the child workflow.",
                    },
                    "dispatch_reason": {
                        "type": "string",
                        "description": "Optional for action='spawn'. Explicit reason for the child workflow.",
                    },
                    "lineage_depth": {
                        "type": "integer",
                        "description": "Optional for action='spawn'. Explicit lineage depth override.",
                    },
                    "coordination_path": {
                        "type": "string",
                        "description": "Path to a chain coordination JSON file. Required for action='chain'.",
                    },
                    "adopt_active": {
                        "type": "boolean",
                        "description": "Whether to adopt an existing active chain run where possible.",
                        "default": True,
                    },
                    "run_id": {"type": "string", "description": "Workflow run ID. Required for 'status', 'inspect', 'cancel', 'retry', and 'repair'."},
                    "label": {"type": "string", "description": "Job label. Required for 'retry', optional for 'inspect'."},
                    "subscription_id": {"type": "string", "description": "Durable worker subscription id. Required for 'claim'."},
                    "last_acked_evidence_seq": {
                        "type": "integer",
                        "description": "Optional previous acknowledgement watermark for 'claim'.",
                    },
                    "work": {
                        "type": "object",
                        "description": "Serialized claim payload from a prior 'claim' call. Required for 'acknowledge'.",
                    },
                    "through_evidence_seq": {
                        "type": "integer",
                        "description": "Optional explicit acknowledgement watermark for 'acknowledge'.",
                    },
                    "kill_if_idle": {
                        "type": "boolean",
                        "description": (
                            "If true, auto-cancel the run when it appears idle and unhealthy "
                            "(only for this status call)."
                        ),
                        "default": False,
                    },
                    "idle_threshold_seconds": {
                        "type": "integer",
                        "description": (
                            "Optional minimum elapsed seconds before auto-cancel triggers when kill_if_idle=true."
                        ),
                        "default": 900,
                    },
                },
            },
        },
    ),
    "praxis_workflow_validate": (
        tool_praxis_workflow_validate,
        {
            "description": (
                "Dry-run a workflow spec to check for errors before executing it. Returns whether "
                "the spec is valid, how many jobs it contains, and which agents each job resolves to.\n\n"
                "USE WHEN: you wrote or modified a workflow spec and want to verify it parses correctly "
                "before committing to a full workflow run.\n\n"
                "EXAMPLE: praxis_workflow_validate(spec_path='artifacts/workflow/my_spec.queue.json')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "spec_path": {"type": "string", "description": "Path to a .queue.json spec file."},
                },
                "required": ["spec_path"],
            },
        },
    ),
}
