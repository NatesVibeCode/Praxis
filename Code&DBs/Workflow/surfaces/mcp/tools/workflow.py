"""Tools: praxis_workflow, praxis_workflow_validate, praxis_generate_plan, praxis_launch_plan, praxis_bind_data_pills, praxis_approve_proposed_plan, praxis_decompose_intent, praxis_compose_plan, praxis_compose_and_launch, praxis_plan_lifecycle."""
from __future__ import annotations

import json
from datetime import datetime, timezone
import threading
import uuid
from typing import Any

from storage.postgres.validators import PostgresConfigurationError

from runtime.failure_projection import project_failure_classification

_POLICY_REASON_CODES = frozenset({"provider_disabled", "route_disabled", "policy_blocked"})
from runtime.claims import ClaimLeaseProposalSnapshot
from runtime.domain import RunState
from runtime.subscriptions import (
    WorkerInboxFact,
    WorkerSubscriptionBatch,
    WorkerSubscriptionCursor,
)
from surfaces._workflow_database import workflow_database_url_for_repo
from surfaces.workflow_bridge import WorkflowBridge, WorkflowClaimableWork, build_live_workflow_bridge
from ..subsystems import _subs, REPO_ROOT, workflow_database_env
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
_WORKFLOW_TOOL_ACTIONS = frozenset(
    {
        "run",
        "spawn",
        "wait",
        "status",
        "chain",
        "inspect",
        "claim",
        "acknowledge",
        "cancel",
        "notifications",
        "retry",
        "repair",
        "list",
        "preview",
    }
)


def _workflow_spec_mod():
    import runtime.workflow_spec as spec_mod

    return spec_mod


def _workflow_database_url() -> str:
    try:
        return workflow_database_url_for_repo(REPO_ROOT, env=workflow_database_env())
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


def _parse_workflow_action(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, str):
        raise ValueError("action must be a non-empty string")
    action = value.strip().lower()
    if not action:
        raise ValueError("action must be a non-empty string")
    if action not in _WORKFLOW_TOOL_ACTIONS:
        raise ValueError(
            "action must be one of: run, spawn, preview, status, inspect, "
            "claim, acknowledge, cancel, list, notifications, retry, repair, or chain"
        )
    return action


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
    raw_error_code = str(job.get("last_error_code") or "").strip()
    if raw_error_code in _POLICY_REASON_CODES:
        try:
            from runtime.workflow.unified import _terminal_failure_classification
        except Exception:
            return None
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
    try:
        from runtime.workflow.unified import _terminal_failure_classification
    except Exception:
        return None
    classification = _terminal_failure_classification(
        error_code=raw_error_code,
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
        raw_error_code = str(j.get("last_error_code") or "").strip()
        error_code = (
            raw_error_code
            if raw_error_code in _POLICY_REASON_CODES
            else (j.get("failure_category") or raw_error_code)
        )
        if error_code:
            entry["error_code"] = error_code
        if raw_error_code and raw_error_code != error_code:
            entry["reason_code"] = raw_error_code
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


def _enrich_failed_submit_response(result: dict[str, Any], *, pg: Any) -> dict[str, Any]:
    """Embed admission rejection rows + next-action hints into a failed
    workflow.submit response.

    When the submit fails, the operator has historically gotten only a wrapped
    error string (``"native runtime authority unavailable: ..."``). The truth
    — which gates blocked which candidates and which operator tool owns each
    gate — already lives in ``private_provider_control_plane_snapshot`` /
    ``effective_provider_circuit_breaker_state``. This helper joins that data
    onto the failure response so the model gets actionable next-steps inline
    instead of having to manually query four other surfaces.
    """
    if pg is None:
        return result
    payload = result.get("command", {}).get("payload") if isinstance(result.get("command"), dict) else None
    if not isinstance(payload, dict):
        return result
    spec_path = payload.get("spec_path")
    repo_root = payload.get("repo_root") or str(REPO_ROOT)
    if not isinstance(spec_path, str):
        return result
    try:
        spec_mod = _workflow_spec_mod()
        from pathlib import Path as _Path
        path_obj = _Path(spec_path)
        full_path = str(path_obj if path_obj.is_absolute() else _Path(repo_root) / path_obj)
        spec = spec_mod.WorkflowSpec.load(full_path)
    except Exception:
        return result
    task_types: set[str] = set()
    auto_jobs: list[tuple[str, str]] = []  # (label, stage_or_task)
    for job in getattr(spec, "jobs", []) or []:
        agent = str(job.get("agent") or "")
        task_type = str(job.get("task_type") or "")
        if task_type:
            task_types.add(task_type)
        if agent.startswith("auto/"):
            stage = agent.split("/", 1)[1]
            task_types.add(stage)
            auto_jobs.append((str(job.get("label", "")), stage))
    if not task_types:
        return result
    runtime_profile_ref = "praxis"
    raw_spec = getattr(spec, "_raw", {}) or {}
    rp = raw_spec.get("runtime_profile_ref")
    if isinstance(rp, str) and rp.strip():
        runtime_profile_ref = rp.strip()
    rejection_rows: list[dict[str, Any]] = []
    try:
        rows = pg.execute(
            """
            SELECT job_type, provider_slug, model_slug, transport_type, adapter_type,
                   is_runnable, breaker_state, manual_override_state,
                   manual_override_reason, primary_removal_reason_code,
                   removal_reasons, credential_availability_state
            FROM private_provider_control_plane_snapshot
            WHERE runtime_profile_ref = $1
              AND job_type = ANY($2::text[])
            ORDER BY job_type, transport_type DESC, provider_slug, model_slug
            """,
            runtime_profile_ref,
            list(task_types),
        )
        for row in rows or []:
            rd = dict(row)
            rejection_rows.append({
                "task_type": rd.get("job_type"),
                "candidate": f"{rd.get('provider_slug')}/{rd.get('model_slug')}",
                "transport_type": rd.get("transport_type"),
                "adapter_type": rd.get("adapter_type"),
                "is_runnable": rd.get("is_runnable"),
                "breaker_state": rd.get("breaker_state"),
                "manual_override_state": rd.get("manual_override_state"),
                "manual_override_reason": rd.get("manual_override_reason"),
                "primary_removal_reason_code": rd.get("primary_removal_reason_code"),
                "removal_reasons": rd.get("removal_reasons") or [],
                "credential_availability_state": rd.get("credential_availability_state"),
            })
    except Exception:
        return result
    next_actions = _next_actions_from_rejections(rejection_rows)
    if rejection_rows or next_actions:
        result["admission_diagnosis"] = {
            "spec_name": getattr(spec, "name", None) or spec_path,
            "runtime_profile_ref": runtime_profile_ref,
            "task_types": sorted(task_types),
            "rejection_rows": rejection_rows[:60],
            "next_actions": next_actions,
            "hint": (
                "These rows show every gate that blocked the requested candidates. "
                "The next_actions list names the operator tool that owns each gate — "
                "use those tools, not raw SQL or migrations, to lift the gate."
            ),
        }
    return result


_REASON_CODE_TO_TOOL: dict[str, dict[str, str]] = {
    "control_panel.transport_turned_off": {
        "tool": "praxis_access_control",
        "action": "enable",
        "concern": "API/CLI transport policy or model-access denial",
    },
    "control_panel.model_access_method_turned_off": {
        "tool": "praxis_access_control",
        "action": "enable",
        "concern": "explicit denial row blocking this provider/model",
    },
    "runtime_profile_route.not_admitted": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "runtime_profile_admitted_routes is missing this candidate",
    },
    "provider_job_catalog.availability_disabled": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "candidate exists but is disabled in the runtime profile job catalog",
    },
    "provider_transport.missing": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "provider_transport_admissions row missing",
    },
    "provider_transport.policy_denied": {
        "tool": "praxis_access_control",
        "action": "enable",
        "concern": "transport policy denies this adapter",
    },
    "provider_transport.disabled": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "transport admission is inactive",
    },
    "circuit_breaker.manual_override_open": {
        "tool": "praxis_circuits",
        "action": "reset",
        "concern": "operator forced this provider's breaker OPEN",
    },
    "circuit_breaker.runtime_open": {
        "tool": "praxis_circuits",
        "action": "list",
        "concern": "circuit breaker is open due to recent failures",
    },
    "credentials.missing": {
        "tool": "praxis_provider_onboard",
        "action": "onboard",
        "concern": "API key / CLI credential missing for this provider",
    },
}


def _next_actions_from_rejections(rejection_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rejection_rows:
        if row.get("is_runnable"):
            continue
        codes: list[str] = []
        primary = str(row.get("primary_removal_reason_code") or "").strip()
        if primary:
            codes.append(primary)
        for r in row.get("removal_reasons") or []:
            if isinstance(r, dict) and r.get("reason_code"):
                codes.append(str(r["reason_code"]).strip())
            elif isinstance(r, str):
                codes.append(r.strip())
        # Manual breaker override surfaces with a different code shape; map it
        if row.get("manual_override_state") == "OPEN":
            codes.append("circuit_breaker.manual_override_open")
        elif row.get("breaker_state") == "OPEN":
            codes.append("circuit_breaker.runtime_open")
        for code in codes:
            if not code or code in seen:
                continue
            seen.add(code)
            hint = _REASON_CODE_TO_TOOL.get(code)
            if hint:
                out.append({
                    "reason_code": code,
                    "tool": hint["tool"],
                    "action": hint["action"],
                    "concern": hint["concern"],
                })
    return out


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
        ControlCommandType,
        ControlIntent,
        request_control_command,
        workflow_retry_idempotency_key,
        workflow_retry_payload_with_guard,
    )
    command_type_value = command_type.value if hasattr(command_type, "value") else str(command_type)
    idempotency_key = _workflow_command_idempotency_key(action)
    command_payload = dict(payload)
    if command_type_value == ControlCommandType.WORKFLOW_RETRY.value:
        command_payload = workflow_retry_payload_with_guard(pg, command_payload)
        idempotency_key = workflow_retry_idempotency_key(
            requested_by_kind="mcp",
            payload=command_payload,
        )

    return request_control_command(
        pg,
        ControlIntent(
            command_type=command_type,
            requested_by_kind="mcp",
            requested_by_ref=requested_by_ref,
            idempotency_key=idempotency_key,
            payload=command_payload,
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
        ControlCommandType,
        ControlIntent,
        execute_control_intent,
        workflow_retry_idempotency_key,
        workflow_retry_payload_with_guard,
    )
    command_type_value = command_type.value if hasattr(command_type, "value") else str(command_type)
    idempotency_key = _workflow_command_idempotency_key(action)
    command_payload = dict(payload)
    if command_type_value == ControlCommandType.WORKFLOW_RETRY.value:
        command_payload = workflow_retry_payload_with_guard(pg, command_payload)
        idempotency_key = workflow_retry_idempotency_key(
            requested_by_kind="mcp",
            payload=command_payload,
        )

    command = execute_control_intent(
        pg,
        ControlIntent(
            command_type=command_type,
            requested_by_kind="mcp",
            requested_by_ref=f"praxis_workflow.{action}",
            idempotency_key=idempotency_key,
            payload=command_payload,
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
    try:
        action = _parse_workflow_action(params.get("action", "run"))
    except ValueError as exc:
        return {"error": str(exc)}

    if action == "wait":
        return {
            "error": (
                "action='wait' is no longer supported; use action='status' for run snapshots "
                "and notifications/progress for inline streaming."
            )
        }

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
        pg, error = _load_pg_conn(action="chain")
        if error is not None:
            return error

        adopt_active = params.get("adopt_active", True)
        if not isinstance(adopt_active, bool):
            if isinstance(adopt_active, str):
                adopt_active = adopt_active.strip().lower() in {"1", "true", "yes", "y", "on"}
            else:
                return {"error": "adopt_active must be a boolean"}

        # Dispatch through CQRS gateway (operation `workflow_chain_submit`,
        # registered 2026-04-28). Gateway records authority_operation_receipts
        # row + emits `workflow_chain.submitted` to authority_events.
        try:
            from runtime.operation_catalog_gateway import execute_operation_from_subsystems

            result = execute_operation_from_subsystems(
                _subs,
                operation_name="workflow_chain_submit",
                payload={
                    "coordination_path": str(coordination_path),
                    "adopt_active": bool(adopt_active),
                    "requested_by_kind": "mcp",
                    "requested_by_ref": "praxis_workflow.chain",
                },
            )
            if isinstance(result, dict) and result.get("ok") is False:
                return _submit_workflow_chain_via_service_bus(
                    pg,
                    coordination_path=str(coordination_path),
                    adopt_active=bool(adopt_active),
                )
            return result
        except Exception:
            return _submit_workflow_chain_via_service_bus(
                pg,
                coordination_path=str(coordination_path),
                adopt_active=bool(adopt_active),
            )

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
        previous_failure = params.get("previous_failure", "")
        retry_delta = params.get("retry_delta", "")
        if not run_id or not label:
            return {"error": "run_id and label are required for action='retry'"}
        if not str(previous_failure or "").strip() or not str(retry_delta or "").strip():
            return {
                "error": (
                    "previous_failure and retry_delta are required for action='retry'"
                ),
                "reason_code": "workflow.retry.explanation_required",
            }

        pg, error = _load_pg_conn(action="retry")
        if error is not None:
            return error
        try:
            from runtime.control_commands import ControlCommandType

            return _execute_workflow_command(
                pg,
                action="retry",
                command_type=ControlCommandType.WORKFLOW_RETRY,
                payload={
                    "run_id": run_id,
                    "label": label,
                    "previous_failure": previous_failure,
                    "retry_delta": retry_delta,
                },
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

    if action == "preview":
        inline_spec = params.get("inline_spec")
        if isinstance(inline_spec, str):
            try:
                inline_spec = json.loads(inline_spec)
            except json.JSONDecodeError as exc:
                return {"error": f"inline_spec must be JSON if provided as a string: {exc}"}
        spec_path = params.get("spec_path")
        if bool(spec_path) == isinstance(inline_spec, dict):
            return {"error": "provide exactly one of spec_path or inline_spec for action='preview'"}

        pg, error = _load_pg_conn(action="preview")
        if error is not None:
            return error
        try:
            from runtime.workflow.unified import preview_workflow_execution

            return preview_workflow_execution(
                pg,
                spec_path=str(spec_path) if spec_path else None,
                inline_spec=dict(inline_spec) if isinstance(inline_spec, dict) else None,
                repo_root=str(params.get("repo_root") or REPO_ROOT),
            )
        except Exception as exc:
            return _structured_runtime_error(exc, action="preview")

    # --- Run or dry-run ---
    spec_path = params.get("spec_path")
    if not spec_path:
        return {"error": "spec_path is required"}

    if action not in {"run", "spawn"}:
        return {
            "error": (
                f"Unsupported action='{action}'. Expected one of: "
                "run, spawn, preview, status, inspect, claim, acknowledge, cancel, list, notifications, retry, repair, chain."
            )
        }

    dry_run = params.get("dry_run", False)

    # Dry runs execute synchronously (fast)
    if dry_run:
        from runtime.workflow.dry_run import dry_run_workflow

        spec_mod = _workflow_spec_mod()
        spec = spec_mod.WorkflowSpec.load(spec_path)
        result = dry_run_workflow(
            spec,
            pg_conn=pg,
            repo_root=REPO_ROOT,
        )
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
        if result.get("error") or result.get("status") == "failed":
            # Enrich the failed response with admission_diagnosis (rejection
            # rows + next_actions per gate) so the operator/model sees which
            # tool to call instead of grepping schemas.
            try:
                result = _enrich_failed_submit_response(result, pg=pg)
            except Exception:  # noqa: BLE001
                pass
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
    except FileNotFoundError as exc:
        return {
            "valid": False,
            "error": str(exc),
        }
    except Exception as exc:
        return {
            "valid": False,
            "error": f"{type(exc).__name__}: {exc}",
        }


def tool_praxis_launch_plan(params: dict) -> dict:
    """Translate a packet list into a workflow spec and submit it — or preview first.

    This is the layer-5 translation primitive, not a planner. Caller (user or
    LLM) owns layers 1-4: extracting data pills, decomposing prose intent into
    steps, reordering by data-flow topology, authoring per-step prompts. This
    tool translates the already-planned packet list and submits through the
    CQRS bus.

    Modes:
    - preview_only=false (default): translate + submit in one call; returns
      a LaunchReceipt with run_id.
    - preview_only=true: translate + preview only; returns a ProposedPlan
      payload with the spec_dict, preview (resolved agents + rendered
      prompts + execution bundles), and packet_declarations showing what
      the caller declared vs what the platform derived. No submission.
      Use to inspect before approving the run. Proof-launch approval
      requires machine-checkable provider freshness evidence from either
      fresh route truth or a recent provider availability refresh receipt.
    """
    approved_payload = params.get("approved_plan")
    if approved_payload is not None:
        if not isinstance(approved_payload, dict):
            return {
                "ok": False,
                "error": "approved_plan must be the ApprovedPlan dict from praxis_approve_proposed_plan",
                "reason_code": "approved_plan.invalid",
            }
        try:
            pg_conn = _subs.get_pg_conn()
        except Exception as exc:
            return {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "reason_code": "postgres.authority.unavailable",
            }
        try:
            from runtime.spec_materializer import (
                ApprovalHashMismatchError,
                ApprovedPlan,
                LaunchSubmitFailedError,
                ProposedPlan,
                ProviderFreshnessGateError,
                launch_approved,
            )

            proposed_payload = approved_payload.get("proposed") or {}
            proposed = ProposedPlan(
                spec_dict=dict(proposed_payload.get("spec_dict") or {}),
                preview=dict(proposed_payload.get("preview") or {}),
                warnings=list(proposed_payload.get("warnings") or []),
                workflow_id=str(proposed_payload.get("workflow_id") or ""),
                spec_name=str(proposed_payload.get("spec_name") or ""),
                total_jobs=int(proposed_payload.get("total_jobs") or 0),
                packet_declarations=list(proposed_payload.get("packet_declarations") or []),
                binding_summary=dict(proposed_payload.get("binding_summary") or {}),
                unresolved_routes=list(proposed_payload.get("unresolved_routes") or []),
                provider_freshness=dict(proposed_payload.get("provider_freshness") or {}),
            )
            approved = ApprovedPlan(
                proposed=proposed,
                approved_by=str(approved_payload.get("approved_by") or ""),
                approved_at=str(approved_payload.get("approved_at") or ""),
                proposal_hash=str(approved_payload.get("proposal_hash") or ""),
                approval_note=approved_payload.get("approval_note"),
            )
            receipt = launch_approved(approved, conn=pg_conn)
        except ApprovalHashMismatchError as exc:
            return {
                "ok": False,
                "error": str(exc),
                "reason_code": "approval.hash_mismatch",
            }
        except LaunchSubmitFailedError as exc:
            return {
                "ok": False,
                "error": str(exc),
                "reason_code": "launch.submit_failed",
                "submit_status": exc.status,
                "submit_error_code": exc.error_code,
                "submit_error_detail": exc.error_detail,
                "spec_name": exc.spec_name,
                "submit_result": exc.submit_result,
            }
        except ProviderFreshnessGateError as exc:
            return {
                "ok": False,
                "error": str(exc),
                "reason_code": "provider_freshness.invalid",
            }
        except ValueError as exc:
            return {"ok": False, "error": str(exc), "reason_code": "approval.invalid"}
        except Exception as exc:
            return _structured_runtime_error(exc, action="launch_approved")

        payload = receipt.to_dict()
        payload["ok"] = True
        payload["mode"] = "launched_approved"
        return payload

    plan = params.get("plan")
    if not isinstance(plan, dict):
        return {
            "error": (
                "plan must be a dict with 'name' and either 'packets' or a "
                "'from_*' shortcut (from_bugs / from_roadmap_items), OR pass "
                "'approved_plan' to submit a previously approved plan"
            )
        }
    has_packets = bool(plan.get("packets"))
    has_from_source = bool(
        plan.get("from_bugs")
        or plan.get("from_roadmap_items")
        or plan.get("from_ideas")
        or plan.get("from_friction")
    )
    if has_packets and has_from_source:
        return {
            "error": (
                "plan accepts either explicit 'packets' OR 'from_*' shortcuts, "
                "not both — remove one to resolve the ambiguity"
            )
        }
    if not has_packets and not has_from_source:
        return {
            "error": (
                "plan must supply either 'packets', 'from_bugs', "
                "'from_roadmap_items', 'from_ideas', or 'from_friction'"
            )
        }

    workdir = params.get("workdir")
    preview_only = bool(params.get("preview_only"))

    try:
        pg_conn = _subs.get_pg_conn()
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "reason_code": "postgres.authority.unavailable",
        }

    try:
        if preview_only:
            from runtime.spec_materializer import propose_plan

            proposed = propose_plan(plan, conn=pg_conn, workdir=workdir)
            payload = proposed.to_dict()
            payload["ok"] = True
            payload["mode"] = "preview"
            return payload

        from runtime.operation_catalog_gateway import execute_operation_from_subsystems

        result = execute_operation_from_subsystems(
            _subs,
            operation_name="launch_plan",
            payload={"plan": plan, "workdir": workdir},
        )
    except Exception as exc:
        return _structured_runtime_error(exc, action="launch_plan")

    if not isinstance(result, dict):
        return _structured_runtime_error(
            RuntimeError(f"launch_plan returned non-dict: {type(result).__name__}"),
            action="launch_plan",
        )
    return result


def tool_praxis_plan_lifecycle(params: dict) -> dict:
    """Read every plan.* authority event for one workflow_id in order.

    Q-side of the planning stack's CQRS pattern: gateway-dispatched plan
    commands emit plan.composed / plan.launched through receipt-backed
    authority_events; this tool pulls the canonical stream back for Moon,
    CLI, or ad-hoc inspection.
    """
    workflow_id = params.get("workflow_id")
    if not isinstance(workflow_id, str) or not workflow_id.strip():
        return {
            "ok": False,
            "error": "workflow_id is required",
            "reason_code": "workflow_id.invalid",
        }

    try:
        pg_conn = _subs.get_pg_conn()
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "reason_code": "postgres.authority.unavailable",
        }

    try:
        from runtime.intent_composition import get_plan_lifecycle

        lifecycle = get_plan_lifecycle(workflow_id, conn=pg_conn)
    except Exception as exc:
        return _structured_runtime_error(exc, action="plan_lifecycle")

    payload = lifecycle.to_dict()
    payload["ok"] = True
    return payload


def tool_praxis_compose_and_launch(params: dict) -> dict:
    """End-to-end: prose intent → compose → approve → launch in one call.

    Kickoff-first by default (wait=False): spawns the compose+launch chain on a
    daemon thread, returns immediately with a correlation_id for trace lookup.
    The compose pipeline (LLM synthesis + N parallel fork-out authors +
    validation + approve + launch) runs without blocking the FastAPI request
    handler — required for swarm-scale concurrent compose calls.

    Pass ``wait=True`` for inline result. Synchronous calls under MCP often
    exceed the 60s tool-call timeout because the LLM fan-out alone is 8-15s.

    For trusted automation (CI, scripts, experienced operators). Fails
    closed by default if any job has unresolved routes or unbound pills.
    approved_by is required — no anonymous automation. Approval is
    hash-bound to the exact spec_dict so tampering between compose and
    submit still fails closed.
    """
    import threading
    import time
    import uuid

    intent = params.get("intent")
    if not isinstance(intent, str) or not intent.strip():
        return {
            "ok": False,
            "error": "intent must be a non-empty string",
            "reason_code": "intent.invalid",
        }
    approved_by = params.get("approved_by")
    if not isinstance(approved_by, str) or not approved_by.strip():
        return {
            "ok": False,
            "error": "approved_by is required — no anonymous automation",
            "reason_code": "approved_by.invalid",
        }

    refuse_unresolved_routes = params.get("refuse_unresolved_routes")
    if refuse_unresolved_routes is None:
        refuse_unresolved_routes = True
    refuse_unbound_pills = params.get("refuse_unbound_pills")
    if refuse_unbound_pills is None:
        refuse_unbound_pills = True

    wait = bool(params.get("wait", False))

    runtime_kwargs: dict[str, Any] = {
        "approved_by": approved_by,
        "approval_note": params.get("approval_note"),
        "plan_name": params.get("plan_name"),
        "why": params.get("why"),
        "workdir": params.get("workdir"),
        "default_write_scope": params.get("default_write_scope"),
        "default_stage": str(params.get("default_stage") or "build"),
        "refuse_unresolved_routes": bool(refuse_unresolved_routes),
        "refuse_unbound_pills": bool(refuse_unbound_pills),
    }

    def _invoke_synchronous(conn: Any) -> dict[str, Any]:
        from runtime.intent_composition import (
            ComposeAndLaunchBlocked,
            compose_and_launch,
        )
        from runtime.spec_materializer import (
            ApprovalHashMismatchError,
            LaunchSubmitFailedError,
        )

        try:
            receipt = compose_and_launch(intent, conn=conn, **runtime_kwargs)
        except ComposeAndLaunchBlocked as exc:
            return {
                "ok": False,
                "error": str(exc),
                "reason_code": "compose_and_launch.blocked",
                "blocked_reasons": exc.reasons,
            }
        except ApprovalHashMismatchError as exc:
            return {
                "ok": False,
                "error": str(exc),
                "reason_code": "approval.hash_mismatch",
            }
        except LaunchSubmitFailedError as exc:
            return {
                "ok": False,
                "error": str(exc),
                "reason_code": "launch.submit_failed",
                "submit_status": exc.status,
                "submit_error_code": exc.error_code,
                "submit_error_detail": exc.error_detail,
                "spec_name": exc.spec_name,
                "submit_result": exc.submit_result,
            }
        except ValueError as exc:
            return {"ok": False, "error": str(exc), "reason_code": "compose.invalid"}
        except Exception as exc:
            return _structured_runtime_error(exc, action="compose_and_launch")

        payload = receipt.to_dict()
        payload["ok"] = True
        return payload

    if wait:
        try:
            pg_conn = _subs.get_pg_conn()
        except Exception as exc:
            return {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "reason_code": "postgres.authority.unavailable",
            }
        return _invoke_synchronous(pg_conn)

    # Kickoff-first path: spawn a daemon thread for the compose+launch chain.
    # Mirrors tool_praxis_compose_plan_via_llm's kickoff pattern. The thread
    # uses its own subsystems (own DB conn) so it's safe across thread
    # boundaries. compose_and_launch eventually calls launch_approved which
    # writes its own gateway receipt — the run_id surfaces through receipts
    # under operation_ref='launch-plan' and 'compose-plan-via-llm'.
    from runtime.operation_catalog_gateway import (
        CURRENT_CALLER_CONTEXT,
        CallerContext,
        spawn_threaded,
    )
    kickoff_id = f"compose_launch_kickoff_{uuid.uuid4().hex[:12]}"
    kickoff_started_at = time.time()
    kickoff_correlation_id = str(uuid.uuid4())
    _ctx_token = CURRENT_CALLER_CONTEXT.set(
        CallerContext(
            cause_receipt_id=None,
            correlation_id=kickoff_correlation_id,
            transport_kind="workflow",
        )
    )

    def _run_compose_launch_in_background() -> None:
        try:
            local_subs = _subs.__class__()
        except Exception:  # noqa: BLE001
            return
        try:
            local_conn = local_subs.get_pg_conn()
        except Exception:  # noqa: BLE001
            return
        try:
            _invoke_synchronous(local_conn)
        except Exception:  # noqa: BLE001
            # Failures surface through the receipts written by compose_and_launch's
            # nested gateway calls. Nothing to log here since the caller already
            # got back the kickoff handle.
            pass

    spawn_threaded(
        _run_compose_launch_in_background,
        name=f"compose_launch_kickoff:{kickoff_id}",
    )
    CURRENT_CALLER_CONTEXT.reset(_ctx_token)

    return {
        "ok": True,
        "kickoff": True,
        "kickoff_id": kickoff_id,
        "kickoff_started_at": kickoff_started_at,
        "correlation_id": kickoff_correlation_id,
        "intent": intent,
        "next_step": (
            "compose+launch is running in the background. Walk the trace: "
            f"praxis_trace(correlation_id='{kickoff_correlation_id}'). "
            "Run-id appears in authority_operation_receipts under "
            "operation_ref='launch-plan' once compose finishes."
        ),
        "wait_for_synchronous": (
            "Pass wait=true to block until compose+launch returns inline. "
            "MCP tool-call timeouts may fire; kickoff-first is recommended for swarms."
        ),
    }


def tool_praxis_compose_plan(params: dict) -> dict:
    """Chain Layer 2 → Layer 1 → Layer 5 in one call.

    Takes prose intent with explicit step markers, decomposes into steps,
    translates each step into a PlanPacket, then runs propose_plan (which
    prepares, previews, and binds data pills per packet). Returns a
    ProposedPlan ready for caller approval and launch. No submission.

    Fails closed if the prose has no step markers unless the caller passes
    allow_single_step=true to accept the whole intent as one step. This is
    deliberate — free-prose decomposition is real LLM work that should not
    happen silently.
    """
    intent = params.get("intent")
    if not isinstance(intent, str) or not intent.strip():
        return {
            "ok": False,
            "error": "intent must be a non-empty string",
            "reason_code": "intent.invalid",
        }

    try:
        pg_conn = _subs.get_pg_conn()
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "reason_code": "postgres.authority.unavailable",
        }

    try:
        from runtime.operation_catalog_gateway import execute_operation_from_subsystems

        result = execute_operation_from_subsystems(
            _subs,
            operation_name="compose_plan",
            payload={
                "intent": intent,
                "plan_name": params.get("plan_name"),
                "why": params.get("why"),
                "workdir": params.get("workdir"),
                "allow_single_step": bool(params.get("allow_single_step")),
                "write_scope_per_step": params.get("write_scope_per_step"),
                "default_write_scope": params.get("default_write_scope"),
                "default_stage": str(params.get("default_stage") or "build"),
            },
        )
    except Exception as exc:
        return _structured_runtime_error(exc, action="compose_plan")

    if not isinstance(result, dict):
        return _structured_runtime_error(
            RuntimeError(f"compose_plan returned non-dict: {type(result).__name__}"),
            action="compose_plan",
        )
    return result


def tool_praxis_decompose_intent(params: dict) -> dict:
    """Split prose intent into ordered steps — deterministic, honest scope.

    Layer 2 (Decompose) of the planning stack. Parses explicit step markers
    from the prose: numbered lists ('1. X\\n2. Y'), bulleted lists
    ('- X\\n- Y'), or ordered-phrase sequences ('first X, then Y, finally
    Z'). Returns a DecomposedIntent with one entry per step.

    Fails closed when no explicit markers are found; the caller either
    rewords the intent with markers, wraps this tool with an LLM extractor
    that adds markers upstream, or passes allow_single_step=true to accept
    the whole intent as one step. This is deliberate — free-prose
    decomposition is real LLM work that should not happen silently.
    """
    intent = params.get("intent")
    if not isinstance(intent, str) or not intent.strip():
        return {
            "ok": False,
            "error": "intent must be a non-empty string",
            "reason_code": "intent.invalid",
        }
    allow_single_step = bool(params.get("allow_single_step"))
    try:
        from runtime.intent_decomposition import (
            DecompositionRequiresLLMError,
            decompose_intent,
        )

        result = decompose_intent(intent, allow_single_step=allow_single_step)
    except DecompositionRequiresLLMError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "decomposition.requires_llm",
        }
    except ValueError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "intent.invalid",
        }
    except Exception as exc:
        return _structured_runtime_error(exc, action="decompose_intent")

    payload = result.to_dict()
    payload["ok"] = True
    return payload


def tool_praxis_approve_proposed_plan(params: dict) -> dict:
    """Approve a ProposedPlan so launch_approved can submit it.

    Takes the ProposedPlan payload that came back from praxis_launch_plan
    with preview_only=true, wraps it with an explicit approval record
    (approved_by + timestamp + hash), and returns an ApprovedPlan payload.
    The hash binds the approval to the exact spec_dict — any modification
    between approve and launch will fail closed at launch time.

    Use when launch requires explicit approval (agent-initiated launches,
    user-facing UI confirmations, budget-gated workflows). For no-approval
    launches, keep using praxis_launch_plan in submit mode directly.
    """
    proposed_payload = params.get("proposed")
    if not isinstance(proposed_payload, dict):
        return {
            "ok": False,
            "error": "proposed must be a ProposedPlan dict from praxis_launch_plan(preview_only=true)",
            "reason_code": "proposed.invalid",
        }
    approved_by = params.get("approved_by")
    if not isinstance(approved_by, str) or not approved_by.strip():
        return {
            "ok": False,
            "error": "approved_by is required — identifier of the approver",
            "reason_code": "approved_by.invalid",
        }

    try:
        from runtime.spec_materializer import (
            ProposedPlan,
            approve_proposed_plan,
            ProviderFreshnessGateError,
        )

        proposed = ProposedPlan(
            spec_dict=dict(proposed_payload.get("spec_dict") or {}),
            preview=dict(proposed_payload.get("preview") or {}),
            warnings=list(proposed_payload.get("warnings") or []),
            workflow_id=str(proposed_payload.get("workflow_id") or ""),
            spec_name=str(proposed_payload.get("spec_name") or ""),
            total_jobs=int(proposed_payload.get("total_jobs") or 0),
            packet_declarations=list(proposed_payload.get("packet_declarations") or []),
            binding_summary=dict(proposed_payload.get("binding_summary") or {}),
            unresolved_routes=list(proposed_payload.get("unresolved_routes") or []),
            provider_freshness=dict(proposed_payload.get("provider_freshness") or {}),
        )
        approved = approve_proposed_plan(
            proposed,
            approved_by=approved_by,
            approval_note=params.get("approval_note"),
        )
    except ProviderFreshnessGateError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "reason_code": "provider_freshness.invalid",
        }
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "reason_code": "approval.invalid"}
    except Exception as exc:
        return _structured_runtime_error(exc, action="approve_proposed_plan")

    payload = approved.to_dict()
    payload["ok"] = True
    return payload


def tool_praxis_bind_data_pills(params: dict) -> dict:
    """Extract and validate data-pill references from prose intent.

    Layer 1 (Bind) of the planning stack: takes prose, returns the
    ``object.field`` references that resolve to real rows in the data
    dictionary authority. Deterministic (no LLM). Honest about scope —
    only finds explicit ``object.field`` spans. Loose-prose binding
    ("user's first name") is not attempted; wrap with an LLM extractor if
    that path is needed.

    Returns a BoundIntent payload with three splits the caller confirms
    before decomposing intent into packets:
      - bound: pills that resolved to a real field (with type + provenance)
      - ambiguous: pills that matched multiple rows (caller disambiguates)
      - unbound: pills that looked like refs but did not resolve (caller
        fixes typos or drops hallucinated fields)
    """
    intent = params.get("intent")
    if not isinstance(intent, str):
        return {"ok": False, "error": "intent must be a string", "reason_code": "intent.invalid"}
    object_kinds = params.get("object_kinds")
    if object_kinds is not None and not isinstance(object_kinds, list):
        return {
            "ok": False,
            "error": "object_kinds must be a list of strings when provided",
            "reason_code": "object_kinds.invalid",
        }

    try:
        pg_conn = _subs.get_pg_conn()
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "reason_code": "postgres.authority.unavailable",
        }

    try:
        from runtime.intent_binding import bind_data_pills

        result = bind_data_pills(intent, conn=pg_conn, object_kinds=object_kinds)
    except Exception as exc:
        return _structured_runtime_error(exc, action="bind_data_pills")

    payload = result.to_dict()
    payload["ok"] = True
    return payload


def tool_praxis_generate_plan(params: dict) -> dict:
    """Shared CQRS plan-generation front door for MCP callers."""
    action = str(params.get("action") or "generate_plan").strip() or "generate_plan"
    if action not in {"generate_plan", "materialize_plan"}:
        return {
            "ok": False,
            "error": "action must be one of generate_plan|materialize_plan",
            "reason_code": "plan_generation.action.invalid",
        }
    intent = params.get("intent") or params.get("prose")
    if not isinstance(intent, str) or not intent.strip():
        return {"ok": False, "error": "intent must be a non-empty string", "reason_code": "intent.invalid"}
    try:
        match_limit = int(params.get("match_limit", 5))
    except (TypeError, ValueError):
        return {
            "ok": False,
            "error": "match_limit must be an integer",
            "reason_code": "match_limit.invalid",
        }

    try:
        pg_conn = _subs.get_pg_conn()
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "reason_code": "postgres.authority.unavailable",
        }

    try:
        if action == "generate_plan":
            from runtime.operation_catalog_gateway import execute_operation_from_subsystems

            return execute_operation_from_subsystems(
                _subs,
                operation_name="compile_preview",
                payload={"intent": intent, "match_limit": match_limit},
            )

        enable_llm = params.get("enable_llm")
        if enable_llm is not None and not isinstance(enable_llm, bool):
            return {
                "ok": False,
                "error": "enable_llm must be a boolean when provided",
                "reason_code": "enable_llm.invalid",
            }
        enable_full_compose = params.get("enable_full_compose")
        if enable_full_compose is not None and not isinstance(enable_full_compose, bool):
            return {
                "ok": False,
                "error": "enable_full_compose must be a boolean when provided",
                "reason_code": "enable_full_compose.invalid",
            }

        # Dispatch through the registered CQRS materialization operation so
        # the gateway records a receipt and emits the authority event.
        from runtime.operation_catalog_gateway import execute_operation_from_subsystems

        payload = {
            "intent": intent,
            "match_limit": match_limit,
        }
        workflow_id_raw = params.get("workflow_id")
        if isinstance(workflow_id_raw, str) and workflow_id_raw.strip():
            payload["workflow_id"] = workflow_id_raw.strip()
        title_raw = params.get("title")
        if isinstance(title_raw, str) and title_raw.strip():
            payload["title"] = title_raw.strip()
        if isinstance(enable_llm, bool):
            payload["enable_llm"] = enable_llm
        if isinstance(enable_full_compose, bool):
            payload["enable_full_compose"] = enable_full_compose

        materialized = execute_operation_from_subsystems(
            _subs,
            operation_name="compile_materialize",
            payload=payload,
        )
        if not isinstance(materialized, dict) or materialized.get("ok") is False:
            return materialized if isinstance(materialized, dict) else {"ok": False, "result": materialized}
        materialized_workflow_id = str(materialized.get("workflow_id") or "").strip()
        if not materialized_workflow_id:
            return materialized
        build = execute_operation_from_subsystems(
            _subs,
            operation_name="workflow_build_get",
            payload={"workflow_id": materialized_workflow_id},
        )
        return {
            "ok": True,
            "action": "materialize_plan",
            "workflow_id": materialized_workflow_id,
            "graph_summary": materialized.get("graph_summary"),
            "operation_receipt": materialized.get("operation_receipt"),
            "materialization": materialized,
            "build": build,
        }
    except Exception as exc:
        return _structured_runtime_error(exc, action="plan_generation_cqrs")


def tool_praxis_suggest_plan_atoms(params: dict) -> dict:
    """Layer 0 (Suggest): pills + step types + parameters from free prose."""
    intent = params.get("intent")
    if not isinstance(intent, str):
        return {"ok": False, "error": "intent must be a string", "reason_code": "intent.invalid"}
    try:
        pg_conn = _subs.get_pg_conn()
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}",
                "reason_code": "postgres.authority.unavailable"}
    try:
        from runtime.intent_suggestion import suggest_plan_atoms
        result = suggest_plan_atoms(intent, conn=pg_conn)
    except Exception as exc:
        return _structured_runtime_error(exc, action="suggest_plan_atoms")
    payload = result.to_dict()
    payload["ok"] = True
    return payload


def tool_praxis_synthesize_skeleton(params: dict) -> dict:
    """Layer 0.5 (Synthesize): atoms + deterministic depends_on / floors / gate scaffolds."""
    intent = params.get("intent")
    if not isinstance(intent, str):
        return {"ok": False, "error": "intent must be a string", "reason_code": "intent.invalid"}
    try:
        pg_conn = _subs.get_pg_conn()
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}",
                "reason_code": "postgres.authority.unavailable"}
    try:
        from runtime.intent_dependency import synthesize_skeleton
        from runtime.intent_suggestion import suggest_plan_atoms
        atoms = suggest_plan_atoms(intent, conn=pg_conn)
        skeleton = synthesize_skeleton(atoms, conn=pg_conn)
    except Exception as exc:
        return _structured_runtime_error(exc, action="synthesize_skeleton")
    payload = skeleton.to_dict()
    payload["atoms"] = atoms.to_dict()
    payload["ok"] = True
    return payload


def tool_praxis_compose_plan_via_llm(params: dict) -> dict:
    """End-to-end: atoms → skeleton → synthesis (1 LLM call) → fork-out N parallel
    authors → validate.

    Kickoff-first by default (wait=False): spawns the compose work on a daemon
    thread, returns immediately with an ``operation_ref`` handle. The caller
    queries ``praxis_receipts(action='search', query='compose-plan-via-llm')``
    to retrieve the result once the gateway writes the receipt.

    Pass ``wait=True`` when a UI needs the result inline. Synchronous calls under
    MCP frequently exceed the 60s tool-call timeout — that's why kickoff-first
    is the new default for the MCP path.
    """
    import threading
    import time
    import uuid

    intent = params.get("intent")
    if not isinstance(intent, str):
        return {"ok": False, "error": "intent must be a string", "reason_code": "intent.invalid"}
    concurrency = params.get("concurrency", 20)
    try:
        concurrency = max(1, min(100, int(concurrency)))
    except (TypeError, ValueError):
        return {"ok": False, "error": "concurrency must be an integer 1-100",
                "reason_code": "concurrency.invalid"}
    plan_name = params.get("plan_name")
    why = params.get("why")
    wait = bool(params.get("wait", False))

    try:
        pg_conn = _subs.get_pg_conn()
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}",
                "reason_code": "postgres.authority.unavailable"}

    if wait:
        # Synchronous path for callers that need the plan inline.
        try:
            from runtime.compose_plan_via_llm import compose_plan_via_llm
            result = compose_plan_via_llm(
                intent, conn=pg_conn,
                plan_name=plan_name if isinstance(plan_name, str) else None,
                why=why if isinstance(why, str) else None,
                concurrency=concurrency,
            )
        except Exception as exc:
            return _structured_runtime_error(exc, action="compose_plan_via_llm")
        return result.to_dict()

    # Kickoff-first path: spawn a daemon thread that runs the compose pipeline
    # and writes the receipt via the standard runtime path. Return immediately
    # with a tracking handle. The caller polls via praxis_receipts.
    kickoff_id = f"compose_kickoff_{uuid.uuid4().hex[:12]}"
    kickoff_started_at = time.time()

    # Causal tracing: mint a correlation_id at kickoff and set the
    # CURRENT_CALLER_CONTEXT ContextVar so every gateway call inside the
    # background compose pipeline (synthesis, fork-out authors, validation)
    # gets stamped with the same correlation_id. The kickoff itself is not
    # a registered gateway operation, so there is no parent receipt — the
    # whole compose subtree shares this fresh root correlation. The
    # correlation_id flows back in the response so callers can pass it to
    # praxis_trace as the trace anchor.
    from runtime.operation_catalog_gateway import (
        CURRENT_CALLER_CONTEXT,
        CallerContext,
        spawn_threaded,
    )
    kickoff_correlation_id = str(uuid.uuid4())
    _ctx_token = CURRENT_CALLER_CONTEXT.set(
        CallerContext(
            cause_receipt_id=None,
            correlation_id=kickoff_correlation_id,
            transport_kind="workflow",
        )
    )

    def _run_compose_in_background() -> None:
        # Each background thread needs its own DB connection — sync-bridge
        # connections are not safe to share across threads.
        try:
            local_subs = _subs.__class__()
        except Exception:  # noqa: BLE001
            return
        try:
            # Dispatch through the CQRS gateway so a receipt + plan.composed
            # event get written under the kickoff's correlation_id. The
            # bare runtime.compose_plan_via_llm function bypasses the
            # gateway entirely — switching to execute_operation_from_subsystems
            # is what makes praxis_trace(correlation_id=...) actually surface
            # the compose work.
            from runtime.operation_catalog_gateway import (
                execute_operation_from_subsystems,
            )
            payload: dict[str, Any] = {"intent": intent, "concurrency": concurrency}
            if isinstance(plan_name, str):
                payload["plan_name"] = plan_name
            if isinstance(why, str):
                payload["why"] = why
            execute_operation_from_subsystems(
                local_subs,
                operation_name="compose_plan_via_llm",
                payload=payload,
            )
        except Exception:  # noqa: BLE001
            # The receipt the gateway writes will carry the failure; nothing
            # to surface here since the caller has already returned.
            pass

    # spawn_threaded snapshots the current ContextVar context so the
    # background thread inherits CURRENT_CALLER_CONTEXT — without it,
    # threading.Thread starts with empty ContextVars and the compose
    # subtree gets a disconnected fresh correlation per gateway call.
    spawn_threaded(
        _run_compose_in_background,
        name=f"compose_kickoff:{kickoff_id}",
    )
    # Reset the caller context on this MCP call so the kickoff's
    # correlation_id does not leak into any other work the daemon thread
    # does after this function returns. The background thread already
    # snapshotted Context inside spawn_threaded, so reset here is safe.
    CURRENT_CALLER_CONTEXT.reset(_ctx_token)

    return {
        "ok": True,
        "kickoff": True,
        "kickoff_id": kickoff_id,
        "kickoff_started_at": kickoff_started_at,
        "correlation_id": kickoff_correlation_id,
        "operation_ref": "compose-plan-via-llm",
        "intent": intent,
        "next_step": (
            "compose is running in the background; results land in "
            "authority_operation_receipts under operation_ref='compose-plan-via-llm'. "
            f"Walk the trace: praxis_trace(correlation_id='{kickoff_correlation_id}'). "
            "Or fetch the receipt: praxis_receipts(action='search', query='compose-plan-via-llm')."
        ),
        "wait_for_synchronous": (
            "Pass wait=true to block until the pipeline finishes when a caller "
            "needs the plan inline. MCP tool-call timeouts may fire; kickoff-first is recommended."
        ),
    }


def tool_praxis_compose_experiment(params: dict) -> dict:
    """Parallel matrix runner: fire N compose_plan_via_llm calls side-by-side
    with knob variation (model_slug / temperature / max_tokens) and return a
    ranked comparison report.

    Inputs:
      - intent (str, required): prose forwarded to every child compose call
      - configs (list of dicts, required, non-empty): one entry per child
        run; each may contain {provider_slug, model_slug, temperature,
        max_tokens}. Missing keys keep task-level defaults.
      - plan_name (str, optional)
      - concurrency (int, optional, default 5): per-child fork-out
      - max_workers (int, optional, default 8): parent fan-out cap
    """
    intent = params.get("intent")
    if not isinstance(intent, str) or not intent.strip():
        return {"ok": False, "error": "intent must be a non-empty string",
                "reason_code": "intent.invalid"}
    configs = params.get("configs")
    if not isinstance(configs, list) or not configs:
        return {"ok": False, "error": "configs must be a non-empty list of override dicts",
                "reason_code": "configs.invalid"}
    try:
        concurrency = max(1, min(100, int(params.get("concurrency", 5))))
    except (TypeError, ValueError):
        return {"ok": False, "error": "concurrency must be an integer 1-100",
                "reason_code": "concurrency.invalid"}
    try:
        max_workers = max(1, min(32, int(params.get("max_workers", 8))))
    except (TypeError, ValueError):
        return {"ok": False, "error": "max_workers must be an integer 1-32",
                "reason_code": "max_workers.invalid"}
    plan_name = params.get("plan_name")

    # Dispatch through the operation gateway so the run produces a parent
    # receipt + the compose.experiment.completed event automatically. The
    # gateway resolves the binding via operation_catalog_registry and calls
    # runtime.operations.commands.compose_experiment_command.handle_compose_experiment.
    try:
        from runtime.operation_catalog_gateway import execute_operation_from_subsystems
        result = execute_operation_from_subsystems(
            _subs,
            operation_name="compose_experiment",
            payload={
                "intent": intent,
                "configs": configs,
                "plan_name": plan_name if isinstance(plan_name, str) else None,
                "concurrency": concurrency,
                "max_workers": max_workers,
                "caller_ref": "mcp.praxis_compose_experiment",
            },
        )
    except Exception as exc:
        return _structured_runtime_error(exc, action="compose_experiment")
    return result if isinstance(result, dict) else {"ok": True, "result": result}


def tool_praxis_promote_experiment_winner(params: dict) -> dict:
    """Promote one compose-experiment leg back into task_type_routing.

    Inputs:
      - source_experiment_receipt_id (str, required): receipt id from
        praxis_compose_experiment.
      - source_config_index (int, required): the winning config index in
        that experiment receipt.
      - target_task_type (str, optional): explicit override when the
        source config omitted base_task_type.
    """
    source_experiment_receipt_id = params.get("source_experiment_receipt_id")
    if not isinstance(source_experiment_receipt_id, str) or not source_experiment_receipt_id.strip():
        return {
            "ok": False,
            "error": "source_experiment_receipt_id must be a non-empty string",
            "reason_code": "source_experiment_receipt_id.invalid",
        }
    try:
        raw_source_config_index = params.get("source_config_index")
        if isinstance(raw_source_config_index, bool):
            raise ValueError
        source_config_index = int(raw_source_config_index)
        if source_config_index < 0:
            raise ValueError
    except (TypeError, ValueError):
        return {
            "ok": False,
            "error": "source_config_index must be a non-negative integer",
            "reason_code": "source_config_index.invalid",
        }
    target_task_type = params.get("target_task_type")
    if target_task_type is not None and (not isinstance(target_task_type, str) or not target_task_type.strip()):
        return {
            "ok": False,
            "error": "target_task_type must be a non-empty string when provided",
            "reason_code": "target_task_type.invalid",
        }

    try:
        from runtime.operation_catalog_gateway import execute_operation_from_subsystems

        result = execute_operation_from_subsystems(
            _subs,
            operation_name="experiment_promote_winner",
            payload={
                "source_experiment_receipt_id": source_experiment_receipt_id,
                "source_config_index": source_config_index,
                "target_task_type": target_task_type if isinstance(target_task_type, str) else None,
                "caller_ref": "mcp.praxis_promote_experiment_winner",
            },
        )
    except Exception as exc:
        return _structured_runtime_error(exc, action="experiment_promote_winner")
    return result if isinstance(result, dict) else {"ok": True, "result": result}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_suggest_plan_atoms": (
        tool_praxis_suggest_plan_atoms,
        {
            "description": (
                "Layer 0 (Suggest): free prose → pills + step types + parameters. "
                "Deterministic; no LLM call; no order or count produced."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {"intent": {"type": "string"}},
                "required": ["intent"],
            },
        },
    ),
    "praxis_synthesize_skeleton": (
        tool_praxis_synthesize_skeleton,
        {
            "description": (
                "Layer 0.5 (Synthesize): atoms + skeleton with deterministic depends_on, "
                "consumes/produces/capabilities floors, scaffolded gates from data dictionary."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {"intent": {"type": "string"}},
                "required": ["intent"],
            },
        },
    ),
    "praxis_compose_plan_via_llm": (
        tool_praxis_compose_plan_via_llm,
        {
            "description": (
                "End-to-end LLM plan composition: atoms → skeleton → ONE synthesis LLM call "
                "(few-sentence plan statement) → N parallel fork-out author calls "
                "(each shares the synthesis as cached prefix) → validate.\n\n"
                "KICKOFF-FIRST BY DEFAULT (wait=False): the call returns immediately "
                "with a kickoff handle while the compose pipeline runs on a background "
                "thread. The receipt lands in `authority_operation_receipts` under "
                "`operation_ref='compose-plan-via-llm'` when complete; query it via "
                "`praxis_receipts(action='search', query='compose-plan-via-llm')`. "
                "This avoids the 60s MCP tool-call timeout for full pipeline runs.\n\n"
                "Pass `wait=True` only when a caller needs the plan inline. Synchronous calls "
                "via MCP often exceed the tool-call timeout."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {"type": "string"},
                    "plan_name": {"type": "string"},
                    "why": {"type": "string"},
                    "concurrency": {"type": "integer", "default": 20},
                    "wait": {
                        "type": "boolean",
                        "default": False,
                        "description": (
                            "False (default): kickoff-first; tool returns "
                            "immediately, work runs in background, receipt lands "
                            "asynchronously. True: synchronous inline result; "
                            "tool blocks until full pipeline completes."
                        ),
                    },
                },
                "required": ["intent"],
            },
        },
    ),
    "praxis_compose_experiment": (
        tool_praxis_compose_experiment,
        {
            "description": (
                "Parallel matrix runner: fire N compose_plan_via_llm calls side-by-side, "
                "each with a different LLM knob configuration. Returns a ranked report "
                "(success-first, wall-time-asc). Each child run produces its own "
                "compose-plan-via-llm receipt + plan.composed event; the matrix run "
                "produces a parent receipt + a compose.experiment.completed event.\n\n"
                "TWO CONFIG SHAPES:\n"
                " 1. base_task_type + overrides (preferred). Inherit from a row in "
                "    task_type_routing (provider, model, temperature, max_tokens) and "
                "    layer per-leg deltas. e.g. {base_task_type: 'plan_synthesis', "
                "    overrides: {temperature: 0.7}}.\n"
                " 2. flat dict (escape hatch). Specify everything ad-hoc: "
                "    {model_slug: 'x/y', temperature: 0.7, max_tokens: 4096}.\n\n"
                "USE WHEN: comparing model/temperature/cap settings on the same intent "
                "before pinning one in task_type_routing. Inheriting from a base lets "
                "you say 'test plan_synthesis at higher temp' rather than re-stating "
                "every knob."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {"type": "string"},
                    "configs": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "base_task_type": {
                                    "type": "string",
                                    "description": "Name of a task_type_routing row to inherit from (provider, model, temperature, max_tokens).",
                                },
                                "overrides": {
                                    "type": "object",
                                    "properties": {
                                        "provider_slug": {"type": "string"},
                                        "model_slug": {"type": "string"},
                                        "temperature": {"type": "number"},
                                        "max_tokens": {"type": "integer"},
                                    },
                                    "additionalProperties": False,
                                },
                                "provider_slug": {"type": "string"},
                                "model_slug": {"type": "string"},
                                "temperature": {"type": "number"},
                                "max_tokens": {"type": "integer"},
                            },
                        },
                    },
                    "plan_name": {"type": "string"},
                    "concurrency": {"type": "integer", "default": 5},
                    "max_workers": {"type": "integer", "default": 8},
                },
                "required": ["intent", "configs"],
            },
        },
    ),
    "praxis_promote_experiment_winner": (
        tool_praxis_promote_experiment_winner,
        {
            "description": (
                "Promote one compose-experiment leg into the canonical task_type_routing row "
                "for that task type. The winning leg's temperature and max_tokens are applied; "
                "provider/model changes remain visible only in the returned diff."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_experiment_receipt_id": {"type": "string"},
                    "source_config_index": {"type": "integer", "minimum": 0},
                    "target_task_type": {"type": "string"},
                },
                "required": ["source_experiment_receipt_id", "source_config_index"],
            },
        },
    ),
    "praxis_generate_plan": (
        tool_praxis_generate_plan,
        {
            "description": (
                "Shared CQRS plan-generation front door. action='generate_plan' recognizes messy "
                "prose, matches spans to authority, returns suggestions and gaps, and does not "
                "mutate state. action='materialize_plan' creates or updates a draft workflow "
                "through the canonical workflow build mutation.\n\n"
                "USE WHEN: MCP callers need the same plan-generation behavior as CLI/API/UI.\n\n"
                "DO NOT USE TO: silently launch a workflow run. Materialize creates build state only; "
                "launch still goes through approval/run surfaces."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["generate_plan", "materialize_plan"],
                        "default": "generate_plan",
                    },
                    "intent": {"type": "string", "description": "Messy prose to turn into a plan."},
                    "workflow_id": {
                        "type": "string",
                        "description": "Existing or desired workflow id for materialize.",
                    },
                    "title": {"type": "string", "description": "Workflow title for materialize."},
                    "enable_llm": {
                        "type": "boolean",
                        "description": "Optional compiler LLM switch. Omitted means use compiler policy.",
                    },
                    "enable_full_compose": {
                        "type": "boolean",
                        "description": (
                            "Pipeline selector for materialize. True (default) routes through "
                            "compose_plan_via_llm (synthesis + N-way fork-out, plan_synthesis + "
                            "plan_fork_author task types). False routes through compile_prose "
                            "(compile_synthesize → compile_pill_match → compile_author → "
                            "compile_finalize sub-tasks; runs voting-based binding-gate "
                            "auto-resolution)."
                        ),
                    },
                    "match_limit": {
                        "type": "integer",
                        "description": "Maximum authority candidates per recognized span.",
                    },
                },
                "required": ["intent"],
            },
        },
    ),
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
                "  - action='preview' assembles the exact worker-facing execution payload without "
                "creating a workflow run.\n"
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
                "  Preview inputs:  praxis_workflow(action='preview', spec_path='artifacts/workflow/my_spec.queue.json')\n"
                "  Spawn child:     praxis_workflow(action='spawn', spec_path='...', parent_run_id='workflow_parent', dispatch_reason='phase.spawn')\n"
                "  Force kickoff:   praxis_workflow(action='run', spec_path='...', wait=false)\n"
                "  Check status:    praxis_workflow(action='status', run_id='workflow_abc123')\n"
                "  Retry a failure: praxis_workflow(action='retry', run_id='workflow_abc123', label='build_step', previous_failure='receipt-backed failure', retry_delta='what changed this time')\n"
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
                    "inline_spec": {
                        "type": "object",
                        "description": "Inline workflow spec. Allowed for action='preview' when no spec_path is provided.",
                    },
                    "repo_root": {
                        "type": "string",
                        "description": "Optional repo root for preview path resolution. Defaults to the workflow repo root.",
                    },
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
                            "'preview' (assemble the worker-facing execution payload without creating a run), "
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
                        "enum": ["run", "spawn", "preview", "status", "inspect", "claim", "acknowledge", "cancel", "list", "notifications", "retry", "repair", "chain"],
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
    "praxis_launch_plan": (
        tool_praxis_launch_plan,
        {
            "description": (
                "Translate a packet list into a workflow spec and submit it — or preview first. "
                "This is the layer-5 translation primitive, not a planner. Caller (user or LLM) "
                "owns upstream planning: (1) extract data pills from intent, (2) decompose prose "
                "into steps, (3) reorder by data-flow, (4) author per-step prompts. This tool "
                "translates the already-planned packet list through the capability catalog and "
                "submits through the CQRS bus.\n\n"
                "MODES:\n"
                "  - preview_only=false (default): translate + submit in one call; returns "
                "LaunchReceipt with run_id.\n"
                "  - preview_only=true: translate + preview only; returns ProposedPlan with the "
                "spec_dict, a preview payload (resolved agents, rendered prompts, execution "
                "bundles), and packet_declarations showing what the caller declared vs what the "
                "platform derived. No submission.\n\n"
                "USE WHEN: you have a packet list (each with description + write + stage) to run "
                "as a single workflow_run. Optional per-packet fields: label, read, depends_on, "
                "bug_ref, complexity ('low' triggers prefer_cost routing).\n\n"
                "DO NOT USE TO: do the planning itself. This tool will not extract fields from "
                "prose, will not split a paragraph into steps, will not reorder by data-flow, and "
                "will not author real prompts beyond the stage template shim. If you need those, "
                "you (the caller) do them before calling this tool.\n\n"
                "EXAMPLE (submit): praxis_launch_plan(plan={\"name\": \"bug_wave_0\", \"packets\": ["
                "{\"description\": \"fix bug evidence authority\", "
                "\"write\": [\"Code&DBs/Workflow/runtime/bugs.py\"], "
                "\"stage\": \"build\", \"bug_ref\": \"BUG-175EB9F3\"}]})\n\n"
                "EXAMPLE (preview first): praxis_launch_plan(preview_only=true, plan={...}) → "
                "inspect ProposedPlan → praxis_launch_plan(plan={...}) to submit."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "plan": {
                        "type": "object",
        "description": (
            "Plan to materialize and launch. Required: name PLUS either 'packets' "
            "(explicit list) or 'from_bugs' (bug-ID list that materializes "
            "packets through derive_bug_packets with wave-dependency wiring). "
            "Supplying both is rejected as an ambiguity error. Optional: "
            "workflow_id (auto-generated if absent), why (narrative), phase "
            "(default 'build'), workdir (defaults to caller's workdir), "
            "program_id (used as the bug-resolution program ID when from_bugs "
            "is set; defaults to 'plan.<name>'). Proof launches can also carry "
            "provider_route_truth, provider_availability_refresh, or the canonical "
            "provider_freshness payload for machine-checkable freshness evidence."
        ),
                        "properties": {
                            "name": {"type": "string"},
                            "why": {"type": "string"},
                            "workflow_id": {"type": "string"},
                            "phase": {"type": "string"},
                            "workdir": {"type": "string"},
                            "program_id": {"type": "string"},
                            "provider_route_truth": {
                                "type": "object",
                                "description": "Fresh route-truth evidence for proof-launch approval.",
                            },
                            "provider_availability_refresh": {
                                "type": "object",
                                "description": "Recent provider availability refresh receipt for proof-launch approval.",
                            },
                            "provider_freshness": {
                                "type": "object",
                                "description": "Canonical machine-checkable freshness payload; must include route truth or a refresh receipt.",
                            },
                            "from_bugs": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": (
                                    "Bug IDs to materialize into clustered PlanPackets via "
                                    "derive_bug_packets. Each cluster becomes one packet; "
                                    "wave dependencies become per-packet depends_on edges. "
                                    "Unknown or missing bug IDs are silently dropped."
                                ),
                            },
                            "from_roadmap_items": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": (
                                    "Roadmap item IDs to materialize into PlanPackets. Each "
                                    "active item becomes one packet whose description is built "
                                    "from title + summary + acceptance_criteria.must_have. "
                                    "Stage defaults to 'fix' when the roadmap item has a "
                                    "source_bug_id, else 'build'. Completed items are dropped. "
                                    "from_bugs and from_roadmap_items can be combined."
                                ),
                            },
                            "from_ideas": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": (
                                    "Operator idea IDs to materialize into PlanPackets. Each "
                                    "open idea becomes one build-stage packet; promoted, "
                                    "rejected, superseded, and archived ideas are dropped — "
                                    "those have moved to roadmap or bug surfaces. Combines "
                                    "with other from_* shortcuts."
                                ),
                            },
                            "from_friction": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": (
                                    "friction_event IDs to materialize into fix-stage packets. "
                                    "Description carries friction_type + source + job_label + "
                                    "message. No lifecycle filter — events are events. "
                                    "Combines with other from_* shortcuts."
                                ),
                            },
                            "packets": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "description": {"type": "string"},
                                        "write": {"type": "array", "items": {"type": "string"}},
                                        "stage": {
                                            "type": "string",
                                            "enum": ["build", "fix", "review", "test", "research"],
                                        },
                                        "label": {"type": "string"},
                                        "read": {"type": "array", "items": {"type": "string"}},
                                        "depends_on": {"type": "array", "items": {"type": "string"}},
                                        "bug_ref": {"type": "string"},
                                        "bug_refs": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "description": (
                                                "All bugs this packet resolves when it covers a "
                                                "cluster. bug_ref (singular) is the primary."
                                            ),
                                        },
                                        "agent": {"type": "string"},
                                        "complexity": {"type": "string", "enum": ["low", "moderate", "high"]},
                                    },
                                    "required": ["description", "write", "stage"],
                                },
                            },
                        },
                        "required": ["name"],
                    },
                    "workdir": {
                        "type": "string",
                        "description": "Optional override for the workflow's workdir.",
                    },
                    "preview_only": {
                        "type": "boolean",
                        "default": False,
                        "description": (
                            "If true, translate the plan and return a ProposedPlan (spec_dict + "
                            "preview + packet_declarations) without submitting. Use to inspect "
                            "what will actually run before committing to resources."
                        ),
                    },
                    "approved_plan": {
                        "type": "object",
                        "description": (
                            "Submit a previously approved plan — the ApprovedPlan payload "
                            "returned by praxis_approve_proposed_plan. Fails closed if the "
                            "spec_dict hash no longer matches the approval (tampering guard). "
                            "When this field is set, 'plan' and 'preview_only' are ignored."
                        ),
                    },
                },
            },
        },
    ),
    "praxis_plan_lifecycle": (
        tool_praxis_plan_lifecycle,
        {
            "description": (
                "Q-side of the planning stack: read every plan.* authority_event for one "
                "workflow_id in order. Pair with gateway-backed praxis_compose_plan / "
                "praxis_launch_plan on the C side.\n\n"
                "USE WHEN: an operator or Moon wants to inspect what happened to a plan "
                "— composed when, launched with which run_id, and the event payload "
                "that was receipt-backed by the command gateway.\n\n"
                "DO NOT USE TO: read workflow_run status. That's a separate Q on the "
                "workflow_runs + workflow_jobs tables, surfaced by praxis_workflow's "
                "status / stream actions.\n\n"
                "EXAMPLE: praxis_plan_lifecycle(workflow_id='plan.deadbeef12345678')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "workflow_id": {
                        "type": "string",
                        "description": (
                            "The workflow_id the plan was composed under — carried in "
                            "ProposedPlan.workflow_id and LaunchReceipt.workflow_id."
                        ),
                    },
                },
                "required": ["workflow_id"],
            },
        },
    ),
    "praxis_compose_and_launch": (
        tool_praxis_compose_and_launch,
        {
            "description": (
                "End-to-end: prose intent → compose → approve → launch in one call. "
                "Compose the ProposedPlan through Layers 2 → 1 → 5, wrap with an explicit "
                "approval record (approved_by + hash), and submit through the CQRS "
                "control-command bus.\n\n"
                "USE WHEN: automation (CI, scripts, experienced operators) needs to run "
                "the full pipeline without stopping for a manual approval tool call. The "
                "pipeline still fails closed on safety checks: unresolved routes, unbound "
                "pills, and invalid approvals all block the submit.\n\n"
                "DO NOT USE TO: skip review for untrusted input. Anonymous approval is "
                "rejected — approved_by is required. Tampering between compose and "
                "submit still fails at launch_approved via the spec_dict hash check.\n\n"
                "EXAMPLE: praxis_compose_and_launch(intent='1. Add timezone column\\n"
                "2. Backfill UTC\\n3. Update UI', approved_by='ci@praxis')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Prose with explicit step markers.",
                    },
                    "approved_by": {
                        "type": "string",
                        "description": "Identifier of the approver (required).",
                    },
                    "approval_note": {"type": "string"},
                    "plan_name": {"type": "string"},
                    "why": {"type": "string"},
                    "workdir": {"type": "string"},
                    "allow_single_step": {"type": "boolean", "default": False},
                    "write_scope_per_step": {
                        "type": "array",
                        "items": {"type": "array", "items": {"type": "string"}},
                    },
                    "default_write_scope": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "default_stage": {
                        "type": "string",
                        "enum": ["build", "fix", "review", "test", "research"],
                        "default": "build",
                    },
                    "refuse_unresolved_routes": {"type": "boolean", "default": True},
                    "refuse_unbound_pills": {"type": "boolean", "default": True},
                },
                "required": ["intent", "approved_by"],
            },
        },
    ),
    "praxis_compose_plan": (
        tool_praxis_compose_plan,
        {
            "description": (
                "Chain Layer 2 (decompose) → Layer 1 (bind) → Layer 5 (translate + preview) "
                "in one call. Takes prose intent with explicit step markers, returns a "
                "ProposedPlan ready for approval and launch.\n\n"
                "USE WHEN: you have a numbered / bulleted / first-then-finally intent and "
                "want the full pre-submit pipeline in one shot. Composes with "
                "praxis_approve_proposed_plan + praxis_launch_plan(approved_plan=...) for "
                "the full approval-gated launch.\n\n"
                "DO NOT USE TO: skip planning. Free prose without step markers fails closed "
                "with reason_code='decomposition.requires_llm' — reword the intent, wrap "
                "with an LLM extractor upstream, or pass allow_single_step=true to treat "
                "the whole intent as one step.\n\n"
                "EXAMPLE: praxis_compose_plan(intent='1. Add timezone column\\n2. Backfill "
                "UTC\\n3. Update UI', plan_name='timezone_rollout')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Prose with explicit step markers (numbered, bulleted, or first/then/finally).",
                    },
                    "plan_name": {
                        "type": "string",
                        "description": "Name for the resulting Plan. Auto-generated if absent.",
                    },
                    "why": {
                        "type": "string",
                        "description": "Optional narrative about why this plan is being launched.",
                    },
                    "workdir": {
                        "type": "string",
                        "description": "Optional override for the workflow's workdir.",
                    },
                    "allow_single_step": {
                        "type": "boolean",
                        "default": False,
                        "description": "Accept prose without step markers as a single step.",
                    },
                    "write_scope_per_step": {
                        "type": "array",
                        "items": {"type": "array", "items": {"type": "string"}},
                        "description": (
                            "Optional per-step write scope. Length must match the decomposed "
                            "step count. When absent, default_write_scope is used for every "
                            "step (workspace root by default, with a warning)."
                        ),
                    },
                    "default_write_scope": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Fallback write scope when no per-step scope is provided.",
                    },
                    "default_stage": {
                        "type": "string",
                        "enum": ["build", "fix", "review", "test", "research"],
                        "default": "build",
                        "description": "Stage used when a step has no stage_hint from its verb.",
                    },
                },
                "required": ["intent"],
            },
        },
    ),
    "praxis_decompose_intent": (
        tool_praxis_decompose_intent,
        {
            "description": (
                "Layer 2 (Decompose) of the planning stack: split prose intent into ordered "
                "steps by parsing explicit step markers (numbered lists, bulleted lists, or "
                "first/then/finally ordering). Deterministic — does NOT do free-prose "
                "semantic decomposition.\n\n"
                "USE WHEN: you have a paragraph that already lists the steps explicitly and "
                "want them split into one record per step so the caller can turn each into "
                "a PlanPacket.\n\n"
                "DO NOT USE TO: decompose free prose. If the intent has no explicit markers "
                "this tool fails with reason_code='decomposition.requires_llm' — reword the "
                "intent with markers, wrap with an LLM extractor, or pass allow_single_step=true "
                "to accept the whole intent as one step.\n\n"
                "EXAMPLE: praxis_decompose_intent(intent='1. Add timezone column\\n2. Backfill "
                "existing rows with UTC\\n3. Update the profile UI.')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Prose describing the work, with explicit step markers.",
                    },
                    "allow_single_step": {
                        "type": "boolean",
                        "default": False,
                        "description": (
                            "If true, prose without step markers is accepted as a single "
                            "step instead of raising. Use only when you are confident the "
                            "intent is one step."
                        ),
                    },
                },
                "required": ["intent"],
            },
        },
    ),
    "praxis_approve_proposed_plan": (
        tool_praxis_approve_proposed_plan,
        {
            "description": (
                "Approve a ProposedPlan so launch_approved can submit it. "
                "Takes the ProposedPlan payload from praxis_launch_plan(preview_only=true), "
                "wraps it with approved_by + timestamp + hash, and returns an ApprovedPlan. "
                "The hash binds the approval to the exact spec_dict — tampering between "
                "approve and launch fails closed at launch time. The ProposedPlan must "
                "already carry machine-checkable provider freshness evidence.\n\n"
                "USE WHEN: launch must be explicit and audit-friendly — agent-initiated "
                "launches, user-facing UI confirmations, budget-gated workflows.\n\n"
                "DO NOT USE TO: skip approval for direct launches. For those, use "
                "praxis_launch_plan in submit mode directly.\n\n"
                "FLOW: praxis_launch_plan(preview_only=true, plan={...}) → inspect ProposedPlan → "
                "praxis_approve_proposed_plan(proposed={...}, approved_by='nate@praxis') → "
                "praxis_launch_plan(approved_plan={...})."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "proposed": {
                        "type": "object",
                        "description": (
                            "The ProposedPlan payload returned by "
                            "praxis_launch_plan(preview_only=true)."
                        ),
                    },
                    "approved_by": {
                        "type": "string",
                        "description": (
                            "Identifier of the approver: operator email, agent slug, "
                            "or CI system name."
                        ),
                    },
                    "approval_note": {
                        "type": "string",
                        "description": "Optional free-form note captured with the approval.",
                    },
                },
                "required": ["proposed", "approved_by"],
            },
        },
    ),
    "praxis_bind_data_pills": (
        tool_praxis_bind_data_pills,
        {
            "description": (
                "Layer 1 (Bind) of the planning stack: extract and validate "
                "``object.field`` data-pill references from prose intent against the "
                "data dictionary authority. Deterministic — matches explicit "
                "``snake_case.field_path`` spans in the prose; does not infer loose "
                "references like \"the user's name.\" Returns bound / ambiguous / "
                "unbound splits the caller confirms before decomposing intent into "
                "packets.\n\n"
                "USE WHEN: you have prose intent and want to confirm every field ref "
                "you're about to build packets around actually exists in authority.\n\n"
                "DO NOT USE TO: infer missing references. If the prose only says \"fix "
                "the user's name,\" this tool returns nothing bound — that's honest; "
                "the caller needs to decide which field is meant and write it "
                "explicitly.\n\n"
                "EXAMPLE: praxis_bind_data_pills(intent='Update users.first_name "
                "whenever users.email changes.')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Prose describing what the caller wants done.",
                    },
                    "object_kinds": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Optional allowlist of object kinds. References outside this "
                            "list resolve as unbound with reason='object_kind_not_allowlisted'."
                        ),
                    },
                },
                "required": ["intent"],
            },
        },
    ),
}
