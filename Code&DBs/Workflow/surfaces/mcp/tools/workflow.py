"""Tools: praxis_workflow, praxis_workflow_validate."""
from __future__ import annotations

import json
from datetime import datetime, timezone
import uuid
from typing import Any

from runtime.failure_projection import project_failure_classification
from ..subsystems import _subs, REPO_ROOT


_TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "dead_letter"}
_RUN_STREAM_PATH = "/api/workflow-runs/{run_id}/stream"
_RUN_STATUS_PATH = "/api/workflow-runs/{run_id}/status"
_STALE_HEARTBEAT_SECONDS = 180
_STALE_READY_SECONDS = 600
_STALE_PENDING_SECONDS = 900
_NO_PROGRESS_SECONDS = 300
_IDLE_PROGRESS_SECONDS = 900
_RUN_ACTIVITY_LOOKBACK_SECONDS = 1800


def _workflow_spec_mod():
    import runtime.workflow_spec as spec_mod

    return spec_mod


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


def _classify_job_failure(job: dict) -> dict[str, Any] | None:
    """Classify a failed job and return machine-readable failure metadata."""
    failure_category = str(job.get("failure_category") or "").strip()
    if failure_category:
        return project_failure_classification(
            failure_category=failure_category,
            is_transient=bool(job.get("is_transient", False)),
            stdout_preview=str(job.get("stdout_preview") or ""),
        )
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
    kill_if_idle: bool = False,
    idle_threshold_seconds: int | None = None,
) -> dict:
    """Build a status payload with richer health and failure diagnostics."""
    from runtime.workflow.unified import get_run_status, summarize_run_health, summarize_run_recovery

    status_data = get_run_status(pg, run_id)
    if status_data is None:
        return {"run_id": run_id, "status": "not_found"}

    now = datetime.now(timezone.utc)
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


def _run_submit_result_payload(result: dict[str, Any], include_warning: bool = False) -> dict:
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


def _poll_run_to_completion(
    pg,
    run_id: str,
    *,
    spec_name: str,
    total_jobs: int,
    emitter: Any | None = None,
    poll_interval: float = 3.0,
    max_poll_seconds: float = 3600.0,
) -> dict[str, Any]:
    """Poll a workflow run until terminal, emitting per-job progress."""
    import time as _time
    from runtime.workflow.unified import get_run_status

    if emitter is not None:
        emitter.log(f"Launched {spec_name} ({total_jobs} jobs) — run_id={run_id}")
        emitter.emit(progress=0, total=total_jobs, message=f"Submitted {spec_name}")

    seen_terminal: set[str] = set()
    deadline = _time.monotonic() + max_poll_seconds

    while _time.monotonic() < deadline:
        _time.sleep(poll_interval)
        status_data = get_run_status(pg, run_id)
        if status_data is None:
            continue

        # Emit per-job updates for newly terminal jobs
        for j in status_data.get("jobs", []):
            jlabel = j.get("label", "")
            jstatus = j.get("status", "")
            if jstatus in _TERMINAL_STATUSES and jlabel not in seen_terminal:
                seen_terminal.add(jlabel)
                agent = j.get("resolved_agent") or j.get("agent_slug", "")
                dur = j.get("duration_ms", 0)
                err = j.get("failure_category") or j.get("last_error_code", "")
                msg = f"{jlabel}: {jstatus}"
                if agent:
                    msg += f" [{agent}]"
                if dur:
                    msg += f" ({dur / 1000:.1f}s)"
                if err:
                    msg += f" — {err}"
                level = "info" if jstatus == "succeeded" else "error"
                if emitter is not None:
                    emitter.log(msg, level=level)
                    emitter.emit(
                        progress=len(seen_terminal),
                        total=total_jobs,
                        message=msg,
                    )

        run_status = status_data.get("status", "")
        if run_status in _TERMINAL_STATUSES:
            break

    # Build final summary
    now = datetime.now(timezone.utc)
    payload = _run_status_payload(pg, run_id)
    if emitter is not None:
        passed = sum(1 for j in payload.get("jobs", []) if j.get("status") == "succeeded")
        failed = total_jobs - passed
        emitter.log(
            f"Done: {spec_name} — {payload.get('status', 'unknown')} "
            f"({passed} passed, {failed} failed, {payload.get('elapsed_seconds', 0):.0f}s)",
            level="info" if payload.get("status") == "succeeded" else "error",
        )
    return payload


def _submit_workflow_via_service_bus(pg, *, spec_path: str, spec_name: str, total_jobs: int) -> dict[str, Any]:
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
    )
    return render_workflow_submit_response(
        command,
        spec_name=spec_name,
        total_jobs=total_jobs,
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
            )
        except Exception as exc:
            return _structured_runtime_error(exc, action="status")

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

    if action != "run":
        return {
            "error": (
                f"Unsupported action='{action}'. Expected one of: "
                "run, status, inspect, cancel, list, notifications, retry."
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
    # stream per-job progress back through the MCP progress emitter.
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
        result = _submit_workflow_via_service_bus(
            pg,
            spec_path=spec_path,
            spec_name=getattr(spec, "name", spec_path),
            total_jobs=total_jobs,
        )
        if result.get("error"):
            return result

        run_id = result["run_id"]

        if not wait or _progress_emitter is None:
            return _run_submit_result_payload(result)

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
                "USE WHEN: you need to run a workflow spec, check on a running workflow, retry a "
                "failed job, or cancel a run.\n\n"
                "WORKFLOW CONTRACT:\n"
                "  - action='run' submits and streams per-job progress inline (default wait=true).\n"
                "  - Pass wait=false for fire-and-forget (returns run_id immediately).\n"
                "  - Progress is emitted via MCP notifications as each job completes.\n"
                "  - The final return value is the full status payload with all job results.\n"
                "  - Use action='status' for a snapshot of a running or completed workflow.\n"
                "  - Read health.likely_failed, health.signals to detect stuck runs.\n"
                "  - Use kill_if_idle=true on a status call if the run is clearly idle and unhealthy.\n\n"
                "EXAMPLES:\n"
                "  Run and wait:    praxis_workflow(action='run', spec_path='artifacts/workflow/my_spec.queue.json')\n"
                "  Fire-and-forget: praxis_workflow(action='run', spec_path='...', wait=false)\n"
                "  Check status:    praxis_workflow(action='status', run_id='workflow_abc123')\n"
                "  Retry a failure: praxis_workflow(action='retry', run_id='workflow_abc123', label='build_step')\n"
                "  Cancel a run:    praxis_workflow(action='cancel', run_id='workflow_abc123')\n"
                "  List recent:     praxis_workflow(action='list')\n\n"
                "DO NOT USE: for asking questions about the system (use praxis_query), checking health "
                "(use praxis_health), or searching past results (use praxis_receipts)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "spec_path": {"type": "string", "description": "Path to a .queue.json spec file. Required for 'run'."},
                    "wait": {
                        "type": "boolean",
                        "description": "If true (default), poll and stream per-job results inline. If false, return immediately with run_id.",
                        "default": True,
                    },
                    "dry_run": {"type": "boolean", "description": "If true, simulate without actual execution.", "default": False},
                    "action": {
                        "type": "string",
                        "description": (
                            "Operation: 'run' (default — submits and returns run_id), "
                            "'status' (poll a running/completed workflow with health heuristics), "
                            "'inspect' (deep job inspection of all fields), "
                            "'cancel' (cancel a workflow run), "
                            "'list' (show recent workflows), "
                            "'notifications' (drain pending completion events), "
                            "'retry' (re-queue a failed job)."
                        ),
                        "enum": ["run", "status", "inspect", "cancel", "list", "notifications", "retry"],
                        "default": "run",
                    },
                    "run_id": {"type": "string", "description": "Workflow run ID. Required for 'status', 'inspect', 'cancel', and 'retry'."},
                    "label": {"type": "string", "description": "Job label. Required for 'retry', optional for 'inspect'."},
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
