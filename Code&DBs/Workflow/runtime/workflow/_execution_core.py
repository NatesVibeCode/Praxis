"""Execution dispatch: execute_job and its helper wrappers.

Extracted from unified.py — contains the main job execution function
called by the worker loop after a job has been claimed.
"""
from __future__ import annotations

import json
import logging
import subprocess
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from ._shared import _circuit_breakers, _json_safe, _json_loads_maybe, _WORKFLOW_TERMINAL_STATES
from ._claiming import mark_running, complete_job
from ._routing import _runtime_profile_ref_for_run
from ._workflow_state import _workflow_run_envelope
from ._context_building import (
    _capture_submission_baseline_if_required,
    _execution_model_messages,
    _extract_verification_paths as _ctx_extract_verification_paths,
    _persist_runtime_context_for_job,
    _render_execution_context_shard,
    _resolve_job_prompt_authority,
    _runtime_execution_bundle,
    _runtime_execution_context_shard,
    _terminal_failure_classification,
    _verification_artifact_refs,
)
from runtime.receipt_store import proof_metrics
from runtime.scope_resolver import resolve_scope
from runtime._workflow_database import resolve_runtime_database_url
from runtime.execution_transport import resolve_execution_transport
from runtime.workflow.execution_backends import (
    execute_api as _execution_backends_execute_api,
    execute_cli as _execution_backends_execute_cli,
    execute_integration as _execution_backends_execute_integration,
)
from runtime.workflow.execution_bundle import build_execution_bundle, render_execution_bundle
from runtime.workflow.receipt_writer import (
    write_job_receipt as _receipt_writer_write_job_receipt,
    write_output as _receipt_writer_write_output,
    extract_transcript_text as _extract_transcript_text,
    is_transcript_output as _is_transcript_output,
)
from runtime.workflow.submission_capture import (
    WorkflowSubmissionServiceError,
    capture_submission_baseline_for_job as _submission_capture_baseline_for_job,
)
from runtime.workflow.submission_gate import resolve_submission_for_job as _resolve_submission
from runtime.workflow.verification_runtime import (
    extract_verification_paths as _verification_runtime_extract_verification_paths,
    get_verify_bindings as _verification_runtime_get_verify_bindings,
    run_post_execution_verification as _verification_runtime_run_post_execution_verification,
)
from registry.native_runtime_profile_sync import resolve_native_runtime_profile_config

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


def _spec_snapshot_job_for_verify(
    conn: "SyncPostgresConnection",
    run_id: str,
    label: str,
    *,
    run_row: dict | None = None,
) -> dict:
    """Read a job's spec snapshot entry to get verify_command/outcome_goal."""
    source_row = run_row
    if source_row is None:
        source_row = dict((
            conn.execute(
                "SELECT request_envelope FROM workflow_runs WHERE run_id = $1",
                run_id,
            )
            or [{}]
        )[0])
    envelope = _workflow_run_envelope(source_row)
    snapshot = _json_loads_maybe(envelope.get("spec_snapshot"), {}) or {}
    for job in (snapshot.get("jobs") or []):
        if isinstance(job, dict) and str(job.get("label") or job.get("slug") or "").strip() == label:
            return dict(job)
    return {}


def _approval_checkpoint_for_job(
    conn: "SyncPostgresConnection",
    *,
    run_id: str,
    workflow_id: str,
    job_id: int,
    label: str,
    question: str,
) -> dict[str, Any]:
    checkpoint_card_id = f"workflow_job:{job_id}"
    rows = conn.execute(
        """SELECT checkpoint_id, card_id, model_id, authority_level, question, status, decided_by, decided_at, notes, created_at
           FROM authority_checkpoints
           WHERE card_id = $1 AND model_id = $2
           ORDER BY created_at DESC
           LIMIT 1""",
        checkpoint_card_id,
        workflow_id,
    )
    if rows:
        return dict(rows[0])

    from runtime.canonical_checkpoints import request_authority_checkpoint

    return request_authority_checkpoint(
        conn,
        card_id=checkpoint_card_id,
        model_id=workflow_id or run_id,
        authority_level="approval",
        question=question or f"Approve job {label} before execution.",
    )

logger = logging.getLogger(__name__)


def _resolve_host_workspace_path(raw_path: str, *, base: Path) -> str:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = (base / candidate).resolve()
    return str(candidate)


def _path_within(path: str, root: str | Path) -> bool:
    candidate = Path(path).resolve()
    boundary = Path(root).resolve()
    try:
        candidate.relative_to(boundary)
    except ValueError:
        return False
    return True


def _load_active_fork_ownership(
    conn: "SyncPostgresConnection",
    *,
    run_row: dict,
    repo_root: str,
) -> dict[str, object] | None:
    normalized_repo_root = str(Path(repo_root).resolve())
    run_id = str(run_row.get("run_id") or "").strip()
    if not run_id:
        return None

    try:
        rows = conn.execute(
            """
            SELECT
                fork_worktree_binding_id,
                binding_scope,
                sandbox_session_id,
                runtime_profile_ref,
                fork_ref,
                worktree_ref,
                materialized_repo_root,
                materialized_workdir
            FROM fork_worktree_bindings
            WHERE workflow_run_id = $1
              AND binding_status = 'active'
              AND retired_at IS NULL
            ORDER BY created_at DESC, fork_worktree_binding_id
            """,
            run_id,
        )
    except (AssertionError, NotImplementedError):
        return None
    if not rows:
        return None
    if len(rows) > 1:
        raise RuntimeError(
            f"execution workspace resolution is ambiguous for run {run_id!r}: "
            f"found {len(rows)} active fork/worktree bindings",
        )

    row = dict(rows[0])
    resolved_repo_root = str(row.get("materialized_repo_root") or "").strip()
    resolved_workdir = str(row.get("materialized_workdir") or "").strip()
    if resolved_repo_root:
        resolved_repo_root = _resolve_host_workspace_path(
            resolved_repo_root,
            base=Path(normalized_repo_root),
        )
    if resolved_workdir:
        resolved_workdir = _resolve_host_workspace_path(
            resolved_workdir,
            base=Path(resolved_repo_root or normalized_repo_root),
        )
    return {
        "fork_worktree_binding_id": str(row.get("fork_worktree_binding_id") or "").strip() or run_id,
        "binding_scope": str(row.get("binding_scope") or "").strip() or None,
        "sandbox_session_id": str(row.get("sandbox_session_id") or "").strip() or None,
        "runtime_profile_ref": str(
            row.get("runtime_profile_ref")
            or _workflow_run_envelope(run_row).get("runtime_profile_ref")
            or ""
        ).strip() or None,
        "fork_ref": str(row.get("fork_ref") or "").strip() or None,
        "worktree_ref": str(row.get("worktree_ref") or "").strip() or None,
        "materialized_repo_root": resolved_repo_root or None,
        "materialized_workdir": resolved_workdir or None,
    }


def _resolve_execution_workspace(
    *,
    repo_root: str,
    execution_bundle: dict[str, object] | None,
) -> dict[str, object]:
    normalized_repo_root = str(Path(repo_root).resolve())
    default_workspace = {
        "repo_root": normalized_repo_root,
        "workdir": normalized_repo_root,
        "fork_ownership": None,
    }
    if not isinstance(execution_bundle, dict):
        return default_workspace

    fork_ownership = (
        dict(execution_bundle.get("fork_ownership"))
        if isinstance(execution_bundle.get("fork_ownership"), dict)
        else None
    )
    if not fork_ownership:
        return default_workspace

    binding_id = str(fork_ownership.get("fork_worktree_binding_id") or "").strip() or "unknown"
    materialized_repo_root = str(fork_ownership.get("materialized_repo_root") or "").strip()
    materialized_workdir = str(fork_ownership.get("materialized_workdir") or "").strip()
    if not materialized_repo_root or not materialized_workdir:
        raise RuntimeError(
            "active fork/worktree binding "
            f"{binding_id!r} is missing materialized_repo_root/materialized_workdir",
        )

    resolved_repo_root = _resolve_host_workspace_path(
        materialized_repo_root,
        base=Path(normalized_repo_root),
    )
    resolved_workdir = _resolve_host_workspace_path(
        materialized_workdir,
        base=Path(resolved_repo_root),
    )
    runtime_profile_ref = str(fork_ownership.get("runtime_profile_ref") or "").strip()
    boundary_root = Path(normalized_repo_root)
    if runtime_profile_ref:
        try:
            boundary_root = Path(
                resolve_native_runtime_profile_config(runtime_profile_ref).repo_root
            ).resolve()
        except Exception as exc:
            raise RuntimeError(
                "failed to resolve runtime-profile workspace boundary for "
                f"sharded execution on binding {binding_id!r}: {exc}",
            ) from exc

    if not _path_within(resolved_repo_root, boundary_root):
        raise RuntimeError(
            "sharded execution rejected active fork/worktree binding "
            f"{binding_id!r}: materialized_repo_root escapes the declared workspace boundary",
        )
    if not _path_within(resolved_workdir, resolved_repo_root):
        raise RuntimeError(
            "sharded execution rejected active fork/worktree binding "
            f"{binding_id!r}: materialized_workdir must stay under materialized_repo_root",
        )
    if not _path_within(resolved_workdir, boundary_root):
        raise RuntimeError(
            "sharded execution rejected active fork/worktree binding "
            f"{binding_id!r}: materialized_workdir escapes the declared workspace boundary",
        )

    fork_ownership["materialized_repo_root"] = resolved_repo_root
    fork_ownership["materialized_workdir"] = resolved_workdir
    return {
        "repo_root": resolved_repo_root,
        "workdir": resolved_workdir,
        "fork_ownership": fork_ownership,
    }

__all__ = ["execute_job"]


def _cli_readiness_error_code(reason: str | None) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized.startswith("missing env var:"):
        return "credential.env_var_missing"
    if normalized.startswith("unknown provider:"):
        return "credential.provider_unknown"
    return "setup_failure"


# ── Execution (called by worker after claiming) ──────────────────────

def execute_job(
    conn: SyncPostgresConnection,
    job: dict,
    repo_root: str,
) -> None:
    """Execute a single claimed job. Writes results back to workflow_jobs."""
    job_id = job["id"]
    label = job["label"]
    agent_slug = job.get("resolved_agent") or job["agent_slug"]
    run_id = job["run_id"]

    # Circuit breaker gate: if the provider is tripped, skip to failover immediately
    # Skip for integration jobs (they don't use LLM providers)
    provider_slug = agent_slug.split("/")[0] if "/" in agent_slug else agent_slug
    circuit_breakers = _circuit_breakers()
    if provider_slug != "integration" and circuit_breakers and not circuit_breakers.allow_request(provider_slug):
        logger.warning("Circuit breaker OPEN for %s — skipping to failover", provider_slug)
        complete_job(conn, job_id, status="failed", error_code="rate_limited",
                     duration_ms=0, stdout_preview=f"Circuit breaker open for {provider_slug}")
        return

    mark_running(conn, job_id)
    start = time.monotonic()
    logger.info("Executing job %d: %s (agent=%s, run=%s)", job_id, label, agent_slug, run_id)

    # If the run was cancelled while we were waiting to claim/execute, stop
    # immediately and treat this job as cancelled.
    run_rows = conn.execute(
        "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1",
        run_id,
    )
    run_row = dict(run_rows[0]) if run_rows else {}
    if not run_row or run_row.get("current_state") in _WORKFLOW_TERMINAL_STATES:
        duration_ms = int((time.monotonic() - start) * 1000)
        complete_job(
            conn,
            job_id,
            status="cancelled",
            exit_code=1,
            duration_ms=max(duration_ms, 0),
            stdout_preview="Run was cancelled before execution",
            error_code="run_cancelled",
        )
        return


    # Check for integration execution FIRST (bypasses LLM entirely)
    integration_id = job.get("integration_id")
    integration_action = job.get("integration_action")
    if integration_id and integration_action:
        try:
            result = _execute_integration(job, conn)
        except Exception as exc:
            duration_ms = int((time.monotonic() - start) * 1000)
            complete_job(conn, job_id, status="failed", error_code="integration_exception",
                         duration_ms=duration_ms, stdout_preview=str(exc)[:2000])
            return

        duration_ms = int((time.monotonic() - start) * 1000)
        output_path = _write_output(repo_root, run_id, job_id, label, result)
        receipt_id = _write_job_receipt(
            conn,
            run_id,
            job_id,
            label,
            agent_slug,
            result,
            duration_ms,
            output_path=output_path,
        )
        complete_job(
            conn, job_id,
            status=result.get("status", "failed"),
            exit_code=result.get("exit_code", 0 if result.get("status") == "succeeded" else 1),
            output_path=output_path,
            receipt_id=receipt_id,
            stdout_preview=_make_stdout_preview(result),
            token_input=0, token_output=0, cost_usd=0.0,
            duration_ms=duration_ms,
            error_code=result.get("error_code", "") or result.get("error", ""),
        )
        return

    # Resolve agent config
    from registry.agent_config import AgentRegistry
    from storage.postgres.connection import SyncPostgresConnection as _PG, get_workflow_pool
    registry = AgentRegistry.load_from_postgres(conn)
    agent_config = registry.get(agent_slug)
    runtime_profile_ref = _runtime_profile_ref_for_run(conn, run_id)

    # Last-resort: if slug is still auto/, resolve via task_type_router now
    if agent_config is None and agent_slug.startswith("auto/"):
        if runtime_profile_ref:
            logger.warning(
                "auto/ slug %s reached execution unresolved under runtime profile %s — failing closed",
                agent_slug,
                runtime_profile_ref,
            )
        else:
            logger.warning("auto/ slug %s reached execution unresolved — resolving now", agent_slug)
            from runtime.task_type_router import TaskTypeRouter
            router = TaskTypeRouter(conn)
            decision = router.resolve(agent_slug)
            resolved = f"{decision.provider_slug}/{decision.model_slug}"
            agent_config = registry.get(resolved)
            if agent_config:
                agent_slug = resolved
                conn.execute(
                    "UPDATE workflow_jobs SET resolved_agent = $1 WHERE id = $2",
                    resolved, job_id,
                )

    if agent_config is None:
        duration_ms = int((time.monotonic() - start) * 1000)
        complete_job(conn, job_id, status="failed", error_code="agent_not_found",
                     duration_ms=duration_ms, stdout_preview=f"No agent config for: {agent_slug}")
        return

    route_task_type = str(job.get("route_task_type") or job.get("task_type") or "").strip()
    if "/" in agent_slug:
        from runtime.task_type_router import TaskTypeRouter

        router = TaskTypeRouter(conn)
        resolve_explicit_eligibility = getattr(router, "resolve_explicit_eligibility", None)
        eligibility = (
            resolve_explicit_eligibility(
                agent_slug,
                task_type=route_task_type or None,
            )
            if callable(resolve_explicit_eligibility)
            else None
        )
        if eligibility is not None and eligibility.eligibility_status != "eligible":
            duration_ms = int((time.monotonic() - start) * 1000)
            task_fragment = f" for task type '{route_task_type}'" if route_task_type else ""
            rationale = eligibility.rationale or "provider/model rejected by route eligibility policy"
            complete_job(
                conn,
                job_id,
                status="failed",
                error_code=eligibility.reason_code or "provider_disabled",
                duration_ms=duration_ms,
                stdout_preview=(
                    f"Route eligibility blocked {agent_slug}{task_fragment}: "
                    f"{rationale} (decision_ref={eligibility.decision_ref})"
                )[:2000],
            )
            return

    prompt, _, _, execution_bundle, execution_context_shard = _resolve_job_prompt_authority(
        conn, job=job, run_row=run_row,
    )

    try:
        fork_ownership = (
            dict(execution_bundle.get("fork_ownership"))
            if isinstance(execution_bundle, dict)
            and isinstance(execution_bundle.get("fork_ownership"), dict)
            else None
        )
        if fork_ownership is None:
            fork_ownership = _load_active_fork_ownership(
                conn,
                run_row=run_row,
                repo_root=repo_root,
            )
        workspace_bundle = execution_bundle
        if fork_ownership is not None:
            workspace_bundle = (
                {
                    **execution_bundle,
                    "fork_ownership": fork_ownership,
                }
                if isinstance(execution_bundle, dict)
                else {"fork_ownership": fork_ownership}
            )
        execution_workspace = _resolve_execution_workspace(
            repo_root=repo_root,
            execution_bundle=workspace_bundle,
        )
    except Exception as exc:
        duration_ms = int((time.monotonic() - start) * 1000)
        complete_job(
            conn,
            job_id,
            status="failed",
            error_code="workspace_resolution_failed",
            duration_ms=duration_ms,
            stdout_preview=str(exc)[:2000],
        )
        return
    execution_repo_root = str(execution_workspace["repo_root"])
    execution_workdir = str(execution_workspace["workdir"])
    fork_ownership = (
        dict(execution_workspace["fork_ownership"])
        if isinstance(execution_workspace.get("fork_ownership"), dict)
        else None
    )

    # Single prompt assembly path: prompt + platform context + shard + bundle
    platform_context = _build_platform_context(execution_repo_root)
    full_prompt = f"{prompt}\n\n{platform_context}" if platform_context else prompt

    if execution_context_shard is None:
        execution_context_shard = _runtime_execution_context_shard(
            conn,
            job=job,
            run_row=run_row,
            repo_root=execution_repo_root,
        )
    execution_context_shard_text = _render_execution_context_shard(execution_context_shard)
    if execution_context_shard_text:
        full_prompt = (
            f"{full_prompt}\n\n{execution_context_shard_text}"
            if full_prompt
            else execution_context_shard_text
        )

    if execution_bundle is None:
        execution_bundle = _runtime_execution_bundle(
            conn,
            job=job,
            run_row=run_row,
            repo_root=execution_repo_root,
            execution_context_shard=execution_context_shard,
        )
    if fork_ownership is not None and isinstance(execution_bundle, dict):
        execution_bundle = {
            **execution_bundle,
            "fork_ownership": fork_ownership,
        }
    execution_bundle_text = render_execution_bundle(execution_bundle)
    if execution_bundle_text:
        full_prompt = f"{full_prompt}\n\n{execution_bundle_text}" if full_prompt else execution_bundle_text

    workflow_id_for_run = str(_workflow_run_envelope(run_row).get("workflow_id") or "").strip() or run_id
    approval_required = bool(execution_bundle.get("approval_required")) if isinstance(execution_bundle, dict) else False
    if approval_required:
        approval_question = str(execution_bundle.get("approval_question") or "").strip() if isinstance(execution_bundle, dict) else ""
        checkpoint = _approval_checkpoint_for_job(
            conn,
            run_id=run_id,
            workflow_id=workflow_id_for_run,
            job_id=int(job_id),
            label=label,
            question=approval_question,
        )
        checkpoint_status = str(checkpoint.get("status") or "").strip().lower()
        if checkpoint_status == "approved":
            logger.info(
                "Approval checkpoint already approved for %s/%s; continuing",
                run_id,
                label,
            )
        elif checkpoint_status in {"rejected", "escalated"}:
            duration_ms = int((time.monotonic() - start) * 1000)
            complete_job(
                conn,
                job_id,
                status="failed",
                error_code="human_rejected" if checkpoint_status == "rejected" else "authority_escalated",
                duration_ms=duration_ms,
                stdout_preview=str(checkpoint.get("notes") or approval_question or "Approval checkpoint was not approved")[:2000],
            )
            return
        else:
            approval_note = approval_question or f"Approve job {label} before execution."
            conn.execute(
                """UPDATE workflow_jobs
                   SET status = 'approval_required',
                       claimed_by = NULL,
                       claimed_at = NULL,
                       heartbeat_at = NULL,
                       started_at = NULL,
                       attempt = GREATEST(attempt - 1, 0),
                       last_error_code = 'approval_required',
                       stdout_preview = $2
                   WHERE id = $1
                     AND status IN ('claimed', 'running')""",
                job_id,
                approval_note[:2000],
            )
            logger.info(
                "Job %d (%s) paused for approval checkpoint %s",
                job_id,
                label,
                checkpoint.get("checkpoint_id"),
            )
            return

    _persist_runtime_context_for_job(
        conn,
        run_id=run_id,
        workflow_id=workflow_id_for_run,
        job_label=label,
        execution_context_shard=execution_context_shard,
        execution_bundle=execution_bundle,
    )

    try:
        _capture_submission_baseline_if_required(
            conn,
            run_id=run_id,
            workflow_id=workflow_id_for_run,
            job_label=label,
            repo_root=execution_workdir,
            execution_context_shard=execution_context_shard,
            execution_bundle=execution_bundle,
        )
    except WorkflowSubmissionServiceError as exc:
        duration_ms = int((time.monotonic() - start) * 1000)
        complete_job(
            conn,
            job_id,
            status="failed",
            error_code=exc.reason_code,
            duration_ms=duration_ms,
            stdout_preview=str(exc)[:2000],
        )
        return

    # Execute based on transport
    transport = resolve_execution_transport(agent_config)
    transport_kind = transport.transport_kind

    try:
        if transport_kind in {"cli", "mcp"}:
            from runtime.agent_spawner import AgentSpawner

            readiness = AgentSpawner().preflight(agent_slug)
            if not readiness.ready:
                duration_ms = int((time.monotonic() - start) * 1000)
                complete_job(
                    conn,
                    job_id,
                    status="failed",
                    error_code=_cli_readiness_error_code(readiness.reason),
                    duration_ms=duration_ms,
                    stdout_preview=readiness.reason or "CLI provider not ready",
                )
                return
            result = _execute_cli(
                agent_config,
                full_prompt,
                execution_workdir,
                execution_bundle=execution_bundle,
            )
        elif transport_kind == "api":
            # Resolve reasoning effort from task_type_routing for this agent + task type.
            # Falls back to provider_model_candidates default if no task-type-specific row.
            _reasoning_effort: str | None = None
            _route_task_type = route_task_type
            _provider, _, _model = agent_slug.partition("/")
            if _route_task_type and _provider and _model:
                _rc_rows = conn.execute(
                    """SELECT reasoning_control FROM task_type_routing
                       WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3
                       LIMIT 1""",
                    _route_task_type, _provider, _model,
                )
                if _rc_rows:
                    _rc = _rc_rows[0].get("reasoning_control") or {}
                    _level = str(_rc.get("default_level") or "").strip()
                    if _level and _level != "none":
                        _reasoning_effort = _level
            if _reasoning_effort is None and _provider and _model:
                _pmc_rows = conn.execute(
                    """SELECT reasoning_control FROM provider_model_candidates
                       WHERE provider_slug = $1 AND model_slug = $2
                       LIMIT 1""",
                    _provider, _model,
                )
                if _pmc_rows:
                    _pmc_rc = _pmc_rows[0].get("reasoning_control") or {}
                    _pmc_level = str(_pmc_rc.get("default") or "").strip()
                    if _pmc_level and _pmc_level != "none":
                        _reasoning_effort = _pmc_level
            result = _execute_api(
                agent_config,
                full_prompt,
                execution_workdir,
                execution_bundle=execution_bundle,
                reasoning_effort=_reasoning_effort,
            )
        else:
            raise NotImplementedError("Unsupported execution transport")
    except Exception as exc:
        duration_ms = int((time.monotonic() - start) * 1000)
        complete_job(conn, job_id, status="failed", error_code="execution_exception",
                     duration_ms=duration_ms, stdout_preview=str(exc)[:2000])
        return

    duration_ms = int((time.monotonic() - start) * 1000)

    verification_outcome = _run_post_execution_verification(
        conn,
        run_id=run_id,
        job_id=job_id,
        label=label,
        repo_root=execution_workdir,
        result=result,
    )
    result = verification_outcome["result"]
    final_status = verification_outcome["final_status"]
    final_error_code = verification_outcome["final_error_code"]
    verification_summary = verification_outcome["verification_summary"]
    verify_bindings = verification_outcome["verification_bindings"]
    verification_error = verification_outcome["verification_error"]
    verification_artifact_refs = _verification_artifact_refs(verify_bindings)
    attempt_no = max(1, int(job.get("attempt") or 1))
    gate = _resolve_submission(
        conn,
        run_id=run_id,
        workflow_id=workflow_id_for_run,
        job_label=label,
        attempt_no=attempt_no,
        execution_bundle=execution_bundle,
        result=result,
        final_status=final_status,
        final_error_code=final_error_code,
        verification_artifact_refs=verification_artifact_refs,
    )
    submission_state = gate.submission_state
    final_status = gate.final_status
    final_error_code = gate.final_error_code
    result = gate.result

    # ── Outcome gate: run verify_command if specified ─────────────────────
    if final_status == "succeeded":
        acceptance_contract = (
            dict(execution_bundle.get("acceptance_contract"))
            if isinstance(execution_bundle, dict)
            and isinstance(execution_bundle.get("acceptance_contract"), dict)
            else {}
        )
        acceptance_verify_refs = acceptance_contract.get("verify_refs")
        verify_cmd = str(job.get("verify_command") or "").strip()
        spec_job: dict | None = None
        if not verify_cmd and not acceptance_verify_refs and not verification_artifact_refs:
            # Check spec snapshot for verify_command
            spec_job = _spec_snapshot_job_for_verify(conn, run_id, label, run_row=run_row)
            verify_cmd = str(spec_job.get("verify_command") or "").strip()
        if verify_cmd and not acceptance_verify_refs and not verification_artifact_refs:
            outcome_goal = str(
                job.get("outcome_goal")
                or (spec_job or _spec_snapshot_job_for_verify(conn, run_id, label, run_row=run_row)).get("outcome_goal")
                or ""
            ).strip()
            try:
                verify_result = subprocess.run(
                    verify_cmd,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    cwd=execution_workdir,
                )
                if verify_result.returncode != 0:
                    final_status = "failed"
                    final_error_code = "outcome_gate_failed"
                    gate_msg = f"Outcome gate failed: {outcome_goal or verify_cmd}"
                    if verify_result.stderr:
                        gate_msg += f"\n{verify_result.stderr[:500]}"
                    if verify_result.stdout:
                        gate_msg += f"\n{verify_result.stdout[:500]}"
                    result = {
                        **result,
                        "stderr": (
                            str(result.get("stderr") or "") + f"\n{gate_msg}"
                        ).strip(),
                    }
                    logger.warning(
                        "Outcome gate failed for %s/%s: %s", run_id, label, outcome_goal or verify_cmd,
                    )
                else:
                    logger.info(
                        "Outcome gate passed for %s/%s: %s", run_id, label, outcome_goal or verify_cmd,
                    )
            except subprocess.TimeoutExpired:
                final_status = "failed"
                final_error_code = "outcome_gate_timeout"
                result = {
                    **result,
                    "stderr": (
                        str(result.get("stderr") or "")
                        + f"\nOutcome gate timed out after 60s: {verify_cmd}"
                    ).strip(),
                }
            except Exception as exc:
                logger.warning("Outcome gate error for %s/%s: %s", run_id, label, exc)

    # Write receipt to disk (preserves existing artifact flow)
    output_path = _write_output(execution_repo_root, run_id, job_id, label, result)

    # Write canonical receipt row
    receipt_id = _write_job_receipt(
        conn,
        run_id,
        job_id,
        label,
        agent_slug,
        result,
        duration_ms,
        repo_root=execution_workdir,
        output_path=output_path,
        final_status=final_status,
        final_error_code=final_error_code,
        verification_summary=verification_summary,
        verification_bindings=verify_bindings,
        verification_error=verification_error,
        submission=submission_state,
    )

    complete_job(
        conn, job_id,
        status=final_status,
        exit_code=result.get("exit_code"),
        output_path=output_path,
        receipt_id=receipt_id,
        stdout_preview=(result.get("stdout") or result.get("stderr") or "")[:2000],
        token_input=result.get("token_input", 0),
        token_output=result.get("token_output", 0),
        cost_usd=result.get("cost_usd", 0.0),
        duration_ms=duration_ms,
        error_code=final_error_code,
    )


def _get_verify_bindings(conn: SyncPostgresConnection, run_id: str) -> list[str] | None:
    """Compatibility wrapper for verification binding extraction."""
    return _verification_runtime_get_verify_bindings(conn, run_id)


def _extract_verification_paths(bindings: list[dict] | None) -> list[str]:
    """Compatibility wrapper for verification path extraction."""
    return _verification_runtime_extract_verification_paths(bindings)


def _run_post_execution_verification(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
    job_id: int,
    label: str,
    repo_root: str,
    result: dict,
) -> dict:
    """Compatibility wrapper for post-execution verification."""
    return _verification_runtime_run_post_execution_verification(
        conn,
        run_id=run_id,
        job_id=job_id,
        label=label,
        repo_root=repo_root,
        result=result,
        initial_status=result.get("status", "failed"),
        initial_error_code=result.get("error_code", ""),
        binding_loader=_get_verify_bindings,
        logger=logger,
    )


def _execute_integration(job: dict, conn) -> dict:
    """Compatibility wrapper for integration execution backends."""
    return _execution_backends_execute_integration(job, conn, logger=logger)


def _execute_cli(agent_config, prompt: str, workdir: str, execution_bundle: dict[str, object] | None = None) -> dict:
    """Compatibility wrapper for CLI execution backends."""
    return _execution_backends_execute_cli(
        agent_config,
        prompt,
        workdir,
        execution_bundle=execution_bundle,
    )


def _execute_api(
    agent_config,
    prompt: str,
    workdir: str,
    execution_bundle: dict[str, object] | None = None,
    reasoning_effort: str | None = None,
) -> dict:
    """Compatibility wrapper for transport-backed execution backends."""
    return _execution_backends_execute_api(
        agent_config,
        prompt,
        workdir=workdir,
        execution_bundle=execution_bundle,
        reasoning_effort=reasoning_effort,
    )


    # _classify_error removed — use classify_failure_from_stderr() from
    # runtime.failure_classifier instead (consolidated in _execute_cli above).


def _build_platform_context(repo_root: str) -> str:
    """Minimal platform context injected into prompts."""
    try:
        database_url = str(resolve_runtime_database_url(required=False) or "unavailable")
    except Exception:
        database_url = "unavailable"
    return f"""--- PLATFORM CONTEXT ---
Host repo root (persistence/output authority): {repo_root}
Command workspace: sandboxed workflow execution typically runs inside a hydrated workspace such as /workspace.
Use the live command workspace for shell commands and relative paths; do not assume the host repo path exists inside the sandbox.
Database: {database_url}
--- END PLATFORM CONTEXT ---"""



def _write_output(repo_root: str, run_id: str, job_id: int, label: str, result: dict) -> str:
    """Compatibility wrapper for job artifact writing."""
    return _receipt_writer_write_output(repo_root, run_id, job_id, label, result)


def _make_stdout_preview(result: dict) -> str:
    """Build the stdout_preview for DB storage.

    For transcript outputs (streamed JSONL event streams from CLI agents), extract
    the agent_message text so the preview contains the actual content rather than
    raw event-stream preamble.
    """
    raw = str(result.get("stdout") or result.get("stderr") or "")
    if _is_transcript_output(raw):
        extracted = _extract_transcript_text(raw)
        return extracted[:2000] if extracted else raw[:2000]
    return raw[:2000]


def _write_job_receipt(
    conn: SyncPostgresConnection,
    run_id: str,
    job_id: int,
    label: str,
    agent_slug: str,
    result: dict,
    duration_ms: int,
    *,
    repo_root: str = "",
    output_path: str = "",
    final_status: str | None = None,
    final_error_code: str | None = None,
    verification_summary=None,
    verification_bindings: list[dict] | None = None,
    verification_error: str | None = None,
    submission: dict[str, object] | None = None,
) -> str:
    """Compatibility wrapper for canonical receipt writing."""
    return _receipt_writer_write_job_receipt(
        conn,
        run_id,
        job_id,
        label,
        agent_slug,
        result,
        duration_ms,
        repo_root=repo_root,
        output_path=output_path,
        final_status=final_status,
        final_error_code=final_error_code,
        verification_summary=verification_summary,
        verification_bindings=verification_bindings,
        verification_error=verification_error,
        submission=submission,
        failure_classifier=_terminal_failure_classification,
        verification_path_extractor=_extract_verification_paths,
    )
