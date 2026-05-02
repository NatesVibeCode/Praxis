"""Execution dispatch: execute_job and its helper wrappers.

Extracted from unified.py — contains the main job execution function
called by the worker loop after a job has been claimed.
"""
from __future__ import annotations

import logging
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Mapping

from ._shared import _circuit_breakers, _WORKFLOW_TERMINAL_STATES
from ._claiming import mark_running, complete_job
from ._routing import _route_candidate_entries_from_plan, _runtime_profile_ref_for_run
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
    assemble_full_prompt,
    build_platform_context,
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
from runtime.workflow.submission_capture import WorkflowSubmissionServiceError
from runtime.workflow.submission_gate import resolve_submission_for_job as _resolve_submission
from runtime.workflow.verification_runtime import (
    extract_verification_paths as _verification_runtime_extract_verification_paths,
    get_verify_bindings as _verification_runtime_get_verify_bindings,
    run_post_execution_verification as _verification_runtime_run_post_execution_verification,
)
from registry.native_runtime_profile_sync import resolve_native_runtime_profile_config

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


def _positive_int_or_none(value: object) -> int | None:
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _route_execution_knobs(
    conn: "SyncPostgresConnection",
    *,
    route_task_type: str,
    provider_slug: str,
    model_slug: str,
    transport_type: str = "",
) -> dict[str, object]:
    """Read provider request knobs from task route authority.

    ``provider_model_candidates`` says a model exists. ``task_type_routing``
    says how this task should call it. Execution must honor the latter so
    model-specific API requirements such as Kimi's larger output budget do not
    disappear when an auto route resolves to a concrete provider/model slug.
    """
    normalized_task_type = str(route_task_type or "").strip()
    normalized_provider = str(provider_slug or "").strip()
    normalized_model = str(model_slug or "").strip()
    normalized_transport = str(transport_type or "").strip().upper()
    if normalized_transport not in {"CLI", "API"}:
        normalized_transport = ""
    route_max_tokens: int | None = None
    reasoning_effort: str | None = None

    if normalized_task_type and normalized_provider and normalized_model:
        rows = conn.execute(
            """SELECT max_tokens, reasoning_control
               FROM task_type_routing
               WHERE task_type = $1
                 AND provider_slug = $2
                 AND model_slug = $3
                 AND ($4::text IS NULL OR transport_type = $4)
               LIMIT 1""",
            normalized_task_type,
            normalized_provider,
            normalized_model,
            normalized_transport or None,
        )
        if rows:
            row = rows[0]
            route_max_tokens = _positive_int_or_none(row.get("max_tokens"))
            route_reasoning = row.get("reasoning_control") or {}
            if isinstance(route_reasoning, dict):
                level = str(route_reasoning.get("default_level") or "").strip()
                if level and level != "none":
                    reasoning_effort = level

    if reasoning_effort is None and normalized_provider and normalized_model:
        rows = conn.execute(
            """SELECT reasoning_control
               FROM provider_model_candidates
               WHERE provider_slug = $1
                 AND model_slug = $2
                 AND ($3::text IS NULL OR transport_type = $3)
               LIMIT 1""",
            normalized_provider,
            normalized_model,
            normalized_transport or None,
        )
        if rows:
            candidate_reasoning = rows[0].get("reasoning_control") or {}
            if isinstance(candidate_reasoning, dict):
                level = str(candidate_reasoning.get("default") or "").strip()
                if level and level != "none":
                    reasoning_effort = level

    return {
        "max_output_tokens": route_max_tokens,
        "reasoning_effort": reasoning_effort,
    }


def _agent_config_with_max_output_tokens(agent_config: object, max_output_tokens: int | None) -> object:
    if max_output_tokens is None:
        return agent_config
    try:
        return replace(agent_config, max_output_tokens=max_output_tokens)
    except TypeError:
        attrs = dict(getattr(agent_config, "__dict__", {}) or {})
        attrs["max_output_tokens"] = max_output_tokens
        return SimpleNamespace(**attrs)


def _route_candidate_binding_from_envelope(
    request_envelope: Mapping[str, object],
    *,
    job_label: str,
    agent_slug: str,
) -> dict[str, object]:
    manifest = request_envelope.get("route_plan_manifest")
    if not isinstance(manifest, dict):
        return {}
    jobs = manifest.get("jobs")
    if not isinstance(jobs, dict):
        return {}
    route_plan = jobs.get(job_label)
    if not isinstance(route_plan, dict):
        return {}
    entries = _route_candidate_entries_from_plan(route_plan)
    return dict(entries.get(agent_slug) or {})


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
    if normalized.startswith("missing credential:") or normalized.startswith("missing env var:"):
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

    # Provider slug is used by the API-only circuit breaker gate after transport
    # is resolved. CLI/MCP routes are subscription/local transports and must not
    # be blocked by API cost breakers.
    provider_slug = agent_slug.split("/")[0] if "/" in agent_slug else agent_slug

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
    run_envelope = _workflow_run_envelope(run_row)
    route_binding = _route_candidate_binding_from_envelope(
        run_envelope,
        job_label=str(label),
        agent_slug=str(agent_slug),
    )
    route_transport_type = str(route_binding.get("transport_type") or "").strip().upper()
    if route_transport_type not in {"CLI", "API"}:
        route_transport_type = ""
    route_candidate_ref = str(route_binding.get("candidate_ref") or "").strip()


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
    agent_config = None
    if route_transport_type and "/" in agent_slug:
        route_provider, _, route_model = str(agent_slug).partition("/")
        agent_config = AgentRegistry.load_from_postgres_for_route(
            conn,
            provider_slug=route_provider,
            model_slug=route_model,
            transport_type=route_transport_type,
            candidate_ref=route_candidate_ref or None,
        )
        if route_candidate_ref and agent_config is None:
            duration_ms = int((time.monotonic() - start) * 1000)
            complete_job(
                conn,
                job_id,
                status="failed",
                error_code="route_candidate_mismatch",
                duration_ms=duration_ms,
                stdout_preview=(
                    f"Route selected candidate_ref={route_candidate_ref} for {agent_slug} "
                    f"transport_type={route_transport_type}, but execution could not load that "
                    "exact active candidate; refusing fallback."
                )[:2000],
            )
            return
    agent_config = agent_config or registry.get(agent_slug)
    runtime_profile_ref = _runtime_profile_ref_for_run(conn, run_id)

    # Last-resort: if slug is still auto/, resolve via task_type_router now
    if agent_config is None and agent_slug.startswith("auto/"):
        duration_ms = int((time.monotonic() - start) * 1000)
        if runtime_profile_ref:
            logger.warning(
                "auto/ slug %s reached execution unresolved under runtime profile %s — failing closed",
                agent_slug,
                runtime_profile_ref,
            )
            complete_job(
                conn,
                job_id,
                status="failed",
                error_code="route_unresolved_runtime_profile",
                duration_ms=duration_ms,
                stdout_preview=(
                    f"Unresolved auto route reached execution for runtime profile "
                    f"{runtime_profile_ref}: {agent_slug}"
                ),
            )
        else:
            logger.warning(
                "auto/ slug %s reached execution unresolved without runtime profile — failing closed",
                agent_slug,
            )
            complete_job(
                conn,
                job_id,
                status="failed",
                error_code="route_unresolved_missing_runtime_profile",
                duration_ms=duration_ms,
                stdout_preview=(
                    "Unresolved auto route reached execution without runtime_profile_ref; "
                    f"refusing broad provider catalog routing: {agent_slug}"
                ),
            )
        return

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

    # Single prompt assembly path: prompt + platform context + shard + bundle.
    # BUG-D3CD86B8: shared with preview via assemble_full_prompt so preview
    # can surface the exact backend-bound string instead of drifting.
    platform_context = build_platform_context(execution_repo_root)

    if execution_context_shard is None:
        execution_context_shard = _runtime_execution_context_shard(
            conn,
            job=job,
            run_row=run_row,
            repo_root=execution_repo_root,
        )
    execution_context_shard_text = _render_execution_context_shard(execution_context_shard)

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

    full_prompt = assemble_full_prompt(
        prompt=prompt,
        platform_context=platform_context,
        execution_context_shard_text=execution_context_shard_text,
        execution_bundle_text=execution_bundle_text,
    )

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
    if route_transport_type:
        expected_transport_kind = "cli" if route_transport_type == "CLI" else "api"
        if transport_kind != expected_transport_kind:
            duration_ms = int((time.monotonic() - start) * 1000)
            complete_job(
                conn,
                job_id,
                status="failed",
                error_code="route_transport_mismatch",
                duration_ms=duration_ms,
                stdout_preview=(
                    f"Route selected transport_type={route_transport_type} for {agent_slug}, "
                    f"but execution resolved transport_kind={transport_kind}; refusing drift."
                )[:2000],
            )
            return

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
            circuit_breakers = None
            if provider_slug != "integration":
                try:
                    circuit_breakers = _circuit_breakers()
                except Exception as exc:
                    logger.error("Circuit breaker gate unavailable for %s: %s", provider_slug, exc)
                    complete_job(
                        conn,
                        job_id,
                        status="failed",
                        error_code="circuit_breaker.unavailable",
                        duration_ms=int((time.monotonic() - start) * 1000),
                        stdout_preview=str(exc),
                    )
                    return
                if circuit_breakers is None:
                    message = f"Circuit breaker gate returned no registry for {provider_slug}"
                    logger.error(message)
                    complete_job(
                        conn,
                        job_id,
                        status="failed",
                        error_code="circuit_breaker.unavailable",
                        duration_ms=int((time.monotonic() - start) * 1000),
                        stdout_preview=message,
                    )
                    return
                if not circuit_breakers.allow_request(provider_slug):
                    logger.warning("Circuit breaker OPEN for %s — blocking API execution", provider_slug)
                    complete_job(
                        conn,
                        job_id,
                        status="failed",
                        error_code="rate_limited",
                        duration_ms=int((time.monotonic() - start) * 1000),
                        stdout_preview=f"Circuit breaker open for {provider_slug}",
                    )
                    return
            _route_task_type = route_task_type
            _provider, _, _model = agent_slug.partition("/")
            _knobs = _route_execution_knobs(
                conn,
                route_task_type=_route_task_type,
                provider_slug=_provider,
                model_slug=_model,
                transport_type=route_transport_type,
            )
            _agent_config = _agent_config_with_max_output_tokens(
                agent_config,
                _knobs.get("max_output_tokens") if isinstance(_knobs.get("max_output_tokens"), int) else None,
            )
            result = _execute_api(
                _agent_config,
                full_prompt,
                execution_workdir,
                execution_bundle=execution_bundle,
                reasoning_effort=(
                    str(_knobs["reasoning_effort"])
                    if _knobs.get("reasoning_effort") is not None
                    else None
                ),
            )
        else:
            raise NotImplementedError("Unsupported execution transport")
    except Exception as exc:
        duration_ms = int((time.monotonic() - start) * 1000)
        complete_job(conn, job_id, status="failed", error_code="execution_exception",
                     duration_ms=duration_ms, stdout_preview=str(exc)[:2000])
        return

    duration_ms = int((time.monotonic() - start) * 1000)
    attempt_no = max(1, int(job.get("attempt") or 1))

    pre_verification_gate = _resolve_submission(
        conn,
        run_id=run_id,
        workflow_id=workflow_id_for_run,
        job_label=label,
        attempt_no=attempt_no,
        execution_bundle=execution_bundle,
        result=result,
        final_status=result.get("status", "failed"),
        final_error_code=result.get("error_code", ""),
        verification_artifact_refs=[],
        enforce_verification_contract=False,
        enforce_acceptance_contract=False,
    )
    if pre_verification_gate.final_error_code in {
        "workflow_submission.required_missing",
        "workflow_submission.lookup_failed",
    }:
        submission_state = pre_verification_gate.submission_state
        final_status = pre_verification_gate.final_status
        final_error_code = pre_verification_gate.final_error_code
        result = pre_verification_gate.result
        verification_summary = None
        verify_bindings = None
        verification_error = None
    else:
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


def _get_verify_bindings(
    conn: SyncPostgresConnection,
    run_id: str,
    label: str | None = None,
) -> list[str] | None:
    """Compatibility wrapper for verification binding extraction."""
    return _verification_runtime_get_verify_bindings(conn, run_id, label=label)


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
    """Backward-compat alias for build_platform_context.

    BUG-D3CD86B8: the canonical assembler now lives in _context_building.py
    so preview and execution share one authority. Any remaining callers of
    this private name still get the same string.
    """
    return build_platform_context(repo_root)



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
