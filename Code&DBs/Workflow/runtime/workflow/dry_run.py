"""Dry-run simulation for workflow specs.

When Postgres authority is available, dry-run delegates route/admission shape
to ``preview_workflow_execution`` so the simulation reflects the live runtime
lane instead of the retired ``workflow_pipeline`` facade.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any
from typing import Optional

from runtime.governance import GovernanceFilter


@dataclass(frozen=True)
class DryRunJobResult:
    job_label: str
    agent_slug: str
    status: str
    exit_code: Optional[int]
    duration_seconds: float
    verify_passed: Optional[bool]
    retry_count: int


@dataclass(frozen=True)
class DryRunResult:
    spec_name: str
    total_jobs: int
    succeeded: int
    failed: int
    skipped: int
    blocked: int
    duration_seconds: float
    receipts_written: tuple[str, ...]
    job_results: tuple[DryRunJobResult, ...]


def _spec_snapshot(spec: Any) -> dict[str, Any]:
    raw = dict(getattr(spec, "_raw", {}) or {})
    if raw:
        return json.loads(json.dumps(raw, default=str))
    snapshot: dict[str, Any] = {
        "name": getattr(spec, "name", "inline"),
        "workflow_id": getattr(spec, "workflow_id", "workflow.inline"),
        "phase": getattr(spec, "phase", "build"),
        "jobs": list(getattr(spec, "jobs", []) or []),
        "verify_refs": list(getattr(spec, "verify_refs", []) or []),
        "outcome_goal": getattr(spec, "outcome_goal", ""),
        "anti_requirements": list(getattr(spec, "anti_requirements", []) or []),
    }
    workspace_ref = getattr(spec, "workspace_ref", None)
    runtime_profile_ref = getattr(spec, "runtime_profile_ref", None)
    if workspace_ref:
        snapshot["workspace_ref"] = workspace_ref
    if runtime_profile_ref:
        snapshot["runtime_profile_ref"] = runtime_profile_ref
    return json.loads(json.dumps(snapshot, default=str))


def _ensure_preview_workdir(spec_snapshot: dict[str, Any], repo_root: str) -> dict[str, Any]:
    normalized = json.loads(json.dumps(spec_snapshot, default=str))
    jobs = normalized.get("jobs")
    if not isinstance(jobs, list):
        return normalized
    has_explicit_workdir = any(
        isinstance(job, dict) and str(job.get("workdir") or "").strip()
        for job in jobs
    )
    top_level_workdir = str(normalized.get("workdir") or "").strip()
    if not top_level_workdir and not has_explicit_workdir:
        normalized["workdir"] = repo_root
    return normalized


def _preview_jobs_by_label(
    spec,
    *,
    pg_conn: Any,
    repo_root: str,
) -> dict[str, dict[str, Any]]:
    from runtime.workflow.unified import preview_workflow_execution

    preview_payload = preview_workflow_execution(
        pg_conn,
        inline_spec=_ensure_preview_workdir(_spec_snapshot(spec), repo_root),
        repo_root=repo_root,
    )
    preview_jobs = preview_payload.get("jobs")
    if not isinstance(preview_jobs, list):
        return {}
    return {
        str(job.get("label") or ""): dict(job)
        for job in preview_jobs
        if isinstance(job, dict) and str(job.get("label") or "").strip()
    }


def _job_blocked_by_authority(
    preview_job: dict[str, Any] | None,
    *,
    job: dict[str, Any],
    governance: GovernanceFilter,
) -> bool:
    if preview_job is not None:
        if str(preview_job.get("route_status") or "").strip() == "unresolved":
            return True
        rendered_prompt = str(
            preview_job.get("rendered_full_prompt")
            or preview_job.get("rendered_prompt")
            or preview_job.get("prompt")
            or job.get("prompt")
            or ""
        )
        return not governance.scan_prompt(rendered_prompt).passed
    return not governance.scan_prompt(str(job.get("prompt") or "")).passed


def dry_run_workflow(
    spec,
    *,
    pg_conn: Any = None,
    repo_root: str | None = None,
) -> DryRunResult:
    """Simulate one workflow spec through runtime preview and dependency gates."""
    started = time.monotonic()
    preview_repo_root = str(repo_root or os.getcwd()).strip() or os.getcwd()
    governance = GovernanceFilter()
    preview_jobs = (
        _preview_jobs_by_label(spec, pg_conn=pg_conn, repo_root=preview_repo_root)
        if pg_conn is not None
        else {}
    )
    job_results: list[DryRunJobResult] = []
    receipts_written: list[str] = []
    completed: dict[str, DryRunJobResult] = {}
    pending = {job["label"]: dict(job) for job in spec.jobs}

    while pending:
        progressed = False
        for label, job in list(pending.items()):
            depends_on = list(job.get("depends_on", []) or [])
            if any(dep not in completed for dep in depends_on):
                continue

            agent_slug = str(job["agent"])
            if any(completed[dep].status != "succeeded" for dep in depends_on):
                result = DryRunJobResult(
                    job_label=label,
                    agent_slug=agent_slug,
                    status="blocked",
                    exit_code=None,
                    duration_seconds=0.0,
                    verify_passed=None,
                    retry_count=0,
                )
            else:
                result = DryRunJobResult(
                    job_label=label,
                    agent_slug=agent_slug,
                    status=(
                        "blocked"
                        if _job_blocked_by_authority(
                            preview_jobs.get(label),
                            job=job,
                            governance=governance,
                        )
                        else "succeeded"
                    ),
                    exit_code=0,
                    duration_seconds=0.0,
                    verify_passed=None,
                    retry_count=0,
                )
                if result.status == "blocked":
                    result = DryRunJobResult(
                        job_label=result.job_label,
                        agent_slug=result.agent_slug,
                        status=result.status,
                        exit_code=None,
                        duration_seconds=result.duration_seconds,
                        verify_passed=result.verify_passed,
                        retry_count=result.retry_count,
                    )

            completed[label] = result
            job_results.append(result)
            receipts_written.append(f"dry_run:{label}")
            pending.pop(label)
            progressed = True

        if progressed:
            continue

        for label, job in list(pending.items()):
            result = DryRunJobResult(
                job_label=label,
                agent_slug=str(job["agent"]),
                status="blocked",
                exit_code=None,
                duration_seconds=0.0,
                verify_passed=None,
                retry_count=0,
            )
            completed[label] = result
            job_results.append(result)
            receipts_written.append(f"dry_run:{label}")
            pending.pop(label)

    duration_seconds = round(time.monotonic() - started, 2)
    succeeded = sum(1 for result in job_results if result.status == "succeeded")
    failed = sum(1 for result in job_results if result.status == "failed")
    blocked = sum(1 for result in job_results if result.status == "blocked")
    skipped = sum(1 for result in job_results if result.status == "skipped")

    return DryRunResult(
        spec_name=spec.name,
        total_jobs=len(spec.jobs),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        blocked=blocked,
        duration_seconds=duration_seconds,
        receipts_written=tuple(receipts_written),
        job_results=tuple(job_results),
    )
