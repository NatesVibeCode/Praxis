"""Dry-run simulation for workflow specs without invoking the legacy runner."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


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


def _pipeline():
    import runtime.workflow_pipeline as dp

    return dp.WorkflowPipeline(
        governance=dp.GovernanceFilter(),
        conflict_resolver=dp.ConflictResolver(),
        loop_detector=dp.LoopDetector(),
        auto_retry=dp.AutoRetryManager(),
        retry_context_builder=dp.RetryContextBuilder(),
        posture_enforcer=dp.PostureEnforcer(dp.Posture.BUILD),
    )


def dry_run_workflow(spec) -> DryRunResult:
    """Simulate one workflow spec through governance/dependency gates only."""
    started = time.monotonic()
    pipeline = _pipeline()
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
                gate = pipeline.pre_dispatch(
                    {
                        "job_label": label,
                        "label": label,
                        "prompt": job.get("prompt", ""),
                        "agent_slug": str(agent_slug or ""),
                    }
                )
                result = DryRunJobResult(
                    job_label=label,
                    agent_slug=agent_slug,
                    status="succeeded" if gate.passed else "blocked",
                    exit_code=0 if gate.passed else None,
                    duration_seconds=0.0,
                    verify_passed=None,
                    retry_count=0,
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
