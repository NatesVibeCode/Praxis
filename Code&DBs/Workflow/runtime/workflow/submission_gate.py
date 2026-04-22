"""Submission gate: resolve, auto-seal, and enforce the submission contract for a job.

Single entry point called by the execution core after an agent completes.
All submission-related logic lives here — nothing submission-specific leaks
into _execution_core.py.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

@dataclass
class SubmissionGateResult:
    submission_state: dict[str, Any] | None
    final_status: str        # "succeeded" | "failed"
    final_error_code: str    # "" when succeeded
    result: dict[str, Any]   # execution result dict, may have stderr appended


def _result_with_stderr(result: dict[str, Any], message: str) -> dict[str, Any]:
    return {
        **result,
        "stderr": (
            str(result.get("stderr") or "")
            + f"\n{message}"
        ).strip(),
    }


def resolve_submission_for_job(
    conn: "SyncPostgresConnection",
    *,
    run_id: str,
    workflow_id: str | None,
    job_label: str,
    attempt_no: int,
    execution_bundle: dict[str, Any] | None,
    result: dict[str, Any],
    final_status: str,
    final_error_code: str,
    verification_artifact_refs: list[str],
    enforce_verification_contract: bool = True,
    enforce_acceptance_contract: bool = True,
) -> SubmissionGateResult:
    """Resolve the submission state for a completed job.

    Stages in order:
    1. Check whether the agent already sealed a submission (via tool call).
    2. Re-check for a submission before final enforcement.
    3. If still missing and required, mark the job failed.
    4. Enforce verify_refs and acceptance status.
    """
    from runtime.workflow.submission_capture import (
        WorkflowSubmissionServiceError,
        attach_verification_artifact_refs_for_job,
        get_submission_for_job_attempt,
    )
    from runtime.workflow._context_building import _submission_required_for_bundle

    submission_required = _submission_required_for_bundle(execution_bundle)
    submission_state: dict[str, Any] | None = None

    verification_required = bool(
        ((execution_bundle or {}).get("completion_contract") or {}).get("verification_required")
    )

    # ── Stage 1: check for an existing sealed submission ────────────────────
    try:
        if verification_artifact_refs:
            submission_state = attach_verification_artifact_refs_for_job(
                conn,
                run_id=run_id,
                job_label=job_label,
                attempt_no=attempt_no,
                verification_artifact_refs=verification_artifact_refs,
            )
        if submission_state is None:
            submission_state = get_submission_for_job_attempt(
                conn,
                run_id=run_id,
                job_label=job_label,
                attempt_no=attempt_no,
            )
    except WorkflowSubmissionServiceError as exc:
        # Log but do NOT mark failed — Stage 2 auto-seal may still recover.
        # Stage 3 will enforce the contract if no submission materialises.
        logger.warning(
            "submission receipt sync failed for %s/%s: %s", run_id, job_label, exc
        )

    # ── Stage 2: re-check for agent-sealed submission before enforcing ─────
    # The agent may have sealed a submission via MCP during execution that
    # Stage 1 missed due to commit timing.  One more lookup before rejecting.
    if final_status == "succeeded" and submission_required and submission_state is None:
        try:
            submission_state = get_submission_for_job_attempt(
                conn,
                run_id=run_id,
                job_label=job_label,
                attempt_no=attempt_no,
            )
        except Exception as exc:
            final_status = "failed"
            final_error_code = "workflow_submission.lookup_failed"
            result = _result_with_stderr(
                result,
                (
                    "submission lookup failed before final enforcement: "
                    f"{type(exc).__name__}: {exc}"
                ),
            )
            logger.warning(
                "submission final lookup failed for %s/%s: %s",
                run_id,
                job_label,
                exc,
            )

    # ── Stage 3: enforce the submission contract ───────────────────────────
    if final_status == "succeeded" and submission_required and submission_state is None:
        final_status = "failed"
        final_error_code = "workflow_submission.required_missing"
        result = {
            **result,
            "stderr": (
                str(result.get("stderr") or "")
                + "\nsubmission_required=true but no sealed submission exists for the current attempt"
            ).strip(),
        }

    # ── Stage 4: enforce verification contract ─────────────────────────────
    # If the completion contract says verification is required, the job must
    # have run verify_refs and they must have passed. If verification never
    # ran (no verify_refs, so no artifact refs) the job fails here.
    if (
        enforce_verification_contract
        and verification_required
        and final_status == "succeeded"
        and not verification_artifact_refs
    ):
        final_status = "failed"
        final_error_code = "verification.required_not_run"
        result = {
            **result,
            "stderr": (
                str(result.get("stderr") or "")
                + "\nverification_required=true but no verify_refs were executed"
            ).strip(),
        }

    # ── Stage 5: enforce acceptance contract ───────────────────────────────
    # If the submission was sealed and has an acceptance_status of "failed",
    # the job fails regardless of other status.
    if enforce_acceptance_contract and final_status == "succeeded" and isinstance(submission_state, dict):
        acceptance_status = str(submission_state.get("acceptance_status") or "").strip().lower()
        if acceptance_status == "failed":
            final_status = "failed"
            final_error_code = "acceptance.contract_failed"
            hard_failures = []
            acceptance_report = submission_state.get("acceptance_report")
            if isinstance(acceptance_report, dict):
                hard_failures = acceptance_report.get("hard_failures") or []
            failure_summary = "; ".join(str(f) for f in hard_failures[:5]) if hard_failures else "acceptance contract not met"
            result = {
                **result,
                "stderr": (
                    str(result.get("stderr") or "")
                    + f"\nacceptance_status=failed: {failure_summary}"
                ).strip(),
            }

    return SubmissionGateResult(
        submission_state=submission_state,
        final_status=final_status,
        final_error_code=final_error_code,
        result=result,
    )
