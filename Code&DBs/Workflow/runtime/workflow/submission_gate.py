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


def _auto_seal_text_only(
    conn: "SyncPostgresConnection",
    *,
    run_id: str,
    workflow_id: str,
    job_label: str,
    attempt_no: int,
    summary: str,
    result_kind: str,
) -> dict[str, Any]:
    """Insert a minimal submission row for text-only output (no write_scope).

    Bypasses the baseline-comparison pipeline entirely. Used when the agent
    produced text output but no baseline was captured because the job has
    no write_scope (research, debate, architecture tasks).
    """
    import json
    import uuid
    from datetime import datetime, timezone

    submission_id = f"sub_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)
    # Truncate summary to 250k chars to stay within DB limits
    truncated = summary[:250_000] if len(summary) > 250_000 else summary

    conn.execute(
        """INSERT INTO workflow_job_submissions
           (submission_id, run_id, workflow_id, job_label, attempt_no,
            result_kind, summary, primary_paths, comparison_status, sealed_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10)
           ON CONFLICT (run_id, job_label, attempt_no) DO NOTHING""",
        submission_id, run_id, workflow_id, job_label, attempt_no,
        result_kind, truncated, json.dumps([]), "text_only", now,
    )
    return {
        "submission_id": submission_id,
        "result_kind": result_kind,
        "comparison_status": "text_only",
        "sealed_at": now.isoformat(),
    }


@dataclass
class SubmissionGateResult:
    submission_state: dict[str, Any] | None
    final_status: str        # "succeeded" | "failed"
    final_error_code: str    # "" when succeeded
    result: dict[str, Any]   # execution result dict, may have stderr appended


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
) -> SubmissionGateResult:
    """Resolve the submission state for a completed job.

    Three stages in order:
    1. Check whether the agent already sealed a submission (via tool call).
    2. If not, and agent succeeded with non-empty output, auto-seal the output.
    3. If still missing and required, mark the job failed.
    """
    from runtime.workflow.submission_capture import (
        WorkflowSubmissionServiceError,
        attach_verification_artifact_refs_for_job,
        get_submission_for_job_attempt,
        submit_artifact_bundle,
        submit_research_result,
    )
    from runtime.workflow.receipt_writer import extract_transcript_text, is_transcript_output
    from runtime.workflow._context_building import _submission_required_for_bundle

    submission_required = _submission_required_for_bundle(execution_bundle)
    submission_state: dict[str, Any] | None = None

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

    # ── Stage 2: auto-seal when agent produced output but skipped the tool ──
    if final_status == "succeeded" and submission_required and submission_state is None:
        raw_output = str(result.get("stdout") or "").strip()
        output_text = (
            extract_transcript_text(raw_output)
            if is_transcript_output(raw_output)
            else raw_output
        )
        if output_text:
            result_kind = str(
                ((execution_bundle or {}).get("completion_contract") or {}).get("result_kind")
                or "research_result"
            )
            try:
                submit_fn = (
                    submit_artifact_bundle
                    if result_kind == "artifact_bundle"
                    else submit_research_result
                )
                submission_state = submit_fn(
                    run_id=run_id,
                    workflow_id=workflow_id or run_id,
                    job_label=job_label,
                    summary=output_text,
                    primary_paths=[],
                    result_kind=result_kind,
                    conn=conn,
                )
                logger.info(
                    "Auto-sealed submission for %s/%s (result_kind=%s)",
                    run_id, job_label, result_kind,
                )
            except WorkflowSubmissionServiceError:
                # Baseline-dependent seal failed (no write_scope / no baseline).
                # Fall back to a direct text-only submission insert.
                try:
                    submission_state = _auto_seal_text_only(
                        conn,
                        run_id=run_id,
                        workflow_id=workflow_id or run_id,
                        job_label=job_label,
                        attempt_no=attempt_no,
                        summary=output_text,
                        result_kind=result_kind,
                    )
                    logger.info(
                        "Auto-sealed text-only submission for %s/%s",
                        run_id, job_label,
                    )
                except Exception as exc2:
                    logger.warning(
                        "Text-only auto-seal failed for %s/%s: %s",
                        run_id, job_label, exc2,
                    )
            except Exception as exc:
                logger.warning(
                    "Auto-seal failed for %s/%s: %s", run_id, job_label, exc
                )
                # Fall back to direct text-only insert (same as WorkflowSubmissionServiceError path)
                try:
                    submission_state = _auto_seal_text_only(
                        conn,
                        run_id=run_id,
                        workflow_id=workflow_id or run_id,
                        job_label=job_label,
                        attempt_no=attempt_no,
                        summary=output_text,
                        result_kind=result_kind,
                    )
                    logger.info(
                        "Auto-sealed text-only submission (fallback) for %s/%s",
                        run_id, job_label,
                    )
                except Exception as exc2:
                    logger.warning(
                        "Text-only fallback seal failed for %s/%s: %s",
                        run_id, job_label, exc2,
                    )

    # ── Stage 2b: re-check for agent-sealed submission before enforcing ────
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
        except Exception:
            pass

    # ── Stage 3: enforce the contract ───────────────────────────────────────
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

    return SubmissionGateResult(
        submission_state=submission_state,
        final_status=final_status,
        final_error_code=final_error_code,
        result=result,
    )
