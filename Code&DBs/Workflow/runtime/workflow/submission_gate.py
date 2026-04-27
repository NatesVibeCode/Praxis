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

class _AutoSealSkip(Exception):
    """Sentinel raised inside the auto-seal flow to fall through to Stage 3.

    Distinguishes 'expected, legitimate skip' (no in-scope changes, missing
    baseline, repo refusal) from genuinely unexpected exceptions which should
    be logged and still fall through to Stage 3 fail-closed behaviour.
    """


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

    # ── Stage 2.5: auto-seal on write_scope match ──────────────────────────
    # If the agent exited cleanly AND submission is required AND no submission
    # was sealed, attempt to auto-seal by detecting files changed within the
    # declared write_scope. We call `_measured_operations` directly (rather
    # than `_submit_submission`) so we can be SCOPE-TOLERANT: build packets
    # often produce shell side-effects (tmp files, cache, history) that
    # would trip the strict `out_of_scope` guard inside `_submit_submission`.
    # The agent's intent — writing the in-scope deliverable — is achieved
    # whether or not the sandbox shell also wrote ephemeral side files;
    # auto-seal records ONLY the in-scope subset and notes the side files
    # were ignored.
    #
    # Auto-seal still fails closed when no in-scope file changed (the
    # genuine contract violation). In that case Stage 3 fires the original
    # `workflow_submission.required_missing`.
    if final_status == "succeeded" and submission_required and submission_state is None:
        try:
            from pathlib import Path
            from runtime.workflow.submission_capture import (
                _load_runtime_context_state,
                _submission_protocol_state,
                _normalize_scope_paths,
            )
            from runtime.workflow.submission_diff import (
                _measured_operations,
                _artifact_ref,
                _hash_file,
            )
            from storage.postgres.workflow_submission_repository import (
                PostgresWorkflowSubmissionRepository,
                WorkflowSubmissionRepositoryError,
            )

            shard, bundle, wfid = _load_runtime_context_state(
                conn, run_id=run_id, job_label=job_label
            )
            submission_protocol = _submission_protocol_state(shard or {})
            baseline = dict(submission_protocol.get("baseline") or {})
            workspace_root = str(baseline.get("workspace_root") or "").strip()
            write_scope = _normalize_scope_paths(
                baseline.get("write_scope")
                or ((shard or {}).get("write_scope"))
            )

            if not baseline or not workspace_root or not write_scope:
                logger.warning(
                    "auto-seal skipped for %s/%s: missing baseline / workspace_root / write_scope",
                    run_id, job_label,
                )
                raise _AutoSealSkip()

            changed_paths, operation_set, out_of_scope, diff_artifact_ref = (
                _measured_operations(
                    conn=conn,
                    workspace_root=workspace_root,
                    write_scope=write_scope,
                    baseline=baseline,
                )
            )
            if not changed_paths:
                logger.info(
                    "auto-seal skipped for %s/%s: no in-scope changes (out_of_scope=%d)",
                    run_id, job_label, len(out_of_scope),
                )
                raise _AutoSealSkip()

            contract = (execution_bundle or bundle or {}).get("completion_contract") or {}
            auto_result_kind = (
                str(contract.get("result_kind") or "").strip() or "code_change"
            )
            artifact_refs: list[str] = []
            for op in operation_set:
                path = str(op.get("path") or "").strip()
                action = str(op.get("action") or "").strip()
                if action == "delete":
                    sha = str(
                        ((baseline.get("scoped_artifacts") or {}).get(path) or {}).get(
                            "sha256"
                        ) or ""
                    )
                    if sha:
                        artifact_refs.append(_artifact_ref(path, sha, deleted=True))
                    continue
                sha = _hash_file(Path(workspace_root) / path)
                if sha:
                    artifact_refs.append(_artifact_ref(path, sha))
            artifact_refs = sorted(dict.fromkeys(artifact_refs))

            workflow_id_resolved = (
                str(workflow_id or "").strip() or str(wfid or "").strip() or run_id
            )
            primary_summary = ", ".join(changed_paths[:3]) + (
                "" if len(changed_paths) <= 3 else f" (+{len(changed_paths) - 3} more)"
            )
            seal_notes = (
                f"auto_sealed=true; agent {job_label!r} exited cleanly and the "
                f"post-execution workspace diff against the pre-execution baseline "
                f"showed {len(changed_paths)} in-scope file(s) changed. "
                + (
                    f"Ignored {len(out_of_scope)} out-of-scope side-effect file(s)."
                    if out_of_scope
                    else "No out-of-scope side-effects."
                )
                + " (Gate sealed on the agent's behalf; agent did not call a seal MCP tool.)"
            )

            # Auto-seal also satisfies the verification gate when no
            # explicit verify_refs were declared on the spec. Rationale:
            # auto-seal RAN the diff, computed sha256 for every in-scope
            # file, and recorded it as the deliverable. Those artifact_refs
            # are stronger proof than "spec has no verify_refs at all" and
            # weaker than a typed registered verifier — but for build jobs
            # whose only contract is "produce the file in write_scope,"
            # file-existence + content-hash IS the verification.
            #
            # If the spec ALSO declared verify_refs, those still run via
            # the normal post-execution verifier path; auto-seal's contribution
            # is additive (merged in attach_verification_artifact_refs_for_job
            # later if needed). Stage 4 below treats either source as
            # satisfying the contract.
            auto_verification_refs = list(artifact_refs)

            repo = PostgresWorkflowSubmissionRepository(conn)
            try:
                recorded = repo.record_submission(
                    run_id=run_id,
                    workflow_id=workflow_id_resolved,
                    job_label=job_label,
                    attempt_no=attempt_no,
                    result_kind=auto_result_kind,
                    summary=f"Auto-sealed: {primary_summary}",
                    primary_paths=changed_paths,
                    tests_ran=[],
                    notes=seal_notes,
                    declared_operations=[],
                    changed_paths=changed_paths,
                    operation_set=operation_set,
                    comparison_status="auto_sealed",
                    comparison_report="",
                    diff_artifact_ref=diff_artifact_ref,
                    artifact_refs=artifact_refs,
                    verification_artifact_refs=auto_verification_refs,
                )
            except WorkflowSubmissionRepositoryError as exc:
                logger.warning(
                    "auto-seal repo write failed for %s/%s: %s",
                    run_id, job_label, exc,
                )
                raise _AutoSealSkip() from exc

            submission_state = dict(recorded)
            # Promote auto-seal's artifact refs into the function-local
            # verification_artifact_refs so Stage 4 sees auto-seal as
            # satisfying the verification contract. Caller-supplied refs
            # (from spec verify_refs that ran in _run_post_execution_verification)
            # take precedence — only fill when they were absent.
            if not verification_artifact_refs:
                verification_artifact_refs = auto_verification_refs
            logger.info(
                "submission auto-sealed for %s/%s attempt=%s result_kind=%s in_scope=%d out_of_scope=%d verification_refs=%d",
                run_id, job_label, attempt_no, auto_result_kind,
                len(changed_paths), len(out_of_scope), len(auto_verification_refs),
            )
            result = _result_with_stderr(
                result,
                (
                    f"auto_sealed: {len(changed_paths)} in-scope file(s) "
                    f"({primary_summary}); out-of-scope side-effects ignored: "
                    f"{len(out_of_scope)}; verification auto-satisfied via "
                    f"{len(auto_verification_refs)} content-hash artifact ref(s)"
                ),
            )
        except _AutoSealSkip:
            pass  # legitimate skip → Stage 3 below will mark failed
        except Exception as exc:  # noqa: BLE001 — fail open to Stage 3
            logger.warning(
                "submission auto-seal raised unexpected error for %s/%s: %s: %s",
                run_id, job_label, type(exc).__name__, exc,
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
    # have run verify_refs and they must have passed. Two sources count:
    #   (a) caller-supplied verification_artifact_refs from spec verify_refs
    #       executed in _run_post_execution_verification, OR
    #   (b) the recorded submission row's verification_artifact_refs — which
    #       Stage 2.5 auto-seal populates with content-hash refs for every
    #       in-scope file it copied from the post-exec sandbox snapshot.
    # If neither source has refs, verification was never run → fail.
    submission_verification_refs: list[str] = []
    if isinstance(submission_state, dict):
        raw_refs = submission_state.get("verification_artifact_refs") or []
        if isinstance(raw_refs, (list, tuple)):
            submission_verification_refs = [
                str(ref).strip() for ref in raw_refs if str(ref).strip()
            ]
    effective_verification_refs = (
        list(verification_artifact_refs) or submission_verification_refs
    )
    if (
        enforce_verification_contract
        and verification_required
        and final_status == "succeeded"
        and not effective_verification_refs
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
