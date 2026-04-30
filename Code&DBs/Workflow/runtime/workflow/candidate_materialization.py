"""Code-change candidate materialization service.

This service keeps the dangerous part small and explicit: a candidate may
touch live source only after its sealed submission, verifier, review/routing
policy, and gate evidence all line up.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import tempfile
from typing import Any

from policy import gate as gate_policy
from runtime.bug_evidence import EVIDENCE_KIND_VERIFICATION_RUN, EVIDENCE_ROLE_VALIDATES_FIX
from runtime.bug_tracker import BugStatus, BugTracker
from runtime.sandbox_artifacts import ArtifactStore
from runtime.verifier_authority import run_registered_verifier
from runtime.workflow.candidate_authoring import (
    CandidateAuthoringError,
    CandidatePatchProjection,
    derive_candidate_patch_from_sources,
    source_context_from_worktree,
)
from runtime.workflow.submission_capture import _submission_manifest_hash
from runtime.workflow.submission_policy import _insert_gate_evaluation, _insert_promotion_decision
from runtime.workspace_paths import repo_root as default_repo_root
from runtime.workspace_paths import to_repo_ref
from storage.postgres import PostgresBugEvidenceRepository


DEFAULT_POLICY_SNAPSHOT_REF = "policy_snapshot:code_change_candidate_materialize_v0"
DEFAULT_TARGET_REF = "repo:canonical"


class CandidateMaterializationError(RuntimeError):
    """Raised when a candidate cannot safely materialize."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _json_object(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _json_list(value: object) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return list(parsed) if isinstance(parsed, list) else []
    return []


def _candidate_row(conn: Any, *, candidate_id: str) -> dict[str, Any]:
    row = conn.fetchrow(
        """
        SELECT c.candidate_id::text AS candidate_id,
               c.submission_id,
               c.bug_id,
               c.base_head_ref,
               c.source_context_refs,
               c.intended_files,
               c.proposal_payload,
               c.patch_artifact_ref,
               c.patch_sha256,
               c.verifier_ref,
               c.verifier_inputs,
               c.review_routing,
               c.next_actor_kind,
               c.materialization_status,
               c.routing_decision_record,
               c.temp_verifier_run_id,
               c.final_verifier_run_id,
               c.gate_evaluation_id,
               c.promotion_decision_id,
               c.last_error,
               s.run_id,
               s.workflow_id,
               s.job_label,
               s.result_kind,
               s.summary,
               s.primary_paths,
               s.tests_ran,
               s.notes,
               s.declared_operations,
               s.changed_paths,
               s.operation_set,
               s.comparison_status,
               s.comparison_report,
               s.acceptance_status,
               s.acceptance_report,
               s.diff_artifact_ref,
               s.artifact_refs,
               s.verification_artifact_refs,
               s.sealed_at
          FROM code_change_candidate_payloads c
          JOIN workflow_job_submissions s
            ON s.submission_id = c.submission_id
         WHERE c.candidate_id = $1::uuid
        """,
        candidate_id,
    )
    if row is None:
        raise CandidateMaterializationError(
            "code_change_candidate.not_found",
            "candidate_id did not resolve to a code-change candidate",
            details={"candidate_id": candidate_id},
        )
    return dict(row)


def _latest_review(conn: Any, *, submission_id: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT review_id,
               reviewer_job_label,
               reviewer_role,
               decision,
               summary,
               notes,
               evidence_refs,
               reviewed_at
          FROM workflow_job_submission_reviews
         WHERE submission_id = $1
         ORDER BY reviewed_at DESC, review_id DESC
         LIMIT 1
        """,
        submission_id,
    )
    return None if row is None else dict(row)


def _set_candidate_state(
    conn: Any,
    *,
    candidate_id: str,
    materialization_status: str | None = None,
    next_actor_kind: str | None = None,
    last_error: Mapping[str, Any] | None = None,
    temp_verifier_run_id: str | None = None,
    final_verifier_run_id: str | None = None,
    gate_evaluation_id: str | None = None,
    promotion_decision_id: str | None = None,
    patch_artifact_ref: str | None = None,
    patch_sha256: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE code_change_candidate_payloads
           SET materialization_status = COALESCE($2, materialization_status),
               next_actor_kind = COALESCE($3, next_actor_kind),
               last_error = COALESCE($4::jsonb, last_error),
               temp_verifier_run_id = COALESCE($5, temp_verifier_run_id),
               final_verifier_run_id = COALESCE($6, final_verifier_run_id),
               gate_evaluation_id = COALESCE($7, gate_evaluation_id),
               promotion_decision_id = COALESCE($8, promotion_decision_id),
               patch_artifact_ref = COALESCE($9, patch_artifact_ref),
               patch_sha256 = COALESCE($10, patch_sha256),
               updated_at = now()
         WHERE candidate_id = $1::uuid
        """,
        candidate_id,
        materialization_status,
        next_actor_kind,
        json.dumps(dict(last_error), sort_keys=True, default=str) if last_error is not None else None,
        temp_verifier_run_id,
        final_verifier_run_id,
        gate_evaluation_id,
        promotion_decision_id,
        patch_artifact_ref,
        patch_sha256,
    )


def _set_submission_acceptance(
    conn: Any,
    *,
    submission_id: str,
    acceptance_status: str,
    acceptance_report: Mapping[str, Any],
) -> None:
    conn.execute(
        """
        UPDATE workflow_job_submissions
           SET acceptance_status = $2,
               acceptance_report = $3::jsonb
         WHERE submission_id = $1
        """,
        submission_id,
        acceptance_status,
        json.dumps(dict(acceptance_report), sort_keys=True, default=str),
    )


def _run_git(repo_root: Path, args: Sequence[str], *, input_text: str | None = None) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
        timeout=120.0,
    )
    if completed.returncode != 0:
        raise CandidateMaterializationError(
            "code_change_candidate.git_failed",
            "git command failed during candidate materialization",
            details={
                "args": list(args),
                "returncode": completed.returncode,
                "stderr": completed.stderr.strip(),
                "stdout": completed.stdout.strip(),
            },
        )
    return completed.stdout.strip()


def current_head_ref(repo_root: Path) -> str:
    return _run_git(repo_root, ["rev-parse", "HEAD"])


def _dirty_intended_paths(repo_root: Path, paths: Sequence[str]) -> list[str]:
    if not paths:
        return []
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "status", "--porcelain", "--", *paths],
        capture_output=True,
        text=True,
        check=False,
        timeout=20.0,
    )
    if completed.returncode != 0:
        raise CandidateMaterializationError(
            "code_change_candidate.git_failed",
            "could not inspect working tree status",
            details={"stderr": completed.stderr.strip(), "paths": list(paths)},
        )
    dirty: list[str] = []
    for line in completed.stdout.splitlines():
        text = line.strip()
        if not text:
            continue
        dirty.append(text[3:] if len(text) > 3 else text)
    return dirty


def _staged_paths(repo_root: Path) -> list[str]:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=False,
        timeout=20.0,
    )
    if completed.returncode != 0:
        raise CandidateMaterializationError(
            "code_change_candidate.git_failed",
            "could not inspect staged source changes",
            details={"stderr": completed.stderr.strip()},
        )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _assert_index_clean(repo_root: Path) -> None:
    staged = _staged_paths(repo_root)
    if staged:
        raise CandidateMaterializationError(
            "code_change_candidate.index_dirty",
            "materialization requires an empty git index so the promotion commit has one authority",
            details={"staged_paths": staged},
        )


def _commit_live_apply(
    repo_root: Path,
    *,
    candidate_id: str,
    intended_files: Sequence[str],
    materialized_by: str,
) -> str:
    intended_refs = tuple(dict.fromkeys(to_repo_ref(path) for path in intended_files if str(path).strip()))
    if not intended_refs:
        raise CandidateMaterializationError(
            "code_change_candidate.no_intended_files",
            "candidate materialization needs intended files before creating a source commit",
            details={"candidate_id": candidate_id},
        )

    _assert_index_clean(repo_root)
    try:
        _run_git(repo_root, ["add", "--", *intended_refs])
        staged = _staged_paths(repo_root)
        intended_set = set(intended_refs)
        unexpected = sorted(set(staged) - intended_set)
        if unexpected or not staged:
            raise CandidateMaterializationError(
                "code_change_candidate.commit_scope_invalid",
                "candidate promotion commit scope did not match intended files",
                details={
                    "candidate_id": candidate_id,
                    "staged_paths": staged,
                    "unexpected_paths": unexpected,
                    "intended_files": list(intended_refs),
                },
            )

        actor = " ".join(str(materialized_by or "system").split())
        message = (
            f"Materialize code-change candidate {candidate_id}\n\n"
            f"Materialized-by: {actor}"
        )
        _run_git(
            repo_root,
            [
                "-c",
                "user.name=Praxis Candidate Materializer",
                "-c",
                "user.email=praxis-candidate@local",
                "commit",
                "--no-gpg-sign",
                "--no-verify",
                "-m",
                message,
            ],
        )
    except Exception:
        try:
            _run_git(repo_root, ["reset", "--", *intended_refs])
        except Exception:
            pass
        raise
    return current_head_ref(repo_root)


def _capture_patch_artifact(
    conn: Any,
    *,
    candidate_id: str,
    projection: CandidatePatchProjection,
) -> str:
    record = ArtifactStore(conn).capture(
        f"code_change_candidates/{candidate_id}.diff",
        projection.unified_diff,
        f"code_change_candidate:{candidate_id}",
    )
    return f"sandbox_artifact:{record.artifact_id}"


def _populate_authority_supersession_registry(
    conn: Any,
    *,
    candidate_id: str,
    promotion_decision_id: str | None,
    materialized_by: str,
) -> int:
    """For a materialized candidate, write supersession rows for each
    validated impact whose intent is replace or retire. Rows already present
    (matching successor/predecessor pair, not rolled back) are left alone."""

    impact_rows = conn.fetch(
        """
        SELECT impact_id::text                  AS impact_id,
               intent::text                     AS intent,
               unit_kind::text                  AS successor_unit_kind,
               unit_ref                         AS successor_unit_ref,
               predecessor_unit_kind::text      AS predecessor_unit_kind,
               predecessor_unit_ref,
               subsumption_evidence_ref,
               rollback_path,
               notes
          FROM candidate_authority_impacts
         WHERE candidate_id = $1::uuid
           AND intent IN ('replace', 'retire')
           AND validation_status IN ('validated', 'runtime_addition')
           AND predecessor_unit_kind IS NOT NULL
           AND predecessor_unit_ref IS NOT NULL
        """,
        candidate_id,
    )
    if not impact_rows:
        return 0

    inserted = 0
    decision_ref = (
        "decision.architecture_policy.platform_architecture.candidate_authority_impact_contract"
    )
    for row in impact_rows:
        impact = dict(row)
        intent = impact["intent"]
        successor_kind: str | None
        successor_ref: str | None
        if intent == "replace":
            successor_kind = impact["successor_unit_kind"]
            successor_ref = impact["successor_unit_ref"]
        else:
            # intent == retire; no successor unit declared. Use the
            # candidate_id ref as a placeholder successor to record the
            # retirement event without a live replacement.
            successor_kind = "operation_ref"
            successor_ref = f"retired:{candidate_id}"

        supersession_status = "compat" if intent == "replace" else "pending_retire"
        obligation_summary_parts: list[str] = []
        if impact.get("notes"):
            obligation_summary_parts.append(str(impact["notes"]))
        obligation_summary_parts.append(
            f"Materialized via candidate {candidate_id}; intent={intent}; "
            f"materialized_by={materialized_by}."
        )
        obligation_summary = " ".join(obligation_summary_parts)

        evidence: dict[str, Any] = {
            "candidate_id": candidate_id,
            "promotion_decision_id": promotion_decision_id,
            "materialized_by": materialized_by,
            "intent": intent,
        }
        if impact.get("subsumption_evidence_ref"):
            evidence["subsumption_evidence_ref"] = impact["subsumption_evidence_ref"]
        if impact.get("rollback_path"):
            evidence["rollback_path"] = impact["rollback_path"]

        result = conn.execute(
            """
            INSERT INTO authority_supersession_registry (
                successor_unit_kind,
                successor_unit_ref,
                predecessor_unit_kind,
                predecessor_unit_ref,
                supersession_status,
                obligation_summary,
                obligation_evidence,
                source_candidate_id,
                source_impact_id,
                source_decision_ref
            ) VALUES (
                $1::candidate_authority_unit_kind,
                $2,
                $3::candidate_authority_unit_kind,
                $4,
                $5::authority_supersession_status,
                $6,
                $7::jsonb,
                $8::uuid,
                $9::uuid,
                $10
            )
            ON CONFLICT (
                successor_unit_kind,
                successor_unit_ref,
                predecessor_unit_kind,
                predecessor_unit_ref
            )
            WHERE supersession_status <> 'rolled_back'
            DO UPDATE SET
                supersession_status = EXCLUDED.supersession_status,
                obligation_summary  = EXCLUDED.obligation_summary,
                obligation_evidence = EXCLUDED.obligation_evidence,
                source_candidate_id = EXCLUDED.source_candidate_id,
                source_impact_id    = EXCLUDED.source_impact_id,
                source_decision_ref = EXCLUDED.source_decision_ref,
                updated_at          = now()
            """,
            successor_kind,
            successor_ref,
            impact["predecessor_unit_kind"],
            impact["predecessor_unit_ref"],
            supersession_status,
            obligation_summary,
            json.dumps(evidence, sort_keys=True, default=str),
            candidate_id,
            impact["impact_id"],
            decision_ref,
        )
        inserted += 1
        _ = result  # asyncpg result rowcount is not needed here
    return inserted


def _candidate_submission_projection(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "submission_id": candidate["submission_id"],
        "result_kind": candidate["result_kind"],
        "summary": candidate["summary"],
        "comparison_status": candidate["comparison_status"],
        "acceptance_status": candidate["acceptance_status"],
        "diff_artifact_ref": candidate.get("diff_artifact_ref"),
        "primary_paths": _json_list(candidate.get("primary_paths")),
        "changed_paths": _json_list(candidate.get("changed_paths")),
        "operation_set": _json_list(candidate.get("operation_set")),
        "artifact_refs": _json_list(candidate.get("artifact_refs")),
        "verification_artifact_refs": _json_list(candidate.get("verification_artifact_refs")),
        "acceptance_report": _json_object(candidate.get("acceptance_report")),
    }


def _assert_review_preconditions(candidate: Mapping[str, Any], latest_review: Mapping[str, Any] | None) -> bool:
    routing = str(candidate.get("review_routing") or "").strip()
    acceptance_status = str(candidate.get("acceptance_status") or "").strip()
    latest_decision = str(latest_review.get("decision") or "").strip() if latest_review else ""
    if acceptance_status == "rejected" or latest_decision == "reject":
        raise CandidateMaterializationError(
            "code_change_candidate.review_rejected",
            "candidate has a rejecting review and cannot materialize",
            details={"submission_id": candidate.get("submission_id")},
        )
    if latest_decision == "request_changes":
        raise CandidateMaterializationError(
            "code_change_candidate.review_requested_changes",
            "candidate has a request-changes review and cannot materialize",
            details={"submission_id": candidate.get("submission_id")},
        )
    if routing == "auto_apply":
        routing_record = _json_object(candidate.get("routing_decision_record"))
        if not routing_record:
            raise CandidateMaterializationError(
                "code_change_candidate.routing_record_missing",
                "auto_apply candidates require a durable routing_decision_record",
                details={"candidate_id": candidate.get("candidate_id")},
            )
        return True
    if routing == "human_review":
        return latest_decision == "approve"
    raise CandidateMaterializationError(
        "code_change_candidate.routing_unsupported",
        "this materializer only supports V0 routing values",
        details={"review_routing": routing},
    )


def _rewrite_path_for_worktree(value: str, *, repo_root: Path, worktree_root: Path) -> str:
    text = str(value or "").strip()
    if not text:
        return text
    candidate = Path(text)
    if candidate.is_absolute():
        try:
            relative = candidate.resolve().relative_to(repo_root.resolve())
        except (OSError, ValueError):
            return text
        return str((worktree_root / relative).resolve())
    repo_ref = to_repo_ref(text)
    target = (worktree_root / repo_ref).resolve()
    return str(target) if target.exists() else text


def _verifier_inputs_for_worktree(
    inputs: Mapping[str, Any],
    *,
    repo_root: Path,
    worktree_root: Path,
) -> dict[str, Any]:
    rewritten = dict(inputs)
    for key in ("path", "file", "target", "module"):
        value = rewritten.get(key)
        if isinstance(value, str):
            rewritten[key] = _rewrite_path_for_worktree(
                value,
                repo_root=repo_root,
                worktree_root=worktree_root,
            )
    rewritten["workdir"] = str(worktree_root)
    return rewritten


def _verifier_target(inputs: Mapping[str, Any], *, fallback_ref: str) -> tuple[str, str]:
    for key in ("target_ref", "path", "file", "target", "module"):
        value = inputs.get(key)
        if isinstance(value, str) and value.strip():
            return ("path" if key != "target_ref" else "candidate", value.strip())
    return "candidate", fallback_ref


def _run_candidate_verifier(
    *,
    conn: Any,
    candidate: Mapping[str, Any],
    inputs: Mapping[str, Any],
    fallback_ref: str,
) -> dict[str, Any]:
    target_kind, target_ref = _verifier_target(inputs, fallback_ref=fallback_ref)
    return run_registered_verifier(
        str(candidate["verifier_ref"]),
        inputs=dict(inputs),
        target_kind=target_kind,
        target_ref=target_ref,
        conn=conn,
        record_run=True,
        promote_bug=False,
    )


def _worktree_patch_projection(
    *,
    repo_root: Path,
    base_head_ref: str,
    candidate: Mapping[str, Any],
) -> tuple[CandidatePatchProjection, Path, tempfile.TemporaryDirectory[str]]:
    temp = tempfile.TemporaryDirectory(prefix="praxis-candidate-")
    temp_path = Path(temp.name)
    try:
        temp_path.rmdir()
        _run_git(repo_root, ["worktree", "add", "--detach", str(temp_path), base_head_ref])
        proposal = _json_object(candidate.get("proposal_payload"))
        intended_files = proposal.get("intended_files") or candidate.get("intended_files") or []
        context = source_context_from_worktree(
            worktree_root=temp_path,
            intended_files=[str(path) for path in intended_files],
        )
        projection = derive_candidate_patch_from_sources(
            proposal_payload=proposal,
            source_context_refs=context,
        )
        return projection, temp_path, temp
    except Exception:
        temp.cleanup()
        raise


def _cleanup_worktree(repo_root: Path, temp: tempfile.TemporaryDirectory[str], temp_path: Path) -> None:
    try:
        _run_git(repo_root, ["worktree", "remove", "--force", str(temp_path)])
    except Exception:
        pass
    temp.cleanup()


def _apply_projection_to_worktree(temp_path: Path, projection: CandidatePatchProjection) -> None:
    for path, content in projection.new_contents.items():
        target = (temp_path / path).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def _insert_candidate_gate(
    conn: Any,
    *,
    candidate: Mapping[str, Any],
    current_head: str,
    verification_run_id: str,
) -> Any:
    submission_projection = _candidate_submission_projection(candidate)
    manifest_hash = _submission_manifest_hash(submission_projection)
    gate_evaluation = gate_policy.evaluate_gate(
        proposal_id=f"proposal:{candidate['submission_id']}",
        workflow_id=str(candidate["workflow_id"]),
        run_id=str(candidate["run_id"]),
        validation_receipt_ref=verification_run_id,
        proposal_manifest_hash=manifest_hash,
        validated_head_ref=str(candidate["base_head_ref"]),
        target_kind=gate_policy.CANONICAL_TARGET_KIND,
        target_ref=DEFAULT_TARGET_REF,
        policy_snapshot_ref=DEFAULT_POLICY_SNAPSHOT_REF,
        decided_by="code_change_candidate.materialize",
        validation_passed=True,
        proposal_receipt_present=True,
        validated_manifest_hash=manifest_hash,
        current_head_ref=current_head,
    )
    _insert_gate_evaluation(conn, gate_evaluation=gate_evaluation)
    return gate_evaluation


def _insert_candidate_promotion(
    conn: Any,
    *,
    gate_evaluation: Any,
    current_head: str,
    canonical_commit_ref: str,
    materialized_by: str,
) -> Any:
    now = datetime.now(timezone.utc)
    promotion_decision = gate_policy.decide_promotion(
        gate_evaluation=gate_evaluation,
        policy_snapshot_ref=DEFAULT_POLICY_SNAPSHOT_REF,
        decided_by=materialized_by,
        current_head_ref=current_head,
        proposal_manifest_hash=gate_evaluation.proposal_manifest_hash,
        validation_receipt_ref=gate_evaluation.validation_receipt_ref,
        target_kind=gate_evaluation.target_kind,
        target_ref=gate_evaluation.target_ref,
        promotion_intent_at=now,
        finalized_at=now,
        canonical_commit_ref=canonical_commit_ref,
    )
    _insert_promotion_decision(conn, promotion_decision=promotion_decision)
    return promotion_decision


def _rollback_live_apply(repo_root: Path, unified_diff: str) -> None:
    try:
        _run_git(repo_root, ["apply", "-R", "--check"], input_text=unified_diff)
        _run_git(repo_root, ["apply", "-R"], input_text=unified_diff)
    except Exception:
        pass


def materialize_candidate(
    conn: Any,
    *,
    candidate_id: str,
    materialized_by: str,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    """Run the V0 materialization saga for one code-change candidate."""

    root = Path(repo_root) if repo_root is not None else default_repo_root()
    root = root.resolve()
    candidate = _candidate_row(conn, candidate_id=candidate_id)
    if candidate["materialization_status"] == "materialized":
        return {"ok": True, "status": "already_materialized", "candidate": candidate}

    latest_review = _latest_review(conn, submission_id=str(candidate["submission_id"]))
    try:
        review_approved = _assert_review_preconditions(candidate, latest_review)
        intended_files = [str(path) for path in candidate.get("intended_files") or []]
        dirty_paths = _dirty_intended_paths(root, intended_files)
        if dirty_paths:
            raise CandidateMaterializationError(
                "code_change_candidate.live_paths_dirty",
                "live source paths for this candidate have uncommitted changes",
                details={"dirty_paths": dirty_paths},
            )
        current_head = current_head_ref(root)
        if current_head != str(candidate["base_head_ref"]):
            _set_candidate_state(
                conn,
                candidate_id=candidate_id,
                materialization_status="blocked_stale_head",
                next_actor_kind="human",
                last_error={
                    "reason_code": "code_change_candidate.stale_head",
                    "base_head_ref": candidate["base_head_ref"],
                    "current_head_ref": current_head,
                },
            )
            return {
                "ok": False,
                "status": "blocked_stale_head",
                "candidate_id": candidate_id,
                "base_head_ref": candidate["base_head_ref"],
                "current_head_ref": current_head,
            }

        _set_candidate_state(
            conn,
            candidate_id=candidate_id,
            materialization_status="in_progress",
            next_actor_kind="system",
            last_error={},
        )
        projection, temp_path, temp = _worktree_patch_projection(
            repo_root=root,
            base_head_ref=str(candidate["base_head_ref"]),
            candidate=candidate,
        )
    except CandidateAuthoringError as exc:
        _set_candidate_state(
            conn,
            candidate_id=candidate_id,
            materialization_status="aborted",
            next_actor_kind="human",
            last_error={"reason_code": exc.reason_code, "message": str(exc), "details": exc.details},
        )
        return {"ok": False, "status": "aborted", "reason_code": exc.reason_code, "details": exc.details}
    except CandidateMaterializationError as exc:
        _set_candidate_state(
            conn,
            candidate_id=candidate_id,
            materialization_status="aborted",
            next_actor_kind="human",
            last_error={"reason_code": exc.reason_code, "message": str(exc), "details": exc.details},
        )
        return {"ok": False, "status": "aborted", "reason_code": exc.reason_code, "details": exc.details}

    try:
        _apply_projection_to_worktree(temp_path, projection)
        patch_artifact_ref = _capture_patch_artifact(conn, candidate_id=candidate_id, projection=projection)
        _set_candidate_state(
            conn,
            candidate_id=candidate_id,
            patch_artifact_ref=patch_artifact_ref,
            patch_sha256=projection.patch_sha256,
        )

        temp_inputs = _verifier_inputs_for_worktree(
            _json_object(candidate.get("verifier_inputs")),
            repo_root=root,
            worktree_root=temp_path,
        )
        temp_verification = _run_candidate_verifier(
            conn=conn,
            candidate=candidate,
            inputs=temp_inputs,
            fallback_ref=str(candidate_id),
        )
        temp_verification_run_id = str(temp_verification.get("verification_run_id") or "")
        _set_candidate_state(
            conn,
            candidate_id=candidate_id,
            temp_verifier_run_id=temp_verification_run_id or None,
        )
        if temp_verification.get("status") != "passed":
            _set_candidate_state(
                conn,
                candidate_id=candidate_id,
                materialization_status="blocked_verifier_failed",
                next_actor_kind="human",
                last_error={
                    "reason_code": "code_change_candidate.temp_verifier_failed",
                    "verification": temp_verification,
                },
            )
            _set_submission_acceptance(
                conn,
                submission_id=str(candidate["submission_id"]),
                acceptance_status="pending_review",
                acceptance_report={
                    "reason_code": "code_change_candidate.temp_verifier_failed",
                    "candidate_id": candidate_id,
                    "verification_run_id": temp_verification_run_id,
                },
            )
            return {
                "ok": False,
                "status": "blocked_verifier_failed",
                "candidate_id": candidate_id,
                "verification": temp_verification,
            }

        if not review_approved:
            _set_candidate_state(
                conn,
                candidate_id=candidate_id,
                materialization_status="pending",
                next_actor_kind="human",
                last_error={},
            )
            _set_submission_acceptance(
                conn,
                submission_id=str(candidate["submission_id"]),
                acceptance_status="pending_review",
                acceptance_report={
                    "reason_code": "code_change_candidate.review_required",
                    "candidate_id": candidate_id,
                    "temp_verification_run_id": temp_verification_run_id,
                    "patch_artifact_ref": patch_artifact_ref,
                    "patch_sha256": projection.patch_sha256,
                },
            )
            return {
                "ok": False,
                "status": "awaiting_human_review",
                "candidate_id": candidate_id,
                "patch_artifact_ref": patch_artifact_ref,
                "patch_sha256": projection.patch_sha256,
                "temp_verification": temp_verification,
            }

        gate_evaluation = _insert_candidate_gate(
            conn,
            candidate=candidate,
            current_head=current_head,
            verification_run_id=temp_verification_run_id,
        )
        if gate_evaluation.decision.value != "accept":
            status = "blocked_stale_head" if gate_evaluation.reason_code == gate_policy.GATE_REJECT_HEAD_MISMATCH else "aborted"
            _set_candidate_state(
                conn,
                candidate_id=candidate_id,
                materialization_status=status,
                next_actor_kind="human",
                gate_evaluation_id=gate_evaluation.gate_evaluation_id,
                last_error={
                    "reason_code": gate_evaluation.reason_code,
                    "gate_evaluation": asdict(gate_evaluation),
                },
            )
            return {
                "ok": False,
                "status": status,
                "candidate_id": candidate_id,
                "gate_evaluation": asdict(gate_evaluation),
            }

        _assert_index_clean(root)
        _run_git(root, ["apply", "--check"], input_text=projection.unified_diff)
        _run_git(root, ["apply"], input_text=projection.unified_diff)

        final_inputs = dict(_json_object(candidate.get("verifier_inputs")))
        final_inputs["workdir"] = str(root)
        final_verification = _run_candidate_verifier(
            conn=conn,
            candidate=candidate,
            inputs=final_inputs,
            fallback_ref=str(candidate_id),
        )
        final_verification_run_id = str(final_verification.get("verification_run_id") or "")
        if final_verification.get("status") != "passed":
            _rollback_live_apply(root, projection.unified_diff)
            _set_candidate_state(
                conn,
                candidate_id=candidate_id,
                materialization_status="aborted",
                next_actor_kind="human",
                final_verifier_run_id=final_verification_run_id or None,
                gate_evaluation_id=gate_evaluation.gate_evaluation_id,
                last_error={
                    "reason_code": "code_change_candidate.final_verifier_failed",
                    "verification": final_verification,
                },
            )
            return {
                "ok": False,
                "status": "aborted",
                "candidate_id": candidate_id,
                "verification": final_verification,
            }

        try:
            canonical_commit_ref = _commit_live_apply(
                root,
                candidate_id=candidate_id,
                intended_files=intended_files,
                materialized_by=materialized_by,
            )
        except CandidateMaterializationError:
            _rollback_live_apply(root, projection.unified_diff)
            raise

        promotion_decision = _insert_candidate_promotion(
            conn,
            gate_evaluation=gate_evaluation,
            current_head=current_head,
            canonical_commit_ref=canonical_commit_ref,
            materialized_by=materialized_by,
        )
        PostgresBugEvidenceRepository(conn).upsert_bug_evidence_link(
            bug_id=str(candidate["bug_id"]),
            evidence_kind=EVIDENCE_KIND_VERIFICATION_RUN,
            evidence_ref=final_verification_run_id,
            evidence_role=EVIDENCE_ROLE_VALIDATES_FIX,
            created_by=materialized_by,
            notes="Final verifier run for materialized code-change candidate.",
        )
        resolved_bug = BugTracker(conn).resolve(
            str(candidate["bug_id"]),
            BugStatus.FIXED,
            resolution_summary=(
                f"Code-change candidate {candidate_id} materialized with "
                f"verification run {final_verification_run_id}."
            ),
        )
        _set_candidate_state(
            conn,
            candidate_id=candidate_id,
            materialization_status="materialized",
            next_actor_kind="none",
            final_verifier_run_id=final_verification_run_id,
            gate_evaluation_id=gate_evaluation.gate_evaluation_id,
            promotion_decision_id=promotion_decision.promotion_decision_id,
            last_error={},
        )
        _set_submission_acceptance(
            conn,
            submission_id=str(candidate["submission_id"]),
            acceptance_status="materialized",
            acceptance_report={
                "candidate_id": candidate_id,
                "gate_evaluation_id": gate_evaluation.gate_evaluation_id,
                "promotion_decision_id": promotion_decision.promotion_decision_id,
                "final_verification_run_id": final_verification_run_id,
                "canonical_commit_ref": canonical_commit_ref,
            },
        )

        supersession_rows_written = _populate_authority_supersession_registry(
            conn,
            candidate_id=candidate_id,
            promotion_decision_id=promotion_decision.promotion_decision_id,
            materialized_by=materialized_by,
        )

        return {
            "ok": True,
            "status": "materialized",
            "candidate_id": candidate_id,
            "patch_artifact_ref": patch_artifact_ref,
            "patch_sha256": projection.patch_sha256,
            "canonical_commit_ref": canonical_commit_ref,
            "gate_evaluation": asdict(gate_evaluation),
            "promotion_decision": asdict(promotion_decision),
            "final_verification": final_verification,
            "bug": None if resolved_bug is None else {"bug_id": resolved_bug.bug_id, "status": resolved_bug.status.value},
            "supersession_rows_written": supersession_rows_written,
            "event_payload": {
                "candidate_id": candidate_id,
                "submission_id": candidate["submission_id"],
                "bug_id": candidate["bug_id"],
                "patch_sha256": projection.patch_sha256,
                "canonical_commit_ref": canonical_commit_ref,
                "final_verification_run_id": final_verification_run_id,
                "promotion_decision_id": promotion_decision.promotion_decision_id,
                "supersession_rows_written": supersession_rows_written,
            },
        }
    except CandidateMaterializationError as exc:
        _set_candidate_state(
            conn,
            candidate_id=candidate_id,
            materialization_status="aborted",
            next_actor_kind="human",
            last_error={"reason_code": exc.reason_code, "message": str(exc), "details": exc.details},
        )
        return {"ok": False, "status": "aborted", "reason_code": exc.reason_code, "details": exc.details}
    finally:
        _cleanup_worktree(root, temp, temp_path)


__all__ = [
    "CandidateMaterializationError",
    "current_head_ref",
    "materialize_candidate",
]
