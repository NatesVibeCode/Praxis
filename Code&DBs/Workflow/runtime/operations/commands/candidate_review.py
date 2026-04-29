"""Gateway command for reviewing code-change candidates."""

from __future__ import annotations

from typing import Any, Literal
import json

from pydantic import BaseModel, Field, field_validator

from storage.postgres.workflow_submission_repository import (
    PostgresWorkflowSubmissionRepository,
    WorkflowSubmissionRepositoryError,
)


class ReviewCodeChangeCandidate(BaseModel):
    """Input for `code_change_candidate.review`."""

    candidate_id: str = Field(..., min_length=1)
    reviewer_ref: str = Field(..., min_length=1)
    decision: Literal["approve", "reject", "request_changes"]
    reasons: list[str] = Field(default_factory=list)
    override_reasons: list[str] = Field(default_factory=list)
    summary: str | None = None
    evidence_refs: list[str] = Field(default_factory=list)

    @field_validator("decision", mode="before")
    @classmethod
    def _normalize_decision(cls, value: object) -> str:
        return str(value or "").strip().lower()

    @field_validator("reasons", "override_reasons", "evidence_refs", mode="before")
    @classmethod
    def _normalize_string_list(cls, value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            text = value.strip()
            return [text] if text else []
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]


def _candidate(conn: Any, *, candidate_id: str) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT c.candidate_id::text AS candidate_id,
               c.submission_id,
               c.bug_id,
               c.review_routing,
               c.materialization_status,
               s.run_id,
               s.workflow_id,
               s.acceptance_status
          FROM code_change_candidate_payloads c
          JOIN workflow_job_submissions s
            ON s.submission_id = c.submission_id
         WHERE c.candidate_id = $1::uuid
        """,
        candidate_id,
    )
    return None if row is None else dict(row)


def _reviewer_role(reviewer_ref: str) -> str:
    normalized = reviewer_ref.strip().lower()
    if normalized.startswith("human:") or normalized in {"human", "operator"}:
        return "human"
    if normalized.startswith("llm:") or normalized.startswith("agent:"):
        return "llm_review"
    if normalized.startswith("system:") or normalized == "system":
        return "system"
    return "review"


def _review_summary(command: ReviewCodeChangeCandidate) -> str:
    if command.summary and command.summary.strip():
        return command.summary.strip()
    if command.reasons:
        return "; ".join(command.reasons)[:500]
    return f"candidate review decision: {command.decision}"


def _update_projection(
    conn: Any,
    *,
    candidate: dict[str, Any],
    review: dict[str, Any],
    command: ReviewCodeChangeCandidate,
) -> None:
    if command.decision == "approve":
        acceptance_status = "accepted"
        next_actor_kind = "system"
        materialization_status = "pending"
    elif command.decision == "request_changes":
        acceptance_status = "needs_revision"
        next_actor_kind = "human"
        materialization_status = "needs_revision"
    else:
        acceptance_status = "rejected"
        next_actor_kind = "none"
        materialization_status = str(candidate.get("materialization_status") or "pending")

    acceptance_report = {
        "canonical_review_id": review["review_id"],
        "latest_decision": command.decision,
        "reviewer_ref": command.reviewer_ref,
        "reasons": list(command.reasons),
        "override_reasons": list(command.override_reasons),
    }
    conn.execute(
        """
        UPDATE workflow_job_submissions
           SET acceptance_status = $2,
               acceptance_report = $3::jsonb
         WHERE submission_id = $1
        """,
        candidate["submission_id"],
        acceptance_status,
        json.dumps(acceptance_report, sort_keys=True, default=str),
    )
    conn.execute(
        """
        UPDATE code_change_candidate_payloads
           SET next_actor_kind = $2,
               materialization_status = $3,
               updated_at = now()
         WHERE candidate_id = $1::uuid
        """,
        command.candidate_id,
        next_actor_kind,
        materialization_status,
    )


def handle_review_candidate(
    command: ReviewCodeChangeCandidate,
    subsystems: Any,
) -> dict[str, Any]:
    """Append a canonical review row for a code-change candidate."""

    conn = subsystems.get_pg_conn()
    candidate = _candidate(conn, candidate_id=command.candidate_id)
    if candidate is None:
        return {
            "ok": False,
            "reason_code": "code_change_candidate.not_found",
            "error": "candidate_id did not resolve to a code-change candidate",
            "candidate_id": command.candidate_id,
        }

    repository = PostgresWorkflowSubmissionRepository(conn)
    try:
        review = repository.record_review(
            submission_id=str(candidate["submission_id"]),
            run_id=str(candidate["run_id"]),
            workflow_id=str(candidate["workflow_id"]),
            reviewer_job_label=command.reviewer_ref,
            reviewer_role=_reviewer_role(command.reviewer_ref),
            decision=command.decision,
            summary=_review_summary(command),
            notes=json.dumps(
                {
                    "reasons": command.reasons,
                    "override_reasons": command.override_reasons,
                },
                sort_keys=True,
                default=str,
            ),
            evidence_refs=command.evidence_refs,
        )
    except WorkflowSubmissionRepositoryError as exc:
        return {
            "ok": False,
            "reason_code": exc.reason_code,
            "error": str(exc),
            "details": getattr(exc, "details", {}),
        }

    _update_projection(conn, candidate=candidate, review=review, command=command)
    return {
        "ok": True,
        "candidate_id": command.candidate_id,
        "submission_id": candidate["submission_id"],
        "review": review,
        "event_payload": {
            "candidate_id": command.candidate_id,
            "submission_id": candidate["submission_id"],
            "bug_id": candidate["bug_id"],
            "review_id": review["review_id"],
            "decision": command.decision,
            "reviewer_ref": command.reviewer_ref,
        },
    }


__all__ = [
    "ReviewCodeChangeCandidate",
    "handle_review_candidate",
]
