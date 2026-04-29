"""Gateway command for submitting structured code-change candidates."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class SubmitCodeChangeCandidate(BaseModel):
    """Input for `code_change_candidate.submit`."""

    run_id: str = Field(..., min_length=1)
    workflow_id: str = Field(..., min_length=1)
    job_label: str = Field(..., min_length=1)
    bug_id: str = Field(..., min_length=1)
    proposal_payload: dict[str, Any]
    source_context_refs: dict[str, Any] | list[dict[str, Any]]
    base_head_ref: str | None = None
    review_routing: str = "human_review"
    verifier_ref: str | None = None
    verifier_inputs: dict[str, Any] | None = None
    summary: str | None = None
    notes: str | None = None
    routing_decision_record: dict[str, Any] | None = None

    @field_validator("run_id", "workflow_id", "job_label", "bug_id", "review_routing", mode="before")
    @classmethod
    def _strip_required_text(cls, value: object) -> str:
        return str(value or "").strip()

    @field_validator("base_head_ref", "verifier_ref", "summary", "notes", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @field_validator("review_routing")
    @classmethod
    def _check_review_routing(cls, value: str) -> str:
        if value not in {"auto_apply", "human_review"}:
            raise ValueError("review_routing must be auto_apply or human_review in V0")
        return value

    @model_validator(mode="after")
    def _check_source_context_refs(self) -> "SubmitCodeChangeCandidate":
        if not self.source_context_refs:
            raise ValueError("source_context_refs is required")
        return self


def handle_submit_candidate(command: SubmitCodeChangeCandidate, subsystems: Any) -> dict[str, Any]:
    """Seal a candidate through the existing workflow submission service."""

    from runtime.workflow.submission_capture import submit_code_change_candidate

    payload = submit_code_change_candidate(
        run_id=command.run_id,
        workflow_id=command.workflow_id,
        job_label=command.job_label,
        bug_id=command.bug_id,
        proposal_payload=command.proposal_payload,
        source_context_refs=command.source_context_refs,
        base_head_ref=command.base_head_ref,
        review_routing=command.review_routing,
        verifier_ref=command.verifier_ref,
        verifier_inputs=command.verifier_inputs,
        summary=command.summary,
        notes=command.notes,
        routing_decision_record=command.routing_decision_record,
        conn=subsystems.get_pg_conn(),
    )
    candidate = payload.get("code_change_candidate") if isinstance(payload, dict) else {}
    return {
        "ok": True,
        "submission": payload,
        "candidate": candidate,
        "event_payload": {
            "candidate_id": (candidate or {}).get("candidate_id"),
            "submission_id": payload.get("submission_id") if isinstance(payload, dict) else None,
            "bug_id": command.bug_id,
            "review_routing": command.review_routing,
        },
    }


__all__ = [
    "SubmitCodeChangeCandidate",
    "handle_submit_candidate",
]
