"""Legacy submit/evidence drift repair for native frontdoor admission.

This module quarantines the compatibility path for runs that have one of these
broken shapes:

- a control-plane run row with no canonical evidence
- a run that only has the initial claim_received evidence bundle

Steady-state callers should treat this as a bounded repair authority, not as
the normal submit contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Protocol

from runtime.admission_evidence import (
    AdmissionEvidenceRecord,
    append_admission_transition_async,
    persist_submission_evidence_async,
)
from runtime.domain import RouteIdentity, RunState
from runtime.persistent_evidence import PostgresEvidenceWriter
from storage.postgres import (
    PostgresWriteError,
    WorkflowAdmissionSubmission,
    WorkflowAdmissionWriteResult,
    persist_workflow_admission,
)
from surfaces.api._frontdoor_serialize import _json_loads_maybe

_SUBMIT_RUN_QUERY = """
SELECT
    run_id,
    workflow_id,
    request_id,
    request_digest,
    workflow_definition_id,
    admitted_definition_hash,
    current_state,
    run_idempotency_key,
    context_bundle_id,
    authority_context_digest,
    admission_decision_id,
    request_envelope
FROM workflow_runs
WHERE run_id = $1
"""


class _RepairConnection(Protocol):
    async def fetchrow(self, query: str, *args: object) -> Any:
        """Return one row."""

    async def execute(self, query: str, *args: object) -> Any:
        """Execute one statement."""

    def transaction(self) -> Any:
        """Open a transaction context manager."""


def _route_identity_from_submission(
    submission: WorkflowAdmissionSubmission,
) -> RouteIdentity:
    run = submission.run
    return RouteIdentity(
        workflow_id=run.workflow_id,
        run_id=run.run_id,
        request_id=run.request_id,
        authority_context_ref=submission.decision.authority_context_ref,
        authority_context_digest=run.authority_context_digest,
        claim_id=f"claim:{run.run_id}",
        lease_id=None,
        proposal_id=None,
        promotion_decision_id=None,
        attempt_no=1,
        transition_seq=0,
    )


def _admission_evidence_from_submission(
    submission: WorkflowAdmissionSubmission,
) -> AdmissionEvidenceRecord:
    return AdmissionEvidenceRecord(
        route_identity=_route_identity_from_submission(submission),
        request_payload=dict(submission.run.request_envelope),
        admitted_definition_ref=submission.run.workflow_definition_id,
        admitted_definition_hash=submission.run.admitted_definition_hash,
        current_state=RunState(submission.run.current_state),
        reason_code=submission.decision.reason_code,
        decided_at=submission.decision.decided_at,
        validation_result_ref=submission.decision.validation_result_ref,
        authority_context_ref=submission.decision.authority_context_ref,
        admission_decision_id=submission.decision.admission_decision_id,
        request_digest=submission.run.request_digest,
    )


def _bootstrap_submission(
    submission: WorkflowAdmissionSubmission,
) -> WorkflowAdmissionSubmission:
    return WorkflowAdmissionSubmission(
        decision=submission.decision,
        run=replace(
            submission.run,
            current_state=RunState.CLAIM_RECEIVED.value,
            terminal_reason_code=None,
            started_at=None,
            finished_at=None,
            last_event_id=None,
        ),
    )


def _row_request_envelope(row: Mapping[str, Any]) -> dict[str, Any]:
    envelope = _json_loads_maybe(row.get("request_envelope"), {})
    if not isinstance(envelope, Mapping):
        return {}
    return dict(envelope)


def _run_row_matches_submission(
    row: Mapping[str, Any],
    submission: WorkflowAdmissionSubmission,
    *,
    expected_state: str,
) -> bool:
    run = submission.run
    return (
        row.get("run_id") == run.run_id
        and row.get("workflow_id") == run.workflow_id
        and row.get("request_id") == run.request_id
        and row.get("request_digest") == run.request_digest
        and row.get("workflow_definition_id") == run.workflow_definition_id
        and row.get("admitted_definition_hash") == run.admitted_definition_hash
        and row.get("run_idempotency_key") == run.run_idempotency_key
        and row.get("context_bundle_id") == run.context_bundle_id
        and row.get("authority_context_digest") == run.authority_context_digest
        and row.get("admission_decision_id") == run.admission_decision_id
        and row.get("current_state") == expected_state
        and _row_request_envelope(row) == dict(run.request_envelope)
    )


def _timeline_state(timeline: tuple[Any, ...]) -> str | None:
    if not timeline:
        return None
    last_row = timeline[-1]
    record = getattr(last_row, "record", None)
    status = getattr(record, "status", None)
    if isinstance(status, str) and status:
        return status
    payload = getattr(record, "payload", None)
    if isinstance(payload, Mapping):
        to_state = payload.get("to_state")
        if isinstance(to_state, str) and to_state:
            return to_state
    return None


async def repair_or_seed_submission_evidence(
    conn: _RepairConnection,
    *,
    submission: WorkflowAdmissionSubmission,
) -> WorkflowAdmissionWriteResult:
    """Repair legacy submit/evidence drift or seed fresh canonical evidence."""

    admission = _admission_evidence_from_submission(submission)
    bootstrap_submission = _bootstrap_submission(submission)

    async with conn.transaction():
        writer = PostgresEvidenceWriter(conn=conn)
        existing_row = await conn.fetchrow(_SUBMIT_RUN_QUERY, submission.run.run_id)
        timeline = tuple(
            await writer.evidence_timeline_async(admission.route_identity.run_id)
        )

        if existing_row is not None and timeline:
            if (
                _run_row_matches_submission(
                    existing_row,
                    submission,
                    expected_state=submission.run.current_state,
                )
                and _timeline_state(timeline) == submission.run.current_state
            ):
                return WorkflowAdmissionWriteResult(
                    admission_decision_id=submission.decision.admission_decision_id,
                    run_id=submission.run.run_id,
                )
            if _timeline_state(timeline) != RunState.CLAIM_RECEIVED.value:
                raise PostgresWriteError(
                    "postgres.duplicate_submission_conflict",
                    "workflow run already exists with conflicting evidence state",
                    details={
                        "run_id": submission.run.run_id,
                        "current_state": existing_row.get("current_state"),
                        "evidence_state": _timeline_state(timeline),
                    },
                )

        if existing_row is None:
            await persist_workflow_admission(conn, submission=bootstrap_submission)
        elif not (
            _run_row_matches_submission(
                existing_row,
                submission,
                expected_state=submission.run.current_state,
            )
            or _run_row_matches_submission(
                existing_row,
                bootstrap_submission,
                expected_state=RunState.CLAIM_RECEIVED.value,
            )
        ):
            raise PostgresWriteError(
                "postgres.duplicate_submission_conflict",
                "workflow run already exists with different canonical content",
                details={"run_id": submission.run.run_id},
            )

        if not timeline:
            await conn.execute(
                """
                UPDATE workflow_runs
                SET current_state = $2,
                    terminal_reason_code = NULL,
                    started_at = NULL,
                    finished_at = NULL,
                    last_event_id = NULL
                WHERE run_id = $1
                """,
                submission.run.run_id,
                RunState.CLAIM_RECEIVED.value,
            )
            submission_result = await persist_submission_evidence_async(
                writer,
                admission=admission,
            )
            submission_evidence_seq = submission_result.evidence_seq
        else:
            submission_evidence_seq = timeline[-1].evidence_seq

        if _timeline_state(timeline) != submission.run.current_state:
            await conn.execute(
                """
                UPDATE workflow_runs
                SET current_state = $2,
                    terminal_reason_code = NULL,
                    started_at = NULL,
                    finished_at = NULL
                WHERE run_id = $1
                """,
                submission.run.run_id,
                RunState.CLAIM_RECEIVED.value,
            )
            await append_admission_transition_async(
                writer,
                admission=admission,
                submission_evidence_seq=submission_evidence_seq,
            )

    return WorkflowAdmissionWriteResult(
        admission_decision_id=submission.decision.admission_decision_id,
        run_id=submission.run.run_id,
    )


__all__ = ["repair_or_seed_submission_evidence"]
