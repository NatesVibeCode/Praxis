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
from hashlib import sha256
import json
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
        authority_context=submission.authority_context,
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


def _canonical_hash(payload: Mapping[str, Any]) -> str:
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(payload_json.encode("utf-8")).hexdigest()


async def _persist_submission_context_bundle(
    conn: _RepairConnection,
    submission: WorkflowAdmissionSubmission,
) -> None:
    bundle = submission.authority_context
    if bundle is None:
        return
    bundle_payload = getattr(bundle, "bundle_payload", None)
    if not isinstance(bundle_payload, Mapping):
        return
    runtime_profile_payload = bundle_payload.get("runtime_profile")
    workspace_payload = bundle_payload.get("workspace")
    if not isinstance(runtime_profile_payload, Mapping) or not isinstance(workspace_payload, Mapping):
        return
    context_bundle_id = str(getattr(bundle, "context_bundle_id", "") or "").strip()
    if not context_bundle_id:
        return
    source_decision_refs = list(getattr(bundle, "source_decision_refs", ()) or ())
    resolved_at = getattr(bundle, "resolved_at", None) or submission.decision.decided_at
    sandbox_profile_ref = str(
        getattr(bundle, "sandbox_profile_ref", None)
        or runtime_profile_payload.get("sandbox_profile_ref")
        or getattr(bundle, "runtime_profile_ref", "")
    )
    await conn.execute(
        """
        INSERT INTO context_bundles (
            context_bundle_id,
            workflow_id,
            run_id,
            workspace_ref,
            runtime_profile_ref,
            model_profile_id,
            provider_policy_id,
            bundle_version,
            bundle_hash,
            bundle_payload,
            source_decision_refs,
            resolved_at,
            sandbox_profile_ref
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12, $13)
        ON CONFLICT (context_bundle_id) DO NOTHING
        """,
        context_bundle_id,
        str(getattr(bundle, "workflow_id", "") or submission.run.workflow_id),
        str(getattr(bundle, "run_id", "") or submission.run.run_id),
        str(getattr(bundle, "workspace_ref", "") or workspace_payload.get("workspace_ref") or ""),
        str(getattr(bundle, "runtime_profile_ref", "") or runtime_profile_payload.get("runtime_profile_ref") or ""),
        str(getattr(bundle, "model_profile_id", "") or runtime_profile_payload.get("model_profile_id") or ""),
        str(getattr(bundle, "provider_policy_id", "") or runtime_profile_payload.get("provider_policy_id") or ""),
        int(getattr(bundle, "bundle_version", 1) or 1),
        str(getattr(bundle, "bundle_hash", "") or submission.run.authority_context_digest),
        json.dumps(dict(bundle_payload), sort_keys=True, default=str),
        json.dumps(source_decision_refs, sort_keys=True, default=str),
        resolved_at,
        sandbox_profile_ref,
    )
    anchors = (
        (
            "registry_workspace_authority",
            str(getattr(bundle, "workspace_ref", "") or workspace_payload.get("workspace_ref") or ""),
            dict(workspace_payload),
            0,
        ),
        (
            "registry_runtime_profile_authority",
            str(getattr(bundle, "runtime_profile_ref", "") or runtime_profile_payload.get("runtime_profile_ref") or ""),
            dict(runtime_profile_payload),
            1,
        ),
    )
    for anchor_kind, anchor_ref, anchor_payload, position_index in anchors:
        if not anchor_ref:
            continue
        await conn.execute(
            """
            INSERT INTO context_bundle_anchors (
                context_bundle_anchor_id,
                context_bundle_id,
                anchor_ref,
                anchor_kind,
                content_hash,
                anchor_payload,
                position_index,
                anchored_at
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
            ON CONFLICT (context_bundle_id, anchor_kind, anchor_ref) DO NOTHING
            """,
            f"context_bundle_anchor:{context_bundle_id}:{anchor_kind}:{anchor_ref}",
            context_bundle_id,
            anchor_ref,
            anchor_kind,
            _canonical_hash(anchor_payload),
            json.dumps(anchor_payload, sort_keys=True, default=str),
            position_index,
            resolved_at,
        )


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

        await _persist_submission_context_bundle(conn, submission)
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
