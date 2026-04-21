"""Workflow admission persistence for the Postgres control plane."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _require_mapping,
    _require_text,
    _require_utc,
)

_VALID_ADMISSION_DECISIONS = {"admit", "reject"}


@dataclass(frozen=True, slots=True)
class WorkflowAdmissionDecisionWrite:
    """Explicit admission decision row for the control plane."""

    admission_decision_id: str
    workflow_id: str
    request_id: str
    decision: str
    reason_code: str
    decided_at: datetime
    decided_by: str
    policy_snapshot_ref: str
    validation_result_ref: str
    authority_context_ref: str


@dataclass(frozen=True, slots=True)
class WorkflowRunWrite:
    """Explicit workflow run row for the control plane."""

    run_id: str
    workflow_id: str
    request_id: str
    request_digest: str
    authority_context_digest: str
    workflow_definition_id: str
    admitted_definition_hash: str
    run_idempotency_key: str
    schema_version: int
    request_envelope: Mapping[str, Any]
    context_bundle_id: str
    admission_decision_id: str
    current_state: str
    requested_at: datetime
    admitted_at: datetime
    terminal_reason_code: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_event_id: str | None = None


@dataclass(frozen=True, slots=True)
class WorkflowAdmissionSubmission:
    """Atomic control-plane submission for one admitted or rejected run."""

    decision: WorkflowAdmissionDecisionWrite
    run: WorkflowRunWrite


@dataclass(frozen=True, slots=True)
class WorkflowAdmissionWriteResult:
    """Minimal durable result for one control-plane write."""

    admission_decision_id: str
    run_id: str


def _validate_submission(submission: WorkflowAdmissionSubmission) -> None:
    decision = submission.decision
    run = submission.run

    _require_text(decision.admission_decision_id, field_name="decision.admission_decision_id")
    _require_text(decision.workflow_id, field_name="decision.workflow_id")
    _require_text(decision.request_id, field_name="decision.request_id")
    _require_text(decision.reason_code, field_name="decision.reason_code")
    _require_text(decision.decided_by, field_name="decision.decided_by")
    _require_text(decision.policy_snapshot_ref, field_name="decision.policy_snapshot_ref")
    _require_text(decision.validation_result_ref, field_name="decision.validation_result_ref")
    _require_text(decision.authority_context_ref, field_name="decision.authority_context_ref")
    if decision.decision not in _VALID_ADMISSION_DECISIONS:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "decision.decision must be admit or reject",
            details={"field": "decision.decision", "value": decision.decision},
        )

    _require_text(run.run_id, field_name="run.run_id")
    _require_text(run.workflow_id, field_name="run.workflow_id")
    _require_text(run.request_id, field_name="run.request_id")
    _require_text(run.request_digest, field_name="run.request_digest")
    _require_text(run.authority_context_digest, field_name="run.authority_context_digest")
    _require_text(run.workflow_definition_id, field_name="run.workflow_definition_id")
    _require_text(run.admitted_definition_hash, field_name="run.admitted_definition_hash")
    _require_text(run.run_idempotency_key, field_name="run.run_idempotency_key")
    _require_text(run.context_bundle_id, field_name="run.context_bundle_id")
    _require_text(run.admission_decision_id, field_name="run.admission_decision_id")
    _require_text(run.current_state, field_name="run.current_state")
    if run.schema_version <= 0:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "run.schema_version must be a positive integer",
            details={"field": "run.schema_version"},
        )

    if decision.workflow_id != run.workflow_id or decision.request_id != run.request_id:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "decision.workflow_id and decision.request_id must match the run row",
            details={
                "decision.workflow_id": decision.workflow_id,
                "run.workflow_id": run.workflow_id,
                "decision.request_id": decision.request_id,
                "run.request_id": run.request_id,
            },
        )
    if decision.admission_decision_id != run.admission_decision_id:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "decision.admission_decision_id must match run.admission_decision_id",
            details={
                "decision.admission_decision_id": decision.admission_decision_id,
                "run.admission_decision_id": run.admission_decision_id,
            },
        )

    requested_at = _require_utc(run.requested_at, field_name="run.requested_at")
    admitted_at = _require_utc(run.admitted_at, field_name="run.admitted_at")
    if requested_at > admitted_at:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "run.requested_at must be before or equal to run.admitted_at",
            details={
                "requested_at": requested_at.isoformat(),
                "admitted_at": admitted_at.isoformat(),
            },
        )

    started_at = run.started_at
    if started_at is not None:
        started_at = _require_utc(started_at, field_name="run.started_at")
        if started_at < admitted_at:
            raise PostgresWriteError(
                "postgres.invalid_submission",
                "run.started_at must be on or after run.admitted_at",
                details={
                    "started_at": started_at.isoformat(),
                    "admitted_at": admitted_at.isoformat(),
                },
            )

    finished_at = run.finished_at
    if finished_at is not None:
        finished_at = _require_utc(finished_at, field_name="run.finished_at")
        if started_at is not None and finished_at < started_at:
            raise PostgresWriteError(
                "postgres.invalid_submission",
                "run.finished_at must be on or after run.started_at",
                details={
                    "finished_at": finished_at.isoformat(),
                    "started_at": started_at.isoformat(),
                },
            )


def _json_value(value: object) -> object:
    import json
    if isinstance(value, str):
        return json.loads(value)
    return value


def _admission_decision_matches(
    row: asyncpg.Record,
    *,
    decision: WorkflowAdmissionDecisionWrite,
) -> bool:
    return (
        row["admission_decision_id"] == decision.admission_decision_id
        and row["workflow_id"] == decision.workflow_id
        and row["request_id"] == decision.request_id
        and row["decision"] == decision.decision
        and row["reason_code"] == decision.reason_code
        and row["decided_by"] == decision.decided_by
        and row["policy_snapshot_ref"] == decision.policy_snapshot_ref
        and row["validation_result_ref"] == decision.validation_result_ref
        and row["authority_context_ref"] == decision.authority_context_ref
    )


async def _insert_or_assert_admission_decision(
    conn: asyncpg.Connection,
    *,
    decision: WorkflowAdmissionDecisionWrite,
) -> None:
    inserted_decision_id = await conn.fetchval(
        """
        INSERT INTO admission_decisions (
            admission_decision_id,
            workflow_id,
            request_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_result_ref,
            authority_context_ref
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (admission_decision_id) DO NOTHING
        RETURNING admission_decision_id
        """,
        decision.admission_decision_id,
        decision.workflow_id,
        decision.request_id,
        decision.decision,
        decision.reason_code,
        decision.decided_at,
        decision.decided_by,
        decision.policy_snapshot_ref,
        decision.validation_result_ref,
        decision.authority_context_ref,
    )
    if inserted_decision_id is not None:
        return

    existing_row = await conn.fetchrow(
        """
        SELECT
            admission_decision_id,
            workflow_id,
            request_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_result_ref,
            authority_context_ref
        FROM admission_decisions
        WHERE admission_decision_id = $1
        """,
        decision.admission_decision_id,
    )
    if existing_row is not None and _admission_decision_matches(
        existing_row,
        decision=decision,
    ):
        return

    raise PostgresWriteError(
        "postgres.duplicate_submission_conflict",
        "admission decision already exists with different canonical content",
        details={"admission_decision_id": decision.admission_decision_id},
    )


def _workflow_run_matches(
    row: asyncpg.Record,
    *,
    run: WorkflowRunWrite,
) -> bool:
    return (
        row["run_id"] == run.run_id
        and row["workflow_id"] == run.workflow_id
        and row["request_id"] == run.request_id
        and row["request_digest"] == run.request_digest
        and row["authority_context_digest"] == run.authority_context_digest
        and row["workflow_definition_id"] == run.workflow_definition_id
        and row["admitted_definition_hash"] == run.admitted_definition_hash
        and row["run_idempotency_key"] == run.run_idempotency_key
        and row["schema_version"] == run.schema_version
        and _json_value(row["request_envelope"]) == run.request_envelope
        and row["context_bundle_id"] == run.context_bundle_id
        and row["admission_decision_id"] == run.admission_decision_id
        and row["current_state"] == run.current_state
        and row["terminal_reason_code"] == run.terminal_reason_code
        and row["started_at"] == run.started_at
        and row["finished_at"] == run.finished_at
        and row["last_event_id"] == run.last_event_id
    )


async def _insert_or_assert_workflow_run(
    conn: asyncpg.Connection,
    *,
    run: WorkflowRunWrite,
    request_envelope: Mapping[str, Any],
) -> None:
    inserted_run_id = await conn.fetchval(
        """
        INSERT INTO workflow_runs (
            run_id,
            workflow_id,
            request_id,
            request_digest,
            authority_context_digest,
            workflow_definition_id,
            admitted_definition_hash,
            run_idempotency_key,
            schema_version,
            request_envelope,
            context_bundle_id,
            admission_decision_id,
            current_state,
            terminal_reason_code,
            requested_at,
            admitted_at,
            started_at,
            finished_at,
            last_event_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10::jsonb, $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        ON CONFLICT DO NOTHING
        RETURNING run_id
        """,
        run.run_id,
        run.workflow_id,
        run.request_id,
        run.request_digest,
        run.authority_context_digest,
        run.workflow_definition_id,
        run.admitted_definition_hash,
        run.run_idempotency_key,
        run.schema_version,
        _encode_jsonb(request_envelope, field_name="run.request_envelope"),
        run.context_bundle_id,
        run.admission_decision_id,
        run.current_state,
        run.terminal_reason_code,
        run.requested_at,
        run.admitted_at,
        run.started_at,
        run.finished_at,
        run.last_event_id,
    )
    if inserted_run_id is not None:
        return

    existing_row = await conn.fetchrow(
        """
        SELECT
            run_id,
            workflow_id,
            request_id,
            request_digest,
            authority_context_digest,
            workflow_definition_id,
            admitted_definition_hash,
            run_idempotency_key,
            schema_version,
            request_envelope,
            context_bundle_id,
            admission_decision_id,
            current_state,
            terminal_reason_code,
            requested_at,
            admitted_at,
            started_at,
            finished_at,
            last_event_id
        FROM workflow_runs
        WHERE run_id = $1
           OR (workflow_id = $2 AND run_idempotency_key = $3)
        ORDER BY requested_at DESC
        LIMIT 1
        """,
        run.run_id,
        run.workflow_id,
        run.run_idempotency_key,
    )
    if existing_row is not None and _workflow_run_matches(existing_row, run=run):
        return

    raise PostgresWriteError(
        "postgres.duplicate_submission_conflict",
        "workflow run already exists with different canonical content",
        details={
            "run_id": run.run_id,
            "workflow_id": run.workflow_id,
            "run_idempotency_key": run.run_idempotency_key,
        },
    )


async def persist_workflow_admission(
    conn: asyncpg.Connection,
    *,
    submission: WorkflowAdmissionSubmission,
) -> WorkflowAdmissionWriteResult:
    """Persist one atomic workflow admission/control-plane write."""
    # Import here to avoid circular dependency
    from .definitions import _persist_workflow_definition

    _validate_submission(submission)
    decision = submission.decision
    run = submission.run
    request_envelope = _require_mapping(
        run.request_envelope,
        field_name="run.request_envelope",
    )

    try:
        async with conn.transaction():
            await _persist_workflow_definition(
                conn,
                submission=submission,
                request_envelope=request_envelope,
            )
            await _insert_or_assert_admission_decision(
                conn,
                decision=decision,
            )
            await _insert_or_assert_workflow_run(
                conn,
                run=run,
                request_envelope=request_envelope,
            )
            if decision.decision == "admit":
                await conn.execute(
                    """INSERT INTO capability_grants (
                           capability_grant_id,
                           workflow_id,
                           run_id,
                           subject_type,
                           subject_id,
                           capability_name,
                           grant_state,
                           reason_code,
                           decision_ref,
                           scope_json,
                           granted_at
                       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
                       ON CONFLICT (capability_grant_id) DO NOTHING""",
                    f"cg_{run.run_id}",
                    run.workflow_id,
                    run.run_id,
                    "workflow_run",
                    run.run_id,
                    "execute_workflow",
                    "active",
                    decision.reason_code,
                    decision.admission_decision_id,
                    "{}",
                    run.admitted_at,
                )
    except asyncpg.PostgresError as exc:
        raise PostgresWriteError(
            "postgres.write_failed",
            "failed to persist workflow admission",
            details={"sqlstate": getattr(exc, "sqlstate", None)},
        ) from exc

    return WorkflowAdmissionWriteResult(
        admission_decision_id=decision.admission_decision_id,
        run_id=run.run_id,
    )
