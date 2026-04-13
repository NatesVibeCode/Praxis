"""Explicit sync Postgres repository for workflow result submissions."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import uuid
from collections.abc import Mapping
from typing import Any

import asyncpg

from .validators import _encode_jsonb, _require_text


class WorkflowSubmissionRepositoryError(RuntimeError):
    """Raised when workflow submission storage cannot complete safely."""

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


_VALID_DECLARED_OPERATION_ACTIONS = {"create", "update", "delete", "rename"}

_SUBMISSION_COLUMNS = """
    submission_id,
    run_id,
    workflow_id,
    job_label,
    attempt_no,
    result_kind,
    summary,
    primary_paths,
    tests_ran,
    notes,
    declared_operations,
    changed_paths,
    operation_set,
    comparison_status,
    comparison_report,
    acceptance_status,
    acceptance_report,
    diff_artifact_ref,
    artifact_refs,
    verification_artifact_refs,
    sealed_at
"""

_REVIEW_COLUMNS = """
    review_id,
    submission_id,
    run_id,
    workflow_id,
    reviewer_job_label,
    reviewer_role,
    decision,
    summary,
    notes,
    evidence_refs,
    reviewed_at
"""


def _row_dict(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise WorkflowSubmissionRepositoryError(
            "workflow_submission.write_failed",
            f"{operation} returned no row",
        )
    return dict(row)


def _normalize_timestamp(value: object | None, *, field_name: str) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise WorkflowSubmissionRepositoryError(
            "workflow_submission.invalid_input",
            f"{field_name} must be a datetime when provided",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise WorkflowSubmissionRepositoryError(
            "workflow_submission.invalid_input",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _normalize_optional_text(value: object | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _normalize_attempt_no(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise WorkflowSubmissionRepositoryError(
            "workflow_submission.invalid_input",
            f"{field_name} must be a positive integer",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _normalize_text_list(
    value: object | None,
    *,
    field_name: str,
) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise WorkflowSubmissionRepositoryError(
            "workflow_submission.invalid_input",
            f"{field_name} must be a list of strings",
            details={"field": field_name},
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str):
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.invalid_input",
                f"{field_name}[{index}] must be a string",
                details={"field": f"{field_name}[{index}]"},
            )
        normalized.append(_require_text(item, field_name=f"{field_name}[{index}]"))
    return normalized


def _normalize_json_value(value: object, *, field_name: str) -> Any:
    try:
        return json.loads(_encode_jsonb(value, field_name=field_name))
    except Exception as exc:  # pragma: no cover - _encode_jsonb is already strict
        raise WorkflowSubmissionRepositoryError(
            "workflow_submission.invalid_input",
            f"{field_name} must be JSON serializable",
            details={"field": field_name},
        ) from exc


def _normalize_declared_operations(value: object | None) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise WorkflowSubmissionRepositoryError(
            "workflow_submission.invalid_input",
            "declared_operations must be a list of mappings",
            details={"field": "declared_operations"},
        )
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.invalid_input",
                f"declared_operations[{index}] must be a mapping",
                details={"field": f"declared_operations[{index}]"},
            )
        keys = set(item.keys())
        allowed_keys = {"path", "action", "from_path"}
        unexpected = sorted(keys - allowed_keys)
        if unexpected:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.invalid_input",
                f"declared_operations[{index}] contains unexpected keys",
                details={
                    "field": f"declared_operations[{index}]",
                    "unexpected_keys": unexpected,
                },
            )
        path = _require_text(item.get("path"), field_name=f"declared_operations[{index}].path")
        action = _require_text(
            item.get("action"),
            field_name=f"declared_operations[{index}].action",
        )
        if action not in _VALID_DECLARED_OPERATION_ACTIONS:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.invalid_input",
                f"declared_operations[{index}].action must be one of {sorted(_VALID_DECLARED_OPERATION_ACTIONS)}",
                details={
                    "field": f"declared_operations[{index}].action",
                    "value": action,
                },
            )
        normalized_item: dict[str, Any] = {
            "path": path,
            "action": action,
        }
        if "from_path" in item and item["from_path"] is not None:
            normalized_item["from_path"] = _require_text(
                item["from_path"],
                field_name=f"declared_operations[{index}].from_path",
            )
        normalized.append(normalized_item)
    return normalized


def _normalize_submission_payload(
    *,
    run_id: str,
    workflow_id: str,
    job_label: str,
    attempt_no: int,
    result_kind: str,
    summary: str,
    primary_paths: object | None,
    tests_ran: object | None,
    notes: object | None,
    declared_operations: object | None,
    changed_paths: object | None,
    operation_set: object | None,
    comparison_status: str,
    comparison_report: object | None,
    acceptance_status: str,
    acceptance_report: object | None,
    diff_artifact_ref: object | None,
    artifact_refs: object | None,
    verification_artifact_refs: object | None,
    sealed_at: object | None,
) -> dict[str, Any]:
    normalized_payload = {
        "run_id": _require_text(run_id, field_name="run_id"),
        "workflow_id": _require_text(workflow_id, field_name="workflow_id"),
        "job_label": _require_text(job_label, field_name="job_label"),
        "attempt_no": _normalize_attempt_no(attempt_no, field_name="attempt_no"),
        "result_kind": _require_text(result_kind, field_name="result_kind"),
        "summary": _require_text(summary, field_name="summary"),
        "primary_paths": _normalize_text_list(primary_paths, field_name="primary_paths"),
        "tests_ran": _normalize_text_list(tests_ran, field_name="tests_ran"),
        "notes": _normalize_optional_text(notes, field_name="notes"),
        "declared_operations": _normalize_declared_operations(declared_operations),
        "changed_paths": _normalize_text_list(changed_paths, field_name="changed_paths"),
        "operation_set": _normalize_json_value(
            [] if operation_set is None else operation_set,
            field_name="operation_set",
        ),
        "comparison_status": _require_text(
            comparison_status,
            field_name="comparison_status",
        ),
        "comparison_report": _normalize_json_value(
            {} if comparison_report is None else comparison_report,
            field_name="comparison_report",
        ),
        "acceptance_status": _require_text(
            acceptance_status,
            field_name="acceptance_status",
        ),
        "acceptance_report": _normalize_json_value(
            {} if acceptance_report is None else acceptance_report,
            field_name="acceptance_report",
        ),
        "diff_artifact_ref": _normalize_optional_text(
            diff_artifact_ref,
            field_name="diff_artifact_ref",
        ),
        "artifact_refs": _normalize_text_list(artifact_refs, field_name="artifact_refs"),
        "verification_artifact_refs": _normalize_text_list(
            verification_artifact_refs,
            field_name="verification_artifact_refs",
        ),
        "sealed_at": _normalize_timestamp(sealed_at, field_name="sealed_at"),
    }
    return normalized_payload


def _submission_row_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    def _json_field(value: object, *, default: Any) -> Any:
        if value is None:
            return default
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return default
        return value

    return {
        "run_id": _require_text(row["run_id"], field_name="run_id"),
        "workflow_id": _require_text(row["workflow_id"], field_name="workflow_id"),
        "job_label": _require_text(row["job_label"], field_name="job_label"),
        "attempt_no": int(row["attempt_no"]),
        "result_kind": _require_text(row["result_kind"], field_name="result_kind"),
        "summary": _require_text(row["summary"], field_name="summary"),
        "primary_paths": list(_json_field(row.get("primary_paths"), default=[])),
        "tests_ran": list(_json_field(row.get("tests_ran"), default=[])),
        "notes": row["notes"],
        "declared_operations": list(_json_field(row.get("declared_operations"), default=[])),
        "changed_paths": list(_json_field(row.get("changed_paths"), default=[])),
        "operation_set": _json_field(row.get("operation_set"), default=[]),
        "comparison_status": _require_text(
            row["comparison_status"],
            field_name="comparison_status",
        ),
        "comparison_report": _json_field(row.get("comparison_report"), default={}),
        "acceptance_status": _require_text(
            row.get("acceptance_status") or "not_requested",
            field_name="acceptance_status",
        ),
        "acceptance_report": _json_field(row.get("acceptance_report"), default={}),
        "diff_artifact_ref": row["diff_artifact_ref"],
        "artifact_refs": list(_json_field(row.get("artifact_refs"), default=[])),
        "verification_artifact_refs": list(
            _json_field(row.get("verification_artifact_refs"), default=[])
        ),
    }


def _submission_fields_from_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "run_id": payload["run_id"],
        "workflow_id": payload["workflow_id"],
        "job_label": payload["job_label"],
        "attempt_no": payload["attempt_no"],
        "result_kind": payload["result_kind"],
        "summary": payload["summary"],
        "primary_paths": payload["primary_paths"],
        "tests_ran": payload["tests_ran"],
        "notes": payload["notes"],
        "declared_operations": payload["declared_operations"],
        "changed_paths": payload["changed_paths"],
        "operation_set": payload["operation_set"],
        "comparison_status": payload["comparison_status"],
        "comparison_report": payload["comparison_report"],
        "acceptance_status": payload["acceptance_status"],
        "acceptance_report": payload["acceptance_report"],
        "diff_artifact_ref": payload["diff_artifact_ref"],
        "artifact_refs": payload["artifact_refs"],
        "verification_artifact_refs": payload["verification_artifact_refs"],
    }


def _submission_difference_fields(
    existing_payload: Mapping[str, Any],
    requested_payload: Mapping[str, Any],
) -> list[str]:
    fields = [
        "workflow_id",
        "result_kind",
        "summary",
        "primary_paths",
        "tests_ran",
        "notes",
        "declared_operations",
        "changed_paths",
        "operation_set",
        "comparison_status",
        "comparison_report",
        "acceptance_status",
        "acceptance_report",
        "diff_artifact_ref",
        "artifact_refs",
        "verification_artifact_refs",
    ]
    return [
        field
        for field in fields
        if existing_payload[field] != requested_payload[field]
    ]


class PostgresWorkflowSubmissionRepository:
    """Owns canonical sealed workflow submissions and append-only reviews."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def record_submission(
        self,
        *,
        run_id: str,
        workflow_id: str,
        job_label: str,
        attempt_no: int,
        result_kind: str,
        summary: str,
        primary_paths: object | None,
        tests_ran: object | None = None,
        notes: object | None = None,
        declared_operations: object | None = None,
        changed_paths: object | None = None,
        operation_set: object | None = None,
        comparison_status: str,
        comparison_report: object | None = None,
        acceptance_status: str = "not_requested",
        acceptance_report: object | None = None,
        diff_artifact_ref: object | None = None,
        artifact_refs: object | None = None,
        verification_artifact_refs: object | None = None,
        submission_id: str | None = None,
        sealed_at: object | None = None,
    ) -> dict[str, Any]:
        normalized_payload = _normalize_submission_payload(
            run_id=run_id,
            workflow_id=workflow_id,
            job_label=job_label,
            attempt_no=attempt_no,
            result_kind=result_kind,
            summary=summary,
            primary_paths=primary_paths,
            tests_ran=tests_ran,
            notes=notes,
            declared_operations=declared_operations,
            changed_paths=changed_paths,
            operation_set=operation_set,
            comparison_status=comparison_status,
            comparison_report=comparison_report,
            acceptance_status=acceptance_status,
            acceptance_report=acceptance_report,
            diff_artifact_ref=diff_artifact_ref,
            artifact_refs=artifact_refs,
            verification_artifact_refs=verification_artifact_refs,
            sealed_at=sealed_at,
        )
        normalized_submission_id = _require_text(
            submission_id or f"workflow_job_submission:{uuid.uuid4().hex}",
            field_name="submission_id",
        )

        try:
            row = self._conn.fetchrow(
                f"""
                INSERT INTO workflow_job_submissions (
                    submission_id,
                    run_id,
                    workflow_id,
                    job_label,
                    attempt_no,
                    result_kind,
                    summary,
                    primary_paths,
                    tests_ran,
                    notes,
                    declared_operations,
                    changed_paths,
                    operation_set,
                    comparison_status,
                    comparison_report,
                    acceptance_status,
                    acceptance_report,
                    diff_artifact_ref,
                    artifact_refs,
                    verification_artifact_refs,
                    sealed_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8::jsonb, $9::jsonb, $10, $11::jsonb, $12::jsonb,
                    $13::jsonb, $14, $15::jsonb, $16, $17::jsonb, $18,
                    $19::jsonb, $20::jsonb, $21
                )
                ON CONFLICT (run_id, job_label, attempt_no) DO NOTHING
                RETURNING {_SUBMISSION_COLUMNS}
                """,
                normalized_submission_id,
                normalized_payload["run_id"],
                normalized_payload["workflow_id"],
                normalized_payload["job_label"],
                normalized_payload["attempt_no"],
                normalized_payload["result_kind"],
                normalized_payload["summary"],
                _encode_jsonb(normalized_payload["primary_paths"], field_name="primary_paths"),
                _encode_jsonb(normalized_payload["tests_ran"], field_name="tests_ran"),
                normalized_payload["notes"],
                _encode_jsonb(
                    normalized_payload["declared_operations"],
                    field_name="declared_operations",
                ),
                _encode_jsonb(normalized_payload["changed_paths"], field_name="changed_paths"),
                _encode_jsonb(normalized_payload["operation_set"], field_name="operation_set"),
                normalized_payload["comparison_status"],
                _encode_jsonb(
                    normalized_payload["comparison_report"],
                    field_name="comparison_report",
                ),
                normalized_payload["acceptance_status"],
                _encode_jsonb(
                    normalized_payload["acceptance_report"],
                    field_name="acceptance_report",
                ),
                normalized_payload["diff_artifact_ref"],
                _encode_jsonb(normalized_payload["artifact_refs"], field_name="artifact_refs"),
                _encode_jsonb(
                    normalized_payload["verification_artifact_refs"],
                    field_name="verification_artifact_refs",
                ),
                normalized_payload["sealed_at"],
            )
        except asyncpg.PostgresError as exc:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.write_failed",
                "failed to record workflow submission",
                details={
                    "run_id": normalized_payload["run_id"],
                    "job_label": normalized_payload["job_label"],
                    "attempt_no": normalized_payload["attempt_no"],
                    "sqlstate": getattr(exc, "sqlstate", None),
                },
            ) from exc

        if row is not None:
            return dict(row)

        existing_row = self._conn.fetchrow(
            f"""
            SELECT {_SUBMISSION_COLUMNS}
            FROM workflow_job_submissions
            WHERE run_id = $1 AND job_label = $2 AND attempt_no = $3
            """,
            normalized_payload["run_id"],
            normalized_payload["job_label"],
            normalized_payload["attempt_no"],
        )
        if existing_row is None:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.write_failed",
                "workflow submission insert conflicted but no existing row was found",
                details={
                    "run_id": normalized_payload["run_id"],
                    "job_label": normalized_payload["job_label"],
                    "attempt_no": normalized_payload["attempt_no"],
                },
            )

        existing_payload = _submission_row_payload(existing_row)
        requested_payload = _submission_fields_from_payload(normalized_payload)
        if existing_payload != requested_payload:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.conflict",
                "workflow submission already exists with different sealed payload",
                details={
                    "run_id": normalized_payload["run_id"],
                    "job_label": normalized_payload["job_label"],
                    "attempt_no": normalized_payload["attempt_no"],
                    "submission_id": existing_row["submission_id"],
                    "mismatched_fields": ",".join(
                        _submission_difference_fields(existing_payload, requested_payload)
                    ),
                },
            )

        return dict(existing_row)

    def fetch_submission_by_id(self, *, submission_id: str) -> dict[str, Any] | None:
        row = self._conn.fetchrow(
            f"""
            SELECT {_SUBMISSION_COLUMNS}
            FROM workflow_job_submissions
            WHERE submission_id = $1
            """,
            _require_text(submission_id, field_name="submission_id"),
        )
        return None if row is None else dict(row)

    def fetch_submission_by_run_job_attempt(
        self,
        *,
        run_id: str,
        job_label: str,
        attempt_no: int,
    ) -> dict[str, Any] | None:
        row = self._conn.fetchrow(
            f"""
            SELECT {_SUBMISSION_COLUMNS}
            FROM workflow_job_submissions
            WHERE run_id = $1 AND job_label = $2 AND attempt_no = $3
            """,
            _require_text(run_id, field_name="run_id"),
            _require_text(job_label, field_name="job_label"),
            _normalize_attempt_no(attempt_no, field_name="attempt_no"),
        )
        return None if row is None else dict(row)

    def fetch_latest_submission_summary_by_run_job(
        self,
        *,
        run_id: str,
        job_label: str,
    ) -> dict[str, Any] | None:
        row = self._conn.fetchrow(
            f"""
            SELECT {_SUBMISSION_COLUMNS}
            FROM workflow_job_submissions
            WHERE run_id = $1 AND job_label = $2
            ORDER BY attempt_no DESC, sealed_at DESC, submission_id DESC
            LIMIT 1
            """,
            _require_text(run_id, field_name="run_id"),
            _require_text(job_label, field_name="job_label"),
        )
        return None if row is None else dict(row)

    def list_latest_submission_summaries_for_run(
        self,
        *,
        run_id: str,
    ) -> tuple[dict[str, Any], ...]:
        rows = self._conn.execute(
            f"""
            SELECT DISTINCT ON (job_label) {_SUBMISSION_COLUMNS}
            FROM workflow_job_submissions
            WHERE run_id = $1
            ORDER BY job_label ASC, attempt_no DESC, sealed_at DESC, submission_id DESC
            """,
            _require_text(run_id, field_name="run_id"),
        )
        return tuple(dict(row) for row in rows)

    def update_submission_verification_artifact_refs(
        self,
        *,
        submission_id: str,
        verification_artifact_refs: object | None,
    ) -> dict[str, Any]:
        normalized_submission_id = _require_text(
            submission_id,
            field_name="submission_id",
        )
        normalized_refs = _normalize_text_list(
            verification_artifact_refs,
            field_name="verification_artifact_refs",
        )
        row = self._conn.fetchrow(
            f"""
            UPDATE workflow_job_submissions
            SET verification_artifact_refs = $2::jsonb
            WHERE submission_id = $1
            RETURNING {_SUBMISSION_COLUMNS}
            """,
            normalized_submission_id,
            _encode_jsonb(
                normalized_refs,
                field_name="verification_artifact_refs",
            ),
        )
        if row is None:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.not_found",
                "submission_id did not resolve to a sealed submission row",
                details={"submission_id": normalized_submission_id},
            )
        return dict(row)

    def update_submission_acceptance(
        self,
        *,
        submission_id: str,
        acceptance_status: str,
        acceptance_report: object | None,
    ) -> dict[str, Any]:
        normalized_submission_id = _require_text(
            submission_id,
            field_name="submission_id",
        )
        normalized_status = _require_text(
            acceptance_status,
            field_name="acceptance_status",
        )
        normalized_report = _normalize_json_value(
            {} if acceptance_report is None else acceptance_report,
            field_name="acceptance_report",
        )
        row = self._conn.fetchrow(
            f"""
            UPDATE workflow_job_submissions
            SET acceptance_status = $2,
                acceptance_report = $3::jsonb
            WHERE submission_id = $1
            RETURNING {_SUBMISSION_COLUMNS}
            """,
            normalized_submission_id,
            normalized_status,
            _encode_jsonb(
                normalized_report,
                field_name="acceptance_report",
            ),
        )
        if row is None:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.not_found",
                "submission_id did not resolve to a sealed submission row",
                details={"submission_id": normalized_submission_id},
            )
        return dict(row)

    def record_review(
        self,
        *,
        submission_id: str,
        run_id: str,
        workflow_id: str,
        reviewer_job_label: str,
        reviewer_role: str,
        decision: str,
        summary: str,
        notes: object | None = None,
        evidence_refs: object | None = None,
        review_id: str | None = None,
        reviewed_at: object | None = None,
    ) -> dict[str, Any]:
        normalized_submission_id = _require_text(
            submission_id,
            field_name="submission_id",
        )
        submission_row = self.fetch_submission_by_id(submission_id=normalized_submission_id)
        if submission_row is None:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.not_found",
                "submission_id did not resolve to a sealed submission row",
                details={"submission_id": normalized_submission_id},
            )
        if _require_text(run_id, field_name="run_id") != submission_row["run_id"]:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.conflict",
                "review run_id does not match the sealed submission row",
                details={
                    "submission_id": normalized_submission_id,
                    "expected_run_id": submission_row["run_id"],
                    "actual_run_id": run_id,
                },
            )
        if _require_text(workflow_id, field_name="workflow_id") != submission_row["workflow_id"]:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.conflict",
                "review workflow_id does not match the sealed submission row",
                details={
                    "submission_id": normalized_submission_id,
                    "expected_workflow_id": submission_row["workflow_id"],
                    "actual_workflow_id": workflow_id,
                },
            )

        normalized_review_id = _require_text(
            review_id or f"workflow_job_submission_review:{uuid.uuid4().hex}",
            field_name="review_id",
        )
        normalized_reviewed_at = _normalize_timestamp(
            reviewed_at,
            field_name="reviewed_at",
        )
        normalized_notes = _normalize_optional_text(notes, field_name="notes")
        normalized_evidence_refs = _normalize_text_list(
            evidence_refs,
            field_name="evidence_refs",
        )
        normalized_decision = _require_text(decision, field_name="decision")
        if normalized_decision not in {"approve", "request_changes", "reject"}:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.invalid_input",
                "decision must be one of 'approve', 'request_changes', or 'reject'",
                details={"field": "decision", "value": normalized_decision},
            )

        try:
            row = self._conn.fetchrow(
                f"""
                INSERT INTO workflow_job_submission_reviews (
                    review_id,
                    submission_id,
                    run_id,
                    workflow_id,
                    reviewer_job_label,
                    reviewer_role,
                    decision,
                    summary,
                    notes,
                    evidence_refs,
                    reviewed_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11
                )
                RETURNING {_REVIEW_COLUMNS}
                """,
                normalized_review_id,
                normalized_submission_id,
                submission_row["run_id"],
                submission_row["workflow_id"],
                _require_text(reviewer_job_label, field_name="reviewer_job_label"),
                _require_text(reviewer_role, field_name="reviewer_role"),
                normalized_decision,
                _require_text(summary, field_name="summary"),
                normalized_notes,
                _encode_jsonb(normalized_evidence_refs, field_name="evidence_refs"),
                normalized_reviewed_at,
            )
        except asyncpg.PostgresError as exc:
            raise WorkflowSubmissionRepositoryError(
                "workflow_submission.write_failed",
                "failed to record workflow submission review",
                details={
                    "submission_id": normalized_submission_id,
                    "review_id": normalized_review_id,
                    "sqlstate": getattr(exc, "sqlstate", None),
                },
            ) from exc
        return _row_dict(row, operation="creating workflow submission review")

    def fetch_review_by_id(self, *, review_id: str) -> dict[str, Any] | None:
        row = self._conn.fetchrow(
            f"""
            SELECT {_REVIEW_COLUMNS}
            FROM workflow_job_submission_reviews
            WHERE review_id = $1
            """,
            _require_text(review_id, field_name="review_id"),
        )
        return None if row is None else dict(row)

    def list_reviews_for_submission(
        self,
        *,
        submission_id: str,
    ) -> tuple[dict[str, Any], ...]:
        rows = self._conn.execute(
            f"""
            SELECT {_REVIEW_COLUMNS}
            FROM workflow_job_submission_reviews
            WHERE submission_id = $1
            ORDER BY reviewed_at ASC, review_id ASC
            """,
            _require_text(submission_id, field_name="submission_id"),
        )
        return tuple(dict(row) for row in rows)

    def fetch_latest_review_summary_by_submission_id(
        self,
        *,
        submission_id: str,
    ) -> dict[str, Any] | None:
        row = self._conn.fetchrow(
            f"""
            SELECT {_REVIEW_COLUMNS}
            FROM workflow_job_submission_reviews
            WHERE submission_id = $1
            ORDER BY reviewed_at DESC, review_id DESC
            LIMIT 1
            """,
            _require_text(submission_id, field_name="submission_id"),
        )
        return None if row is None else dict(row)

    def fetch_latest_review_summary_by_run_job(
        self,
        *,
        run_id: str,
        job_label: str,
    ) -> dict[str, Any] | None:
        submission = self.fetch_latest_submission_summary_by_run_job(
            run_id=run_id,
            job_label=job_label,
        )
        if submission is None:
            return None
        return self.fetch_latest_review_summary_by_submission_id(
            submission_id=submission["submission_id"],
        )
