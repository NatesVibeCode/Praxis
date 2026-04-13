"""DB-backed workflow submission service and measured capture helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import logging
import subprocess
import uuid
from typing import Any

from runtime.sandbox_artifacts import ArtifactStore
from runtime.workflow.job_runtime_context import (
    load_workflow_job_runtime_context,
    persist_workflow_job_runtime_contexts,
)
from runtime.workflow.submission_diff import (
    _artifact_ref,
    _comparison_result,
    _hash_file,
    _measured_operations,
    _read_artifact_text,
    _scope_allows_path,
    _workspace_manifest,
)
from runtime.workflow.submission_policy import (
    _PUBLISH_REVIEW_ROLE_TASK_TYPES,
    evaluate_publish_policy,
)
from storage.postgres.workflow_submission_repository import (
    PostgresWorkflowSubmissionRepository,
    WorkflowSubmissionRepositoryError,
)
from surfaces.mcp.subsystems import _subs


logger = logging.getLogger(__name__)

_REVIEW_ROLE_TASK_TYPES = frozenset(
    {
        "review",
        "code_review",
        "verifier",
        "reviewer",
        "orchestrator",
        "ops_review",
        "publish",
        "publish_policy",
    }
)

class WorkflowSubmissionServiceError(RuntimeError):
    """Raised when workflow submission work cannot be completed safely."""

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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _strip_str(value: str | None) -> str | None:
    """Return stripped string or None if empty."""
    return (value.strip() or None) if isinstance(value, str) else None


def _optional_datetime(value: object | None, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise WorkflowSubmissionServiceError(
                "workflow_submission.invalid_input",
                f"{field_name} must be timezone-aware",
                details={"field": field_name},
            )
        return value.astimezone(timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WorkflowSubmissionServiceError(
            "workflow_submission.invalid_input",
            f"{field_name} must be an ISO-8601 timestamp when provided",
            details={"field": field_name, "value": text},
        ) from exc
    if normalized.tzinfo is None or normalized.utcoffset() is None:
        normalized = normalized.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc)


def _normalize_timestamp(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    text = str(value or "").strip()
    return text


def _normalize_text(value: object, *, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise WorkflowSubmissionServiceError(
            "workflow_submission.invalid_input",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return text


def _normalize_text_list(value: object | None, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [_normalize_text(value, field_name=field_name)]
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        raise WorkflowSubmissionServiceError(
            "workflow_submission.invalid_input",
            f"{field_name} must be a list of strings",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(_normalize_text(item, field_name=f"{field_name}[{index}]"))
    return normalized


def _normalize_declared_operations(value: object | None) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise WorkflowSubmissionServiceError(
            "workflow_submission.invalid_input", "declared_operations must be a list of objects",
            details={"field": "declared_operations"},
        )
    result: list[dict[str, str]] = []
    for i, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise WorkflowSubmissionServiceError(
                "workflow_submission.invalid_input", f"declared_operations[{i}] must be an object",
                details={"field": f"declared_operations[{i}]"},
            )
        action = _normalize_text(item.get("action"), field_name=f"declared_operations[{i}].action").lower()
        if action not in {"create", "update", "delete", "rename"}:
            raise WorkflowSubmissionServiceError(
                "workflow_submission.invalid_input",
                "declared_operations action must be create, update, delete, or rename",
                details={"field": f"declared_operations[{i}].action", "action": action},
            )
        entry: dict[str, str] = {"path": _normalize_path(item.get("path"), field_name=f"declared_operations[{i}].path"), "action": action}
        if item.get("from_path") is not None:
            entry["from_path"] = _normalize_path(item["from_path"], field_name=f"declared_operations[{i}].from_path")
        result.append(entry)
    return result


def _normalize_path(value: object, *, field_name: str) -> str:
    text = _normalize_text(value, field_name=field_name)
    if text.startswith("file:"):
        text = text[5:]
    normalized = Path(text).as_posix().lstrip("./")
    return normalized


def _normalize_scope_paths(value: object | None) -> list[str]:
    try:
        normalized = [_normalize_path(item, field_name="write_scope") for item in _normalize_text_list(value, field_name="write_scope")]
    except WorkflowSubmissionServiceError:
        return []
    return list(dict.fromkeys(normalized))


def _current_job_row(conn, *, run_id: str, job_label: str) -> dict[str, Any]:
    row = conn.fetchrow(
        """
        SELECT id, run_id, label, attempt, route_task_type, status
        FROM workflow_jobs
        WHERE run_id = $1 AND label = $2
        LIMIT 1
        """,
        _normalize_text(run_id, field_name="run_id"),
        _normalize_text(job_label, field_name="job_label"),
    )
    if row is None:
        raise WorkflowSubmissionServiceError(
            "workflow_submission.job_not_found",
            "workflow job was not found for the current run/job label",
            details={"run_id": run_id, "job_label": job_label},
        )
    return dict(row)


def _workflow_request_id(conn, *, run_id: str) -> str:
    row = conn.fetchrow(
        "SELECT request_id FROM workflow_runs WHERE run_id = $1",
        _normalize_text(run_id, field_name="run_id"),
    )
    return str((row or {}).get("request_id") or f"request:{run_id}")



def _submission_manifest_hash(submission: Mapping[str, Any]) -> str:
    _s = lambda k: str(submission.get(k) or "").strip()  # noqa: E731
    _l = lambda k: list(submission.get(k) or [])  # noqa: E731
    payload = {
        "submission_id": _s("submission_id"), "result_kind": _s("result_kind"),
        "summary": _s("summary"), "comparison_status": _s("comparison_status"),
        "diff_artifact_ref": _s("diff_artifact_ref"),
        "primary_paths": _l("primary_paths"), "changed_paths": _l("changed_paths"),
        "operation_set": _l("operation_set"), "artifact_refs": _l("artifact_refs"),
        "verification_artifact_refs": _l("verification_artifact_refs"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _git_head_ref(workspace_root: str | None) -> str | None:
    normalized_workspace_root = str(workspace_root or "").strip()
    if not normalized_workspace_root:
        return None
    try:
        completed = subprocess.run(
            ["git", "-C", normalized_workspace_root, "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    head_ref = completed.stdout.strip()
    return head_ref or None


def _target_workspace_root(
    conn,
    *,
    run_id: str,
    target_job_label: str,
) -> str | None:
    target_execution_context_shard, _, _ = _load_runtime_context_state(
        conn,
        run_id=run_id,
        job_label=target_job_label,
    )
    submission_protocol = _submission_protocol_state(target_execution_context_shard)
    baseline = dict(submission_protocol.get("baseline") or {})
    workspace_root = str(baseline.get("workspace_root") or "").strip()
    return workspace_root or None


def _next_evidence_seq(conn, *, run_id: str) -> int:
    value = conn.fetchval(
        """
        SELECT GREATEST(
            COALESCE((SELECT MAX(evidence_seq) FROM workflow_events WHERE run_id = $1), 0),
            COALESCE((SELECT MAX(evidence_seq) FROM receipts WHERE run_id = $1), 0)
        )
        """,
        _normalize_text(run_id, field_name="run_id"),
    )
    return int(value or 0) + 1


def _emit_workflow_event(
    conn,
    *,
    run_id: str,
    workflow_id: str,
    job_label: str,
    event_type: str,
    reason_code: str,
    payload: Mapping[str, Any],
) -> str:
    event_id = f"workflow_event:{uuid.uuid4().hex}"
    evidence_seq = _next_evidence_seq(conn, run_id=run_id)
    conn.execute(
        """
        INSERT INTO workflow_events (
            event_id,
            event_type,
            schema_version,
            workflow_id,
            run_id,
            request_id,
            causation_id,
            node_id,
            occurred_at,
            evidence_seq,
            actor_type,
            reason_code,
            payload
        ) VALUES (
            $1, $2, 1, $3, $4, $5, NULL, $6, $7, $8, $9, $10, $11::jsonb
        )
        ON CONFLICT DO NOTHING
        """,
        event_id,
        event_type,
        workflow_id,
        run_id,
        _workflow_request_id(conn, run_id=run_id),
        job_label,
        _utc_now(),
        evidence_seq,
        "workflow_submission",
        reason_code,
        json.dumps(dict(payload), sort_keys=True, default=str),
    )
    return event_id


def _load_runtime_context_state(
    conn,
    *,
    run_id: str,
    job_label: str,
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    runtime_context = load_workflow_job_runtime_context(
        conn,
        run_id=run_id,
        job_label=job_label,
    )
    if runtime_context is None:
        return {}, {}, None
    return (
        dict(runtime_context.get("execution_context_shard") or {}),
        dict(runtime_context.get("execution_bundle") or {}),
        str(runtime_context.get("workflow_id") or "").strip() or None,
    )


def _persist_runtime_context_state(
    conn,
    *,
    run_id: str,
    job_label: str,
    workflow_id: str | None,
    execution_context_shard: Mapping[str, Any],
    execution_bundle: Mapping[str, Any],
) -> None:
    persist_workflow_job_runtime_contexts(
        conn,
        run_id=run_id,
        workflow_id=workflow_id,
        execution_context_shards={job_label: dict(execution_context_shard)},
        execution_bundles={job_label: dict(execution_bundle)},
    )


def _submission_protocol_state(execution_context_shard: Mapping[str, Any]) -> dict[str, Any]:
    value = execution_context_shard.get("submission_protocol")
    return dict(value) if isinstance(value, Mapping) else {}


def _set_submission_protocol_state(
    execution_context_shard: Mapping[str, Any],
    submission_protocol: Mapping[str, Any],
) -> dict[str, Any]:
    updated = dict(execution_context_shard)
    updated["submission_protocol"] = dict(submission_protocol)
    return updated


def _completion_contract(execution_bundle: Mapping[str, Any]) -> dict[str, Any]:
    value = execution_bundle.get("completion_contract")
    return dict(value) if isinstance(value, Mapping) else {}


def capture_submission_baseline_for_job(
    conn,
    *,
    run_id: str,
    workflow_id: str | None,
    job_label: str,
    workspace_root: str,
    write_scope: Sequence[str] | None,
    execution_context_shard: Mapping[str, Any] | None = None,
    execution_bundle: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_run_id = _normalize_text(run_id, field_name="run_id")
    normalized_job_label = _normalize_text(job_label, field_name="job_label")
    normalized_workspace_root = str(Path(_normalize_text(workspace_root, field_name="workspace_root")).resolve())
    existing_shard, existing_bundle, existing_workflow_id = _load_runtime_context_state(
        conn,
        run_id=normalized_run_id,
        job_label=normalized_job_label,
    )
    active_shard = dict(existing_shard or execution_context_shard or {})
    active_bundle = dict(existing_bundle or execution_bundle or {})
    normalized_write_scope = _normalize_scope_paths(write_scope or active_shard.get("write_scope"))
    completion_contract = _completion_contract(active_bundle)
    if not completion_contract.get("submission_required"):
        return {"status": "skipped", "reason": "submission_not_required"}

    if not normalized_write_scope:
        # No write_scope: submission is text-only (research/debate via MCP tool).
        # Skip baseline capture — comparison will be skipped at submission time.
        return {"status": "skipped", "reason": "no_write_scope_for_text_submission"}

    artifact_store = ArtifactStore(conn)
    baseline_artifact_sandbox_id = (
        f"workflow_submission_baseline:{normalized_run_id}:{normalized_job_label}"
    )
    workspace_manifest = _workspace_manifest(normalized_workspace_root)
    scoped_artifacts: dict[str, dict[str, str]] = {}
    for path in sorted(workspace_manifest):
        if not _scope_allows_path(path, normalized_write_scope):
            continue
        text = _read_artifact_text(Path(normalized_workspace_root) / path)
        if text is None:
            continue
        record = artifact_store.capture(path, text, baseline_artifact_sandbox_id)
        scoped_artifacts[path] = {
            "artifact_id": record.artifact_id,
            "sha256": record.sha256,
        }

    baseline = {
        "captured_at": _utc_now().isoformat(),
        "workspace_root": normalized_workspace_root,
        "write_scope": normalized_write_scope,
        "workspace_manifest": workspace_manifest,
        "scoped_artifacts": scoped_artifacts,
    }
    submission_protocol = _submission_protocol_state(active_shard)
    submission_protocol["baseline"] = baseline
    updated_shard = _set_submission_protocol_state(active_shard, submission_protocol)
    _persist_runtime_context_state(
        conn,
        run_id=normalized_run_id,
        job_label=normalized_job_label,
        workflow_id=workflow_id or existing_workflow_id,
        execution_context_shard=updated_shard,
        execution_bundle=active_bundle,
    )
    return baseline


def _resolve_submission_target(
    repository: PostgresWorkflowSubmissionRepository,
    *,
    run_id: str,
    workflow_id: str,
    submission_id: str | None,
    job_label: str | None,
) -> dict[str, Any]:
    if bool(submission_id) == bool(job_label):
        raise WorkflowSubmissionServiceError(
            "workflow_submission.invalid_input",
            "exactly one of submission_id or job_label is required",
            details={"run_id": run_id},
        )
    if submission_id:
        row = repository.fetch_submission_by_id(submission_id=submission_id)
    else:
        row = repository.fetch_latest_submission_summary_by_run_job(
            run_id=run_id,
            job_label=_normalize_text(job_label, field_name="job_label"),
        )
    if row is None:
        raise WorkflowSubmissionServiceError(
            "workflow_submission.not_found",
            "workflow submission was not found",
            details={"run_id": run_id, "workflow_id": workflow_id},
        )
    if str(row.get("run_id") or "") != run_id or str(row.get("workflow_id") or "") != workflow_id:
        raise WorkflowSubmissionServiceError(
            "workflow_submission.not_found",
            "workflow submission is outside the current run/workflow authority",
            details={"submission_id": row.get("submission_id")},
        )
    return dict(row)


def _serialize_submission(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key in (
        "primary_paths",
        "tests_ran",
        "declared_operations",
        "changed_paths",
        "operation_set",
        "comparison_report",
        "artifact_refs",
        "verification_artifact_refs",
    ):
        value = payload.get(key)
        if isinstance(value, str):
            try:
                payload[key] = json.loads(value)
            except json.JSONDecodeError:
                pass
    if payload.get("sealed_at") is not None:
        payload["sealed_at"] = _normalize_timestamp(payload["sealed_at"])
    return payload


def _serialize_review(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    value = payload.get("evidence_refs")
    if isinstance(value, str):
        try:
            payload["evidence_refs"] = json.loads(value)
        except json.JSONDecodeError:
            pass
    if payload.get("reviewed_at") is not None:
        payload["reviewed_at"] = _normalize_timestamp(payload["reviewed_at"])
    return payload


def _measured_summary(operation_set: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    summary = {"create": 0, "update": 0, "delete": 0, "rename": 0}
    for item in operation_set:
        action = str(item.get("action") or "").strip().lower()
        if action in summary:
            summary[action] += 1
    summary["total"] = sum(summary.values())
    return summary


def _enriched_submission(
    repository: PostgresWorkflowSubmissionRepository,
    submission_row: Mapping[str, Any],
) -> dict[str, Any]:
    submission = _serialize_submission(submission_row)
    reviews = [
        _serialize_review(review)
        for review in repository.list_reviews_for_submission(
            submission_id=str(submission["submission_id"]),
        )
    ]
    latest_review = reviews[-1] if reviews else None
    submission["measured_summary"] = _measured_summary(submission.get("operation_set") or [])
    submission["latest_review"] = latest_review
    submission["review_timeline"] = reviews
    return submission


def _repo(conn=None) -> tuple[Any, PostgresWorkflowSubmissionRepository]:
    active_conn = conn or _subs.get_pg_conn()
    return active_conn, PostgresWorkflowSubmissionRepository(active_conn)


def _submission_from_runtime(
    conn,
    *,
    run_id: str,
    workflow_id: str,
    job_label: str,
    attempt_no: int,
) -> dict[str, Any] | None:
    repository = PostgresWorkflowSubmissionRepository(conn)
    row = repository.fetch_submission_by_run_job_attempt(
        run_id=run_id,
        job_label=job_label,
        attempt_no=attempt_no,
    )
    return None if row is None else _enriched_submission(repository, row)



def attach_verification_artifact_refs_for_job(
    conn,
    *,
    run_id: str,
    job_label: str,
    attempt_no: int,
    verification_artifact_refs: Sequence[str] | None,
) -> dict[str, Any] | None:
    repository = PostgresWorkflowSubmissionRepository(conn)
    row = repository.fetch_submission_by_run_job_attempt(
        run_id=run_id,
        job_label=job_label,
        attempt_no=attempt_no,
    )
    if row is None:
        return None
    current_refs = _normalize_text_list(
        row.get("verification_artifact_refs"),
        field_name="verification_artifact_refs",
    )
    merged_refs = sorted(
        dict.fromkeys(
            [*current_refs, *_normalize_text_list(verification_artifact_refs, field_name="verification_artifact_refs")]
        )
    )
    updated = repository.update_submission_verification_artifact_refs(
        submission_id=str(row["submission_id"]),
        verification_artifact_refs=merged_refs,
    )
    return _enriched_submission(repository, updated)


def get_submission_for_job_attempt(
    conn,
    *,
    run_id: str,
    job_label: str,
    attempt_no: int,
) -> dict[str, Any] | None:
    return _submission_from_runtime(
        conn,
        run_id=run_id,
        workflow_id="",
        job_label=job_label,
        attempt_no=attempt_no,
    )


def _make_submit(
    *,
    run_id: str,
    workflow_id: str,
    job_label: str,
    summary: str,
    primary_paths: Sequence[str],
    result_kind: str,
    tests_ran: Sequence[str] | None,
    notes: str | None,
    declared_operations: Sequence[Mapping[str, Any]] | None,
    conn,
) -> dict[str, Any]:
    return _submit_submission(
        run_id=run_id, workflow_id=workflow_id, job_label=job_label,
        summary=summary, primary_paths=primary_paths, result_kind=result_kind,
        tests_ran=tests_ran, notes=notes, declared_operations=declared_operations, conn=conn,
    )


def submit_code_change(*, run_id: str, workflow_id: str, job_label: str, summary: str,
    primary_paths: Sequence[str], result_kind: str, tests_ran: Sequence[str] | None = None,
    notes: str | None = None, declared_operations: Sequence[Mapping[str, Any]] | None = None,
    conn=None) -> dict[str, Any]:
    return _make_submit(run_id=run_id, workflow_id=workflow_id, job_label=job_label,
        summary=summary, primary_paths=primary_paths, result_kind=result_kind,
        tests_ran=tests_ran, notes=notes, declared_operations=declared_operations, conn=conn)


def submit_research_result(*, run_id: str, workflow_id: str, job_label: str, summary: str,
    primary_paths: Sequence[str], result_kind: str, tests_ran: Sequence[str] | None = None,
    notes: str | None = None, declared_operations: Sequence[Mapping[str, Any]] | None = None,
    conn=None) -> dict[str, Any]:
    return _make_submit(run_id=run_id, workflow_id=workflow_id, job_label=job_label,
        summary=summary, primary_paths=primary_paths, result_kind=result_kind,
        tests_ran=tests_ran, notes=notes, declared_operations=declared_operations, conn=conn)


def submit_artifact_bundle(*, run_id: str, workflow_id: str, job_label: str, summary: str,
    primary_paths: Sequence[str], result_kind: str, tests_ran: Sequence[str] | None = None,
    notes: str | None = None, declared_operations: Sequence[Mapping[str, Any]] | None = None,
    conn=None) -> dict[str, Any]:
    return _make_submit(run_id=run_id, workflow_id=workflow_id, job_label=job_label,
        summary=summary, primary_paths=primary_paths, result_kind=result_kind,
        tests_ran=tests_ran, notes=notes, declared_operations=declared_operations, conn=conn)


def _submit_submission(
    *,
    run_id: str,
    workflow_id: str,
    job_label: str,
    summary: str,
    primary_paths: Sequence[str],
    result_kind: str,
    tests_ran: Sequence[str] | None = None,
    notes: str | None = None,
    declared_operations: Sequence[Mapping[str, Any]] | None = None,
    conn=None,
) -> dict[str, Any]:
    active_conn, repository = _repo(conn)
    normalized_run_id = _normalize_text(run_id, field_name="run_id")
    normalized_workflow_id = _normalize_text(workflow_id, field_name="workflow_id")
    normalized_job_label = _normalize_text(job_label, field_name="job_label")
    normalized_summary = _normalize_text(summary, field_name="summary")
    normalized_primary_paths = [
        _normalize_path(path, field_name="primary_paths")
        for path in _normalize_text_list(primary_paths, field_name="primary_paths")
    ]
    normalized_declared_operations = _normalize_declared_operations(declared_operations)
    job_row = _current_job_row(
        active_conn,
        run_id=normalized_run_id,
        job_label=normalized_job_label,
    )
    attempt_no = max(1, int(job_row.get("attempt") or 1))

    execution_context_shard, execution_bundle, _ = _load_runtime_context_state(
        active_conn,
        run_id=normalized_run_id,
        job_label=normalized_job_label,
    )
    submission_protocol = _submission_protocol_state(execution_context_shard)
    baseline = dict(submission_protocol.get("baseline") or {})
    normalized_result_kind = _normalize_text(result_kind, field_name="result_kind")

    # Text-output tasks (research, debate, analysis) have no baseline or
    # write_scope — they produce text, not file changes.  Seal directly
    # instead of requiring the baseline-comparison pipeline.
    _TEXT_ONLY_RESULT_KINDS = {"research_result", "artifact_bundle"}
    needs_baseline = normalized_result_kind not in _TEXT_ONLY_RESULT_KINDS

    if not baseline and needs_baseline:
        raise WorkflowSubmissionServiceError(
            "workflow_submission.baseline_missing",
            "submission baseline is missing for the current job attempt",
            details={
                "run_id": normalized_run_id,
                "job_label": normalized_job_label,
                "attempt_no": attempt_no,
            },
        )

    if not baseline or not _normalize_scope_paths(
        baseline.get("write_scope") or (execution_context_shard or {}).get("write_scope"),
    ):
        if not needs_baseline:
            # Text-only submission — no workspace diff needed.
            existing = repository.fetch_submission_by_run_job_attempt(
                run_id=normalized_run_id,
                job_label=normalized_job_label,
                attempt_no=attempt_no,
            )
            try:
                recorded = repository.record_submission(
                    run_id=normalized_run_id,
                    workflow_id=normalized_workflow_id,
                    job_label=normalized_job_label,
                    attempt_no=attempt_no,
                    result_kind=normalized_result_kind,
                    summary=normalized_summary,
                    primary_paths=normalized_primary_paths,
                    tests_ran=_normalize_text_list(tests_ran, field_name="tests_ran"),
                    notes=notes,
                    declared_operations=normalized_declared_operations,
                    changed_paths=[],
                    operation_set=[],
                    comparison_status="text_only",
                    comparison_report="",
                    diff_artifact_ref=None,
                    artifact_refs=[],
                    verification_artifact_refs=[],
                )
            except WorkflowSubmissionRepositoryError as exc:
                raise WorkflowSubmissionServiceError(
                    exc.reason_code, str(exc),
                    details=getattr(exc, "details", None),
                ) from exc
            if existing is None:
                _emit_workflow_event(
                    active_conn,
                    run_id=normalized_run_id,
                    workflow_id=normalized_workflow_id,
                    job_label=normalized_job_label,
                    event_type="workflow.job.submission.sealed",
                    reason_code="workflow_submission.sealed",
                    payload={
                        "submission_id": recorded["submission_id"],
                        "job_label": normalized_job_label,
                        "attempt_no": attempt_no,
                        "result_kind": normalized_result_kind,
                        "comparison_status": "text_only",
                    },
                )
            return recorded
        raise WorkflowSubmissionServiceError(
            "workflow_submission.write_scope_missing",
            "submission baseline is missing write_scope authority",
            details={"run_id": normalized_run_id, "job_label": normalized_job_label},
        )

    workspace_root = _normalize_text(
        baseline.get("workspace_root"),
        field_name="baseline.workspace_root",
    )
    write_scope = _normalize_scope_paths(
        baseline.get("write_scope") or execution_context_shard.get("write_scope"),
    )

    for path in normalized_primary_paths:
        if not _scope_allows_path(path, write_scope):
            raise WorkflowSubmissionServiceError(
                "workflow_submission.out_of_scope",
                "primary_paths contains a path outside the declared write scope",
                details={"path": path, "write_scope": list(write_scope)},
            )

    changed_paths, operation_set, out_of_scope, diff_artifact_ref = _measured_operations(
        conn=active_conn,
        workspace_root=workspace_root,
        write_scope=write_scope,
        baseline=baseline,
    )
    if out_of_scope:
        raise WorkflowSubmissionServiceError(
            "workflow_submission.out_of_scope",
            "measured workspace changes escaped the declared write scope",
            details={"paths": out_of_scope, "write_scope": list(write_scope)},
        )

    comparison_status, comparison_report = _comparison_result(
        declared_operations=normalized_declared_operations,
        measured_operations=operation_set,
    )
    artifact_refs: list[str] = []
    for operation in operation_set:
        path = str(operation.get("path") or "").strip()
        action = str(operation.get("action") or "").strip()
        if action == "delete":
            baseline_artifacts = dict(baseline.get("scoped_artifacts") or {})
            sha = str((baseline_artifacts.get(path) or {}).get("sha256") or "")
            if sha:
                artifact_refs.append(_artifact_ref(path, sha, deleted=True))
            continue
        sha = _hash_file(Path(workspace_root) / path)
        if sha:
            artifact_refs.append(_artifact_ref(path, sha))
    artifact_refs = sorted(dict.fromkeys(artifact_refs))

    existing_submission = repository.fetch_submission_by_run_job_attempt(
        run_id=normalized_run_id,
        job_label=normalized_job_label,
        attempt_no=attempt_no,
    )
    try:
        recorded = repository.record_submission(
            run_id=normalized_run_id,
            workflow_id=normalized_workflow_id,
            job_label=normalized_job_label,
            attempt_no=attempt_no,
            result_kind=_normalize_text(result_kind, field_name="result_kind"),
            summary=normalized_summary,
            primary_paths=normalized_primary_paths,
            tests_ran=_normalize_text_list(tests_ran, field_name="tests_ran"),
            notes=notes,
            declared_operations=normalized_declared_operations,
            changed_paths=changed_paths,
            operation_set=operation_set,
            comparison_status=comparison_status,
            comparison_report=comparison_report,
            diff_artifact_ref=diff_artifact_ref,
            artifact_refs=artifact_refs,
            verification_artifact_refs=[],
        )
    except WorkflowSubmissionRepositoryError as exc:
        raise WorkflowSubmissionServiceError(
            exc.reason_code,
            str(exc),
            details=getattr(exc, "details", None),
        ) from exc

    if existing_submission is None:
        _emit_workflow_event(
            active_conn,
            run_id=normalized_run_id,
            workflow_id=normalized_workflow_id,
            job_label=normalized_job_label,
            event_type="workflow.job.submission.sealed",
            reason_code="workflow_submission.sealed",
            payload={
                "submission_id": recorded["submission_id"],
                "job_label": normalized_job_label,
                "attempt_no": attempt_no,
                "result_kind": result_kind,
                "comparison_status": comparison_status,
                "changed_paths": changed_paths,
                "artifact_refs": artifact_refs,
                "diff_artifact_ref": diff_artifact_ref,
            },
        )
        submission_protocol["latest_submission_id"] = recorded["submission_id"]
        updated_shard = _set_submission_protocol_state(execution_context_shard, submission_protocol)
        _persist_runtime_context_state(
            active_conn,
            run_id=normalized_run_id,
            job_label=normalized_job_label,
            workflow_id=normalized_workflow_id,
            execution_context_shard=updated_shard,
            execution_bundle=execution_bundle,
        )
    return _enriched_submission(repository, recorded)


def get_submission(
    *,
    run_id: str,
    workflow_id: str,
    submission_id: str | None = None,
    job_label: str | None = None,
    conn=None,
) -> dict[str, Any]:
    active_conn, repository = _repo(conn)
    del active_conn
    row = _resolve_submission_target(
        repository,
        run_id=_normalize_text(run_id, field_name="run_id"),
        workflow_id=_normalize_text(workflow_id, field_name="workflow_id"),
        submission_id=_strip_str(submission_id),
        job_label=_strip_str(job_label),
    )
    return _enriched_submission(repository, row)


def review_submission(
    *,
    run_id: str,
    workflow_id: str,
    reviewer_job_label: str,
    submission_id: str | None = None,
    job_label: str | None = None,
    decision: str,
    summary: str,
    notes: str | None = None,
    policy_snapshot_ref: str | None = None,
    target_ref: str | None = None,
    current_head_ref: str | None = None,
    promotion_intent_at: datetime | str | None = None,
    finalized_at: datetime | str | None = None,
    canonical_commit_ref: str | None = None,
    conn=None,
) -> dict[str, Any]:
    active_conn, repository = _repo(conn)
    normalized_run_id = _normalize_text(run_id, field_name="run_id")
    normalized_workflow_id = _normalize_text(workflow_id, field_name="workflow_id")
    normalized_reviewer_job_label = _normalize_text(
        reviewer_job_label,
        field_name="reviewer_job_label",
    )
    target_submission = _resolve_submission_target(
        repository,
        run_id=normalized_run_id,
        workflow_id=normalized_workflow_id,
        submission_id=_strip_str(submission_id),
        job_label=_strip_str(job_label),
    )
    reviewer_job = _current_job_row(
        active_conn,
        run_id=normalized_run_id,
        job_label=normalized_reviewer_job_label,
    )
    reviewer_task_type = str(
        reviewer_job.get("route_task_type") or ""
    ).strip().lower()
    reviewer_role = reviewer_task_type if reviewer_task_type in _REVIEW_ROLE_TASK_TYPES else "review"
    try:
        review = repository.record_review(
            submission_id=str(target_submission["submission_id"]),
            run_id=normalized_run_id,
            workflow_id=normalized_workflow_id,
            reviewer_job_label=normalized_reviewer_job_label,
            reviewer_role=reviewer_role,
            decision=_normalize_text(decision, field_name="decision").lower(),
            summary=_normalize_text(summary, field_name="summary"),
            notes=notes,
            evidence_refs=[],
        )
    except WorkflowSubmissionRepositoryError as exc:
        raise WorkflowSubmissionServiceError(
            exc.reason_code,
            str(exc),
            details=getattr(exc, "details", None),
        ) from exc
    _emit_workflow_event(
        active_conn,
        run_id=normalized_run_id,
        workflow_id=normalized_workflow_id,
        job_label=normalized_reviewer_job_label,
        event_type="workflow.job.submission.reviewed",
        reason_code="workflow_submission.reviewed",
        payload={
            "submission_id": target_submission["submission_id"],
            "review_id": review["review_id"],
            "decision": review["decision"],
            "reviewer_job_label": normalized_reviewer_job_label,
            "reviewer_role": reviewer_role,
        },
    )
    enriched_submission = _enriched_submission(repository, target_submission)
    enriched_review = _serialize_review(review)

    policy_projection = None
    if reviewer_role in _PUBLISH_REVIEW_ROLE_TASK_TYPES:
        normalized_submission = _serialize_submission(target_submission)
        sub_id = str(target_submission["submission_id"])
        verify_refs = _normalize_text_list(normalized_submission.get("verification_artifact_refs"), field_name="verification_artifact_refs")
        derived_head_ref = _git_head_ref(_target_workspace_root(
            active_conn, run_id=normalized_run_id,
            target_job_label=_normalize_text(normalized_submission.get("job_label"), field_name="job_label"),
        ))
        effective_head = ((_normalize_text(current_head_ref, field_name="current_head_ref") if current_head_ref is not None else None)
            or derived_head_ref or str(normalized_submission.get("diff_artifact_ref") or "").strip() or sub_id)
        policy_projection = evaluate_publish_policy(
            active_conn, submission=target_submission, submission_id=sub_id,
            run_id=normalized_run_id, workflow_id=normalized_workflow_id,
            reviewer_job_label=normalized_reviewer_job_label, reviewer_role=reviewer_role,
            review_decision=str(review["decision"]), policy_snapshot_ref=policy_snapshot_ref,
            target_ref=target_ref, current_head_ref=effective_head,
            promotion_intent_at=_optional_datetime(promotion_intent_at, field_name="promotion_intent_at"),
            finalized_at=_optional_datetime(finalized_at, field_name="finalized_at"),
            canonical_commit_ref=canonical_commit_ref,
            proposal_id=f"proposal:{_normalize_text(sub_id, field_name='submission_id')}",
            manifest_hash=_submission_manifest_hash(normalized_submission),
            validation_receipt_ref=next(iter(verify_refs), ""),
            has_verification_refs=bool(verify_refs),
        )

    response = {
        **enriched_review,
        "submission_id": target_submission["submission_id"],
        "submission": enriched_submission,
        "review_timeline": enriched_submission["review_timeline"],
    }
    if policy_projection is not None:
        response["policy"] = policy_projection
    return response


def list_latest_submission_summaries_for_run(
    conn,
    *,
    run_id: str,
) -> dict[str, dict[str, Any]]:
    repository = PostgresWorkflowSubmissionRepository(conn)
    result: dict[str, dict[str, Any]] = {}
    for row in repository.list_latest_submission_summaries_for_run(
        run_id=_normalize_text(run_id, field_name="run_id"),
    ):
        submission = _enriched_submission(repository, row)
        result[str(submission["job_label"])] = submission
    return result


__all__ = [
    "WorkflowSubmissionServiceError",
    "attach_verification_artifact_refs_for_job",
    "capture_submission_baseline_for_job",
    "get_submission",
    "get_submission_for_job_attempt",
    "list_latest_submission_summaries_for_run",
    "review_submission",
    "submit_artifact_bundle",
    "submit_code_change",
    "submit_research_result",
]
