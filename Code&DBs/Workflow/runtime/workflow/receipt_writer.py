"""Receipt-writing helpers for unified workflow execution."""

from __future__ import annotations

import json
from io import StringIO
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Callable

from registry.native_runtime_profile_sync import (
    default_native_runtime_profile_ref,
    default_native_workspace_ref,
)
from runtime.receipt_provenance import (
    build_git_provenance,
    build_mutation_provenance,
    build_workspace_provenance,
    build_write_manifest,
    extract_write_paths,
)
from runtime.workflow.verification_runtime import extract_verification_paths
from runtime.workflow._shared import (
    _json_loads_maybe,
    _json_safe,
    _slugify,
    _workflow_run_envelope,
)
from runtime.workflow._routing import _job_touch_entries
from storage.postgres.receipt_repository import PostgresReceiptRepository

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


_ATTEMPTED_VERIFICATION_STATUSES = frozenset({"passed", "failed", "error"})
_DEFAULT_NATIVE_RUNTIME_PROFILE_REF = default_native_runtime_profile_ref()
_DEFAULT_NATIVE_WORKSPACE_REF = default_native_workspace_ref()
_MAX_COMMAND_AGGREGATED_OUTPUT_CHARS = 32_000
_MAX_OUTPUT_ARTIFACT_CHARS = 250_000
_RECURSIVE_OUTPUT_MARKERS = (
    "artifacts/workflow_outputs/",
    "workflow_output_workflow_",
)
_TRANSCRIPT_EVENT_TYPES = frozenset({
    "thread.started",
    "turn.started",
    "turn.completed",
    "item.started",
    "item.completed",
})




def _truncate_text(value: str, limit: int, *, reason: str) -> str:
    if len(value) <= limit:
        return value
    if limit <= 0:
        return f"[truncated: {reason}; removed {len(value)} chars]"
    head = max(limit - 160, 0)
    omitted = len(value) - head
    notice = f"\n[truncated: {reason}; removed {omitted} chars]\n"
    return f"{value[:head]}{notice}"


def _sanitize_command_execution_line(line: str) -> str:
    try:
        payload = json.loads(line)
    except (json.JSONDecodeError, TypeError, ValueError):
        return line
    if not isinstance(payload, dict):
        return line
    item = payload.get("item")
    if not isinstance(item, dict) or item.get("type") != "command_execution":
        return line
    aggregated_output = item.get("aggregated_output")
    if not isinstance(aggregated_output, str) or not aggregated_output:
        return line

    recursive = any(marker in aggregated_output for marker in _RECURSIVE_OUTPUT_MARKERS)
    if recursive:
        sanitized = _truncate_text(
            aggregated_output,
            min(8_000, _MAX_COMMAND_AGGREGATED_OUTPUT_CHARS),
            reason="recursive workflow_output capture",
        )
    elif len(aggregated_output) > _MAX_COMMAND_AGGREGATED_OUTPUT_CHARS:
        sanitized = _truncate_text(
            aggregated_output,
            _MAX_COMMAND_AGGREGATED_OUTPUT_CHARS,
            reason="command output limit",
        )
    else:
        return line

    updated_item = dict(item)
    updated_item["aggregated_output"] = sanitized
    payload = dict(payload)
    payload["item"] = updated_item
    return json.dumps(payload, ensure_ascii=False)


def is_transcript_output(stdout: str) -> bool:
    """Return True when stdout is a streamed event transcript, not a durable artifact."""
    if not stdout:
        return False
    if "thread.started" not in stdout and "item.completed" not in stdout:
        return False

    event_types: set[str] = set()
    scanned = 0
    for raw_line in StringIO(stdout):
        line = raw_line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        event_type = payload.get("type")
        if isinstance(event_type, str):
            scanned += 1
            if event_type in _TRANSCRIPT_EVENT_TYPES:
                event_types.add(event_type)
        if scanned >= 24:
            break
    return (
        "item.completed" in event_types
        and ("thread.started" in event_types or "turn.started" in event_types)
    )


def extract_transcript_text(stdout: str) -> str:
    """Extract agent_message text from a streamed event transcript (JSONL).

    Agents emit their work as structured JSON event streams. Rather than
    discarding the whole transcript, pull out every completed agent_message
    and concatenate them — that is the durable artifact.
    """
    parts: list[str] = []
    for raw_line in StringIO(stdout):
        line = raw_line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("type") == "item.completed":
            item = payload.get("item") or {}
            if isinstance(item, dict) and item.get("type") == "agent_message":
                text = str(item.get("text") or "").strip()
                if text:
                    parts.append(text)
    return "\n\n".join(parts)


def prepare_output_artifact(stdout: str) -> str:
    """Bound workflow-output artifacts so transcripts cannot recursively explode.

    When stdout is a streamed event transcript, extract the agent_message text
    instead of discarding it — that text is the durable debate/research output.
    """
    if not stdout:
        return ""
    if is_transcript_output(stdout):
        extracted = extract_transcript_text(stdout)
        if not extracted:
            return ""
        return _truncate_text(
            extracted,
            _MAX_OUTPUT_ARTIFACT_CHARS,
            reason="workflow output artifact limit",
        )

    sanitized = stdout
    if '"aggregated_output"' in stdout:
        sanitized_lines = [_sanitize_command_execution_line(line) for line in stdout.splitlines()]
        trailing_newline = "\n" if stdout.endswith("\n") else ""
        sanitized = "\n".join(sanitized_lines) + trailing_newline

    return _truncate_text(
        sanitized,
        _MAX_OUTPUT_ARTIFACT_CHARS,
        reason="workflow output artifact limit",
    )


def _job_artifact_basename(prefix: str, run_id: str, job_id: int, label: str, suffix: str) -> str:
    return f"{prefix}_{run_id}_job_{job_id}_{_slugify(label)}{suffix}"




def write_output(repo_root: str, run_id: str, job_id: int, label: str, result: dict[str, Any]) -> str:
    """Write job stdout to artifacts/workflow_outputs/ using the canonical basename."""
    output_dir = Path(repo_root) / "artifacts" / "workflow_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = _job_artifact_basename("workflow_output", run_id, job_id, label, ".md")
    path = output_dir / filename

    stdout = prepare_output_artifact(str(result.get("stdout", "")))
    if stdout:
        path.write_text(stdout, encoding="utf-8")
        return str(path)
    return ""


def write_job_receipt(
    conn: "SyncPostgresConnection",
    run_id: str,
    job_id: int,
    label: str,
    agent_slug: str,
    result: dict[str, Any],
    duration_ms: int,
    *,
    repo_root: str = "",
    output_path: str = "",
    final_status: str | None = None,
    final_error_code: str | None = None,
    verification_summary: Any = None,
    verification_bindings: list[dict[str, Any]] | None = None,
    verification_error: str | None = None,
    failure_classifier: Callable[..., Any] | None = None,
    verification_path_extractor: Callable[[list[dict[str, Any]] | None], list[str]] | None = None,
    submission: Mapping[str, Any] | None = None,
) -> str:
    """Write the canonical workflow job receipt row."""
    now = datetime.now(timezone.utc)
    repository = PostgresReceiptRepository(conn)
    status = final_status or result.get("status", "failed")
    error_code = final_error_code if final_error_code is not None else result.get("error_code", "")
    workflow_row = repository.load_workflow_job_receipt_context(job_id=job_id, run_id=run_id) or {}
    attempt_no = max(1, int(workflow_row.get("attempt") or 1))
    evidence_seq = (int(job_id) * 100) + attempt_no
    transition_seq = evidence_seq
    receipt_id = f"receipt:{run_id}:{job_id}:{attempt_no}"
    workflow_id = str(workflow_row.get("workflow_id") or "")
    request_id = str(workflow_row.get("request_id") or f"req_{run_id}")
    finished_at = workflow_row.get("finished_at") or now
    started_at = workflow_row.get("started_at") or (
        finished_at - timedelta(milliseconds=max(duration_ms, 0))
    )
    envelope = _workflow_run_envelope(workflow_row)
    spec_snapshot = _json_loads_maybe(envelope.get("spec_snapshot"), {}) or {}
    touch_entries = _job_touch_entries(workflow_row)
    workspace_ref = (
        str(envelope.get("workspace_ref") or "").strip()
        or _DEFAULT_NATIVE_WORKSPACE_REF
    )
    runtime_profile_ref = (
        str(envelope.get("runtime_profile_ref") or "").strip()
        or _DEFAULT_NATIVE_RUNTIME_PROFILE_REF
    )
    packet_provenance = _json_safe(
        envelope.get("packet_provenance") or spec_snapshot.get("packet_provenance") or {}
    )
    workspace_root = str(spec_snapshot.get("workdir") or repo_root or "").strip()
    if workspace_root:
        workspace_root = str(Path(workspace_root).resolve())

    failure_classification = None
    if status != "succeeded" and error_code and callable(failure_classifier):
        try:
            failure_classification = failure_classifier(
                error_code=error_code,
                stderr=str(result.get("stderr", "")),
                exit_code=result.get("exit_code"),
            )
        except Exception:
            failure_classification = None

    verification_payload = None
    if verification_summary is not None:
        verification_payload = (
            verification_summary.to_json()
            if hasattr(verification_summary, "to_json")
            else _json_safe(verification_summary)
        )
    safe_verify_bindings = _json_safe(verification_bindings or [])
    path_extractor = verification_path_extractor or extract_verification_paths
    verified_paths = path_extractor(safe_verify_bindings)
    if verification_error:
        verification_status = "error"
    elif verification_payload is not None:
        verification_status = "passed" if verification_payload.get("all_passed") else "failed"
    elif safe_verify_bindings:
        verification_status = "configured"
    else:
        verification_status = "skipped"

    receipt_outputs = {
        "status": status,
        "exit_code": result.get("exit_code"),
        "error_code": error_code,
        "failure_code": error_code,
        "failure_classification": (
            failure_classification.to_dict() if failure_classification is not None else None
        ),
        "duration_ms": duration_ms,
        "token_input": result.get("token_input", 0),
        "token_output": result.get("token_output", 0),
        "cost_usd": result.get("cost_usd", 0.0),
        "stdout_preview": result.get("stdout", "")[:2000],
        "stderr_preview": result.get("stderr", "")[:2000],
        "verification_status": verification_status,
    }
    if verification_payload is not None:
        receipt_outputs["verification"] = verification_payload
    if safe_verify_bindings:
        receipt_outputs["verification_bindings"] = safe_verify_bindings
    if verified_paths and verification_status in _ATTEMPTED_VERIFICATION_STATUSES:
        receipt_outputs["verified_paths"] = verified_paths
    if verification_error:
        receipt_outputs["verification_error"] = verification_error[:500]
    if "workspace_snapshot_cache_hit" in result:
        receipt_outputs["workspace_snapshot_cache_hit"] = bool(
            result.get("workspace_snapshot_cache_hit")
        )
    if isinstance(submission, Mapping):
        submission_id = str(submission.get("submission_id") or "").strip()
        if submission_id:
            receipt_outputs["submission_id"] = submission_id
        result_kind = str(submission.get("result_kind") or "").strip()
        if result_kind:
            receipt_outputs["submission_result_kind"] = result_kind
        comparison_status = str(submission.get("comparison_status") or "").strip()
        if comparison_status:
            receipt_outputs["submission_comparison_status"] = comparison_status
            receipt_outputs["submission_integrity_status"] = comparison_status
        comparison_report = submission.get("comparison_report")
        if comparison_report is not None:
            receipt_outputs["submission_comparison_report"] = _json_safe(comparison_report)
            receipt_outputs["submission_integrity_report"] = _json_safe(comparison_report)
        acceptance_status = str(submission.get("acceptance_status") or "").strip()
        if acceptance_status:
            receipt_outputs["submission_acceptance_status"] = acceptance_status
        acceptance_report = submission.get("acceptance_report")
        if acceptance_report is not None:
            receipt_outputs["submission_acceptance_report"] = _json_safe(acceptance_report)
        measured_summary = submission.get("measured_summary")
        if measured_summary is not None:
            receipt_outputs["submission_measured_summary"] = _json_safe(measured_summary)
        verification_artifact_refs = submission.get("verification_artifact_refs")
        if verification_artifact_refs:
            receipt_outputs["submission_verification_artifact_refs"] = _json_safe(
                verification_artifact_refs
            )

    write_scope = extract_write_paths(
        touch_entries,
        verified_paths,
        spec_snapshot.get("scope_write"),
        spec_snapshot.get("write_scope"),
    )
    if workspace_root:
        receipt_outputs["workspace_provenance"] = build_workspace_provenance(
            workspace_root=workspace_root,
            workspace_ref=workspace_ref,
            runtime_profile_ref=runtime_profile_ref,
            workspace_snapshot_ref=str(result.get("workspace_snapshot_ref") or "").strip() or None,
            packet_provenance=packet_provenance,
        )
        receipt_outputs["git_provenance"] = build_git_provenance(
            workspace_root=workspace_root,
            workspace_ref=workspace_ref,
            runtime_profile_ref=runtime_profile_ref,
            packet_provenance=packet_provenance,
            conn=conn,
        )
    if write_scope:
        receipt_outputs["write_manifest"] = build_write_manifest(
            workspace_root=workspace_root or None,
            write_paths=write_scope,
            source="workflow_unified",
        )
        receipt_outputs["mutation_provenance"] = build_mutation_provenance(
            workspace_root=workspace_root or None,
            write_paths=write_scope,
            touch_entries=touch_entries,
            source="workflow_unified",
        )

    receipt_inputs = {
        "job_id": job_id,
        "job_label": label,
        "agent_slug": agent_slug,
        "attempt": attempt_no,
        "transition_seq": transition_seq,
        "workspace_ref": workspace_ref,
        "runtime_profile_ref": runtime_profile_ref,
    }
    if touch_entries:
        receipt_inputs["touch_keys"] = touch_entries
    if write_scope:
        receipt_inputs["write_scope"] = write_scope
    if workspace_root:
        receipt_inputs["workspace_root"] = workspace_root
    if packet_provenance:
        receipt_inputs["packet_provenance"] = packet_provenance

    receipt_artifacts = {"output_path": output_path} if output_path else {}
    if isinstance(submission, Mapping):
        submission_id = str(submission.get("submission_id") or "").strip()
        if submission_id:
            receipt_artifacts["submission_id"] = submission_id
        diff_artifact_ref = str(submission.get("diff_artifact_ref") or "").strip()
        if diff_artifact_ref:
            receipt_artifacts["submission_diff_artifact_ref"] = diff_artifact_ref
        artifact_refs = submission.get("artifact_refs")
        if artifact_refs:
            receipt_artifacts["submission_artifact_refs"] = _json_safe(artifact_refs)
        verification_artifact_refs = submission.get("verification_artifact_refs")
        if verification_artifact_refs:
            receipt_artifacts["submission_verification_artifact_refs"] = _json_safe(
                verification_artifact_refs
            )

    repository.insert_receipt_if_absent(
        receipt_id=receipt_id,
        workflow_id=workflow_id,
        run_id=run_id,
        request_id=request_id,
        node_id=label,
        attempt_no=attempt_no,
        started_at=started_at,
        finished_at=finished_at,
        evidence_seq=evidence_seq,
        status=status,
        inputs=_json_safe(receipt_inputs),
        outputs=_json_safe(receipt_outputs),
        artifacts=_json_safe(receipt_artifacts),
        failure_code=error_code or None,
    )

    repository.insert_workflow_notification_if_absent(
        run_id=run_id,
        job_label=label,
        spec_name="",
        agent_slug=agent_slug,
        status=status,
        failure_code=error_code or None,
        duration_seconds=duration_ms / 1000.0,
        cpu_percent=result.get("container_cpu_percent"),
        mem_bytes=result.get("container_mem_bytes"),
        created_at=finished_at,
    )
    repository.notify_job_completed(run_id=run_id)

    # Mine constraints from failures — fire-and-forget so it never blocks receipts
    if status == "failed" and error_code:
        try:
            from runtime.constraint_ledger import ConstraintLedger
            ledger = ConstraintLedger(conn)
            ledger.add(
                pattern=error_code,
                constraint_text=f"failure:{error_code}",
                confidence=0.9,
                source_jobs=[label],
            )
        except Exception:
            pass  # never block receipt writing

    return receipt_id
