"""Deterministic data-plane runtime.

The runtime owns file IO, workspace-boundary enforcement, receipts, and
workflow-spec generation for deterministic data jobs.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import uuid
from typing import Any

from contracts.data_contracts import data_job_digest, normalize_data_job
from core.data_ops import (
    aggregate_records,
    apply_plan_records,
    backfill_records,
    checkpoint_records,
    dedupe_records,
    dead_letter_records,
    export_records,
    filter_records,
    join_records,
    merge_records,
    normalize_records,
    plan_digest,
    plan_summary,
    profile_records,
    redact_records,
    repair_records,
    repair_loop_records,
    reconcile_records,
    replay_records,
    split_records,
    sort_records,
    sync_records,
    transform_records,
    validate_records,
)
from runtime.control_plane_manifests import (
    ControlPlaneManifestBoundaryError,
    DATA_APPROVAL_MANIFEST_TYPE,
    DATA_PLAN_MANIFEST_TYPE,
    create_data_approval_manifest,
    create_data_plan_manifest,
    extract_approval_payload,
    extract_plan_payload,
    load_control_plane_manifest,
    transition_data_plan_status,
)


class DataRuntimeBoundaryError(RuntimeError):
    """Raised when a deterministic data job crosses an explicit boundary."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = details or {}


def _raise_control_manifest_boundary(exc: ControlPlaneManifestBoundaryError) -> None:
    raise DataRuntimeBoundaryError(
        exc.reason_code,
        str(exc),
        details=dict(exc.details),
    ) from exc


def _default_workspace_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _json_clone(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_clone(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_clone(item) for item in value]
    return value


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _resolve_workspace_root(job: dict[str, Any], *, workspace_root: str | Path | None = None) -> Path:
    chosen = Path(workspace_root or job.get("workspace_root") or _default_workspace_root())
    resolved = chosen.expanduser().resolve()
    if not resolved.is_dir():
        raise DataRuntimeBoundaryError(
            "data.workspace_root_missing",
            f"workspace root does not exist: {resolved}",
        )
    return resolved


def _resolve_path(root: Path, path_value: str, *, field_name: str) -> Path:
    candidate = Path(path_value).expanduser()
    resolved = candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()
    if not _is_within(resolved, root):
        raise DataRuntimeBoundaryError(
            "data.workspace_boundary_violation",
            f"{field_name} must stay inside workspace root",
            details={"workspace_root": str(root), field_name: str(resolved)},
        )
    return resolved


def _infer_format(path: Path | None, explicit_format: str | None) -> str:
    if explicit_format:
        return explicit_format.strip().lower()
    if path is None:
        return "json"
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix == ".tsv":
        return "tsv"
    if suffix == ".jsonl":
        return "jsonl"
    return "json"


def _coerce_records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise DataRuntimeBoundaryError(
            "data.input.records_invalid",
            "inline records input must be a list of objects",
        )
    records: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise DataRuntimeBoundaryError(
                "data.input.row_invalid",
                f"inline record at index {index} must be an object",
            )
        records.append({str(key): _json_clone(field_value) for key, field_value in item.items()})
    return records


def _load_records(source: dict[str, Any], *, workspace_root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if "records" in source:
        records = _coerce_records(source.get("records"))
        return records, {"kind": "inline_records", "format": "json"}

    path_text = str(source.get("path") or "").strip()
    if not path_text:
        raise DataRuntimeBoundaryError(
            "data.input.path_required",
            "data input source must provide path or records",
        )
    input_path = _resolve_path(workspace_root, path_text, field_name="input_path")
    if not input_path.is_file():
        raise DataRuntimeBoundaryError(
            "data.input.missing",
            f"input file does not exist: {input_path}",
        )

    fmt = _infer_format(input_path, str(source.get("format") or "").strip() or None)
    text = input_path.read_text(encoding="utf-8")
    if fmt == "json":
        parsed = json.loads(text)
        if isinstance(parsed, dict) and isinstance(parsed.get("records"), list):
            parsed = parsed["records"]
        records = _coerce_records(parsed)
    elif fmt == "jsonl":
        rows = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise DataRuntimeBoundaryError(
                    "data.input.row_invalid",
                    f"jsonl row {line_number} must be an object",
                )
            rows.append(payload)
        records = _coerce_records(rows)
    elif fmt in {"csv", "tsv"}:
        delimiter = "," if fmt == "csv" else "\t"
        reader = csv.DictReader(text.splitlines(), delimiter=delimiter)
        records = _coerce_records(list(reader))
    else:
        raise DataRuntimeBoundaryError(
            "data.input.unsupported_format",
            f"unsupported input format: {fmt}",
        )
    return records, {"kind": "file", "path": str(input_path), "format": fmt}


def _load_checkpoint(source: dict[str, Any], *, workspace_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    if not source:
        return {}, {}
    if "path" in source:
        checkpoint_path = _resolve_path(workspace_root, str(source["path"]), field_name="checkpoint_path")
        if not checkpoint_path.is_file():
            raise DataRuntimeBoundaryError(
                "data.checkpoint.missing",
                f"checkpoint file does not exist: {checkpoint_path}",
            )
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("checkpoint"), dict):
            payload = payload["checkpoint"]
        if not isinstance(payload, dict):
            raise DataRuntimeBoundaryError(
                "data.checkpoint.invalid",
                "checkpoint payload must be a JSON object",
            )
        return _json_clone(payload), {"kind": "file", "path": str(checkpoint_path), "format": "json"}
    return _json_clone(source), {"kind": "inline_checkpoint"}


def _control_manifest_authority(record: dict[str, Any]) -> dict[str, Any]:
    manifest = dict(record.get("manifest") or {})
    return {
        "kind": "manifest",
        "manifest_id": str(record.get("id") or ""),
        "manifest_family": str(manifest.get("manifest_family") or ""),
        "manifest_type": str(manifest.get("manifest_type") or ""),
        "status": str(record.get("status") or manifest.get("status") or ""),
        "version": int(record.get("version") or 1),
    }


def _load_control_manifest_record(
    *,
    manifest_id: str,
    expected_type: str,
    pg_conn: Any | None,
) -> dict[str, Any]:
    if pg_conn is None:
        raise DataRuntimeBoundaryError(
            "data.control_manifest.backend_unavailable",
            f"{expected_type} requires a Postgres-backed manifest registry",
            details={"manifest_id": manifest_id, "manifest_type": expected_type},
        )
    try:
        return load_control_plane_manifest(
            pg_conn,
            manifest_id=manifest_id,
            expected_type=expected_type,
        )
    except ControlPlaneManifestBoundaryError as exc:
        _raise_control_manifest_boundary(exc)


def _plan_manifest_id_from_source(source: dict[str, Any]) -> str:
    return str(source.get("manifest_id") or source.get("plan_manifest_id") or "").strip()


def _approval_manifest_id_from_source(source: dict[str, Any]) -> str:
    return str(source.get("manifest_id") or source.get("approval_manifest_id") or "").strip()


def _load_plan(
    source: dict[str, Any],
    *,
    workspace_root: Path,
    pg_conn: Any | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not source:
        return {}, {}
    manifest_id = _plan_manifest_id_from_source(source)
    if manifest_id:
        record = _load_control_manifest_record(
            manifest_id=manifest_id,
            expected_type=DATA_PLAN_MANIFEST_TYPE,
            pg_conn=pg_conn,
        )
        return extract_plan_payload(record), _control_manifest_authority(record)
    if "path" in source:
        plan_path = _resolve_path(workspace_root, str(source["path"]), field_name="plan_path")
        if not plan_path.is_file():
            raise DataRuntimeBoundaryError(
                "data.plan.missing",
                f"plan file does not exist: {plan_path}",
            )
        payload = json.loads(plan_path.read_text(encoding="utf-8"))
        manifest_id = str(payload.get("plan_manifest_id") or payload.get("manifest_id") or "").strip()
        if manifest_id and pg_conn is not None:
            record = _load_control_manifest_record(
                manifest_id=manifest_id,
                expected_type=DATA_PLAN_MANIFEST_TYPE,
                pg_conn=pg_conn,
            )
            return extract_plan_payload(record), _control_manifest_authority(record)
        if isinstance(payload, dict) and isinstance(payload.get("plan"), dict):
            payload = payload["plan"]
        if not isinstance(payload, dict):
            raise DataRuntimeBoundaryError(
                "data.plan.invalid",
                "plan payload must be a JSON object",
            )
        return _json_clone(payload), {"kind": "file", "path": str(plan_path), "format": "json"}
    payload = dict(source)
    manifest_id = _plan_manifest_id_from_source(payload)
    if manifest_id and pg_conn is not None:
        record = _load_control_manifest_record(
            manifest_id=manifest_id,
            expected_type=DATA_PLAN_MANIFEST_TYPE,
            pg_conn=pg_conn,
        )
        return extract_plan_payload(record), _control_manifest_authority(record)
    if isinstance(payload.get("plan"), dict):
        payload = dict(payload["plan"])
    return _json_clone(payload), {"kind": "inline_plan"}


def _load_approval(
    source: dict[str, Any],
    *,
    workspace_root: Path,
    pg_conn: Any | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not source:
        return {}, {}
    manifest_id = _approval_manifest_id_from_source(source)
    if manifest_id:
        record = _load_control_manifest_record(
            manifest_id=manifest_id,
            expected_type=DATA_APPROVAL_MANIFEST_TYPE,
            pg_conn=pg_conn,
        )
        return extract_approval_payload(record), _control_manifest_authority(record)
    if "path" in source:
        approval_path = _resolve_path(workspace_root, str(source["path"]), field_name="approval_path")
        if not approval_path.is_file():
            raise DataRuntimeBoundaryError(
                "data.approval.missing",
                f"approval file does not exist: {approval_path}",
            )
        payload = json.loads(approval_path.read_text(encoding="utf-8"))
        manifest_id = str(payload.get("approval_manifest_id") or payload.get("manifest_id") or "").strip()
        if manifest_id and pg_conn is not None:
            record = _load_control_manifest_record(
                manifest_id=manifest_id,
                expected_type=DATA_APPROVAL_MANIFEST_TYPE,
                pg_conn=pg_conn,
            )
            return extract_approval_payload(record), _control_manifest_authority(record)
        if isinstance(payload, dict) and isinstance(payload.get("approval"), dict):
            payload = payload["approval"]
        if not isinstance(payload, dict):
            raise DataRuntimeBoundaryError(
                "data.approval.invalid",
                "approval payload must be a JSON object",
            )
        return _json_clone(payload), {"kind": "file", "path": str(approval_path), "format": "json"}
    payload = dict(source)
    manifest_id = _approval_manifest_id_from_source(payload)
    if manifest_id and pg_conn is not None:
        record = _load_control_manifest_record(
            manifest_id=manifest_id,
            expected_type=DATA_APPROVAL_MANIFEST_TYPE,
            pg_conn=pg_conn,
        )
        return extract_approval_payload(record), _control_manifest_authority(record)
    if isinstance(payload.get("approval"), dict):
        payload = dict(payload["approval"])
    return _json_clone(payload), {"kind": "inline_approval"}


def _build_approval_manifest(
    plan: dict[str, Any],
    *,
    plan_manifest_id: str | None,
    approved_by: str,
    approval_reason: str,
) -> dict[str, Any]:
    approval = {
        "plan_digest": plan_digest(plan),
        "plan_summary": plan_summary(plan),
        "approved_by": approved_by,
        "approval_reason": approval_reason,
        "approved_at": datetime.now(timezone.utc).isoformat(),
    }
    if plan_manifest_id:
        approval["plan_manifest_id"] = plan_manifest_id
    return approval


def _serialize_records(records: list[dict[str, Any]], *, fmt: str) -> str:
    normalized_format = fmt.strip().lower()
    if normalized_format == "json":
        return json.dumps(records, indent=2, default=str) + "\n"
    if normalized_format == "jsonl":
        return "\n".join(json.dumps(record, default=str) for record in records) + "\n"
    if normalized_format in {"csv", "tsv"}:
        fieldnames: list[str] = sorted({key for record in records for key in record})
        delimiter = "," if normalized_format == "csv" else "\t"
        from io import StringIO

        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        for record in records:
            writer.writerow({field: record.get(field, "") for field in fieldnames})
        return buffer.getvalue()
    raise DataRuntimeBoundaryError(
        "data.output.unsupported_format",
        f"unsupported output format: {fmt}",
    )


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "data-job"


def _result_preview(records: list[dict[str, Any]], *, limit: int = 20) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "record_count": len(records),
        "records_preview": [_json_clone(item) for item in records[:limit]],
        "records_truncated": len(records) > limit,
    }
    if len(records) <= 200:
        payload["records"] = [_json_clone(item) for item in records]
    return payload


def _format_extension(fmt: str) -> str:
    normalized = fmt.strip().lower()
    if normalized in {"json", "jsonl", "csv", "tsv"}:
        return normalized
    raise DataRuntimeBoundaryError(
        "data.output.unsupported_format",
        f"unsupported output format: {fmt}",
    )


def _partition_preview(
    partitions: dict[str, list[dict[str, Any]]],
    *,
    bucket_limit: int = 10,
    row_limit: int = 5,
) -> dict[str, Any]:
    previews: dict[str, Any] = {}
    for index, (name, rows) in enumerate(sorted(partitions.items(), key=lambda item: item[0])):
        if index >= bucket_limit:
            break
        previews[name] = {
            "record_count": len(rows),
            "records_preview": [_json_clone(row) for row in rows[:row_limit]],
            "records_truncated": len(rows) > row_limit,
        }
    return previews


def _maybe_write_output(
    result: dict[str, Any],
    *,
    operation: str,
    output: dict[str, Any],
    workspace_root: Path,
    dry_run: bool,
) -> dict[str, Any]:
    path_value = str(output.get("path") or "").strip()
    if not path_value:
        return {}
    output_path = _resolve_path(workspace_root, path_value, field_name="output_path")
    fmt = _infer_format(output_path, str(output.get("format") or "").strip() or None)
    if "partitions" in result:
        target_dir = output_path if not output_path.suffix else output_path.parent / output_path.stem
        extension = _format_extension(fmt)
        files = [
            {
                "bucket": name,
                "path": str(target_dir / f"{_slugify(name)}.{extension}"),
                "record_count": len(rows),
            }
            for name, rows in sorted(result["partitions"].items(), key=lambda item: item[0])
        ]
        if dry_run:
            return {
                "path": str(target_dir),
                "format": fmt,
                "kind": "partition_directory",
                "written": False,
                "dry_run": True,
                "files": files,
            }
        target_dir.mkdir(parents=True, exist_ok=True)
        for item in files:
            bucket_rows = list(result["partitions"].get(item["bucket"]) or [])
            Path(item["path"]).write_text(_serialize_records(bucket_rows, fmt=fmt), encoding="utf-8")
        return {
            "path": str(target_dir),
            "format": fmt,
            "kind": "partition_directory",
            "written": True,
            "files": files,
        }
    if dry_run:
        return {"path": str(output_path), "format": fmt, "written": False, "dry_run": True}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if "records" in result:
        output_path.write_text(_serialize_records(result["records"], fmt=fmt), encoding="utf-8")
    else:
        output_path.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    return {"path": str(output_path), "format": fmt, "written": True}


def _maybe_write_receipt(
    receipt: dict[str, Any],
    *,
    output: dict[str, Any],
    workspace_root: Path,
    dry_run: bool,
) -> dict[str, Any]:
    receipt_path_value = str(output.get("receipt_path") or "").strip()
    if not receipt_path_value:
        return {}
    receipt_path = _resolve_path(workspace_root, receipt_path_value, field_name="receipt_path")
    if dry_run:
        return {"path": str(receipt_path), "written": False, "dry_run": True}
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, default=str) + "\n", encoding="utf-8")
    return {"path": str(receipt_path), "written": True}


def _persist_data_job_receipt(
    *,
    job: dict[str, Any],
    receipt: dict[str, Any],
    workspace_root: Path,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    """Project deterministic data jobs into the canonical receipt store."""
    try:
        from runtime.receipt_store import write_receipt
    except Exception:
        return

    errors = list(receipt.get("errors") or [])
    first_error = ""
    if errors:
        first = errors[0]
        if isinstance(first, dict):
            first_error = str(
                first.get("error_code")
                or first.get("code")
                or first.get("reason_code")
                or first.get("error")
                or ""
            ).strip()
        else:
            first_error = str(first).strip()

    payload = {
        "receipt_id": f"data_receipt:{uuid.uuid4().hex}",
        "workflow_id": f"data:{job['job_name']}",
        "run_id": f"data_run:{uuid.uuid4().hex}",
        "request_id": f"data_request:{uuid.uuid4().hex}",
        "label": job["job_name"],
        "job_label": job["job_name"],
        "agent_slug": f"integration/praxis_data/{job['operation']}",
        "provider_slug": "praxis",
        "model_slug": "data-plane",
        "status": "succeeded" if receipt.get("ok") else "failed",
        "failure_code": first_error,
        "attempt_no": 1,
        "started_at": started_at,
        "finished_at": finished_at,
        "latency_ms": max(int((finished_at - started_at).total_seconds() * 1000), 0),
        "input_tokens": 0,
        "output_tokens": 0,
        "cost_usd": 0.0,
        "outputs": {
            "ok": bool(receipt.get("ok")),
            "stats": _json_clone(receipt.get("stats") or {}),
            "output": _json_clone(receipt.get("output") or {}),
            "warnings": _json_clone(receipt.get("warnings") or []),
            "errors": _json_clone(errors),
            "executed_at": receipt.get("executed_at"),
            "job_digest": receipt.get("job_digest"),
        },
        "artifacts": {
            **({"output": _json_clone(receipt.get("output"))} if receipt.get("output") else {}),
            **({"receipt": _json_clone(receipt.get("receipt"))} if receipt.get("receipt") else {}),
        },
        "workspace_root": str(workspace_root),
    }
    try:
        write_receipt(payload)
    except Exception:
        pass


def _reconcile_plan_manifest_name(job: dict[str, Any], *, digest: str) -> str:
    return f"{job['job_name']} plan {digest[:12]}"


def _approval_manifest_name(job: dict[str, Any], *, digest: str) -> str:
    return f"{job['job_name']} approval {digest[:12]}"


def execute_data_job(
    payload: dict[str, Any],
    *,
    default_operation: str | None = None,
    workspace_root: str | Path | None = None,
    pg_conn: Any | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute a deterministic data job and return a machine-readable receipt."""

    started_at = datetime.now(timezone.utc)
    job = normalize_data_job(payload, default_operation=default_operation)
    workspace = _resolve_workspace_root(job, workspace_root=workspace_root)
    source_records: list[dict[str, Any]] = []
    source_authority: dict[str, Any] | None = None
    if job.get("input"):
        source_records, source_authority = _load_records(job["input"], workspace_root=workspace)
    secondary_authority: dict[str, Any] | None = None
    checkpoint_authority: dict[str, Any] | None = None
    plan_authority: dict[str, Any] | None = None
    approval_authority: dict[str, Any] | None = None
    plan_manifest_record: dict[str, Any] | None = None
    approval_manifest_record: dict[str, Any] | None = None
    result: dict[str, Any]

    if job["operation"] == "parse":
        result = {"records": source_records, "stats": profile_records(source_records)}
    elif job["operation"] == "profile":
        result = {"stats": profile_records(source_records)}
    elif job["operation"] == "filter":
        result = filter_records(
            source_records,
            predicates=list(job["predicates"]),
            predicate_mode=str(job["predicate_mode"] or "all"),
        )
    elif job["operation"] == "sort":
        result = sort_records(
            source_records,
            sort_spec=list(job["sort"]),
        )
    elif job["operation"] == "normalize":
        result = normalize_records(source_records, job["rules"])
    elif job["operation"] == "repair":
        result = repair_records(
            source_records,
            repairs=job["repairs"],
            predicates=list(job["predicates"]),
            predicate_mode=str(job["predicate_mode"] or "all"),
            drop_fields=list(job["drop_fields"]),
        )
    elif job["operation"] == "repair_loop":
        result = repair_loop_records(
            source_records,
            repairs=job["repairs"],
            backfill=job["backfill"],
            rules=job["rules"],
            schema=job["schema"],
            checks=list(job["checks"]),
            predicates=list(job["predicates"]),
            predicate_mode=str(job["predicate_mode"] or "all"),
            drop_fields=list(job["drop_fields"]),
            keys=list(job["keys"]),
            max_passes=int(job.get("max_passes") or 3),
        )
    elif job["operation"] == "backfill":
        result = backfill_records(
            source_records,
            backfill=job["backfill"],
            predicates=list(job["predicates"]),
            predicate_mode=str(job["predicate_mode"] or "all"),
        )
    elif job["operation"] == "redact":
        result = redact_records(source_records, job["redactions"])
    elif job["operation"] == "checkpoint":
        result = checkpoint_records(
            source_records,
            keys=list(job["keys"]),
            cursor_field=job.get("cursor_field"),
        )
    elif job["operation"] == "replay":
        checkpoint_payload, checkpoint_authority = _load_checkpoint(job["checkpoint"], workspace_root=workspace)
        result = replay_records(
            source_records,
            cursor_field=str(job.get("cursor_field") or checkpoint_payload.get("cursor_field") or ""),
            after=job.get("after", None) if job.get("after", None) is not None else checkpoint_payload.get("watermark", checkpoint_payload.get("cursor_max")),
            before=job.get("before"),
        )
    elif job["operation"] == "approve":
        plan_payload, plan_authority = _load_plan(job["plan"], workspace_root=workspace, pg_conn=pg_conn)
        plan_manifest_id = str(plan_authority.get("manifest_id") or "").strip() if plan_authority else ""
        if pg_conn is not None and not plan_manifest_id:
            try:
                plan_manifest_record = create_data_plan_manifest(
                    pg_conn,
                    plan=plan_payload,
                    compare_fields=list(job.get("compare_fields") or []),
                    job=job,
                    workspace_root=str(workspace),
                    workspace_ref=job.get("workspace_ref"),
                    scope_ref=job.get("scope_ref"),
                    name=_reconcile_plan_manifest_name(job, digest=plan_digest(plan_payload)),
                    description=f"Deterministic data plan for {job['job_name']}",
                    created_by=str(job.get("approved_by") or "praxis_data"),
                    status="draft",
                )
                plan_manifest_id = str(plan_manifest_record["id"])
                plan_authority = _control_manifest_authority(plan_manifest_record)
            except ControlPlaneManifestBoundaryError as exc:
                _raise_control_manifest_boundary(exc)
        approval_manifest = _build_approval_manifest(
            plan_payload,
            plan_manifest_id=plan_manifest_id or None,
            approved_by=str(job.get("approved_by") or ""),
            approval_reason=str(job.get("approval_reason") or ""),
        )
        if pg_conn is not None and plan_manifest_id:
            try:
                approval_manifest_record = create_data_approval_manifest(
                    pg_conn,
                    plan_manifest_id=plan_manifest_id,
                    plan=plan_payload,
                    approved_by=str(approval_manifest["approved_by"]),
                    approval_reason=str(approval_manifest["approval_reason"]),
                    approved_at=str(approval_manifest["approved_at"]),
                    workspace_ref=job.get("workspace_ref"),
                    scope_ref=job.get("scope_ref"),
                    name=_approval_manifest_name(job, digest=str(approval_manifest["plan_digest"])),
                    description=f"Deterministic approval for {job['job_name']}",
                    created_by=str(job.get("approved_by") or "praxis_data"),
                    status="approved",
                )
                transition_data_plan_status(
                    pg_conn,
                    manifest_id=plan_manifest_id,
                    to_status="approved",
                    changed_by=str(job.get("approved_by") or "praxis_data"),
                    change_description="Approved data plan",
                )
                plan_authority = {
                    **(plan_authority or {}),
                    "kind": "manifest",
                    "manifest_id": plan_manifest_id,
                    "manifest_family": "control_plane",
                    "manifest_type": DATA_PLAN_MANIFEST_TYPE,
                    "status": "approved",
                }
                approval_authority = _control_manifest_authority(approval_manifest_record)
            except ControlPlaneManifestBoundaryError as exc:
                _raise_control_manifest_boundary(exc)
        result = {
            "approval": approval_manifest,
            "plan": _json_clone(plan_payload),
            "plan_digest": approval_manifest["plan_digest"],
            "plan_summary": dict(approval_manifest["plan_summary"]),
            **({"plan_manifest_id": plan_manifest_id} if plan_manifest_id else {}),
            **({"approval_manifest_id": str(approval_manifest_record["id"])} if approval_manifest_record else {}),
            "stats": {
                "approved": True,
                **dict(approval_manifest["plan_summary"]),
            },
        }
    elif job["operation"] == "apply":
        plan_payload, plan_authority = _load_plan(job["plan"], workspace_root=workspace, pg_conn=pg_conn)
        approval_payload, approval_authority = _load_approval(job["approval"], workspace_root=workspace, pg_conn=pg_conn)
        secondary_records, secondary_authority = _load_records(job["secondary_input"], workspace_root=workspace)
        result = apply_plan_records(
            secondary_records,
            plan=plan_payload,
            keys=list(job["keys"]),
            approval=approval_payload,
        )
        plan_manifest_id = str(
            (plan_authority or {}).get("manifest_id")
            or approval_payload.get("plan_manifest_id")
            or ""
        ).strip()
        approval_manifest_id = str((approval_authority or {}).get("manifest_id") or "").strip()
        if pg_conn is not None and plan_manifest_id:
            try:
                transition_data_plan_status(
                    pg_conn,
                    manifest_id=plan_manifest_id,
                    to_status="applied",
                    changed_by=str(approval_payload.get("approved_by") or "praxis_data"),
                    change_description="Applied approved data plan",
                )
                if plan_authority:
                    plan_authority = {
                        **plan_authority,
                        "status": "applied",
                    }
            except ControlPlaneManifestBoundaryError as exc:
                _raise_control_manifest_boundary(exc)
        if plan_manifest_id:
            result["plan_manifest_id"] = plan_manifest_id
        if approval_manifest_id:
            result["approval_manifest_id"] = approval_manifest_id
    elif job["operation"] == "validate":
        result = validate_records(
            source_records,
            job["schema"],
            checks=list(job["checks"]),
            keys=list(job["keys"]),
        )
    elif job["operation"] == "transform":
        result = transform_records(source_records, job["mapping"])
    elif job["operation"] == "join":
        secondary_records, secondary_authority = _load_records(job["secondary_input"], workspace_root=workspace)
        result = join_records(
            source_records,
            secondary_records,
            left_keys=list(job["left_keys"] or job["keys"]),
            right_keys=list(job["right_keys"] or job["keys"]),
            join_kind=str(job["join_kind"] or "inner"),
            left_prefix=job.get("left_prefix"),
            right_prefix=job.get("right_prefix"),
        )
    elif job["operation"] == "merge":
        secondary_records, secondary_authority = _load_records(job["secondary_input"], workspace_root=workspace)
        result = merge_records(
            source_records,
            secondary_records,
            keys=list(job["keys"]),
            merge_mode=str(job["merge_mode"] or "full"),
            precedence=str(job["precedence"] or "right"),
        )
    elif job["operation"] == "aggregate":
        result = aggregate_records(
            source_records,
            group_by=list(job["group_by"]),
            aggregations=list(job["aggregations"]),
        )
    elif job["operation"] == "split":
        result = split_records(
            source_records,
            split_by_field=job.get("split_by_field"),
            partitions=list(job["partitions"]),
            split_mode=str(job["split_mode"] or "first_match"),
            include_unmatched=bool(job.get("include_unmatched", True)),
        )
    elif job["operation"] == "export":
        result = export_records(
            source_records,
            fields=list(job["fields"]),
            field_map=job.get("field_map"),
        )
    elif job["operation"] == "dead_letter":
        result = dead_letter_records(
            source_records,
            schema=job["schema"],
            checks=list(job["checks"]),
            predicates=list(job["predicates"]),
            predicate_mode=str(job["predicate_mode"] or "any"),
            keys=list(job["keys"]),
        )
    elif job["operation"] == "dedupe":
        result = dedupe_records(
            source_records,
            keys=list(job["keys"]),
            strategy=str(job["strategy"] or "first"),
            order_field=job.get("order_field"),
        )
    elif job["operation"] == "reconcile":
        secondary_records, secondary_authority = _load_records(job["secondary_input"], workspace_root=workspace)
        result = reconcile_records(
            source_records,
            secondary_records,
            keys=list(job["keys"]),
            compare_fields=list(job["compare_fields"]),
        )
        if pg_conn is not None:
            try:
                plan_manifest_record = create_data_plan_manifest(
                    pg_conn,
                    plan=dict(result["plan"]),
                    compare_fields=list(result.get("compare_fields") or []),
                    job=job,
                    workspace_root=str(workspace),
                    workspace_ref=job.get("workspace_ref"),
                    scope_ref=job.get("scope_ref"),
                    name=_reconcile_plan_manifest_name(job, digest=str(result["plan_digest"])),
                    description=f"Deterministic data plan for {job['job_name']}",
                    created_by="praxis_data",
                    status="draft",
                )
                plan_authority = _control_manifest_authority(plan_manifest_record)
                result["plan_manifest_id"] = str(plan_manifest_record["id"])
            except ControlPlaneManifestBoundaryError as exc:
                _raise_control_manifest_boundary(exc)
    elif job["operation"] == "sync":
        secondary_records, secondary_authority = _load_records(job["secondary_input"], workspace_root=workspace)
        checkpoint_payload = {}
        if job.get("checkpoint"):
            checkpoint_payload, checkpoint_authority = _load_checkpoint(job["checkpoint"], workspace_root=workspace)
        result = sync_records(
            source_records,
            secondary_records,
            keys=list(job["keys"]),
            compare_fields=list(job["compare_fields"]),
            mode=str(job["sync_mode"] or "upsert"),
            batch_size=job.get("batch_size"),
            cursor_field=job.get("cursor_field"),
            checkpoint=checkpoint_payload,
            before=job.get("before"),
        )
    else:
        raise DataRuntimeBoundaryError(
            "data.operation.unsupported",
            f"unsupported data operation: {job['operation']}",
        )

    output_write = _maybe_write_output(
        result,
        operation=job["operation"],
        output=job["output"],
        workspace_root=workspace,
        dry_run=dry_run,
    )

    receipt: dict[str, Any] = {
        "ok": True,
        "schema_version": job["schema_version"],
        "operation": job["operation"],
        "job_name": job["job_name"],
        "job_digest": data_job_digest(job),
        "workspace_root": str(workspace),
        "stats": result.get("stats", {}),
        "output": output_write,
        "warnings": [],
        "errors": [],
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }
    record_authority: dict[str, Any] = {}
    if source_authority:
        record_authority["input"] = source_authority
    if secondary_authority:
        record_authority["secondary_input"] = secondary_authority
    if record_authority:
        receipt["record_authority"] = record_authority
    if checkpoint_authority:
        receipt["checkpoint_authority"] = checkpoint_authority
    if plan_authority:
        receipt["plan_authority"] = plan_authority
    if approval_authority:
        receipt["approval_authority"] = approval_authority

    if "records" in result:
        receipt.update(_result_preview(result["records"]))
    if "duplicate_groups" in result:
        receipt["duplicate_groups"] = _json_clone(result["duplicate_groups"])
    if "conflicts" in result:
        receipt["conflicts"] = _json_clone(result["conflicts"])
    if "violations" in result:
        receipt["violations"] = _json_clone(result["violations"])
    if "plan" in result:
        receipt["plan"] = _json_clone(result["plan"])
        receipt["compare_fields"] = list(result.get("compare_fields") or [])
    if "plan_digest" in result:
        receipt["plan_digest"] = str(result["plan_digest"])
    if "plan_summary" in result:
        receipt["plan_summary"] = _json_clone(result["plan_summary"])
    if "plan_manifest_id" in result:
        receipt["plan_manifest_id"] = str(result["plan_manifest_id"])
    if "approval" in result:
        receipt["approval"] = _json_clone(result["approval"])
    if "approval_manifest_id" in result:
        receipt["approval_manifest_id"] = str(result["approval_manifest_id"])
    if "partitions" in result:
        receipt["partition_counts"] = dict(result.get("partition_counts") or {})
        receipt["partitions_preview"] = _partition_preview(result["partitions"])
    if "checkpoint" in result:
        receipt["checkpoint"] = _json_clone(result["checkpoint"])
    if "replay_window" in result:
        receipt["replay_window"] = _json_clone(result["replay_window"])
    if "batch_manifest" in result:
        receipt["batch_manifest"] = _json_clone(result["batch_manifest"])
    if "passes" in result:
        receipt["passes"] = _json_clone(result["passes"])

    finished_at = datetime.now(timezone.utc)
    _persist_data_job_receipt(
        job=job,
        receipt=receipt,
        workspace_root=workspace,
        started_at=started_at,
        finished_at=finished_at,
    )

    receipt_write = _maybe_write_receipt(receipt, output=job["output"], workspace_root=workspace, dry_run=dry_run)
    if receipt_write:
        receipt["receipt"] = receipt_write
    return receipt


def build_data_workflow_spec(
    payload: dict[str, Any],
    *,
    workspace_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build a workflow spec that executes one deterministic data job."""

    job = normalize_data_job(payload, default_operation=str(payload.get("operation") or "").strip().lower() or None)
    workspace = _resolve_workspace_root(job, workspace_root=workspace_root)
    workflow_id = _slugify(f"{job['job_name']}-{job['operation']}")
    job_label = _slugify(job["job_name"])

    integration_args = _json_clone(job)
    integration_args["workspace_root"] = str(workspace)
    return {
        "name": job["job_name"],
        "workflow_id": workflow_id,
        "phase": "execute",
        "outcome_goal": f"Run deterministic data operation {job['operation']}",
        "anti_requirements": [
            "Do not modify unrelated files",
            "Do not read or write outside the declared workspace root",
        ],
        "jobs": [
            {
                "label": job_label,
                "agent": f"integration/praxis_data/{job['operation']}",
                "integration_id": "praxis_data",
                "integration_action": job["operation"],
                "integration_args": integration_args,
            }
        ],
    }


def write_workflow_spec(
    spec: dict[str, Any],
    *,
    workspace_root: str | Path | None = None,
    output_path: str | None = None,
) -> str:
    root = Path(workspace_root or _default_workspace_root()).expanduser().resolve()
    chosen_path = output_path or (
        root / "artifacts" / "workflow" / "data_ops" / f"{_slugify(str(spec.get('workflow_id') or 'data-job'))}.queue.json"
    )
    resolved = _resolve_path(root, str(chosen_path), field_name="workflow_spec_path")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(spec, indent=2, default=str) + "\n", encoding="utf-8")
    return str(resolved)
