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
from typing import Any

from contracts.data_contracts import data_job_digest, normalize_data_job
from core.data_ops import (
    aggregate_records,
    dedupe_records,
    export_records,
    filter_records,
    join_records,
    merge_records,
    normalize_records,
    profile_records,
    redact_records,
    reconcile_records,
    split_records,
    sort_records,
    sync_records,
    transform_records,
    validate_records,
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


def execute_data_job(
    payload: dict[str, Any],
    *,
    default_operation: str | None = None,
    workspace_root: str | Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute a deterministic data job and return a machine-readable receipt."""

    job = normalize_data_job(payload, default_operation=default_operation)
    workspace = _resolve_workspace_root(job, workspace_root=workspace_root)
    source_records, source_authority = _load_records(job["input"], workspace_root=workspace)
    secondary_authority: dict[str, Any] | None = None
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
    elif job["operation"] == "redact":
        result = redact_records(source_records, job["redactions"])
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
    elif job["operation"] == "sync":
        secondary_records, secondary_authority = _load_records(job["secondary_input"], workspace_root=workspace)
        result = sync_records(
            source_records,
            secondary_records,
            keys=list(job["keys"]),
            compare_fields=list(job["compare_fields"]),
            mode=str(job["sync_mode"] or "upsert"),
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
        "record_authority": {
            "input": source_authority,
            **({"secondary_input": secondary_authority} if secondary_authority else {}),
        },
        "stats": result.get("stats", {}),
        "output": output_write,
        "warnings": [],
        "errors": [],
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }

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
    if "partitions" in result:
        receipt["partition_counts"] = dict(result.get("partition_counts") or {})
        receipt["partitions_preview"] = _partition_preview(result["partitions"])

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
