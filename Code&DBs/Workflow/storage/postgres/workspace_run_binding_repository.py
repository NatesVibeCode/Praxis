"""Postgres persistence for workspace/app-manifest run bindings."""

from __future__ import annotations

import json
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_positive_int,
    _require_text,
)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except (TypeError, json.JSONDecodeError):
                return value
    return value


def _normalize_row(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "workspace_run_binding.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    return {key: _normalize_value(value) for key, value in payload.items()}


def _normalize_rows(rows: Any, *, operation: str) -> list[dict[str, Any]]:
    return [_normalize_row(row, operation=operation) for row in (rows or [])]


def record_manifest_run_binding(
    conn: Any,
    *,
    manifest_id: str,
    workflow_id: str,
    run_id: str,
    operation_receipt_id: str | None = None,
    dispatched_by: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_manifest_id = _require_text(manifest_id, field_name="manifest_id")
    normalized_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    normalized_run_id = _require_text(run_id, field_name="run_id")
    normalized_metadata = dict(_require_mapping(metadata or {}, field_name="metadata"))
    normalized_dispatched_by = _optional_text(dispatched_by, field_name="dispatched_by") or "workspace.compose"
    normalized_receipt_id = _optional_text(operation_receipt_id, field_name="operation_receipt_id")
    manifest_exists = conn.fetchrow(
        "SELECT 1 FROM app_manifests WHERE id = $1 LIMIT 1",
        normalized_manifest_id,
    )
    if manifest_exists is None:
        raise PostgresWriteError(
            "workspace_run_binding.manifest_not_found",
            f"manifest {normalized_manifest_id} was not found",
            details={
                "manifest_id": normalized_manifest_id,
                "workflow_id": normalized_workflow_id,
                "run_id": normalized_run_id,
            },
        )

    row = conn.fetchrow(
        """
        WITH inserted AS (
            INSERT INTO manifest_run_bindings (
                manifest_id,
                workflow_id,
                run_id,
                operation_receipt_id,
                dispatched_by,
                metadata
            )
            SELECT $1, wr.workflow_id, wr.run_id, $4::uuid, $5, $6::jsonb
              FROM workflow_runs AS wr
             WHERE wr.run_id = $3
               AND wr.workflow_id = $2
            ON CONFLICT (manifest_id, run_id) DO NOTHING
            RETURNING *
        )
        SELECT *
          FROM inserted
        UNION ALL
        SELECT *
          FROM manifest_run_bindings
         WHERE manifest_id = $1
           AND run_id = $3
         LIMIT 1
        """,
        normalized_manifest_id,
        normalized_workflow_id,
        normalized_run_id,
        normalized_receipt_id,
        normalized_dispatched_by,
        _encode_jsonb(normalized_metadata, field_name="metadata"),
    )
    if row is None:
        run_row = conn.fetchrow(
            "SELECT workflow_id FROM workflow_runs WHERE run_id = $1 LIMIT 1",
            normalized_run_id,
        )
        details = {
            "manifest_id": normalized_manifest_id,
            "workflow_id": normalized_workflow_id,
            "run_id": normalized_run_id,
        }
        if run_row is None:
            raise PostgresWriteError(
                "workspace_run_binding.run_not_found",
                f"run {normalized_run_id} was not found",
                details=details,
            )
        raise PostgresWriteError(
            "workspace_run_binding.workflow_mismatch",
            f"run {normalized_run_id} is not owned by workflow {normalized_workflow_id}",
            details={**details, "actual_workflow_id": str(run_row["workflow_id"])},
        )
    return _normalize_row(row, operation="record_manifest_run_binding")


def list_manifest_run_bindings(
    conn: Any,
    *,
    manifest_id: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    normalized_manifest_id = _require_text(manifest_id, field_name="manifest_id")
    normalized_limit = _require_positive_int(limit, field_name="limit")
    rows = conn.execute(
        """
        SELECT
            b.manifest_id,
            b.workflow_id,
            b.run_id,
            b.operation_receipt_id::text AS operation_receipt_id,
            b.dispatched_at,
            b.dispatched_by,
            b.metadata,
            wr.current_state AS run_status,
            wr.terminal_reason_code,
            wr.requested_at,
            wr.started_at,
            wr.finished_at,
            latest.receipt_id AS latest_receipt_id,
            latest.status AS latest_receipt_status,
            latest.failure_code AS latest_failure_code,
            latest.finished_at AS latest_receipt_at
          FROM manifest_run_bindings AS b
          JOIN workflow_runs AS wr
            ON wr.run_id = b.run_id
          LEFT JOIN LATERAL (
                SELECT receipt_id, status, failure_code, finished_at
                  FROM receipts
                 WHERE run_id = b.run_id
                 ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST, receipt_id DESC
                 LIMIT 1
          ) AS latest ON TRUE
         WHERE b.manifest_id = $1
         ORDER BY b.dispatched_at DESC, b.run_id DESC
         LIMIT $2
        """,
        normalized_manifest_id,
        normalized_limit,
    )
    return _normalize_rows(rows, operation="list_manifest_run_bindings")


def list_manifest_receipts(
    conn: Any,
    *,
    manifest_id: str,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_manifest_id = _require_text(manifest_id, field_name="manifest_id")
    normalized_limit = _require_positive_int(limit, field_name="limit")
    params: list[Any] = [normalized_manifest_id]
    clauses = ["b.manifest_id = $1"]
    idx = 2
    if status:
        clauses.append(f"r.status = ${idx}")
        params.append(_require_text(status, field_name="status"))
        idx += 1
    params.append(normalized_limit)
    rows = conn.execute(
        """
        SELECT
            b.manifest_id,
            b.dispatched_at,
            b.dispatched_by,
            b.operation_receipt_id::text AS operation_receipt_id,
            wr.current_state AS run_status,
            wr.terminal_reason_code,
            r.receipt_id,
            r.workflow_id,
            r.run_id,
            r.request_id,
            r.node_id,
            r.attempt_no,
            r.started_at,
            r.finished_at,
            r.executor_type,
            r.status,
            r.inputs,
            r.outputs,
            r.artifacts,
            r.failure_code,
            r.decision_refs
          FROM receipts AS r
          JOIN manifest_run_bindings AS b
            ON b.run_id = r.run_id
          LEFT JOIN workflow_runs AS wr
            ON wr.run_id = r.run_id
         WHERE """ + " AND ".join(clauses) + f"""
         ORDER BY COALESCE(r.finished_at, r.started_at) DESC NULLS LAST, r.receipt_id DESC
         LIMIT ${idx}
        """,
        *params,
    )
    return _normalize_rows(rows, operation="list_manifest_receipts")
