"""Postgres persistence for task-environment contract authority."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)


def _normalize_row(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "task_environment_contract.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and key.endswith("_json"):
            try:
                payload[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
    return payload


def _normalize_rows(rows: Any, *, operation: str) -> list[dict[str, Any]]:
    return [_normalize_row(row, operation=operation) for row in (rows or [])]


def _timestamp(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise PostgresWriteError(
                "task_environment_contract.invalid_timestamp",
                f"{field_name} must be an ISO timestamp",
                details={"field_name": field_name, "value": value},
            ) from exc
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise PostgresWriteError(
        "task_environment_contract.invalid_timestamp",
        f"{field_name} must be an ISO timestamp",
        details={"field_name": field_name, "value": value},
    )


def persist_task_environment_contract(
    conn: Any,
    *,
    contract: dict[str, Any],
    evaluation_result: dict[str, Any],
    hierarchy_nodes: list[dict[str, Any]] | None = None,
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    contract_record = dict(_require_mapping(contract, field_name="contract"))
    evaluation = dict(_require_mapping(evaluation_result, field_name="evaluation_result"))
    nodes = [dict(_require_mapping(item, field_name="hierarchy_node")) for item in (hierarchy_nodes or [])]
    contract_id = _require_text(contract_record.get("contract_id"), field_name="contract.contract_id")
    revision_id = _require_text(contract_record.get("revision_id"), field_name="contract.revision_id")
    contract_hash = _require_text(contract_record.get("contract_hash"), field_name="contract.contract_hash")
    dependency_hash = _optional_text(contract_record.get("dependency_hash"), field_name="contract.dependency_hash")
    invalid_states = [dict(item) for item in evaluation.get("invalid_states") or [] if isinstance(item, dict)]
    warnings = [dict(item) for item in evaluation.get("warnings") or [] if isinstance(item, dict)]

    head_row = conn.fetchrow(
        """
        INSERT INTO task_environment_contract_heads (
            contract_id,
            task_ref,
            hierarchy_node_id,
            status,
            current_revision_id,
            current_contract_hash,
            dependency_hash,
            owner_ref,
            steward_ref,
            evaluation_status,
            invalid_state_count,
            warning_count,
            contract_json,
            evaluation_result_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb, $15, $16
        )
        ON CONFLICT (contract_id) DO UPDATE SET
            task_ref = EXCLUDED.task_ref,
            hierarchy_node_id = EXCLUDED.hierarchy_node_id,
            status = EXCLUDED.status,
            current_revision_id = EXCLUDED.current_revision_id,
            current_contract_hash = EXCLUDED.current_contract_hash,
            dependency_hash = EXCLUDED.dependency_hash,
            owner_ref = EXCLUDED.owner_ref,
            steward_ref = EXCLUDED.steward_ref,
            evaluation_status = EXCLUDED.evaluation_status,
            invalid_state_count = EXCLUDED.invalid_state_count,
            warning_count = EXCLUDED.warning_count,
            contract_json = EXCLUDED.contract_json,
            evaluation_result_json = EXCLUDED.evaluation_result_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING *
        """,
        contract_id,
        _require_text(contract_record.get("task_ref"), field_name="contract.task_ref"),
        _require_text(contract_record.get("hierarchy_node_id"), field_name="contract.hierarchy_node_id"),
        _require_text(contract_record.get("status"), field_name="contract.status"),
        revision_id,
        contract_hash,
        dependency_hash,
        _optional_text(contract_record.get("owner_ref"), field_name="contract.owner_ref"),
        _optional_text(contract_record.get("steward_ref"), field_name="contract.steward_ref"),
        _require_text(evaluation.get("status"), field_name="evaluation_result.status"),
        len(invalid_states),
        len(warnings),
        _encode_jsonb(contract_record, field_name="contract"),
        _encode_jsonb(evaluation, field_name="evaluation_result"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    revision_row = conn.fetchrow(
        """
        INSERT INTO task_environment_contract_revisions (
            contract_id,
            revision_id,
            revision_no,
            parent_revision_id,
            contract_hash,
            dependency_hash,
            status,
            effective_from,
            effective_to,
            contract_json,
            evaluation_result_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12, $13
        )
        ON CONFLICT (contract_id, revision_id) DO UPDATE SET
            revision_no = EXCLUDED.revision_no,
            parent_revision_id = EXCLUDED.parent_revision_id,
            contract_hash = EXCLUDED.contract_hash,
            dependency_hash = EXCLUDED.dependency_hash,
            status = EXCLUDED.status,
            effective_from = EXCLUDED.effective_from,
            effective_to = EXCLUDED.effective_to,
            contract_json = EXCLUDED.contract_json,
            evaluation_result_json = EXCLUDED.evaluation_result_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref
        RETURNING *
        """,
        contract_id,
        revision_id,
        int(contract_record.get("revision_no") or 1),
        _optional_text(contract_record.get("parent_revision_id"), field_name="contract.parent_revision_id"),
        contract_hash,
        dependency_hash,
        _require_text(contract_record.get("status"), field_name="contract.status"),
        _timestamp(contract_record.get("effective_from"), field_name="contract.effective_from"),
        (
            _timestamp(contract_record.get("effective_to"), field_name="contract.effective_to")
            if contract_record.get("effective_to")
            else None
        ),
        _encode_jsonb(contract_record, field_name="contract"),
        _encode_jsonb(evaluation, field_name="evaluation_result"),
        _optional_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_text(source_ref, field_name="source_ref"),
    )
    conn.execute(
        "DELETE FROM task_environment_hierarchy_nodes WHERE contract_id = $1 AND revision_id = $2",
        contract_id,
        revision_id,
    )
    if nodes:
        conn.execute_many(
            """
            INSERT INTO task_environment_hierarchy_nodes (
                contract_id,
                node_id,
                revision_id,
                parent_node_id,
                node_type,
                node_name,
                status,
                owner_ref,
                steward_ref,
                node_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
            ON CONFLICT (contract_id, node_id, revision_id) DO UPDATE SET
                parent_node_id = EXCLUDED.parent_node_id,
                node_type = EXCLUDED.node_type,
                node_name = EXCLUDED.node_name,
                status = EXCLUDED.status,
                owner_ref = EXCLUDED.owner_ref,
                steward_ref = EXCLUDED.steward_ref,
                node_json = EXCLUDED.node_json
            """,
            [
                (
                    contract_id,
                    _require_text(item.get("node_id"), field_name="hierarchy_node.node_id"),
                    _require_text(item.get("revision_id"), field_name="hierarchy_node.revision_id"),
                    _optional_text(item.get("parent_node_id"), field_name="hierarchy_node.parent_node_id"),
                    _require_text(item.get("node_type"), field_name="hierarchy_node.node_type"),
                    _require_text(item.get("node_name"), field_name="hierarchy_node.node_name"),
                    _require_text(item.get("status"), field_name="hierarchy_node.status"),
                    _optional_text(item.get("owner_ref"), field_name="hierarchy_node.owner_ref"),
                    _optional_text(item.get("steward_ref"), field_name="hierarchy_node.steward_ref"),
                    _encode_jsonb(item, field_name="hierarchy_node"),
                )
                for item in nodes
            ],
        )
    conn.execute("DELETE FROM task_environment_contract_invalid_states WHERE contract_id = $1 AND revision_id = $2", contract_id, revision_id)
    states = [*invalid_states, *warnings]
    if states:
        conn.execute_many(
            """
            INSERT INTO task_environment_contract_invalid_states (
                contract_id,
                revision_id,
                state_index,
                reason_code,
                severity,
                field_ref,
                state_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            ON CONFLICT (contract_id, revision_id, state_index) DO UPDATE SET
                reason_code = EXCLUDED.reason_code,
                severity = EXCLUDED.severity,
                field_ref = EXCLUDED.field_ref,
                state_json = EXCLUDED.state_json
            """,
            [
                (
                    contract_id,
                    revision_id,
                    index,
                    _require_text(item.get("reason_code"), field_name="invalid_state.reason_code"),
                    _require_text(item.get("severity"), field_name="invalid_state.severity"),
                    _optional_text(item.get("field_ref"), field_name="invalid_state.field_ref"),
                    _encode_jsonb(item, field_name="invalid_state"),
                )
                for index, item in enumerate(states)
            ],
        )
    return {
        "contract": _normalize_row(head_row, operation="persist_task_environment_contract.head"),
        "revision": _normalize_row(revision_row, operation="persist_task_environment_contract.revision"),
        "hierarchy_node_count": len(nodes),
        "invalid_state_count": len(invalid_states),
        "warning_count": len(warnings),
    }


def list_task_environment_contracts(
    conn: Any,
    *,
    task_ref: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT
            contract_id,
            task_ref,
            hierarchy_node_id,
            status,
            current_revision_id,
            current_contract_hash,
            dependency_hash,
            owner_ref,
            steward_ref,
            evaluation_status,
            invalid_state_count,
            warning_count,
            observed_by_ref,
            source_ref,
            created_at,
            updated_at
          FROM task_environment_contract_heads
         WHERE ($1::text IS NULL OR task_ref = $1)
           AND ($2::text IS NULL OR status = $2)
         ORDER BY updated_at DESC, created_at DESC
         LIMIT $3
        """,
        _optional_text(task_ref, field_name="task_ref"),
        _optional_text(status, field_name="status"),
        int(limit),
    )
    return _normalize_rows(rows, operation="list_task_environment_contracts")


def load_task_environment_contract(
    conn: Any,
    *,
    contract_id: str,
    include_history: bool = True,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT *
          FROM task_environment_contract_heads
         WHERE contract_id = $1
        """,
        _require_text(contract_id, field_name="contract_id"),
    )
    if row is None:
        return None
    contract = _normalize_row(row, operation="load_task_environment_contract.head")
    if not include_history:
        return contract
    revisions = conn.fetch(
        """
        SELECT *
          FROM task_environment_contract_revisions
         WHERE contract_id = $1
         ORDER BY revision_no, revision_id
        """,
        _require_text(contract_id, field_name="contract_id"),
    )
    nodes = conn.fetch(
        """
        SELECT node_json
          FROM task_environment_hierarchy_nodes
         WHERE contract_id = $1
         ORDER BY node_id, revision_id
        """,
        _require_text(contract_id, field_name="contract_id"),
    )
    states = conn.fetch(
        """
        SELECT state_json
          FROM task_environment_contract_invalid_states
         WHERE contract_id = $1
         ORDER BY revision_id, state_index
        """,
        _require_text(contract_id, field_name="contract_id"),
    )
    contract["revisions"] = _normalize_rows(revisions, operation="load_task_environment_contract.revisions")
    contract["hierarchy_nodes"] = [
        item.get("node_json")
        for item in _normalize_rows(nodes, operation="load_task_environment_contract.nodes")
        if item.get("node_json") is not None
    ]
    contract["invalid_states"] = [
        item.get("state_json")
        for item in _normalize_rows(states, operation="load_task_environment_contract.states")
        if item.get("state_json") is not None
    ]
    return contract


__all__ = [
    "list_task_environment_contracts",
    "load_task_environment_contract",
    "persist_task_environment_contract",
]
