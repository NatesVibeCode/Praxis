"""Explicit sync Postgres repository for workflow runtime authority writes."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any
import uuid

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)

_MANIFEST_UNSET = object()
_CONTROL_MANIFEST_HEAD_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS control_manifest_heads (
        workspace_ref text NOT NULL,
        scope_ref text NOT NULL,
        manifest_type text NOT NULL,
        manifest_id text NOT NULL REFERENCES app_manifests(id) ON DELETE CASCADE,
        head_status text NOT NULL,
        recorded_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (workspace_ref, scope_ref, manifest_type)
    )
    """.strip(),
    """
    CREATE INDEX IF NOT EXISTS idx_control_manifest_heads_manifest_id
        ON control_manifest_heads(manifest_id)
    """.strip(),
)


def _normalize_manifest_payload(manifest: object) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            "manifest must be a mapping",
            details={"field": "manifest", "value_type": type(manifest).__name__},
        )
    return dict(manifest)


def _normalize_timestamp(value: object | None, *, field_name: str) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            f"{field_name} must be a datetime when provided",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _normalize_json_mapping(
    value: object,
    *,
    field_name: str,
    allow_none: bool = False,
) -> dict[str, Any] | None:
    if value is None:
        if allow_none:
            return None
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            f"{field_name} must be a mapping",
            details={"field": field_name},
        )
    mapping = _require_mapping(value, field_name=field_name)
    return dict(mapping)


def _rewrite_workflow_identity_fields(
    value: Any,
    *,
    old_workflow_id: str,
    new_workflow_id: str,
) -> Any:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return [
            _rewrite_workflow_identity_fields(
                item,
                old_workflow_id=old_workflow_id,
                new_workflow_id=new_workflow_id,
            )
            for item in value
        ]
    if isinstance(value, dict):
        return {
            key: (
                new_workflow_id
                if (
                    isinstance(item, str)
                    and (key == "workflow_id" or key.endswith("_workflow_id"))
                    and item == old_workflow_id
                )
                else _rewrite_workflow_identity_fields(
                    item,
                    old_workflow_id=old_workflow_id,
                    new_workflow_id=new_workflow_id,
                )
            )
            for key, item in value.items()
        }
    return value


def _normalize_string_list(
    value: object | None,
    *,
    field_name: str,
) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            f"{field_name} must be a list of strings",
            details={"field": field_name},
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str):
            raise PostgresWriteError(
                "workflow_runtime.invalid_submission",
                f"{field_name}[{index}] must be a string",
                details={"field": f"{field_name}[{index}]"},
            )
        item_text = item.strip()
        if item_text:
            normalized.append(item_text)
    return normalized


def _normalize_optional_bool(
    value: object | None,
    *,
    field_name: str,
) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            f"{field_name} must be a boolean",
            details={"field": field_name},
        )
    return value


def _trigger_filter_signature(value: object) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            value = {}
    if not isinstance(value, dict):
        value = {}
    return json.dumps(value, sort_keys=True, separators=(",", ":"))

def create_app_manifest(
    conn: Any,
    *,
    manifest_id: str,
    name: str,
    description: str,
    manifest: dict[str, Any],
    created_by: str | None = None,
    intent_history: list[str] | None = None,
    version: int = 4,
    parent_manifest_id: object = _MANIFEST_UNSET,
    status: object = _MANIFEST_UNSET,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    """Insert one manifest row through the storage layer."""

    normalized_manifest_id = _require_text(manifest_id, field_name="manifest_id")
    normalized_name = _require_text(name, field_name="name")
    normalized_description = str(description or "").strip()
    normalized_manifest = _normalize_manifest_payload(manifest)
    normalized_created_by = _optional_text(created_by, field_name="created_by")
    normalized_parent_manifest_id = (
        _optional_text(parent_manifest_id, field_name="parent_manifest_id")
        if parent_manifest_id is not _MANIFEST_UNSET
        else None
    )
    normalized_status = (
        _require_text(status, field_name="status")
        if status is not _MANIFEST_UNSET
        else "active"
    )
    normalized_created_at = _normalize_timestamp(created_at, field_name="created_at")
    normalized_updated_at = _normalize_timestamp(
        updated_at or normalized_created_at,
        field_name="updated_at",
    )

    manifest_json = _encode_jsonb(normalized_manifest, field_name="manifest")
    conn.execute(
        "INSERT INTO app_manifests "
        "(id, name, description, created_by, intent_history, manifest, version, parent_manifest_id, status, created_at, updated_at) "
        "VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9, $10, $11)",
        normalized_manifest_id,
        normalized_name,
        normalized_description,
        normalized_created_by or "system",
        _encode_jsonb(intent_history or [], field_name="intent_history"),
        manifest_json,
        version,
        normalized_parent_manifest_id,
        normalized_status,
        normalized_created_at,
        normalized_updated_at,
    )

    stored_version = conn.fetchval(
        "SELECT EXTRACT(EPOCH FROM updated_at)::bigint FROM app_manifests WHERE id = $1",
        normalized_manifest_id,
    )
    return {
        "id": normalized_manifest_id,
        "name": normalized_name,
        "description": normalized_description,
        "manifest": normalized_manifest,
        "version": int(stored_version or 0),
    }


def load_app_manifest_record(
    conn: Any,
    *,
    manifest_id: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "SELECT * FROM app_manifests WHERE id = $1",
        _require_text(manifest_id, field_name="manifest_id"),
    )
    return None if row is None else dict(row)


def upsert_app_manifest(
    conn: Any,
    *,
    manifest_id: str,
    name: str,
    description: str,
    manifest: dict[str, Any],
    version: int = 4,
    parent_manifest_id: object = _MANIFEST_UNSET,
    status: object = _MANIFEST_UNSET,
) -> dict[str, Any]:
    """Create or update one manifest row through the storage layer."""

    normalized_manifest_id = _require_text(manifest_id, field_name="manifest_id")
    normalized_name = _require_text(name, field_name="name")
    normalized_description = str(description or "").strip()
    normalized_manifest = _normalize_manifest_payload(manifest)
    manifest_json = _encode_jsonb(normalized_manifest, field_name="manifest")
    normalized_parent_manifest_id = (
        _optional_text(parent_manifest_id, field_name="parent_manifest_id")
        if parent_manifest_id is not _MANIFEST_UNSET
        else _MANIFEST_UNSET
    )
    normalized_status = (
        _require_text(status, field_name="status")
        if status is not _MANIFEST_UNSET
        else _MANIFEST_UNSET
    )

    existing = conn.fetchval(
        "SELECT 1 FROM app_manifests WHERE id = $1",
        normalized_manifest_id,
    )
    if existing:
        params: list[Any] = [
            manifest_json,
            normalized_name,
            normalized_description,
            version,
        ]
        assignments = [
            "manifest = $1::jsonb",
            "name = $2",
            "description = $3",
            "version = $4",
            "updated_at = now()",
        ]
        if normalized_parent_manifest_id is not _MANIFEST_UNSET:
            params.append(normalized_parent_manifest_id)
            assignments.append(f"parent_manifest_id = ${len(params)}")
        if normalized_status is not _MANIFEST_UNSET:
            params.append(normalized_status)
            assignments.append(f"status = ${len(params)}")
        params.append(normalized_manifest_id)
        conn.execute(
            f"UPDATE app_manifests SET {', '.join(assignments)} WHERE id = ${len(params)}",
            *params,
        )
    else:
        conn.execute(
            "INSERT INTO app_manifests (id, name, description, manifest, version, parent_manifest_id, status, created_at, updated_at) "
            "VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, now(), now())",
            normalized_manifest_id,
            normalized_name,
            normalized_description,
            manifest_json,
            version,
            None if normalized_parent_manifest_id is _MANIFEST_UNSET else normalized_parent_manifest_id,
            "active" if normalized_status is _MANIFEST_UNSET else normalized_status,
        )

    stored_version = conn.fetchval(
        "SELECT EXTRACT(EPOCH FROM updated_at)::bigint FROM app_manifests WHERE id = $1",
        normalized_manifest_id,
    )
    return {
        "id": normalized_manifest_id,
        "name": normalized_name,
        "description": normalized_description,
        "manifest": normalized_manifest,
        "version": int(stored_version or 0),
    }


def record_app_manifest_history(
    conn: Any,
    *,
    manifest_id: str,
    version: int,
    manifest_snapshot: dict[str, Any],
    change_description: str,
    changed_by: str,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    if not isinstance(version, int):
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            "version must be an integer",
            details={"field": "version"},
        )
    row = conn.fetchrow(
        "INSERT INTO app_manifest_history "
        "(id, manifest_id, version, manifest_snapshot, change_description, changed_by, created_at) "
        "VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7) "
        "RETURNING id, manifest_id, version, created_at",
        uuid.uuid4().hex[:12],
        _require_text(manifest_id, field_name="manifest_id"),
        version,
        _encode_jsonb(
            _normalize_manifest_payload(manifest_snapshot),
            field_name="manifest_snapshot",
        ),
        _require_text(change_description, field_name="change_description"),
        _require_text(changed_by, field_name="changed_by"),
        _normalize_timestamp(created_at, field_name="created_at"),
    )
    if row is None:
        raise PostgresWriteError(
            "workflow_runtime.write_failed",
            "recording app manifest history returned no row",
    )
    return dict(row)


def bootstrap_control_manifest_head_schema(conn: Any) -> None:
    for statement in _CONTROL_MANIFEST_HEAD_SCHEMA_STATEMENTS:
        conn.execute(statement)


def upsert_control_manifest_head(
    conn: Any,
    *,
    workspace_ref: str,
    scope_ref: str,
    manifest_type: str,
    manifest_id: str,
    head_status: str,
) -> dict[str, Any]:
    bootstrap_control_manifest_head_schema(conn)
    row = conn.fetchrow(
        """
        INSERT INTO control_manifest_heads (
            workspace_ref,
            scope_ref,
            manifest_type,
            manifest_id,
            head_status,
            recorded_at
        ) VALUES ($1, $2, $3, $4, $5, now())
        ON CONFLICT (workspace_ref, scope_ref, manifest_type) DO UPDATE
        SET manifest_id = EXCLUDED.manifest_id,
            head_status = EXCLUDED.head_status,
            recorded_at = now()
        RETURNING workspace_ref, scope_ref, manifest_type, manifest_id, head_status, recorded_at
        """,
        _require_text(workspace_ref, field_name="workspace_ref"),
        _require_text(scope_ref, field_name="scope_ref"),
        _require_text(manifest_type, field_name="manifest_type"),
        _require_text(manifest_id, field_name="manifest_id"),
        _require_text(head_status, field_name="head_status"),
    )
    if row is None:
        raise PostgresWriteError(
            "workflow_runtime.write_failed",
            "upserting control manifest head returned no row",
        )
    return dict(row)


def load_control_manifest_head_record(
    conn: Any,
    *,
    workspace_ref: str,
    scope_ref: str,
    manifest_type: str,
) -> dict[str, Any] | None:
    bootstrap_control_manifest_head_schema(conn)
    row = conn.fetchrow(
        """
        SELECT
            h.workspace_ref,
            h.scope_ref,
            h.manifest_type,
            h.manifest_id AS head_manifest_id,
            h.head_status,
            h.recorded_at,
            m.id,
            m.name,
            m.description,
            m.created_by,
            m.intent_history,
            m.manifest,
            m.version,
            m.parent_manifest_id,
            m.status,
            m.created_at,
            m.updated_at
        FROM control_manifest_heads h
        JOIN app_manifests m ON m.id = h.manifest_id
        WHERE h.workspace_ref = $1
          AND h.scope_ref = $2
          AND h.manifest_type = $3
        """,
        _require_text(workspace_ref, field_name="workspace_ref"),
        _require_text(scope_ref, field_name="scope_ref"),
        _require_text(manifest_type, field_name="manifest_type"),
    )
    return None if row is None else dict(row)


def list_control_manifest_head_records(
    conn: Any,
    *,
    workspace_ref: str | None = None,
    scope_ref: str | None = None,
    manifest_type: str | None = None,
    head_status: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    bootstrap_control_manifest_head_schema(conn)
    if not isinstance(limit, int) or limit <= 0:
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            "limit must be a positive integer",
            details={"field": "limit"},
        )
    sql = """
        SELECT
            h.workspace_ref,
            h.scope_ref,
            h.manifest_type,
            h.manifest_id AS head_manifest_id,
            h.head_status,
            h.recorded_at,
            m.id,
            m.name,
            m.description,
            m.created_by,
            m.intent_history,
            m.manifest,
            m.version,
            m.parent_manifest_id,
            m.status,
            m.created_at,
            m.updated_at
        FROM control_manifest_heads h
        JOIN app_manifests m ON m.id = h.manifest_id
        WHERE 1=1
    """
    params: list[Any] = []
    if workspace_ref is not None:
        params.append(_require_text(workspace_ref, field_name="workspace_ref"))
        sql += f" AND h.workspace_ref = ${len(params)}"
    if scope_ref is not None:
        params.append(_require_text(scope_ref, field_name="scope_ref"))
        sql += f" AND h.scope_ref = ${len(params)}"
    if manifest_type is not None:
        params.append(_require_text(manifest_type, field_name="manifest_type"))
        sql += f" AND h.manifest_type = ${len(params)}"
    if head_status is not None:
        params.append(_require_text(head_status, field_name="head_status"))
        sql += f" AND h.head_status = ${len(params)}"
    params.append(limit)
    sql += f" ORDER BY h.recorded_at DESC, h.workspace_ref ASC, h.scope_ref ASC LIMIT ${len(params)}"
    rows = conn.execute(sql, *params)
    return [dict(row) for row in rows]


def list_control_manifest_history_records(
    conn: Any,
    *,
    workspace_ref: str,
    scope_ref: str,
    manifest_type: str,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if not isinstance(limit, int) or limit <= 0:
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            "limit must be a positive integer",
            details={"field": "limit"},
        )
    params: list[Any] = [
        _require_text(workspace_ref, field_name="workspace_ref"),
        _require_text(scope_ref, field_name="scope_ref"),
        _require_text(manifest_type, field_name="manifest_type"),
    ]
    sql = """
        SELECT
            id,
            manifest_id,
            version,
            manifest_snapshot,
            change_description,
            changed_by,
            created_at
        FROM app_manifest_history
        WHERE COALESCE(manifest_snapshot->>'kind', '') = 'praxis_control_manifest'
          AND COALESCE(manifest_snapshot->>'manifest_family', '') = 'control_plane'
          AND manifest_snapshot->>'workspace_ref' = $1
          AND manifest_snapshot->>'scope_ref' = $2
          AND manifest_snapshot->>'manifest_type' = $3
    """
    if status is not None:
        params.append(_require_text(status, field_name="status"))
        sql += f" AND manifest_snapshot->>'status' = ${len(params)}"
    params.append(limit)
    sql += f" ORDER BY created_at DESC, manifest_id ASC, version DESC LIMIT ${len(params)}"
    rows = conn.execute(sql, *params)
    return [dict(row) for row in rows]


def create_authority_checkpoint(
    conn: Any,
    *,
    card_id: str,
    model_id: str,
    authority_level: str,
    question: str,
) -> dict[str, Any]:
    """Create one pending authority checkpoint through the storage layer."""

    row = conn.fetchrow(
        "INSERT INTO authority_checkpoints "
        "(checkpoint_id, card_id, model_id, authority_level, question, status, created_at) "
        "VALUES ($1, $2, $3, $4, $5, 'pending', NOW()) "
        "RETURNING checkpoint_id, status",
        uuid.uuid4().hex[:12],
        _require_text(card_id, field_name="card_id"),
        _require_text(model_id, field_name="model_id"),
        _require_text(authority_level, field_name="authority_level"),
        _require_text(question, field_name="question"),
    )
    if row is None:
        raise PostgresWriteError(
            "workflow_runtime.write_failed",
            "creating authority checkpoint returned no row",
        )
    return dict(row)


def decide_authority_checkpoint(
    conn: Any,
    *,
    checkpoint_id: str,
    decision: str,
    notes: str | None = None,
    decided_by: str | None = None,
) -> dict[str, Any] | None:
    """Persist a checkpoint decision through the storage layer."""

    row = conn.fetchrow(
        "UPDATE authority_checkpoints "
        "SET status = $2, decided_by = $3, decided_at = NOW(), notes = $4 "
        "WHERE checkpoint_id = $1 "
        "RETURNING checkpoint_id, status, decided_at",
        _require_text(checkpoint_id, field_name="checkpoint_id"),
        _require_text(decision, field_name="decision"),
        _optional_text(decided_by, field_name="decided_by"),
        _optional_text(notes, field_name="notes"),
    )
    return None if row is None else dict(row)


def reset_observability_metrics(
    conn: Any,
    *,
    before_date: str | None = None,
) -> dict[str, Any]:
    """Run destructive observability maintenance through a dedicated helper."""

    results: dict[str, Any] = {}
    if before_date:
        for table in ("quality_rollups", "agent_profiles"):
            conn.execute(
                f"DELETE FROM {table} WHERE window_start < $1",
                before_date,
            )
            results[table] = f"deleted rows before {before_date}"
        conn.execute("DELETE FROM failure_catalog")
        results["failure_catalog"] = "cleared"
    else:
        for table in ("quality_rollups", "agent_profiles", "failure_catalog"):
            conn.execute(f"TRUNCATE {table}")
            results[table] = "truncated"

    conn.execute(
        "UPDATE task_type_routing SET recent_successes = 0, recent_failures = 0"
    )
    results["task_type_routing_counters"] = "zeroed"
    results["note"] = (
        "Canonical receipts are preserved. Next rollup cycle will regenerate clean aggregations."
    )
    return results


def load_workflow_record(
    conn: Any,
    *,
    workflow_id: str,
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        "SELECT * FROM public.workflows WHERE id = $1",
        _require_text(workflow_id, field_name="workflow_id"),
    )
    return None if row is None else dict(row)


def workflow_exists(
    conn: Any,
    *,
    workflow_id: str,
) -> bool:
    return bool(
        conn.fetchval(
            "SELECT 1 FROM public.workflows WHERE id = $1",
            _require_text(workflow_id, field_name="workflow_id"),
        )
    )


def persist_workflow_record(
    conn: Any,
    *,
    workflow_id: str,
    name: str,
    description: str,
    definition: dict[str, Any],
    compiled_spec: dict[str, Any] | None,
    tags: list[str] | None = None,
    is_template: bool | None = None,
) -> dict[str, Any]:
    normalized_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    normalized_name = _require_text(name, field_name="name")
    normalized_description = str(description or "")
    normalized_definition = _normalize_json_mapping(definition, field_name="definition")
    normalized_compiled_spec = _normalize_json_mapping(
        compiled_spec,
        field_name="compiled_spec",
        allow_none=True,
    )

    existing = load_workflow_record(conn, workflow_id=normalized_workflow_id)
    effective_tags = (
        _normalize_string_list(tags, field_name="tags")
        if tags is not None
        else _normalize_string_list((existing or {}).get("tags"), field_name="tags")
    )
    effective_is_template = (
        _normalize_optional_bool(is_template, field_name="is_template")
        if is_template is not None
        else None
    )
    if effective_is_template is None:
        existing_template = (existing or {}).get("is_template")
        effective_is_template = bool(existing_template) if existing_template is not None else False

    definition_json = _encode_jsonb(normalized_definition, field_name="definition")
    compiled_spec_json = (
        _encode_jsonb(normalized_compiled_spec, field_name="compiled_spec")
        if normalized_compiled_spec is not None
        else None
    )
    if existing is None:
        row = conn.fetchrow(
            """INSERT INTO public.workflows
                  (id, name, description, definition, compiled_spec, tags, version, is_template)
               VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, 1, $7)
               RETURNING *""",
            normalized_workflow_id,
            normalized_name,
            normalized_description,
            definition_json,
            compiled_spec_json,
            effective_tags,
            effective_is_template,
        )
    else:
        row = conn.fetchrow(
            """UPDATE public.workflows
               SET name = $2,
                   description = $3,
                   definition = $4::jsonb,
                   compiled_spec = $5::jsonb,
                   tags = $6,
                   is_template = $7,
                   version = COALESCE(version, 0) + 1,
                   updated_at = now()
               WHERE id = $1
               RETURNING *""",
            normalized_workflow_id,
            normalized_name,
            normalized_description,
            definition_json,
            compiled_spec_json,
            effective_tags,
            effective_is_template,
        )
    if row is None:
        raise PostgresWriteError(
            "workflow_runtime.write_failed",
            "persisting workflow returned no row",
            details={"workflow_id": normalized_workflow_id},
        )
    return dict(row)


_UNSET = object()


def update_workflow_record(
    conn: Any,
    *,
    workflow_id: str,
    name: object = _UNSET,
    description: object = _UNSET,
    definition: object = _UNSET,
    compiled_spec: object = _UNSET,
    tags: object = _UNSET,
    is_template: object = _UNSET,
) -> dict[str, Any] | None:
    normalized_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    assignments: list[str] = []
    params: list[Any] = [normalized_workflow_id]

    if name is not _UNSET:
        params.append(_require_text(name, field_name="name"))
        assignments.append(f"name = ${len(params)}")
    if description is not _UNSET:
        params.append(str(description or ""))
        assignments.append(f"description = ${len(params)}")
    if definition is not _UNSET:
        normalized_definition = _normalize_json_mapping(definition, field_name="definition")
        params.append(_encode_jsonb(normalized_definition, field_name="definition"))
        assignments.append(f"definition = ${len(params)}::jsonb")
    if compiled_spec is not _UNSET:
        normalized_compiled_spec = _normalize_json_mapping(
            compiled_spec,
            field_name="compiled_spec",
            allow_none=True,
        )
        params.append(
            _encode_jsonb(normalized_compiled_spec, field_name="compiled_spec")
            if normalized_compiled_spec is not None
            else None
        )
        assignments.append(f"compiled_spec = ${len(params)}::jsonb")
    if tags is not _UNSET:
        params.append(_normalize_string_list(tags, field_name="tags"))
        assignments.append(f"tags = ${len(params)}")
    if is_template is not _UNSET:
        params.append(_normalize_optional_bool(is_template, field_name="is_template"))
        assignments.append(f"is_template = ${len(params)}")

    if not assignments:
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            "no workflow fields provided for update",
            details={"workflow_id": normalized_workflow_id},
        )

    assignments.append("version = COALESCE(version, 0) + 1")
    assignments.append("updated_at = now()")
    row = conn.fetchrow(
        f"UPDATE public.workflows SET {', '.join(assignments)} WHERE id = $1 RETURNING *",
        *params,
    )
    return None if row is None else dict(row)


def persist_workflow_build_record(
    conn: Any,
    *,
    workflow_id: str,
    workflow_name: str,
    existing_description: str | None,
    definition: dict[str, Any],
    compiled_spec: dict[str, Any] | None,
) -> dict[str, Any]:
    description_source = (
        str(definition.get("compiled_prose") or "").strip()
        or str(definition.get("source_prose") or "").strip()
        or str(existing_description or "").strip()
        or _require_text(workflow_name, field_name="workflow_name")
    )
    row = update_workflow_record(
        conn,
        workflow_id=workflow_id,
        description=description_source[:200],
        definition=definition,
        compiled_spec=compiled_spec,
    )
    if row is None:
        raise PostgresWriteError(
            "workflow_runtime.write_failed",
            "persisting workflow build returned no row",
            details={"workflow_id": workflow_id},
        )
    return row


def reconcile_workflow_triggers(
    conn: Any,
    *,
    workflow_id: str,
    compiled_spec: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    normalized_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    trigger_specs = compiled_spec.get("triggers", []) if isinstance(compiled_spec, dict) else []
    existing_rows = conn.execute(
        "SELECT id, workflow_id, event_type, filter, cron_expression, enabled, source_trigger_id "
        "FROM workflow_triggers WHERE workflow_id = $1",
        normalized_workflow_id,
    ) or []
    existing_by_source_id: dict[str, dict[str, Any]] = {}
    existing_by_signature: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in existing_rows:
        if not isinstance(row, dict):
            continue
        row_source_trigger_id = _optional_text(row.get("source_trigger_id"), field_name="source_trigger_id")
        if row_source_trigger_id:
            existing_by_source_id[row_source_trigger_id] = row
        signature = (
            str(row.get("event_type") or "").strip() or "manual",
            _trigger_filter_signature(row.get("filter")),
            str(row.get("cron_expression") or "").strip(),
        )
        existing_by_signature.setdefault(signature, []).append(row)

    persisted_rows: list[dict[str, Any]] = []
    desired_source_trigger_ids: set[str] = set()
    managed_row_ids: set[str] = set()
    for raw_trigger in trigger_specs:
        if not isinstance(raw_trigger, dict):
            continue
        event_type = str(raw_trigger.get("event_type") or "").strip() or "manual"
        trigger_filter = raw_trigger.get("filter") if isinstance(raw_trigger.get("filter"), dict) else {}
        cron_expression = _optional_text(raw_trigger.get("cron_expression"), field_name="cron_expression")
        source_trigger_id = _optional_text(raw_trigger.get("source_trigger_id"), field_name="source_trigger_id")
        if not source_trigger_id:
            source_trigger_id = _optional_text(raw_trigger.get("id"), field_name="id")
        if source_trigger_id:
            desired_source_trigger_ids.add(source_trigger_id)

        signature = (
            event_type,
            _trigger_filter_signature(trigger_filter),
            str(cron_expression or "").strip(),
        )
        existing_row = existing_by_source_id.get(source_trigger_id or "") if source_trigger_id else None
        if existing_row is None:
            candidates = existing_by_signature.get(signature, [])
            if len(candidates) == 1:
                candidate = candidates[0]
                if not _optional_text(candidate.get("source_trigger_id"), field_name="source_trigger_id"):
                    existing_row = candidate

        trigger_id = str(existing_row.get("id") or "").strip() if existing_row is not None else ""
        if not trigger_id:
            trigger_id = source_trigger_id or ("trg_" + uuid.uuid4().hex[:12])

        row = upsert_workflow_trigger_record(
            conn,
            trigger_id=trigger_id,
            workflow_id=normalized_workflow_id,
            event_type=event_type,
            trigger_filter=trigger_filter,
            cron_expression=cron_expression,
            enabled=True,
            source_trigger_id=source_trigger_id,
        )
        persisted_rows.append(row)
        managed_row_ids.add(str(row.get("id") or "").strip())

    for row in existing_rows:
        if not isinstance(row, dict):
            continue
        row_source_trigger_id = _optional_text(row.get("source_trigger_id"), field_name="source_trigger_id")
        row_id = str(row.get("id") or "").strip()
        if not row_source_trigger_id:
            continue
        if row_source_trigger_id in desired_source_trigger_ids:
            continue
        if row_id in managed_row_ids:
            continue
        conn.execute("DELETE FROM workflow_triggers WHERE id = $1", row_id)

    return persisted_rows


def upsert_workflow_trigger_record(
    conn: Any,
    *,
    trigger_id: str,
    workflow_id: str,
    event_type: str,
    trigger_filter: dict[str, Any] | None = None,
    cron_expression: str | None = None,
    enabled: bool = True,
    source_trigger_id: object = _UNSET,
) -> dict[str, Any]:
    normalized_trigger_id = _require_text(trigger_id, field_name="trigger_id")
    normalized_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    normalized_event_type = _require_text(event_type, field_name="event_type")
    normalized_filter = _normalize_json_mapping(trigger_filter or {}, field_name="filter")
    normalized_enabled = _normalize_optional_bool(enabled, field_name="enabled")
    assert normalized_enabled is not None
    normalized_cron_expression = _optional_text(cron_expression, field_name="cron_expression")
    normalized_source_trigger_id = (
        _optional_text(source_trigger_id, field_name="source_trigger_id")
        if source_trigger_id is not _UNSET
        else None
    )

    existing = conn.fetchval(
        "SELECT 1 FROM workflow_triggers WHERE id = $1",
        normalized_trigger_id,
    )
    if existing:
        assignments = [
            "workflow_id = $2",
        ]
        params: list[Any] = [normalized_trigger_id, normalized_workflow_id]
        if source_trigger_id is not _UNSET:
            params.append(normalized_source_trigger_id)
            assignments.append(f"source_trigger_id = ${len(params)}")
        params.append(normalized_event_type)
        assignments.append(f"event_type = ${len(params)}")
        params.append(_encode_jsonb(normalized_filter, field_name="filter"))
        assignments.append(f"filter = ${len(params)}::jsonb")
        params.append(normalized_cron_expression)
        assignments.append(f"cron_expression = ${len(params)}")
        params.append(normalized_enabled)
        assignments.append(f"enabled = ${len(params)}")
        row = conn.fetchrow(
            f"UPDATE workflow_triggers SET {', '.join(assignments)} WHERE id = $1 RETURNING *",
            *params,
        )
    else:
        row = conn.fetchrow(
            """INSERT INTO workflow_triggers
                  (id, workflow_id, source_trigger_id, event_type, filter, enabled, cron_expression)
               VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
               RETURNING *""",
            normalized_trigger_id,
            normalized_workflow_id,
            normalized_source_trigger_id,
            normalized_event_type,
            _encode_jsonb(normalized_filter, field_name="filter"),
            normalized_enabled,
            normalized_cron_expression,
        )
    if row is None:
        raise PostgresWriteError(
            "workflow_runtime.write_failed",
            "persisting workflow trigger returned no row",
            details={"trigger_id": normalized_trigger_id},
        )
    return dict(row)


def update_workflow_trigger_record(
    conn: Any,
    *,
    trigger_id: str,
    workflow_id: object = _UNSET,
    source_trigger_id: object = _UNSET,
    event_type: object = _UNSET,
    trigger_filter: object = _UNSET,
    cron_expression: object = _UNSET,
    enabled: object = _UNSET,
) -> dict[str, Any] | None:
    normalized_trigger_id = _require_text(trigger_id, field_name="trigger_id")
    assignments: list[str] = []
    params: list[Any] = [normalized_trigger_id]

    if workflow_id is not _UNSET:
        params.append(_require_text(workflow_id, field_name="workflow_id"))
        assignments.append(f"workflow_id = ${len(params)}")
    if source_trigger_id is not _UNSET:
        params.append(_optional_text(source_trigger_id, field_name="source_trigger_id"))
        assignments.append(f"source_trigger_id = ${len(params)}")
    if event_type is not _UNSET:
        params.append(_require_text(event_type, field_name="event_type"))
        assignments.append(f"event_type = ${len(params)}")
    if trigger_filter is not _UNSET:
        normalized_filter = _normalize_json_mapping(trigger_filter or {}, field_name="filter")
        params.append(_encode_jsonb(normalized_filter, field_name="filter"))
        assignments.append(f"filter = ${len(params)}::jsonb")
    if cron_expression is not _UNSET:
        params.append(_optional_text(cron_expression, field_name="cron_expression"))
        assignments.append(f"cron_expression = ${len(params)}")
    if enabled is not _UNSET:
        params.append(_normalize_optional_bool(enabled, field_name="enabled"))
        assignments.append(f"enabled = ${len(params)}")

    if not assignments:
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            "no trigger fields provided for update",
            details={"trigger_id": normalized_trigger_id},
        )

    row = conn.fetchrow(
        f"UPDATE workflow_triggers SET {', '.join(assignments)} WHERE id = $1 RETURNING *",
        *params,
    )
    return None if row is None else dict(row)


def delete_workflow_record(
    conn: Any,
    *,
    workflow_id: str,
) -> bool:
    normalized_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    exists = workflow_exists(conn, workflow_id=normalized_workflow_id)
    if not exists:
        return False
    conn.execute("DELETE FROM public.workflow_triggers WHERE workflow_id = $1", normalized_workflow_id)
    conn.execute("DELETE FROM public.workflows WHERE id = $1", normalized_workflow_id)
    return True


def rename_workflow_record(
    conn: Any,
    *,
    workflow_id: str,
    new_workflow_id: str,
    name: str | None = None,
) -> dict[str, Any]:
    normalized_old_workflow_id = _require_text(workflow_id, field_name="workflow_id")
    normalized_new_workflow_id = _require_text(new_workflow_id, field_name="new_workflow_id")
    if normalized_old_workflow_id == normalized_new_workflow_id:
        raise PostgresWriteError(
            "workflow_runtime.invalid_submission",
            "new_workflow_id must be different from workflow_id",
            details={"workflow_id": normalized_old_workflow_id},
        )

    existing = load_workflow_record(conn, workflow_id=normalized_old_workflow_id)
    if existing is None:
        raise PostgresWriteError(
            "workflow_runtime.not_found",
            "workflow not found",
            details={"workflow_id": normalized_old_workflow_id},
        )
    if workflow_exists(conn, workflow_id=normalized_new_workflow_id):
        raise PostgresWriteError(
            "workflow_runtime.conflict",
            "target workflow id already exists",
            details={"workflow_id": normalized_new_workflow_id},
        )

    rewritten_definition = _rewrite_workflow_identity_fields(
        _normalize_json_mapping(existing.get("definition"), field_name="definition") or {},
        old_workflow_id=normalized_old_workflow_id,
        new_workflow_id=normalized_new_workflow_id,
    )
    rewritten_compiled_spec = _rewrite_workflow_identity_fields(
        _normalize_json_mapping(existing.get("compiled_spec"), field_name="compiled_spec", allow_none=True),
        old_workflow_id=normalized_old_workflow_id,
        new_workflow_id=normalized_new_workflow_id,
    )
    normalized_name = _require_text(name, field_name="name") if name is not None else _require_text(existing.get("name"), field_name="name")

    row = conn.fetchrow(
        """
        INSERT INTO public.workflows (
            id,
            name,
            description,
            definition,
            compiled_spec,
            tags,
            created_at,
            updated_at,
            version,
            is_template,
            invocation_count,
            last_invoked_at
        )
        SELECT
            $2,
            $3,
            description,
            $4::jsonb,
            $5::jsonb,
            tags,
            created_at,
            now(),
            version,
            is_template,
            invocation_count,
            last_invoked_at
        FROM public.workflows
        WHERE id = $1
        RETURNING *
        """,
        normalized_old_workflow_id,
        normalized_new_workflow_id,
        normalized_name,
        _encode_jsonb(rewritten_definition, field_name="definition"),
        _encode_jsonb(rewritten_compiled_spec, field_name="compiled_spec")
        if rewritten_compiled_spec is not None
        else None,
    )
    if row is None:
        raise PostgresWriteError(
            "workflow_runtime.write_failed",
            "renaming workflow returned no row",
            details={"workflow_id": normalized_old_workflow_id, "new_workflow_id": normalized_new_workflow_id},
        )

    version_rows = conn.execute(
        "SELECT id, definition FROM workflow_versions WHERE workflow_id = $1",
        normalized_old_workflow_id,
    )
    for version_row in version_rows or []:
        rewritten_version_definition = _rewrite_workflow_identity_fields(
            _normalize_json_mapping(version_row.get("definition"), field_name="definition") or {},
            old_workflow_id=normalized_old_workflow_id,
            new_workflow_id=normalized_new_workflow_id,
        )
        conn.execute(
            """UPDATE workflow_versions
               SET workflow_id = $2,
                   definition = $3::jsonb
               WHERE id = $1""",
            version_row["id"],
            normalized_new_workflow_id,
            _encode_jsonb(rewritten_version_definition, field_name="definition"),
        )

    execution_manifest_rows = conn.execute(
        "SELECT execution_manifest_ref, compiled_spec_json FROM workflow_build_execution_manifests WHERE workflow_id = $1",
        normalized_old_workflow_id,
    )
    for execution_manifest_row in execution_manifest_rows or []:
        rewritten_compiled_spec_json = _rewrite_workflow_identity_fields(
            _normalize_json_mapping(
                execution_manifest_row.get("compiled_spec_json"),
                field_name="compiled_spec_json",
            ),
            old_workflow_id=normalized_old_workflow_id,
            new_workflow_id=normalized_new_workflow_id,
        )
        conn.execute(
            """UPDATE workflow_build_execution_manifests
               SET workflow_id = $2,
                   compiled_spec_json = $3::jsonb
               WHERE execution_manifest_ref = $1""",
            execution_manifest_row["execution_manifest_ref"],
            normalized_new_workflow_id,
            _encode_jsonb(rewritten_compiled_spec_json, field_name="compiled_spec_json"),
        )

    dependent_tables = (
        "workflow_triggers",
        "uploaded_files",
        "workflow_build_review_decisions",
        "workflow_build_intents",
        "workflow_build_candidate_manifests",
        "workflow_build_candidate_slots",
        "workflow_build_candidates",
        "workflow_build_review_sessions",
    )
    for table_name in dependent_tables:
        conn.execute(
            f"UPDATE {table_name} SET workflow_id = $2 WHERE workflow_id = $1",
            normalized_old_workflow_id,
            normalized_new_workflow_id,
        )

    conn.execute(
        "DELETE FROM public.workflows WHERE id = $1",
        normalized_old_workflow_id,
    )
    return dict(row)


def record_system_event(
    conn: Any,
    *,
    event_type: str,
    source_id: str,
    source_type: str,
    payload: dict[str, Any],
) -> None:
    conn.execute(
        """INSERT INTO system_events (event_type, source_id, source_type, payload)
           VALUES ($1, $2, $3, $4::jsonb)""",
        _require_text(event_type, field_name="event_type"),
        _require_text(source_id, field_name="source_id"),
        _require_text(source_type, field_name="source_type"),
        _encode_jsonb(_normalize_json_mapping(payload, field_name="payload") or {}, field_name="payload"),
    )


def record_workflow_invocation(
    conn: Any,
    *,
    workflow_id: str,
) -> None:
    conn.execute(
        "UPDATE public.workflows SET invocation_count = invocation_count + 1, last_invoked_at = now() WHERE id = $1",
        _require_text(workflow_id, field_name="workflow_id"),
    )


__all__ = [
    "bootstrap_control_manifest_head_schema",
    "create_app_manifest",
    "create_authority_checkpoint",
    "delete_workflow_record",
    "decide_authority_checkpoint",
    "list_control_manifest_head_records",
    "list_control_manifest_history_records",
    "load_app_manifest_record",
    "load_control_manifest_head_record",
    "load_workflow_record",
    "persist_workflow_build_record",
    "persist_workflow_record",
    "rename_workflow_record",
    "reconcile_workflow_triggers",
    "record_app_manifest_history",
    "record_system_event",
    "record_workflow_invocation",
    "reset_observability_metrics",
    "update_workflow_record",
    "update_workflow_trigger_record",
    "upsert_control_manifest_head",
    "upsert_workflow_trigger_record",
    "upsert_app_manifest",
    "workflow_exists",
]
