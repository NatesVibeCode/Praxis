"""Runtime boundary for app-manifest registry reads and writes."""

from __future__ import annotations

from typing import Any

from storage.postgres.validators import PostgresWriteError
from storage.postgres.workflow_runtime_repository import (
    create_app_manifest,
    load_app_manifest_record,
    record_app_manifest_history,
    upsert_app_manifest,
)


class AppManifestRegistryBoundaryError(RuntimeError):
    """Raised when manifest registry ownership rejects a request."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _manifest_payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AppManifestRegistryBoundaryError("manifest must be a JSON object")
    return dict(value)


def _row_payload(row: dict[str, Any]) -> dict[str, Any]:
    manifest = row.get("manifest")
    if not isinstance(manifest, dict):
        manifest = {}
    return {
        "id": str(row.get("id") or ""),
        "name": _text(row.get("name")),
        "description": str(row.get("description") or "").strip(),
        "status": _text(row.get("status")),
        "version": int(row.get("version") or 0),
        "parent_manifest_id": _text(row.get("parent_manifest_id")) or None,
        "kind": _text(manifest.get("kind")) or None,
        "manifest_family": _text(manifest.get("manifest_family")) or None,
        "manifest_type": _text(manifest.get("manifest_type")) or None,
        "manifest": manifest,
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def _raise_storage_boundary(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise AppManifestRegistryBoundaryError(str(exc), status_code=status_code) from exc


def list_app_manifests(
    conn: Any,
    *,
    status: str | None = None,
    manifest_kind: str | None = None,
    manifest_family: str | None = None,
    manifest_type: str | None = None,
    query: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if not isinstance(limit, int) or limit <= 0:
        raise AppManifestRegistryBoundaryError("limit must be a positive integer")
    sql = """
        SELECT id, name, description, manifest, version, parent_manifest_id, status, created_at, updated_at
        FROM app_manifests
        WHERE 1=1
    """
    params: list[Any] = []
    if _text(status):
        params.append(_text(status))
        sql += f" AND status = ${len(params)}"
    if _text(manifest_kind):
        params.append(_text(manifest_kind))
        sql += f" AND manifest->>'kind' = ${len(params)}"
    if _text(manifest_family):
        params.append(_text(manifest_family))
        sql += f" AND manifest->>'manifest_family' = ${len(params)}"
    if _text(manifest_type):
        params.append(_text(manifest_type))
        sql += f" AND manifest->>'manifest_type' = ${len(params)}"
    if _text(query):
        params.append(_text(query))
        sql += f" AND search_vector @@ plainto_tsquery('english', ${len(params)})"
    params.append(limit)
    sql += f" ORDER BY updated_at DESC, id ASC LIMIT ${len(params)}"
    rows = conn.execute(sql, *params)
    return [_row_payload(dict(row)) for row in rows]


def get_app_manifest(conn: Any, *, manifest_id: str) -> dict[str, Any]:
    normalized_manifest_id = _text(manifest_id)
    if not normalized_manifest_id:
        raise AppManifestRegistryBoundaryError("manifest_id is required")
    row = load_app_manifest_record(conn, manifest_id=normalized_manifest_id)
    if row is None:
        raise AppManifestRegistryBoundaryError(
            f"Manifest not found: {normalized_manifest_id}",
            status_code=404,
        )
    return _row_payload(dict(row))


def upsert_registry_manifest(
    conn: Any,
    *,
    manifest_id: str,
    manifest: dict[str, Any],
    name: str | None = None,
    description: str | None = None,
    status: str | None = None,
    parent_manifest_id: str | None = None,
    changed_by: str = "workflow_registry",
    change_description: str = "Updated registry manifest",
) -> dict[str, Any]:
    normalized_manifest_id = _text(manifest_id)
    if not normalized_manifest_id:
        raise AppManifestRegistryBoundaryError("manifest_id is required")
    normalized_manifest = _manifest_payload(manifest)
    existing = load_app_manifest_record(conn, manifest_id=normalized_manifest_id)
    resolved_name = _text(name) or _text(normalized_manifest.get("name")) or (
        _text(existing.get("name")) if isinstance(existing, dict) else ""
    ) or normalized_manifest_id
    resolved_description = str(description or "").strip()
    if not resolved_description and isinstance(existing, dict):
        resolved_description = str(existing.get("description") or "").strip()
    if not resolved_description:
        resolved_description = str(normalized_manifest.get("description") or "").strip()
    resolved_status = _text(status) or _text(normalized_manifest.get("status")) or (
        _text(existing.get("status")) if isinstance(existing, dict) else ""
    ) or "draft"
    resolved_parent_manifest_id = _text(parent_manifest_id) or (
        _text(existing.get("parent_manifest_id")) if isinstance(existing, dict) else ""
    ) or None
    try:
        if existing is None:
            create_app_manifest(
                conn,
                manifest_id=normalized_manifest_id,
                name=resolved_name,
                description=resolved_description,
                manifest=normalized_manifest,
                created_by=_text(changed_by) or "workflow_registry",
                intent_history=[],
                version=1,
                parent_manifest_id=resolved_parent_manifest_id,
                status=resolved_status,
            )
            record_app_manifest_history(
                conn,
                manifest_id=normalized_manifest_id,
                version=1,
                manifest_snapshot=normalized_manifest,
                change_description=_text(change_description) or "Created registry manifest",
                changed_by=_text(changed_by) or "workflow_registry",
            )
        else:
            next_version = int(existing.get("version") or 1) + 1
            upsert_app_manifest(
                conn,
                manifest_id=normalized_manifest_id,
                name=resolved_name,
                description=resolved_description,
                manifest=normalized_manifest,
                version=next_version,
                parent_manifest_id=resolved_parent_manifest_id,
                status=resolved_status,
            )
            record_app_manifest_history(
                conn,
                manifest_id=normalized_manifest_id,
                version=next_version,
                manifest_snapshot=normalized_manifest,
                change_description=_text(change_description) or "Updated registry manifest",
                changed_by=_text(changed_by) or "workflow_registry",
            )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    return get_app_manifest(conn, manifest_id=normalized_manifest_id)


def retire_app_manifest(
    conn: Any,
    *,
    manifest_id: str,
    changed_by: str = "workflow_registry",
    change_description: str = "Retired registry manifest",
) -> dict[str, Any]:
    record = get_app_manifest(conn, manifest_id=manifest_id)
    manifest = dict(record.get("manifest") or {})
    manifest["status"] = "retired"
    return upsert_registry_manifest(
        conn,
        manifest_id=record["id"],
        manifest=manifest,
        name=record["name"],
        description=record["description"],
        status="retired",
        parent_manifest_id=record["parent_manifest_id"],
        changed_by=changed_by,
        change_description=change_description,
    )


__all__ = [
    "AppManifestRegistryBoundaryError",
    "get_app_manifest",
    "list_app_manifests",
    "retire_app_manifest",
    "upsert_registry_manifest",
]
