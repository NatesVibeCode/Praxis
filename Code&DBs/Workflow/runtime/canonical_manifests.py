"""Canonical runtime ownership for app manifest mutation surfaces."""

from __future__ import annotations

import json
import uuid
from typing import Any

from storage.postgres.object_lifecycle_repository import ensure_object_type_record
from storage.postgres.validators import PostgresWriteError
from storage.postgres.workflow_runtime_repository import (
    create_app_manifest,
    load_app_manifest_record,
    record_app_manifest_history,
    upsert_app_manifest,
)


class ManifestRuntimeBoundaryError(RuntimeError):
    """Raised when canonical manifest ownership rejects a request."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _load_manifest_payload(raw_manifest: Any) -> dict[str, Any]:
    if isinstance(raw_manifest, str):
        try:
            raw_manifest = json.loads(raw_manifest)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ManifestRuntimeBoundaryError(f"manifest payload is not valid JSON: {exc}") from exc
    if not isinstance(raw_manifest, dict):
        return {}
    return dict(raw_manifest)


def _normalize_manifest(
    *,
    manifest_id: str,
    name: str,
    description: str,
    manifest: Any,
) -> dict[str, Any]:
    from runtime.helm_manifest import normalize_helm_bundle

    return normalize_helm_bundle(
        _load_manifest_payload(manifest),
        manifest_id=manifest_id,
        name=name,
        description=description,
    )


def _slug_prefix(name: str) -> str:
    slug = name.lower().replace(" ", "-")
    slug = "".join(char for char in slug if char.isalnum() or char == "-")
    return slug or "manifest"


def _raise_storage_boundary(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise ManifestRuntimeBoundaryError(str(exc), status_code=status_code) from exc


def _persist_object_types(
    conn: Any,
    *,
    object_types: tuple[dict[str, Any], ...] | list[dict[str, Any]],
) -> None:
    for raw_object_type in object_types:
        if not isinstance(raw_object_type, dict):
            continue
        type_id = _text(raw_object_type.get("type_id") or raw_object_type.get("id"))
        if not type_id:
            continue
        fields = raw_object_type.get("fields")
        if fields is None:
            schema = raw_object_type.get("schema")
            if isinstance(schema, dict):
                fields = schema.get("fields")
            elif isinstance(schema, list):
                fields = schema
        try:
            ensure_object_type_record(
                conn,
                type_id=type_id,
                name=_text(raw_object_type.get("name")) or type_id,
                description=_text(raw_object_type.get("description")),
                icon=_text(raw_object_type.get("icon")),
                fields=fields if isinstance(fields, list) else [],
            )
        except PostgresWriteError as exc:
            _raise_storage_boundary(exc)


def _persist_generated_manifest(
    conn: Any,
    *,
    result: Any,
    intent: str,
) -> None:
    try:
        _persist_object_types(conn, object_types=getattr(result, "object_types", ()))
        create_app_manifest(
            conn,
            manifest_id=result.manifest_id,
            name=f"Generated: {intent[:80]}",
            description=result.explanation[:500],
            manifest=result.manifest,
            created_by="manifest_generator",
            intent_history=[intent],
            version=result.version,
        )
        record_app_manifest_history(
            conn,
            manifest_id=result.manifest_id,
            version=result.version,
            manifest_snapshot=result.manifest,
            change_description=f"Initial generation from intent: {intent[:200]}",
            changed_by="manifest_generator",
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)


def save_manifest(
    conn: Any,
    *,
    manifest_id: Any,
    name: Any,
    description: Any = "",
    manifest: Any,
) -> dict[str, Any]:
    normalized_manifest_id = _text(manifest_id)
    normalized_name = _text(name)
    if not normalized_manifest_id:
        raise ManifestRuntimeBoundaryError("id is required")
    if not normalized_name:
        raise ManifestRuntimeBoundaryError("name is required")
    normalized_description = str(description or "").strip()
    normalized_manifest = _normalize_manifest(
        manifest_id=normalized_manifest_id,
        name=normalized_name,
        description=normalized_description,
        manifest=manifest,
    )
    try:
        return upsert_app_manifest(
            conn,
            manifest_id=normalized_manifest_id,
            name=normalized_name,
            description=normalized_description,
            manifest=normalized_manifest,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)


def save_manifest_as(
    conn: Any,
    *,
    name: Any,
    description: Any = "",
    manifest: Any,
) -> dict[str, Any]:
    normalized_name = _text(name)
    if not normalized_name:
        raise ManifestRuntimeBoundaryError("name is required")
    normalized_description = str(description or "").strip()
    manifest_id = f"{_slug_prefix(normalized_name)}-{uuid.uuid4().hex[:6]}"
    normalized_manifest = _normalize_manifest(
        manifest_id=manifest_id,
        name=normalized_name,
        description=normalized_description,
        manifest=manifest,
    )
    try:
        return create_app_manifest(
            conn,
            manifest_id=manifest_id,
            name=normalized_name,
            description=normalized_description,
            manifest=normalized_manifest,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)


def generate_manifest(
    conn: Any,
    *,
    matcher: Any,
    generator: Any,
    intent: Any,
) -> Any:
    normalized_intent = _text(intent)
    if not normalized_intent:
        raise ManifestRuntimeBoundaryError("intent is required")
    matches = matcher.match(normalized_intent)
    result = generator.generate(normalized_intent, matches)
    _persist_generated_manifest(conn, result=result, intent=normalized_intent)
    return result


def refine_manifest(
    conn: Any,
    *,
    generator: Any,
    manifest_id: Any,
    instruction: Any,
) -> Any:
    normalized_manifest_id = _text(manifest_id)
    normalized_instruction = _text(instruction)
    if not normalized_manifest_id:
        raise ManifestRuntimeBoundaryError("manifest_id is required")
    if not normalized_instruction:
        raise ManifestRuntimeBoundaryError("manifest_id and instruction are required")
    existing_row = load_app_manifest_record(conn, manifest_id=normalized_manifest_id)
    if existing_row is None:
        raise ManifestRuntimeBoundaryError(f"Manifest not found: {normalized_manifest_id}", status_code=404)
    try:
        result = generator.refine(normalized_manifest_id, normalized_instruction)
        _persist_object_types(conn, object_types=getattr(result, "object_types", ()))
        record_app_manifest_history(
            conn,
            manifest_id=normalized_manifest_id,
            version=result.version,
            manifest_snapshot=result.manifest,
            change_description=result.changelog or result.explanation,
            changed_by="manifest_generator",
        )
        upsert_app_manifest(
            conn,
            manifest_id=normalized_manifest_id,
            name=_text(existing_row.get("name")) or normalized_manifest_id,
            description=str(existing_row.get("description") or "").strip(),
            manifest=result.manifest,
            version=result.version,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    except ValueError as exc:
        if "not found" in str(exc).lower():
            raise ManifestRuntimeBoundaryError(str(exc), status_code=404) from exc
        raise ManifestRuntimeBoundaryError(str(exc)) from exc
    return result


def generate_manifest_quick(
    conn: Any,
    *,
    matcher: Any,
    generator: Any,
    intent: Any,
    template_id: Any = None,
) -> dict[str, Any]:
    normalized_intent = _text(intent)
    if not normalized_intent:
        raise ManifestRuntimeBoundaryError("intent is required")

    source_row: Any | None = None
    if template_id is not None:
        source_row = conn.fetchrow(
            "SELECT id, name, description, manifest FROM app_manifests WHERE id = $1",
            _text(template_id),
        )
        if source_row is None:
            raise ManifestRuntimeBoundaryError(f"Template not found: {template_id}", status_code=404)
    else:
        matches = matcher.match(normalized_intent)
        source_row = conn.fetchrow(
            """SELECT id, name, description, manifest FROM app_manifests
                    WHERE search_vector @@ plainto_tsquery('english', $1)
                    ORDER BY ts_rank(search_vector, plainto_tsquery('english', $1)) DESC
                    LIMIT 1""",
            normalized_intent,
        )
        if source_row is not None and getattr(matches, "coverage_score", 0.0) >= 0.5:
            return _clone_manifest(conn, source_row=source_row, intent=normalized_intent)
        result = generator.generate(normalized_intent, matches)
        _persist_generated_manifest(conn, result=result, intent=normalized_intent)
        return {
            "manifest_id": result.manifest_id,
            "manifest": result.manifest,
            "method": "generate",
            "confidence": result.confidence,
            "explanation": result.explanation,
        }

    return _clone_manifest(conn, source_row=source_row, intent=normalized_intent)


def _clone_manifest(
    conn: Any,
    *,
    source_row: Any,
    intent: str,
) -> dict[str, Any]:
    source = dict(source_row)
    manifest_id = uuid.uuid4().hex[:12]
    manifest = _normalize_manifest(
        manifest_id=manifest_id,
        name=f"Clone: {source['name']}"[:120],
        description=str(source.get("description") or "").strip(),
        manifest=source["manifest"],
    )
    try:
        create_app_manifest(
            conn,
            manifest_id=manifest_id,
            name=manifest["name"],
            description=str(manifest.get("description") or "").strip(),
            manifest=manifest,
            created_by="generate-quick",
            intent_history=[intent],
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    return {
        "manifest_id": manifest_id,
        "manifest": manifest,
        "cloned_from": source["id"],
        "method": "clone",
    }


__all__ = [
    "ManifestRuntimeBoundaryError",
    "generate_manifest",
    "generate_manifest_quick",
    "refine_manifest",
    "save_manifest",
    "save_manifest_as",
]
