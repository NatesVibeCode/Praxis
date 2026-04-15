"""Registry-owned control-plane manifest authority helpers."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import uuid
from typing import Any

from core.data_ops import plan_digest, plan_summary
from storage.postgres.validators import PostgresWriteError
from storage.postgres.workflow_runtime_repository import (
    bootstrap_control_manifest_head_schema,
    create_app_manifest,
    list_control_manifest_head_records,
    list_control_manifest_history_records,
    load_app_manifest_record,
    load_control_manifest_head_record,
    record_app_manifest_history,
    upsert_app_manifest,
    upsert_control_manifest_head,
)


CONTROL_MANIFEST_KIND = "praxis_control_manifest"
CONTROL_MANIFEST_FAMILY = "control_plane"
DATA_PLAN_MANIFEST_TYPE = "data_plan"
DATA_APPROVAL_MANIFEST_TYPE = "data_approval"
CONTROL_MANIFEST_SCHEMA_VERSION = 1

_PLAN_STATUSES = frozenset(
    {
        "draft",
        "approved",
        "applied",
        "superseded",
        "revoked",
        "expired",
    }
)
_APPROVAL_STATUSES = frozenset(
    {
        "approved",
        "superseded",
        "revoked",
        "expired",
    }
)
_PLAN_TRANSITIONS = {
    "draft": frozenset({"approved", "superseded", "revoked", "expired"}),
    "approved": frozenset({"applied", "superseded", "revoked", "expired"}),
    "applied": frozenset({"superseded", "revoked", "expired"}),
    "superseded": frozenset({"revoked", "expired"}),
    "revoked": frozenset(),
    "expired": frozenset(),
}
_APPROVAL_TRANSITIONS = {
    "approved": frozenset({"superseded", "revoked", "expired"}),
    "superseded": frozenset({"revoked", "expired"}),
    "revoked": frozenset(),
    "expired": frozenset(),
}


class ControlPlaneManifestBoundaryError(RuntimeError):
    """Raised when control-plane manifest ownership rejects a request."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str = "control_manifest.invalid",
        details: dict[str, Any] | None = None,
        status_code: int = 400,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = details or {}
        self.status_code = status_code


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _json_clone(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_clone(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_clone(item) for item in value]
    return value


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_manifest_payload(raw_manifest: Any) -> dict[str, Any]:
    if isinstance(raw_manifest, str):
        try:
            raw_manifest = json.loads(raw_manifest)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ControlPlaneManifestBoundaryError(
                f"manifest payload is not valid JSON: {exc}",
                reason_code="control_manifest.invalid_json",
            ) from exc
    if not isinstance(raw_manifest, dict):
        raise ControlPlaneManifestBoundaryError(
            "manifest payload must be a JSON object",
            reason_code="control_manifest.invalid_payload",
        )
    return {str(key): _json_clone(value) for key, value in raw_manifest.items()}


def _manifest_id(prefix: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", prefix.strip().lower()).strip("-") or "control-manifest"
    return f"{slug}-{uuid.uuid4().hex[:10]}"


def _normalize_status(
    value: Any,
    *,
    field_name: str,
    allowed: frozenset[str],
) -> str:
    normalized = _text(value).lower()
    if normalized not in allowed:
        raise ControlPlaneManifestBoundaryError(
            f"{field_name} must be one of: {', '.join(sorted(allowed))}",
            reason_code="control_manifest.invalid_status",
            details={"field": field_name, "allowed": sorted(allowed), "value": value},
        )
    return normalized


def _raise_storage_boundary(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise ControlPlaneManifestBoundaryError(
        str(exc),
        reason_code=exc.reason_code,
        details=getattr(exc, "details", None),
        status_code=status_code,
    ) from exc


def _status_rules(manifest_type: str) -> tuple[frozenset[str], dict[str, frozenset[str]]]:
    if manifest_type == DATA_PLAN_MANIFEST_TYPE:
        return _PLAN_STATUSES, _PLAN_TRANSITIONS
    if manifest_type == DATA_APPROVAL_MANIFEST_TYPE:
        return _APPROVAL_STATUSES, _APPROVAL_TRANSITIONS
    raise ControlPlaneManifestBoundaryError(
        "manifest type must be data_plan or data_approval",
        reason_code="control_manifest.invalid_type",
        details={"manifest_type": manifest_type},
    )


def _manifest_metadata(row: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "id": str(row.get("id") or row.get("head_manifest_id") or ""),
        "name": _text(row.get("name")) or str(row.get("id") or row.get("head_manifest_id") or ""),
        "description": str(row.get("description") or "").strip(),
        "version": int(row.get("version") or 1),
        "status": str(row.get("status") or manifest.get("status") or "").strip(),
        "parent_manifest_id": _text(row.get("parent_manifest_id")) or None,
        "manifest": manifest,
        "kind": str(manifest.get("kind") or ""),
        "manifest_family": str(manifest.get("manifest_family") or ""),
        "manifest_type": str(manifest.get("manifest_type") or ""),
        "schema_version": int(manifest.get("schema_version") or CONTROL_MANIFEST_SCHEMA_VERSION),
        "workspace_ref": _text(manifest.get("workspace_ref")) or None,
        "scope_ref": _text(manifest.get("scope_ref")) or None,
        "updated_at": row.get("updated_at"),
        "created_at": row.get("created_at"),
    }
    if row.get("head_manifest_id") is not None:
        payload["head_manifest_id"] = str(row.get("head_manifest_id") or "")
        payload["head_status"] = str(row.get("head_status") or payload["status"])
        payload["head_recorded_at"] = row.get("recorded_at")
        payload["is_current_head"] = str(row.get("head_manifest_id") or "") == payload["id"]
    return payload


def _manifest_history_entry(row: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row.get("id") or ""),
        "manifest_id": _text(row.get("manifest_id")) or None,
        "version": int(row.get("version") or 0),
        "change_description": str(row.get("change_description") or "").strip(),
        "changed_by": _text(row.get("changed_by")) or None,
        "created_at": row.get("created_at"),
        "status": _text(manifest.get("status")) or None,
        "kind": str(manifest.get("kind") or ""),
        "manifest_family": str(manifest.get("manifest_family") or ""),
        "manifest_type": str(manifest.get("manifest_type") or ""),
        "workspace_ref": _text(manifest.get("workspace_ref")) or None,
        "scope_ref": _text(manifest.get("scope_ref")) or None,
        "manifest": manifest,
    }


def _coerce_control_manifest(
    raw_manifest: Any,
    *,
    expected_type: str | None = None,
) -> dict[str, Any]:
    manifest = _load_manifest_payload(raw_manifest)
    if str(manifest.get("kind") or "") != CONTROL_MANIFEST_KIND:
        raise ControlPlaneManifestBoundaryError(
            "manifest kind must be praxis_control_manifest",
            reason_code="control_manifest.invalid_kind",
        )
    if str(manifest.get("manifest_family") or "") != CONTROL_MANIFEST_FAMILY:
        raise ControlPlaneManifestBoundaryError(
            "manifest family must be control_plane",
            reason_code="control_manifest.invalid_family",
        )
    manifest_type = str(manifest.get("manifest_type") or "").strip()
    if expected_type and manifest_type != expected_type:
        raise ControlPlaneManifestBoundaryError(
            f"manifest type must be {expected_type}",
            reason_code="control_manifest.invalid_type",
            details={"expected_type": expected_type, "manifest_type": manifest_type},
        )
    _status_rules(manifest_type)
    return manifest


def _resolve_scope_refs(
    *,
    manifest_type: str,
    workspace_root: str | None = None,
    workspace_ref: str | None = None,
    scope_ref: str | None = None,
    job: dict[str, Any] | None = None,
) -> tuple[str, str]:
    resolved_workspace_ref = _text(workspace_ref)
    if not resolved_workspace_ref and isinstance(job, dict):
        resolved_workspace_ref = _text(job.get("workspace_ref"))
    if not resolved_workspace_ref and workspace_root:
        resolved_workspace_ref = f"workspace_root:{workspace_root}"
    if not resolved_workspace_ref:
        resolved_workspace_ref = "workspace:global"

    resolved_scope_ref = _text(scope_ref)
    if not resolved_scope_ref and isinstance(job, dict):
        resolved_scope_ref = _text(job.get("scope_ref"))
    if not resolved_scope_ref and isinstance(job, dict):
        job_name = _text(job.get("job_name"))
        if job_name:
            resolved_scope_ref = f"data_job:{job_name}"
    if not resolved_scope_ref and isinstance(job, dict):
        operation = _text(job.get("operation"))
        if operation:
            resolved_scope_ref = f"data_operation:{operation}"
    if not resolved_scope_ref:
        resolved_scope_ref = f"{manifest_type}:default"
    return resolved_workspace_ref, resolved_scope_ref


def _scope_identity_from_manifest(manifest: dict[str, Any]) -> tuple[str, str]:
    workspace_ref = _text(manifest.get("workspace_ref"))
    scope_ref = _text(manifest.get("scope_ref"))
    if not workspace_ref or not scope_ref:
        raise ControlPlaneManifestBoundaryError(
            "control manifest requires workspace_ref and scope_ref",
            reason_code="control_manifest.scope_missing",
        )
    return workspace_ref, scope_ref


def _plan_manifest_payload(
    *,
    plan: dict[str, Any],
    compare_fields: list[str] | None,
    job: dict[str, Any] | None,
    workspace_root: str | None,
    workspace_ref: str,
    scope_ref: str,
    status: str,
) -> dict[str, Any]:
    payload = {
        "kind": CONTROL_MANIFEST_KIND,
        "manifest_family": CONTROL_MANIFEST_FAMILY,
        "manifest_type": DATA_PLAN_MANIFEST_TYPE,
        "schema_version": CONTROL_MANIFEST_SCHEMA_VERSION,
        "workspace_ref": workspace_ref,
        "scope_ref": scope_ref,
        "status": status,
        "plan": _json_clone(plan),
        "plan_digest": plan_digest(plan),
        "plan_summary": plan_summary(plan),
    }
    if compare_fields:
        payload["compare_fields"] = [str(field) for field in compare_fields if str(field).strip()]
    if job:
        payload["job"] = _json_clone(job)
    if workspace_root:
        payload["workspace_root"] = str(workspace_root)
    return payload


def _approval_payload(
    *,
    workspace_ref: str,
    scope_ref: str,
    plan_manifest_id: str,
    plan: dict[str, Any],
    approved_by: str,
    approval_reason: str,
    approved_at: str,
) -> dict[str, Any]:
    return {
        "workspace_ref": workspace_ref,
        "scope_ref": scope_ref,
        "plan_manifest_id": plan_manifest_id,
        "plan_digest": plan_digest(plan),
        "plan_summary": plan_summary(plan),
        "approved_by": approved_by,
        "approval_reason": approval_reason,
        "approved_at": approved_at,
    }


def _approval_manifest_payload(
    *,
    workspace_ref: str,
    scope_ref: str,
    plan_manifest_id: str,
    plan: dict[str, Any],
    approved_by: str,
    approval_reason: str,
    approved_at: str,
    status: str,
) -> dict[str, Any]:
    approval = _approval_payload(
        workspace_ref=workspace_ref,
        scope_ref=scope_ref,
        plan_manifest_id=plan_manifest_id,
        plan=plan,
        approved_by=approved_by,
        approval_reason=approval_reason,
        approved_at=approved_at,
    )
    return {
        "kind": CONTROL_MANIFEST_KIND,
        "manifest_family": CONTROL_MANIFEST_FAMILY,
        "manifest_type": DATA_APPROVAL_MANIFEST_TYPE,
        "schema_version": CONTROL_MANIFEST_SCHEMA_VERSION,
        "workspace_ref": workspace_ref,
        "scope_ref": scope_ref,
        "status": status,
        "plan_manifest_id": plan_manifest_id,
        "plan_digest": approval["plan_digest"],
        "approved_by": approved_by,
        "approval_reason": approval_reason,
        "approved_at": approved_at,
        "approval": approval,
    }


def _persist_manifest_revision(
    conn: Any,
    *,
    row: dict[str, Any],
    manifest: dict[str, Any],
    status: str,
    changed_by: str,
    change_description: str,
) -> dict[str, Any]:
    next_version = int(row.get("version") or 1) + 1
    try:
        upsert_app_manifest(
            conn,
            manifest_id=str(row["id"]),
            name=str(row["name"]),
            description=str(row.get("description") or ""),
            manifest=manifest,
            version=next_version,
            parent_manifest_id=row.get("parent_manifest_id"),
            status=status,
        )
        record_app_manifest_history(
            conn,
            manifest_id=str(row["id"]),
            version=next_version,
            manifest_snapshot=manifest,
            change_description=_text(change_description) or f"Updated manifest status to {status}",
            changed_by=_text(changed_by) or "praxis_data",
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    updated_row = dict(row)
    updated_row["version"] = next_version
    updated_row["status"] = status
    updated_row["manifest"] = manifest
    return updated_row


def _apply_status_metadata(
    manifest: dict[str, Any],
    *,
    to_status: str,
    changed_by: str,
    changed_at: str,
    superseded_by_manifest_id: str | None = None,
) -> None:
    manifest["status"] = to_status
    manifest["last_status_change_at"] = changed_at
    if changed_by:
        manifest["last_status_change_by"] = changed_by
    if to_status == "approved":
        manifest.setdefault("approved_at", changed_at)
    elif to_status == "applied":
        manifest["applied_at"] = changed_at
    elif to_status == "superseded":
        manifest["superseded_at"] = changed_at
        if superseded_by_manifest_id:
            manifest["superseded_by_manifest_id"] = superseded_by_manifest_id
    elif to_status == "revoked":
        manifest["revoked_at"] = changed_at
        if changed_by:
            manifest["revoked_by"] = changed_by
    elif to_status == "expired":
        manifest["expired_at"] = changed_at


def _transition_manifest_status_from_row(
    conn: Any,
    *,
    row: dict[str, Any],
    to_status: str,
    changed_by: str,
    change_description: str,
    update_head: bool = True,
    superseded_by_manifest_id: str | None = None,
) -> dict[str, Any]:
    manifest = _coerce_control_manifest(row.get("manifest"))
    allowed_statuses, transitions = _status_rules(str(manifest["manifest_type"]))
    normalized_to_status = _normalize_status(to_status, field_name="to_status", allowed=allowed_statuses)
    current_status = _text(row.get("status")) or _text(manifest.get("status"))
    if normalized_to_status == current_status:
        return _manifest_metadata(row, manifest)
    allowed_targets = transitions.get(current_status, frozenset())
    if normalized_to_status not in allowed_targets:
        raise ControlPlaneManifestBoundaryError(
            f"invalid control manifest status transition: {current_status} -> {normalized_to_status}",
            reason_code="control_manifest.invalid_transition",
            details={
                "from_status": current_status,
                "to_status": normalized_to_status,
                "manifest_id": str(row.get("id") or ""),
            },
        )
    changed_at = _now_iso()
    updated_manifest = _json_clone(manifest)
    _apply_status_metadata(
        updated_manifest,
        to_status=normalized_to_status,
        changed_by=_text(changed_by),
        changed_at=changed_at,
        superseded_by_manifest_id=_text(superseded_by_manifest_id) or None,
    )
    updated_row = _persist_manifest_revision(
        conn,
        row=row,
        manifest=updated_manifest,
        status=normalized_to_status,
        changed_by=_text(changed_by) or "praxis_data",
        change_description=change_description,
    )
    metadata = _manifest_metadata(updated_row, updated_manifest)
    if update_head:
        workspace_ref, scope_ref = _scope_identity_from_manifest(updated_manifest)
        head_row = load_control_manifest_head_record(
            conn,
            workspace_ref=workspace_ref,
            scope_ref=scope_ref,
            manifest_type=str(updated_manifest["manifest_type"]),
        )
        if head_row is not None and str(head_row.get("head_manifest_id") or "") == str(updated_row["id"]):
            try:
                upsert_control_manifest_head(
                    conn,
                    workspace_ref=workspace_ref,
                    scope_ref=scope_ref,
                    manifest_type=str(updated_manifest["manifest_type"]),
                    manifest_id=str(updated_row["id"]),
                    head_status=normalized_to_status,
                )
            except PostgresWriteError as exc:
                _raise_storage_boundary(exc)
            metadata["head_manifest_id"] = str(updated_row["id"])
            metadata["head_status"] = normalized_to_status
            metadata["head_recorded_at"] = head_row.get("recorded_at")
            metadata["is_current_head"] = True
    return metadata


def _adopt_control_manifest_head(
    conn: Any,
    *,
    row: dict[str, Any],
    changed_by: str,
    change_description: str,
) -> dict[str, Any]:
    bootstrap_control_manifest_head_schema(conn)
    manifest = _coerce_control_manifest(row.get("manifest"))
    workspace_ref, scope_ref = _scope_identity_from_manifest(manifest)
    manifest_type = str(manifest["manifest_type"])
    current_head = load_control_manifest_head_record(
        conn,
        workspace_ref=workspace_ref,
        scope_ref=scope_ref,
        manifest_type=manifest_type,
    )
    if current_head is not None and str(current_head.get("head_manifest_id") or "") != str(row["id"]):
        current_status = _text(current_head.get("status")) or _text(
            _load_manifest_payload(current_head.get("manifest")).get("status")
        )
        if current_status and current_status != "superseded":
            _transition_manifest_status_from_row(
                conn,
                row=current_head,
                to_status="superseded",
                changed_by=changed_by,
                change_description=f"Superseded by {row['id']}: {change_description}",
                update_head=False,
                superseded_by_manifest_id=str(row["id"]),
            )
    try:
        head_row = upsert_control_manifest_head(
            conn,
            workspace_ref=workspace_ref,
            scope_ref=scope_ref,
            manifest_type=manifest_type,
            manifest_id=str(row["id"]),
            head_status=_text(row.get("status")) or _text(manifest.get("status")) or "",
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    metadata = _manifest_metadata(row, manifest)
    metadata["head_manifest_id"] = str(head_row["manifest_id"])
    metadata["head_status"] = str(head_row["head_status"])
    metadata["head_recorded_at"] = head_row.get("recorded_at")
    metadata["is_current_head"] = True
    return metadata


def create_data_plan_manifest(
    conn: Any,
    *,
    plan: dict[str, Any],
    compare_fields: list[str] | None = None,
    job: dict[str, Any] | None = None,
    workspace_root: str | None = None,
    workspace_ref: str | None = None,
    scope_ref: str | None = None,
    name: str | None = None,
    description: str = "",
    manifest_id: str | None = None,
    created_by: str = "praxis_data",
    status: str = "draft",
) -> dict[str, Any]:
    normalized_plan = _json_clone(dict(plan or {}))
    if not normalized_plan:
        raise ControlPlaneManifestBoundaryError(
            "plan manifest requires a non-empty plan",
            reason_code="control_manifest.plan_required",
        )
    normalized_status = _normalize_status(status, field_name="status", allowed=_PLAN_STATUSES)
    resolved_workspace_ref, resolved_scope_ref = _resolve_scope_refs(
        manifest_type=DATA_PLAN_MANIFEST_TYPE,
        workspace_root=workspace_root,
        workspace_ref=workspace_ref,
        scope_ref=scope_ref,
        job=job,
    )
    payload = _plan_manifest_payload(
        plan=normalized_plan,
        compare_fields=compare_fields,
        job=job,
        workspace_root=workspace_root,
        workspace_ref=resolved_workspace_ref,
        scope_ref=resolved_scope_ref,
        status=normalized_status,
    )
    normalized_manifest_id = _text(manifest_id) or _manifest_id("data-plan")
    normalized_name = _text(name) or f"Data Plan {payload['plan_digest'][:12]}"
    normalized_description = str(description or "").strip() or f"Deterministic data plan for {normalized_name}"
    try:
        create_app_manifest(
            conn,
            manifest_id=normalized_manifest_id,
            name=normalized_name,
            description=normalized_description,
            manifest=payload,
            created_by=created_by,
            intent_history=[f"data_plan:{payload['plan_digest'][:12]}"],
            version=1,
            status=normalized_status,
        )
        record_app_manifest_history(
            conn,
            manifest_id=normalized_manifest_id,
            version=1,
            manifest_snapshot=payload,
            change_description="Created data plan manifest",
            changed_by=created_by,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    row = {
        "id": normalized_manifest_id,
        "name": normalized_name,
        "description": normalized_description,
        "version": 1,
        "status": normalized_status,
        "parent_manifest_id": None,
        "manifest": payload,
    }
    return _adopt_control_manifest_head(
        conn,
        row=row,
        changed_by=created_by,
        change_description="Created data plan manifest",
    )


def create_data_approval_manifest(
    conn: Any,
    *,
    plan_manifest_id: str,
    plan: dict[str, Any],
    approved_by: str,
    approval_reason: str,
    approved_at: str,
    workspace_ref: str | None = None,
    scope_ref: str | None = None,
    name: str | None = None,
    description: str = "",
    manifest_id: str | None = None,
    created_by: str = "praxis_data",
    status: str = "approved",
) -> dict[str, Any]:
    normalized_plan_manifest_id = _text(plan_manifest_id)
    if not normalized_plan_manifest_id:
        raise ControlPlaneManifestBoundaryError(
            "approval manifest requires plan_manifest_id",
            reason_code="control_manifest.plan_manifest_id_required",
        )
    normalized_plan = _json_clone(dict(plan or {}))
    if not normalized_plan:
        raise ControlPlaneManifestBoundaryError(
            "approval manifest requires a non-empty plan",
            reason_code="control_manifest.plan_required",
        )
    normalized_approved_by = _text(approved_by)
    normalized_reason = _text(approval_reason)
    normalized_approved_at = _text(approved_at)
    if not normalized_approved_by or not normalized_reason or not normalized_approved_at:
        raise ControlPlaneManifestBoundaryError(
            "approval manifest requires approved_by, approval_reason, and approved_at",
            reason_code="control_manifest.approval_fields_required",
        )
    normalized_status = _normalize_status(status, field_name="status", allowed=_APPROVAL_STATUSES)
    if not _text(workspace_ref) or not _text(scope_ref):
        plan_record = load_control_plane_manifest(
            conn,
            manifest_id=normalized_plan_manifest_id,
            expected_type=DATA_PLAN_MANIFEST_TYPE,
        )
        workspace_ref = _text(workspace_ref) or _text(plan_record.get("workspace_ref"))
        scope_ref = _text(scope_ref) or _text(plan_record.get("scope_ref"))
    resolved_workspace_ref, resolved_scope_ref = _resolve_scope_refs(
        manifest_type=DATA_APPROVAL_MANIFEST_TYPE,
        workspace_ref=workspace_ref,
        scope_ref=scope_ref,
    )
    payload = _approval_manifest_payload(
        workspace_ref=resolved_workspace_ref,
        scope_ref=resolved_scope_ref,
        plan_manifest_id=normalized_plan_manifest_id,
        plan=normalized_plan,
        approved_by=normalized_approved_by,
        approval_reason=normalized_reason,
        approved_at=normalized_approved_at,
        status=normalized_status,
    )
    normalized_manifest_id = _text(manifest_id) or _manifest_id("data-approval")
    normalized_name = _text(name) or f"Data Approval {payload['plan_digest'][:12]}"
    normalized_description = str(description or "").strip() or f"Approval for plan {normalized_plan_manifest_id}"
    try:
        create_app_manifest(
            conn,
            manifest_id=normalized_manifest_id,
            name=normalized_name,
            description=normalized_description,
            manifest=payload,
            created_by=created_by,
            intent_history=[f"data_approval:{payload['plan_digest'][:12]}"],
            version=1,
            parent_manifest_id=normalized_plan_manifest_id,
            status=normalized_status,
        )
        record_app_manifest_history(
            conn,
            manifest_id=normalized_manifest_id,
            version=1,
            manifest_snapshot=payload,
            change_description="Created data approval manifest",
            changed_by=created_by,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    row = {
        "id": normalized_manifest_id,
        "name": normalized_name,
        "description": normalized_description,
        "version": 1,
        "status": normalized_status,
        "parent_manifest_id": normalized_plan_manifest_id,
        "manifest": payload,
    }
    return _adopt_control_manifest_head(
        conn,
        row=row,
        changed_by=created_by,
        change_description="Created data approval manifest",
    )


def load_control_plane_manifest(
    conn: Any,
    *,
    manifest_id: str,
    expected_type: str | None = None,
) -> dict[str, Any]:
    normalized_manifest_id = _text(manifest_id)
    if not normalized_manifest_id:
        raise ControlPlaneManifestBoundaryError(
            "manifest_id is required",
            reason_code="control_manifest.manifest_id_required",
        )
    row = load_app_manifest_record(conn, manifest_id=normalized_manifest_id)
    if row is None:
        raise ControlPlaneManifestBoundaryError(
            f"Manifest not found: {normalized_manifest_id}",
            reason_code="control_manifest.not_found",
            status_code=404,
        )
    manifest = _coerce_control_manifest(row.get("manifest"), expected_type=expected_type)
    allowed_statuses, _ = _status_rules(str(manifest["manifest_type"]))
    status_value = _text(row.get("status")) or _text(manifest.get("status"))
    if status_value:
        manifest["status"] = _normalize_status(status_value, field_name="status", allowed=allowed_statuses)
    return _manifest_metadata(row, manifest)


def load_control_manifest_head(
    conn: Any,
    *,
    workspace_ref: str,
    scope_ref: str,
    manifest_type: str,
) -> dict[str, Any] | None:
    row = load_control_manifest_head_record(
        conn,
        workspace_ref=_text(workspace_ref),
        scope_ref=_text(scope_ref),
        manifest_type=_text(manifest_type),
    )
    if row is None:
        return None
    manifest = _coerce_control_manifest(row.get("manifest"), expected_type=manifest_type)
    return _manifest_metadata(row, manifest)


def list_control_manifest_heads(
    conn: Any,
    *,
    workspace_ref: str | None = None,
    scope_ref: str | None = None,
    manifest_type: str | None = None,
    status: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = list_control_manifest_head_records(
        conn,
        workspace_ref=_text(workspace_ref) or None,
        scope_ref=_text(scope_ref) or None,
        manifest_type=_text(manifest_type) or None,
        head_status=_text(status) or None,
        limit=limit,
    )
    return [
        _manifest_metadata(row, _coerce_control_manifest(row.get("manifest")))
        for row in rows
    ]


def list_control_manifest_history(
    conn: Any,
    *,
    workspace_ref: str,
    scope_ref: str,
    manifest_type: str,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    rows = list_control_manifest_history_records(
        conn,
        workspace_ref=_text(workspace_ref),
        scope_ref=_text(scope_ref),
        manifest_type=_text(manifest_type),
        status=_text(status) or None,
        limit=limit,
    )
    return [
        _manifest_history_entry(
            row,
            _coerce_control_manifest(row.get("manifest_snapshot"), expected_type=manifest_type),
        )
        for row in rows
    ]


def extract_plan_payload(record_or_manifest: Any) -> dict[str, Any]:
    if isinstance(record_or_manifest, dict) and isinstance(record_or_manifest.get("manifest"), dict):
        manifest = _coerce_control_manifest(record_or_manifest["manifest"], expected_type=DATA_PLAN_MANIFEST_TYPE)
    else:
        manifest = _coerce_control_manifest(record_or_manifest, expected_type=DATA_PLAN_MANIFEST_TYPE)
    plan = manifest.get("plan")
    if not isinstance(plan, dict):
        raise ControlPlaneManifestBoundaryError(
            "data plan manifest must contain a plan object",
            reason_code="control_manifest.plan_missing",
        )
    return _json_clone(plan)


def extract_approval_payload(record_or_manifest: Any) -> dict[str, Any]:
    if isinstance(record_or_manifest, dict) and isinstance(record_or_manifest.get("manifest"), dict):
        manifest = _coerce_control_manifest(record_or_manifest["manifest"], expected_type=DATA_APPROVAL_MANIFEST_TYPE)
    else:
        manifest = _coerce_control_manifest(record_or_manifest, expected_type=DATA_APPROVAL_MANIFEST_TYPE)
    approval = manifest.get("approval")
    payload = _json_clone(approval) if isinstance(approval, dict) else {}
    for key in (
        "workspace_ref",
        "scope_ref",
        "plan_manifest_id",
        "plan_digest",
        "approved_by",
        "approval_reason",
        "approved_at",
    ):
        value = manifest.get(key)
        if value is not None and key not in payload:
            payload[key] = _json_clone(value)
    if not payload:
        raise ControlPlaneManifestBoundaryError(
            "data approval manifest must contain approval fields",
            reason_code="control_manifest.approval_missing",
        )
    return payload


def transition_control_manifest_status(
    conn: Any,
    *,
    manifest_id: str,
    to_status: str,
    changed_by: str,
    change_description: str,
    superseded_by_manifest_id: str | None = None,
) -> dict[str, Any]:
    row = load_control_plane_manifest(conn, manifest_id=manifest_id)
    return _transition_manifest_status_from_row(
        conn,
        row=row,
        to_status=to_status,
        changed_by=changed_by,
        change_description=change_description,
        superseded_by_manifest_id=superseded_by_manifest_id,
    )


def transition_data_plan_status(
    conn: Any,
    *,
    manifest_id: str,
    to_status: str,
    changed_by: str,
    change_description: str,
) -> dict[str, Any]:
    row = load_control_plane_manifest(conn, manifest_id=manifest_id, expected_type=DATA_PLAN_MANIFEST_TYPE)
    return _transition_manifest_status_from_row(
        conn,
        row=row,
        to_status=to_status,
        changed_by=changed_by,
        change_description=change_description,
    )


__all__ = [
    "CONTROL_MANIFEST_FAMILY",
    "CONTROL_MANIFEST_KIND",
    "CONTROL_MANIFEST_SCHEMA_VERSION",
    "ControlPlaneManifestBoundaryError",
    "DATA_APPROVAL_MANIFEST_TYPE",
    "DATA_PLAN_MANIFEST_TYPE",
    "create_data_approval_manifest",
    "create_data_plan_manifest",
    "extract_approval_payload",
    "extract_plan_payload",
    "list_control_manifest_heads",
    "list_control_manifest_history",
    "load_control_manifest_head",
    "load_control_plane_manifest",
    "transition_control_manifest_status",
    "transition_data_plan_status",
]
