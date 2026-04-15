"""Raw control-plane manifest ownership backed by app_manifests."""

from __future__ import annotations

import json
import re
import uuid
from typing import Any

from core.data_ops import plan_digest, plan_summary
from storage.postgres.validators import PostgresWriteError
from storage.postgres.workflow_runtime_repository import (
    create_app_manifest,
    load_app_manifest_record,
    record_app_manifest_history,
    upsert_app_manifest,
)


CONTROL_MANIFEST_KIND = "praxis_control_manifest"
CONTROL_MANIFEST_FAMILY = "control_plane"
DATA_PLAN_MANIFEST_TYPE = "data_plan"
DATA_APPROVAL_MANIFEST_TYPE = "data_approval"
CONTROL_MANIFEST_SCHEMA_VERSION = 1

_PLAN_STATUSES = frozenset({"draft", "approved", "applied"})
_APPROVAL_STATUSES = frozenset({"approved", "superseded"})
_PLAN_TRANSITIONS = {
    "draft": frozenset({"approved"}),
    "approved": frozenset({"applied"}),
    "applied": frozenset(),
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


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "control-manifest"


def _manifest_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


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


def _manifest_metadata(row: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row.get("id") or ""),
        "name": _text(row.get("name")) or str(row.get("id") or ""),
        "description": str(row.get("description") or "").strip(),
        "version": int(row.get("version") or 1),
        "status": str(row.get("status") or manifest.get("status") or "").strip(),
        "parent_manifest_id": _text(row.get("parent_manifest_id")) or None,
        "manifest": manifest,
        "kind": str(manifest.get("kind") or ""),
        "manifest_family": str(manifest.get("manifest_family") or ""),
        "manifest_type": str(manifest.get("manifest_type") or ""),
        "schema_version": int(manifest.get("schema_version") or CONTROL_MANIFEST_SCHEMA_VERSION),
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
    if manifest_type not in {DATA_PLAN_MANIFEST_TYPE, DATA_APPROVAL_MANIFEST_TYPE}:
        raise ControlPlaneManifestBoundaryError(
            "manifest type must be data_plan or data_approval",
            reason_code="control_manifest.invalid_type",
            details={"manifest_type": manifest_type},
        )
    return manifest


def _plan_manifest_payload(
    *,
    plan: dict[str, Any],
    compare_fields: list[str] | None,
    job: dict[str, Any] | None,
    workspace_root: str | None,
    status: str,
) -> dict[str, Any]:
    payload = {
        "kind": CONTROL_MANIFEST_KIND,
        "manifest_family": CONTROL_MANIFEST_FAMILY,
        "manifest_type": DATA_PLAN_MANIFEST_TYPE,
        "schema_version": CONTROL_MANIFEST_SCHEMA_VERSION,
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
    plan_manifest_id: str,
    plan: dict[str, Any],
    approved_by: str,
    approval_reason: str,
    approved_at: str,
) -> dict[str, Any]:
    return {
        "plan_manifest_id": plan_manifest_id,
        "plan_digest": plan_digest(plan),
        "plan_summary": plan_summary(plan),
        "approved_by": approved_by,
        "approval_reason": approval_reason,
        "approved_at": approved_at,
    }


def _approval_manifest_payload(
    *,
    plan_manifest_id: str,
    plan: dict[str, Any],
    approved_by: str,
    approval_reason: str,
    approved_at: str,
    status: str,
) -> dict[str, Any]:
    approval = _approval_payload(
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
        "status": status,
        "plan_manifest_id": plan_manifest_id,
        "plan_digest": approval["plan_digest"],
        "approved_by": approved_by,
        "approval_reason": approval_reason,
        "approved_at": approved_at,
        "approval": approval,
    }


def create_data_plan_manifest(
    conn: Any,
    *,
    plan: dict[str, Any],
    compare_fields: list[str] | None = None,
    job: dict[str, Any] | None = None,
    workspace_root: str | None = None,
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
    payload = _plan_manifest_payload(
        plan=normalized_plan,
        compare_fields=compare_fields,
        job=job,
        workspace_root=workspace_root,
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
    return {
        "id": normalized_manifest_id,
        "name": normalized_name,
        "description": normalized_description,
        "version": 1,
        "status": normalized_status,
        "parent_manifest_id": None,
        "manifest": payload,
        "kind": CONTROL_MANIFEST_KIND,
        "manifest_family": CONTROL_MANIFEST_FAMILY,
        "manifest_type": DATA_PLAN_MANIFEST_TYPE,
        "schema_version": CONTROL_MANIFEST_SCHEMA_VERSION,
    }


def create_data_approval_manifest(
    conn: Any,
    *,
    plan_manifest_id: str,
    plan: dict[str, Any],
    approved_by: str,
    approval_reason: str,
    approved_at: str,
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
    payload = _approval_manifest_payload(
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
    return {
        "id": normalized_manifest_id,
        "name": normalized_name,
        "description": normalized_description,
        "version": 1,
        "status": normalized_status,
        "parent_manifest_id": normalized_plan_manifest_id,
        "manifest": payload,
        "kind": CONTROL_MANIFEST_KIND,
        "manifest_family": CONTROL_MANIFEST_FAMILY,
        "manifest_type": DATA_APPROVAL_MANIFEST_TYPE,
        "schema_version": CONTROL_MANIFEST_SCHEMA_VERSION,
    }


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
    allowed_statuses = _PLAN_STATUSES if manifest["manifest_type"] == DATA_PLAN_MANIFEST_TYPE else _APPROVAL_STATUSES
    status_value = _text(row.get("status")) or _text(manifest.get("status"))
    if status_value:
        manifest["status"] = _normalize_status(status_value, field_name="status", allowed=allowed_statuses)
    return _manifest_metadata(row, manifest)


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
    if isinstance(approval, dict):
        payload = _json_clone(approval)
    else:
        payload = {}
    for key in (
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


def transition_data_plan_status(
    conn: Any,
    *,
    manifest_id: str,
    to_status: str,
    changed_by: str,
    change_description: str,
) -> dict[str, Any]:
    row = load_control_plane_manifest(conn, manifest_id=manifest_id, expected_type=DATA_PLAN_MANIFEST_TYPE)
    normalized_to_status = _normalize_status(to_status, field_name="to_status", allowed=_PLAN_STATUSES)
    current_status = str(row.get("status") or "")
    if normalized_to_status == current_status:
        return row
    allowed_targets = _PLAN_TRANSITIONS.get(current_status, frozenset())
    if normalized_to_status not in allowed_targets:
        raise ControlPlaneManifestBoundaryError(
            f"invalid data plan status transition: {current_status} -> {normalized_to_status}",
            reason_code="control_manifest.invalid_transition",
            details={"from_status": current_status, "to_status": normalized_to_status},
        )
    manifest = _json_clone(row["manifest"])
    manifest["status"] = normalized_to_status
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
            status=normalized_to_status,
        )
        record_app_manifest_history(
            conn,
            manifest_id=str(row["id"]),
            version=next_version,
            manifest_snapshot=manifest,
            change_description=_text(change_description) or f"Updated plan status to {normalized_to_status}",
            changed_by=_text(changed_by) or "praxis_data",
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    row["version"] = next_version
    row["status"] = normalized_to_status
    row["manifest"] = manifest
    return row


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
    "load_control_plane_manifest",
    "transition_data_plan_status",
]
