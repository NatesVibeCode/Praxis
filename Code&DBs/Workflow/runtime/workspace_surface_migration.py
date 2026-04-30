"""Workspace surface migration owner.

This module owns small, explicit app-manifest surface rewrites. It exists so
operators can preview and apply a workspace migration through the CQRS gateway
instead of mutating app_manifests with ad hoc SQL.
"""

from __future__ import annotations

import copy
import json
from typing import Any

from runtime.crypto_authority import canonical_digest_hex
from runtime.helm_manifest import normalize_helm_bundle, resolve_tab
from storage.postgres.validators import PostgresWriteError
from storage.postgres.workflow_runtime_repository import (
    load_app_manifest_record,
    record_app_manifest_history,
    upsert_app_manifest,
)


BLANK_COMPOSE_MIGRATION_REF = "workspace.blank.compose.v1"
_BLANK_MODULES = {"markdown", "search-panel"}


class WorkspaceSurfaceMigrationError(RuntimeError):
    """Raised when a workspace surface migration cannot be previewed/applied."""

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
            raise WorkspaceSurfaceMigrationError(
                f"manifest payload is not valid JSON: {exc}",
            ) from exc
    return dict(raw_manifest) if isinstance(raw_manifest, dict) else {}


def _current_version(row: dict[str, Any]) -> int:
    value = row.get("version")
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return 4


def _surface_module_ids(surface: dict[str, Any]) -> list[str]:
    manifest = surface.get("manifest") if isinstance(surface.get("manifest"), dict) else {}
    quadrants = manifest.get("quadrants") if isinstance(manifest.get("quadrants"), dict) else {}
    modules: list[str] = []
    for quadrant in quadrants.values():
        if not isinstance(quadrant, dict):
            continue
        module_id = _text(quadrant.get("module"))
        if module_id:
            modules.append(module_id)
    return modules


def _is_blank_workspace_candidate(
    *,
    manifest_id: str,
    bundle: dict[str, Any],
    surface: dict[str, Any],
) -> bool:
    if manifest_id.startswith("blank-workspace-"):
        return True
    title = " ".join(
        value
        for value in (
            _text(bundle.get("title")),
            _text(bundle.get("name")),
            _text(surface.get("title")),
        )
        if value
    ).lower()
    modules = set(_surface_module_ids(surface))
    return "blank workspace" in title and bool(modules) and modules.issubset(_BLANK_MODULES)


def _is_default_blank_title(value: str, manifest_id: str) -> bool:
    normalized = value.strip().lower()
    return normalized == "blank workspace" or normalized == manifest_id.lower()


def _compose_workspace_title(current_title: str, manifest_id: str) -> str:
    return "Compose" if _is_default_blank_title(current_title, manifest_id) else current_title


def _compose_workspace_description(description: str) -> str:
    if not description.strip() or "minimal workspace" in description.strip().lower():
        return "Compose intent into a contract, then dispatch the work and inspect the receipts."
    return description


def _digest(value: dict[str, Any], *, purpose: str) -> str:
    return canonical_digest_hex(value, purpose=purpose)


def _target_surface(
    bundle: dict[str, Any],
    *,
    tab_id: str | None = None,
) -> tuple[dict[str, Any] | None, str | None, dict[str, Any] | None]:
    tab = resolve_tab(bundle, tab_id=tab_id)
    surface_id = _text(tab.get("surface_id")) if isinstance(tab, dict) else None
    surfaces = bundle.get("surfaces") if isinstance(bundle.get("surfaces"), dict) else {}
    surface = surfaces.get(surface_id) if surface_id else None
    return tab, surface_id, surface if isinstance(surface, dict) else None


def _build_compose_bundle(
    bundle: dict[str, Any],
    *,
    manifest_id: str,
    name: str,
    description: str,
    surface_id: str,
    migration_ref: str,
    current_hash: str,
) -> dict[str, Any]:
    proposed = copy.deepcopy(bundle)
    title = _compose_workspace_title(_text(proposed.get("title")) or name or manifest_id, manifest_id)
    next_description = _compose_workspace_description(description)
    surfaces = proposed.get("surfaces") if isinstance(proposed.get("surfaces"), dict) else {}
    existing_surface = surfaces.get(surface_id) if isinstance(surfaces.get(surface_id), dict) else {}
    surface_title = _compose_workspace_title(_text(existing_surface.get("title")) or title, manifest_id)
    existing_draft = existing_surface.get("draft") if isinstance(existing_surface.get("draft"), dict) else {}

    surfaces[surface_id] = {
        "id": surface_id,
        "title": surface_title,
        "kind": "compose",
        "draft": copy.deepcopy(existing_draft),
    }
    proposed["surfaces"] = surfaces
    proposed["kind"] = "helm_surface_bundle"
    proposed["version"] = 4
    proposed["id"] = manifest_id
    proposed["name"] = title
    proposed["title"] = title
    proposed["description"] = next_description

    referenced_source_ids: set[str] = set()
    tabs = proposed.get("tabs") if isinstance(proposed.get("tabs"), list) else []
    for tab in tabs:
        if not isinstance(tab, dict):
            continue
        if _text(tab.get("surface_id")) == surface_id:
            tab["source_option_ids"] = []
        else:
            referenced_source_ids.update(
                entry
                for entry in tab.get("source_option_ids", [])
                if isinstance(entry, str) and entry.strip()
            )

    source_options = proposed.get("source_options")
    if isinstance(source_options, dict):
        proposed["source_options"] = {
            option_id: option
            for option_id, option in source_options.items()
            if option_id in referenced_source_ids
        }
    else:
        proposed["source_options"] = {}

    legacy = proposed.get("legacy") if isinstance(proposed.get("legacy"), dict) else {}
    proposed["legacy"] = {
        **legacy,
        "last_surface_migration": {
            "migration_ref": migration_ref,
            "surface_id": surface_id,
            "from_kind": _text(existing_surface.get("kind")) or "quadrant_manifest",
            "to_kind": "compose",
            "preimage_hash": current_hash,
        },
    }
    return normalize_helm_bundle(
        proposed,
        manifest_id=manifest_id,
        name=title,
        description=next_description,
    )


def preview_workspace_surface_migration(
    conn: Any,
    *,
    manifest_id: str,
    migration_ref: str = BLANK_COMPOSE_MIGRATION_REF,
    force: bool = False,
    tab_id: str | None = None,
    include_bundle: bool = False,
) -> dict[str, Any]:
    """Return a deterministic preview for one workspace surface migration."""

    normalized_manifest_id = _text(manifest_id)
    if not normalized_manifest_id:
        raise WorkspaceSurfaceMigrationError("manifest_id is required")
    if migration_ref != BLANK_COMPOSE_MIGRATION_REF:
        raise WorkspaceSurfaceMigrationError(f"unsupported migration_ref: {migration_ref}")

    row = load_app_manifest_record(conn, manifest_id=normalized_manifest_id)
    if row is None:
        raise WorkspaceSurfaceMigrationError(
            f"Manifest not found: {normalized_manifest_id}",
            status_code=404,
        )

    name = _text(row.get("name")) or normalized_manifest_id
    description = _text(row.get("description"))
    current_bundle = normalize_helm_bundle(
        _load_manifest_payload(row.get("manifest")),
        manifest_id=normalized_manifest_id,
        name=name,
        description=description,
    )
    tab, surface_id, surface = _target_surface(current_bundle, tab_id=tab_id)
    current_hash = _digest(current_bundle, purpose="workspace.surface_migration.current")

    base: dict[str, Any] = {
        "manifest_id": normalized_manifest_id,
        "migration_ref": migration_ref,
        "tab_id": _text(tab.get("id")) if isinstance(tab, dict) else None,
        "surface_id": surface_id,
        "current_hash": current_hash,
        "applicable": False,
        "changed": False,
        "current_surface_kind": None,
        "proposed_surface_kind": None,
        "reject_reason": None,
        "diff_summary": [],
    }

    if surface_id is None or surface is None:
        base["reject_reason"] = "workspace has no target surface"
        if include_bundle:
            base["current_bundle"] = current_bundle
        return base

    current_kind = _text(surface.get("kind")) or "quadrant_manifest"
    base["current_surface_kind"] = current_kind
    if current_kind == "compose":
        proposed_bundle = _build_compose_bundle(
            current_bundle,
            manifest_id=normalized_manifest_id,
            name=name,
            description=description,
            surface_id=surface_id,
            migration_ref=migration_ref,
            current_hash=current_hash,
        )
        proposed_hash = _digest(proposed_bundle, purpose="workspace.surface_migration.proposed")
        if proposed_hash != current_hash:
            base.update(
                {
                    "applicable": True,
                    "changed": True,
                    "proposed_hash": proposed_hash,
                    "proposed_surface_kind": "compose",
                    "diff_summary": [
                        f"surface {surface_id}: keep compose",
                        "normalize default blank workspace title to Compose",
                        "preserve existing compose draft",
                    ],
                }
            )
            if include_bundle:
                base["current_bundle"] = current_bundle
                base["proposed_bundle"] = proposed_bundle
            return base
        base["proposed_surface_kind"] = "compose"
        base["reject_reason"] = "workspace surface is already compose"
        if include_bundle:
            base["current_bundle"] = current_bundle
            base["proposed_bundle"] = current_bundle
        return base
    if current_kind != "quadrant_manifest":
        base["reject_reason"] = f"unsupported surface kind: {current_kind}"
        if include_bundle:
            base["current_bundle"] = current_bundle
        return base
    if not force and not _is_blank_workspace_candidate(
        manifest_id=normalized_manifest_id,
        bundle=current_bundle,
        surface=surface,
    ):
        base["reject_reason"] = "workspace does not look like an old blank workspace"
        if include_bundle:
            base["current_bundle"] = current_bundle
        return base

    proposed_bundle = _build_compose_bundle(
        current_bundle,
        manifest_id=normalized_manifest_id,
        name=name,
        description=description,
        surface_id=surface_id,
        migration_ref=migration_ref,
        current_hash=current_hash,
    )
    proposed_hash = _digest(proposed_bundle, purpose="workspace.surface_migration.proposed")
    base.update(
        {
            "applicable": True,
            "changed": proposed_hash != current_hash,
            "proposed_hash": proposed_hash,
            "proposed_surface_kind": "compose",
            "diff_summary": [
                f"surface {surface_id}: {current_kind} -> compose",
                f"tab {base['tab_id']}: remove workspace source buttons",
                "preserve title, manifest id, history, and non-target tabs",
            ],
        }
    )
    if include_bundle:
        base["current_bundle"] = current_bundle
        base["proposed_bundle"] = proposed_bundle
    return base


def apply_workspace_surface_migration(
    conn: Any,
    *,
    manifest_id: str,
    migration_ref: str = BLANK_COMPOSE_MIGRATION_REF,
    changed_by: str = "workspace.surface_migration",
    reason: str | None = None,
    force: bool = False,
    tab_id: str | None = None,
) -> dict[str, Any]:
    """Apply one previewed workspace surface migration through app_manifests."""

    preview = preview_workspace_surface_migration(
        conn,
        manifest_id=manifest_id,
        migration_ref=migration_ref,
        force=force,
        tab_id=tab_id,
        include_bundle=True,
    )
    if not preview.get("applicable"):
        already_compose = preview.get("current_surface_kind") == "compose"
        return {
            "ok": already_compose,
            "applied": False,
            "manifest_id": preview.get("manifest_id"),
            "migration_ref": migration_ref,
            "reject_reason": preview.get("reject_reason"),
            "preview": {key: value for key, value in preview.items() if not key.endswith("_bundle")},
            "event_payload": {
                "manifest_id": preview.get("manifest_id"),
                "migration_ref": migration_ref,
                "surface_id": preview.get("surface_id"),
                "tab_id": preview.get("tab_id"),
                "changed": False,
                "from_hash": preview.get("current_hash"),
                "to_hash": preview.get("current_hash"),
                "changed_by": _text(changed_by) or "workspace.surface_migration",
                "reject_reason": preview.get("reject_reason"),
            },
        }
    if not preview.get("changed"):
        return {
            "ok": True,
            "applied": False,
            "manifest_id": preview.get("manifest_id"),
            "migration_ref": migration_ref,
            "reject_reason": "workspace already matches proposed surface",
            "preview": {key: value for key, value in preview.items() if not key.endswith("_bundle")},
            "event_payload": {
                "manifest_id": preview.get("manifest_id"),
                "migration_ref": migration_ref,
                "surface_id": preview.get("surface_id"),
                "changed": False,
                "from_hash": preview.get("current_hash"),
                "to_hash": preview.get("proposed_hash") or preview.get("current_hash"),
            },
        }

    normalized_manifest_id = _text(manifest_id)
    row = load_app_manifest_record(conn, manifest_id=normalized_manifest_id)
    if row is None:
        raise WorkspaceSurfaceMigrationError(
            f"Manifest not found: {normalized_manifest_id}",
            status_code=404,
        )
    name = _text(row.get("name")) or normalized_manifest_id
    description = _text(row.get("description"))
    version = _current_version(row)
    current_bundle = preview["current_bundle"]
    proposed_bundle = preview["proposed_bundle"]
    proposed_name = _text(proposed_bundle.get("name")) or name
    proposed_description = _text(proposed_bundle.get("description")) or description
    normalized_changed_by = _text(changed_by) or "workspace.surface_migration"
    reason_text = _text(reason) or "Migrate old blank workspace surface to compose"

    try:
        before_history = record_app_manifest_history(
            conn,
            manifest_id=normalized_manifest_id,
            version=version,
            manifest_snapshot=current_bundle,
            change_description=f"Preimage before {migration_ref}: {reason_text}",
            changed_by=normalized_changed_by,
        )
        saved = upsert_app_manifest(
            conn,
            manifest_id=normalized_manifest_id,
            name=proposed_name,
            description=proposed_description,
            manifest=proposed_bundle,
            version=version + 1,
        )
        after_history = record_app_manifest_history(
            conn,
            manifest_id=normalized_manifest_id,
            version=version + 1,
            manifest_snapshot=proposed_bundle,
            change_description=f"Applied {migration_ref}: {reason_text}",
            changed_by=normalized_changed_by,
        )
    except PostgresWriteError as exc:
        raise WorkspaceSurfaceMigrationError(str(exc), status_code=500) from exc

    event_payload = {
        "manifest_id": normalized_manifest_id,
        "migration_ref": migration_ref,
        "surface_id": preview.get("surface_id"),
        "tab_id": preview.get("tab_id"),
        "changed": True,
        "from_hash": preview.get("current_hash"),
        "to_hash": preview.get("proposed_hash"),
        "changed_by": normalized_changed_by,
    }
    return {
        "ok": True,
        "applied": True,
        "manifest_id": normalized_manifest_id,
        "migration_ref": migration_ref,
        "saved": saved,
        "preview": {key: value for key, value in preview.items() if not key.endswith("_bundle")},
        "history": {
            "before": before_history,
            "after": after_history,
        },
        "event_payload": event_payload,
    }


__all__ = [
    "BLANK_COMPOSE_MIGRATION_REF",
    "WorkspaceSurfaceMigrationError",
    "apply_workspace_surface_migration",
    "preview_workspace_surface_migration",
]
