"""Helm manifest normalization helpers."""

from __future__ import annotations

import copy
from typing import Any


SOURCE_FAMILIES = {"workspace", "connected", "reference", "external"}
SOURCE_KINDS = {"object", "manifest", "document", "integration", "web_search", "api", "dataset"}
SOURCE_AVAILABILITY = {"ready", "setup_required", "preview"}
SOURCE_ACTIVATION = {"attach", "open", "configure"}


def normalize_source_option(option_id: str, raw: Any) -> dict[str, Any]:
    item = dict(raw) if isinstance(raw, dict) else {}
    return {
        "id": option_id,
        "label": _text(item.get("label")) or option_id,
        "family": item.get("family") if item.get("family") in SOURCE_FAMILIES else "workspace",
        "kind": item.get("kind") if item.get("kind") in SOURCE_KINDS else "object",
        "availability": item.get("availability") if item.get("availability") in SOURCE_AVAILABILITY else "ready",
        "activation": item.get("activation") if item.get("activation") in SOURCE_ACTIVATION else "attach",
        "reference_slug": _optional_text(item.get("reference_slug")),
        "integration_id": _optional_text(item.get("integration_id")),
        "setup_intent": _optional_text(item.get("setup_intent")),
        "description": _optional_text(item.get("description")),
    }


def adapt_v2_manifest_to_bundle(
    manifest: dict[str, Any],
    *,
    manifest_id: str | None = None,
    name: str | None = None,
    description: str | None = None,
) -> dict[str, Any]:
    title = _text(manifest.get("title")) or name or manifest_id or "Workspace"
    return {
        "version": 4,
        "kind": "helm_surface_bundle",
        "title": title,
        "default_tab_id": "main",
        "tabs": [
            {
                "id": "main",
                "label": title,
                "surface_id": "main",
                "source_option_ids": [],
            }
        ],
        "surfaces": {
            "main": {
                "id": "main",
                "title": title,
                "kind": "quadrant_manifest",
                "manifest": {
                    "version": 2,
                    "grid": _text(manifest.get("grid")) or "4x4",
                    "title": _text(manifest.get("title")) or title,
                    "quadrants": copy.deepcopy(manifest.get("quadrants")) if isinstance(manifest.get("quadrants"), dict) else {},
                },
            }
        },
        "source_options": {},
        "legacy": {"source_manifest_version": 2},
        "id": manifest_id,
        "name": name or title,
        "description": description or "",
    }


def normalize_helm_bundle(
    raw: Any,
    *,
    manifest_id: str | None = None,
    name: str | None = None,
    description: str | None = None,
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return adapt_v2_manifest_to_bundle({}, manifest_id=manifest_id, name=name, description=description)

    if raw.get("version") == 2:
        return adapt_v2_manifest_to_bundle(raw, manifest_id=manifest_id, name=name, description=description)

    tabs_raw = raw.get("tabs")
    surfaces_raw = raw.get("surfaces")
    source_options_raw = raw.get("source_options")

    tabs: list[dict[str, Any]] = []
    if isinstance(tabs_raw, list):
        for index, item in enumerate(tabs_raw):
            if not isinstance(item, dict):
                continue
            tab_id = _text(item.get("id")) or f"tab_{index + 1}"
            surface_id = _text(item.get("surface_id")) or tab_id
            tabs.append(
                {
                    "id": tab_id,
                    "label": _text(item.get("label")) or tab_id,
                    "surface_id": surface_id,
                    "source_option_ids": [
                        entry
                        for entry in (item.get("source_option_ids") if isinstance(item.get("source_option_ids"), list) else [])
                        if isinstance(entry, str)
                    ],
                }
            )

    surfaces: dict[str, Any] = {}
    if isinstance(surfaces_raw, dict):
        for surface_id, item in surfaces_raw.items():
            if not isinstance(item, dict):
                continue
            candidate_manifest = item.get("manifest") if isinstance(item.get("manifest"), dict) else item
            quadrant_manifest = {
                "version": 2,
                "grid": _text(candidate_manifest.get("grid")) or "4x4",
                "title": _text(candidate_manifest.get("title")) or _text(item.get("title")) or str(surface_id),
                "quadrants": copy.deepcopy(candidate_manifest.get("quadrants")) if isinstance(candidate_manifest.get("quadrants"), dict) else {},
            }
            surfaces[str(surface_id)] = {
                "id": _text(item.get("id")) or str(surface_id),
                "title": _text(item.get("title")) or str(surface_id),
                "kind": "quadrant_manifest",
                "manifest": quadrant_manifest,
            }

    if not tabs or not surfaces:
        return adapt_v2_manifest_to_bundle(raw, manifest_id=manifest_id, name=name, description=description)

    source_options: dict[str, Any] = {}
    if isinstance(source_options_raw, dict):
        for option_id, item in source_options_raw.items():
            if not isinstance(option_id, str):
                continue
            source_options[option_id] = normalize_source_option(option_id, item)

    title = _text(raw.get("title")) or name or manifest_id or "Workspace"
    default_tab_id = _text(raw.get("default_tab_id")) or tabs[0]["id"]
    return {
        "version": 4,
        "kind": "helm_surface_bundle",
        "title": title,
        "default_tab_id": default_tab_id,
        "tabs": tabs,
        "surfaces": surfaces,
        "source_options": source_options,
        "legacy": copy.deepcopy(raw.get("legacy")) if isinstance(raw.get("legacy"), dict) else None,
        "id": manifest_id or _optional_text(raw.get("id")),
        "name": name or _text(raw.get("name")) or title,
        "description": description or _text(raw.get("description")),
    }


def validate_helm_bundle(bundle: dict[str, Any], *, valid_block_ids: set[str] | None = None) -> None:
    if not isinstance(bundle, dict):
        raise ValueError("manifest must be a dict")
    if bundle.get("version") != 4:
        raise ValueError("manifest must have version: 4")
    if bundle.get("kind") != "helm_surface_bundle":
        raise ValueError("manifest must have kind: helm_surface_bundle")

    tabs = bundle.get("tabs")
    if not isinstance(tabs, list) or not tabs:
        raise ValueError("manifest must include at least one tab")

    surfaces = bundle.get("surfaces")
    if not isinstance(surfaces, dict) or not surfaces:
        raise ValueError("manifest must include at least one surface")

    for tab in tabs:
        if not isinstance(tab, dict):
            raise ValueError("tab entries must be dicts")
        if not _text(tab.get("id")) or not _text(tab.get("surface_id")):
            raise ValueError("tabs require id and surface_id")
        if _text(tab.get("surface_id")) not in surfaces:
            raise ValueError(f"tab references unknown surface: {tab.get('surface_id')}")

    for surface_id, surface in surfaces.items():
        if not isinstance(surface, dict):
            raise ValueError(f"surface {surface_id} must be a dict")
        manifest = surface.get("manifest")
        if not isinstance(manifest, dict):
            raise ValueError(f"surface {surface_id} missing manifest")
        quadrants = manifest.get("quadrants")
        if not isinstance(quadrants, dict):
            raise ValueError(f"surface {surface_id} quadrants must be a dict")
        for quadrant_id, quadrant in quadrants.items():
            if not isinstance(quadrant, dict):
                raise ValueError(f"quadrant {quadrant_id} must be a dict")
            module_id = _text(quadrant.get("module"))
            if not module_id:
                raise ValueError(f"quadrant {quadrant_id} missing module")
            if valid_block_ids is not None and module_id not in valid_block_ids:
                raise ValueError(f"quadrant {quadrant_id} has unregistered module: {module_id}")

    source_options = bundle.get("source_options")
    if source_options is not None and not isinstance(source_options, dict):
        raise ValueError("source_options must be a dict")


def resolve_tab(bundle: dict[str, Any], tab_id: str | None = None) -> dict[str, Any] | None:
    tabs = bundle.get("tabs") if isinstance(bundle, dict) else None
    if not isinstance(tabs, list) or not tabs:
        return None
    if tab_id:
        for tab in tabs:
            if isinstance(tab, dict) and _text(tab.get("id")) == tab_id:
                return tab
    default_tab_id = _text(bundle.get("default_tab_id")) if isinstance(bundle, dict) else ""
    if default_tab_id:
        for tab in tabs:
            if isinstance(tab, dict) and _text(tab.get("id")) == default_tab_id:
                return tab
    for tab in tabs:
        if isinstance(tab, dict):
            return tab
    return None


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


__all__ = [
    "adapt_v2_manifest_to_bundle",
    "normalize_helm_bundle",
    "normalize_source_option",
    "resolve_tab",
    "validate_helm_bundle",
]
