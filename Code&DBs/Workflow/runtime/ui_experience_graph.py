"""LLM-facing read model for the Praxis app experience.

This is not a visual renderer. It is the operator map an agent should read
before changing the UI: surfaces, controls, authority sources, and source-file
anchors that explain where the experience is shaped.
"""
from __future__ import annotations

from typing import Any


APP_SURFACES: tuple[dict[str, Any], ...] = (
    {
        "id": "dashboard",
        "label": "Overview",
        "kind": "suite",
        "role": "control-plane overview and launch pad",
        "authority_source": "source_code:shell/surfaceRegistry.tsx",
        "primary_files": [
            "Code&DBs/Workflow/surfaces/app/src/dashboard/Dashboard.tsx",
            "Code&DBs/Workflow/surfaces/app/src/dashboard/dashboard.css",
            "Code&DBs/Workflow/surfaces/app/src/dashboard/ChatPanel.tsx",
        ],
        "main_actions": [
            "open Moon builder",
            "open chat",
            "upload knowledge-base file",
            "open/edit/run/delete workflow",
            "open recent run detail",
        ],
    },
    {
        "id": "build",
        "label": "Moon Build",
        "kind": "build",
        "role": "primary workflow design, inspection, and release surface",
        "authority_source": "surface_catalog_registry + workflow build authority",
        "primary_files": [
            "Code&DBs/Workflow/surfaces/app/src/moon/MoonBuildPage.tsx",
            "Code&DBs/Workflow/surfaces/app/src/moon/MoonActionDock.tsx",
            "Code&DBs/Workflow/surfaces/app/src/moon/MoonNodeDetail.tsx",
            "Code&DBs/Workflow/surfaces/app/src/moon/MoonReleaseTray.tsx",
            "Code&DBs/Workflow/surfaces/app/src/moon/moonBuildPresenter.ts",
            "Code&DBs/Workflow/surfaces/app/src/shared/buildGraphDefinition.ts",
        ],
        "main_actions": [
            "compose from prose",
            "choose trigger",
            "assign node route",
            "edit node contract",
            "apply edge gate",
            "attach authority context",
            "release workflow",
            "inspect run overlay",
        ],
    },
    {
        "id": "run-detail",
        "label": "Run Detail",
        "kind": "dynamic-run",
        "role": "execution observer for one workflow run",
        "authority_source": "workflow_runs and run graph projections",
        "primary_files": [
            "Code&DBs/Workflow/surfaces/app/src/dashboard/RunDetailView.tsx",
            "Code&DBs/Workflow/surfaces/app/src/shared/RunGraphView.tsx",
            "Code&DBs/Workflow/surfaces/app/src/dashboard/useLiveRunSnapshot.ts",
        ],
        "main_actions": [
            "inspect job state",
            "select job",
            "view error and duration",
            "jump back to source workflow",
        ],
    },
    {
        "id": "chat",
        "label": "Chat",
        "kind": "assistant",
        "role": "operator conversation and compile entry surface",
        "authority_source": "provider routing policy + workspace chat runtime",
        "primary_files": [
            "Code&DBs/Workflow/surfaces/app/src/dashboard/ChatPanel.tsx",
            "Code&DBs/Workflow/surfaces/app/src/workspace/useChat.ts",
            "Code&DBs/Workflow/surfaces/app/src/workspace/ToolResultRenderer.tsx",
        ],
        "main_actions": [
            "send message",
            "render tool result",
            "compile workflow intent",
        ],
    },
    {
        "id": "manifests",
        "label": "Manifests",
        "kind": "catalog",
        "role": "control-plane manifest discovery and editing",
        "authority_source": "manifest registry",
        "primary_files": [
            "Code&DBs/Workflow/surfaces/app/src/praxis/ManifestCatalogPage.tsx",
            "Code&DBs/Workflow/surfaces/app/src/grid/ManifestEditorPage.tsx",
            "Code&DBs/Workflow/surfaces/app/src/praxis/ManifestBundleView.tsx",
        ],
        "main_actions": [
            "discover manifest",
            "open manifest tab",
            "edit manifest contract",
        ],
    },
    {
        "id": "atlas",
        "label": "Atlas",
        "kind": "graph-map",
        "role": "secondary system map, not the primary app experience",
        "authority_source": "Praxis.db via Atlas read model",
        "primary_files": [
            "Code&DBs/Workflow/surfaces/app/src/atlas/AtlasPage.tsx",
            "Code&DBs/Workflow/runtime/atlas_graph.py",
        ],
        "main_actions": [
            "inspect graph area",
            "filter nodes",
            "switch graph/table map",
        ],
    },
)

SURFACE_ALIASES: dict[str, str] = {
    "moon": "build",
}

CONTROL_SURFACE_ALIASES: dict[str, str] = {
    "build": "moon",
}

SHELL_RELATIONSHIPS: tuple[dict[str, str], ...] = (
    {"source": "dashboard", "relation": "opens", "target": "build"},
    {"source": "dashboard", "relation": "opens", "target": "chat"},
    {"source": "dashboard", "relation": "opens", "target": "run-detail"},
    {"source": "build", "relation": "releases_to", "target": "run-detail"},
    {"source": "build", "relation": "uses_catalog", "target": "surface_catalog_registry"},
    {"source": "build", "relation": "uses_dictionary", "target": "data_dictionary_objects"},
    {"source": "chat", "relation": "can_compile_into", "target": "build"},
    {"source": "manifests", "relation": "opens_dynamic", "target": "manifest-editor"},
    {"source": "atlas", "relation": "maps", "target": "surface_catalog_registry"},
    {"source": "atlas", "relation": "maps", "target": "data_dictionary_objects"},
)

KNOWN_WEAKNESSES: tuple[dict[str, str], ...] = (
    {
        "scope": "shell navigation",
        "problem": "static app surfaces still live in TypeScript registry constants",
        "recommended_change": "promote app surface registry to DB-backed authority or generated read model",
    },
    {
        "scope": "dashboard actions",
        "problem": "dashboard button contracts are mostly source-code and audit-doc derived",
        "recommended_change": "register dashboard action contracts beside Moon surface_catalog rows",
    },
    {
        "scope": "visual quality",
        "problem": "LLMs need source anchors and authority contracts before screenshots are useful",
        "recommended_change": "use this graph first, then inspect renderer files for layout fixes",
    },
)


def _fetch_surface_catalog_controls(conn: Any) -> list[dict[str, Any]]:
    rows = conn.fetch(
        """
        SELECT catalog_item_id, surface_name, label, icon, family, status, drop_kind,
               action_value, gate_family, description, truth_category, truth_badge,
               truth_detail, surface_tier, surface_badge, surface_detail,
               hard_choice, binding_revision, decision_ref
          FROM surface_catalog_registry
         WHERE enabled = true
         ORDER BY surface_name, display_order, catalog_item_id
        """
    )
    controls: list[dict[str, Any]] = []
    for row in rows or []:
        controls.append(
            {
                "id": row["catalog_item_id"],
                "surface": row["surface_name"],
                "label": row["label"],
                "family": row["family"],
                "kind": row["drop_kind"],
                "route_or_gate": row["action_value"] or row["gate_family"],
                "status": row["status"],
                "truth": {
                    "category": row["truth_category"],
                    "badge": row["truth_badge"],
                    "detail": row["truth_detail"],
                },
                "surface_policy": {
                    "tier": row["surface_tier"],
                    "badge": row["surface_badge"],
                    "detail": row["surface_detail"],
                    "hard_choice": row["hard_choice"],
                },
                "description": row["description"],
                "authority_source": "surface_catalog_registry",
                "binding_revision": row["binding_revision"],
                "decision_ref": row["decision_ref"],
            }
        )
    return controls


def _matches_focus(value: Any, focus: str) -> bool:
    if not focus:
        return True
    return focus in str(value).lower()


def _matches_payload(payload: dict[str, Any], focus: str) -> bool:
    if not focus:
        return True
    return focus in " ".join(str(value) for value in payload.values()).lower()


def _app_surface_filter(surface_name: str | None) -> str:
    surface_filter = str(surface_name or "").strip().lower()
    return SURFACE_ALIASES.get(surface_filter, surface_filter)


def _control_surface_filter(surface_name: str | None) -> str:
    surface_filter = _app_surface_filter(surface_name)
    return CONTROL_SURFACE_ALIASES.get(surface_filter, surface_filter)


def build_ui_experience_graph(
    conn: Any,
    *,
    focus: str | None = None,
    surface_name: str | None = None,
    limit: int = 80,
) -> dict[str, Any]:
    max_items = max(1, min(int(limit or 80), 250))
    focus_text = str(focus or "").strip().lower()
    surface_filter = _app_surface_filter(surface_name)
    control_surface_filter = _control_surface_filter(surface_name)

    surfaces = [
        surface
        for surface in APP_SURFACES
        if (not surface_filter or str(surface["id"]).lower() == surface_filter)
        and _matches_payload(surface, focus_text)
    ]
    controls = [
        control
        for control in _fetch_surface_catalog_controls(conn)
        if (
            not control_surface_filter
            or str(control["surface"]).lower() == control_surface_filter
        )
        and _matches_payload(control, focus_text)
    ]
    relationships = [
        edge
        for edge in SHELL_RELATIONSHIPS
        if (
            not surface_filter
            or edge["source"].lower() == surface_filter
            or edge["target"].lower() == surface_filter
        )
        and (
            not focus_text
            or _matches_focus(edge["source"], focus_text)
            or _matches_focus(edge["relation"], focus_text)
            or _matches_focus(edge["target"], focus_text)
        )
    ]

    surface_ids = {str(surface["id"]) for surface in APP_SURFACES}
    controls_by_surface: dict[str, int] = {}
    for control in controls:
        surface = str(control["surface"])
        controls_by_surface[surface] = controls_by_surface.get(surface, 0) + 1

    return {
        "view": "ui_experience_graph",
        "consumer": "llm",
        "source_authority": "Praxis.db surface catalog plus app shell source registry",
        "filters": {
            "focus": focus,
            "surface_name": surface_name,
            "resolved_surface_name": surface_filter or None,
            "resolved_control_surface_name": control_surface_filter or None,
            "limit": max_items,
        },
        "counts": {
            "surfaces_total": len(APP_SURFACES),
            "surfaces_returned": len(surfaces),
            "surface_controls_returned": len(controls),
            "relationships_returned": len(relationships),
        },
        "surfaces": surfaces[:max_items],
        "surface_controls": controls[:max_items],
        "relationships": relationships[:max_items],
        "controls_by_surface": controls_by_surface,
        "known_weaknesses": [
            item
            for item in KNOWN_WEAKNESSES
            if not focus_text or _matches_payload(item, focus_text)
        ],
        "agent_guidance": [
            "Start with build/Moon for primary workflow UX; Atlas is only a secondary map.",
            "For Moon controls and gates, trust surface_catalog_registry over React constants.",
            "For shell/dashboard experience, inspect the listed renderer files and migrate repeated action contracts into DB authority when they become durable.",
            "When changing visual layout, keep authority reads separate from renderer polish so future agents can reason without launching Vite.",
        ],
        "missing_authority": [
            surface["id"]
            for surface in APP_SURFACES
            if surface["id"] in surface_ids
            and surface["authority_source"].startswith("source_code:")
        ],
    }
