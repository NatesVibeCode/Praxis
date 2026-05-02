from __future__ import annotations

from typing import Any

from runtime.ui_experience_graph import build_ui_experience_graph


class FakeConn:
    def fetch(self, query: str) -> list[dict[str, Any]]:
        if "FROM ui_surface_file_anchor_registry" in query:
            return [
                {
                    "surface_name": "build",
                    "source_file": "Code&DBs/Workflow/surfaces/app/src/canvas/CanvasBuildPage.tsx",
                    "anchor_kind": "renderer",
                    "label": "Canvas build page",
                    "notes": "Primary workflow design surface.",
                    "binding_revision": "binding.ui_surface_file_anchor_registry.test",
                    "decision_ref": "architecture-policy::ui-experience-graph::registry-owned-file-anchors",
                },
                {
                    "surface_name": "atlas",
                    "source_file": "Code&DBs/Workflow/surfaces/app/src/atlas/AtlasPage.tsx",
                    "anchor_kind": "renderer",
                    "label": "Atlas page",
                    "notes": "Secondary system map renderer.",
                    "binding_revision": "binding.ui_surface_file_anchor_registry.test",
                    "decision_ref": "architecture-policy::ui-experience-graph::registry-owned-file-anchors",
                },
            ]
        return [
            {
                "catalog_item_id": "trigger-manual",
                "surface_name": "canvas",
                "label": "Manual",
                "icon": "play",
                "family": "trigger",
                "status": "ready",
                "drop_kind": "node",
                "action_value": "trigger.manual",
                "gate_family": None,
                "description": "User-initiated run",
                "truth_category": "runtime",
                "truth_badge": "runtime",
                "truth_detail": "Backed by workflow execution runtime",
                "surface_tier": "primary",
                "surface_badge": "primary",
                "surface_detail": "Available in Canvas",
                "hard_choice": None,
                "binding_revision": "binding.surface_catalog_registry.canvas.bootstrap.20260415",
                "decision_ref": "decision.surface_catalog_registry.canvas.bootstrap.20260415",
            },
            {
                "catalog_item_id": "atlas-filter",
                "surface_name": "atlas",
                "label": "Filter",
                "icon": "filter",
                "family": "navigation",
                "status": "ready",
                "drop_kind": "control",
                "action_value": "filter",
                "gate_family": None,
                "description": "Filter Atlas graph",
                "truth_category": "projection",
                "truth_badge": "secondary",
                "truth_detail": "Backed by Atlas read model",
                "surface_tier": "secondary",
                "surface_badge": "secondary",
                "surface_detail": "Available in Atlas",
                "hard_choice": None,
                "binding_revision": "binding.surface_catalog_registry.atlas.20260424",
                "decision_ref": "decision.surface_catalog_registry.atlas.20260424",
            },
        ]


class MissingAnchorConn(FakeConn):
    def fetch(self, query: str) -> list[dict[str, Any]]:
        if "FROM ui_surface_file_anchor_registry" in query:
            raise RuntimeError("missing registry")
        return super().fetch(query)


def test_build_surface_name_reads_canvas_catalog_controls() -> None:
    graph = build_ui_experience_graph(FakeConn(), surface_name="build")

    assert [surface["id"] for surface in graph["surfaces"]] == ["build"]
    assert graph["surfaces"][0]["primary_files"] == [
        "Code&DBs/Workflow/surfaces/app/src/canvas/CanvasBuildPage.tsx"
    ]
    assert graph["surfaces"][0]["file_anchors"][0]["authority_source"] == "ui_surface_file_anchor_registry"
    assert [control["id"] for control in graph["surface_controls"]] == ["trigger-manual"]
    assert graph["filters"]["resolved_surface_name"] == "build"
    assert graph["filters"]["resolved_control_surface_name"] == "canvas"
    assert "Start with build/Canvas for primary workflow UX; Atlas is only a secondary map." in graph["agent_guidance"]


def test_canvas_alias_reads_primary_build_surface() -> None:
    graph = build_ui_experience_graph(FakeConn(), surface_name="canvas")

    assert [surface["id"] for surface in graph["surfaces"]] == ["build"]
    assert graph["surface_controls"][0]["surface"] == "canvas"


def test_focus_filter_keeps_relevant_release_surface() -> None:
    graph = build_ui_experience_graph(FakeConn(), focus="release")

    assert [surface["id"] for surface in graph["surfaces"]] == ["build"]
    assert graph["counts"]["surfaces_returned"] == 1


def test_missing_anchor_registry_does_not_fallback_to_python_file_literals() -> None:
    graph = build_ui_experience_graph(MissingAnchorConn(), surface_name="build")

    assert graph["surfaces"][0]["primary_files"] == []
    assert graph["missing_file_anchor_authority"] == [
        "dashboard",
        "build",
        "run-detail",
        "chat",
        "manifests",
        "atlas",
    ]
    assert graph["anchor_authority_error"] == "RuntimeError: missing registry"
