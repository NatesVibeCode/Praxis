from __future__ import annotations

from typing import Any

from runtime.ui_experience_graph import build_ui_experience_graph


class FakeConn:
    def fetch(self, _query: str) -> list[dict[str, Any]]:
        return [
            {
                "catalog_item_id": "trigger-manual",
                "surface_name": "moon",
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
                "surface_detail": "Available in Moon",
                "hard_choice": None,
                "binding_revision": "binding.surface_catalog_registry.moon.bootstrap.20260415",
                "decision_ref": "decision.surface_catalog_registry.moon.bootstrap.20260415",
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


def test_build_surface_name_reads_moon_catalog_controls() -> None:
    graph = build_ui_experience_graph(FakeConn(), surface_name="build")

    assert [surface["id"] for surface in graph["surfaces"]] == ["build"]
    assert [control["id"] for control in graph["surface_controls"]] == ["trigger-manual"]
    assert graph["filters"]["resolved_surface_name"] == "build"
    assert graph["filters"]["resolved_control_surface_name"] == "moon"
    assert "Start with build/Moon for primary workflow UX; Atlas is only a secondary map." in graph["agent_guidance"]


def test_moon_alias_reads_primary_build_surface() -> None:
    graph = build_ui_experience_graph(FakeConn(), surface_name="moon")

    assert [surface["id"] for surface in graph["surfaces"]] == ["build"]
    assert graph["surface_controls"][0]["surface"] == "moon"


def test_focus_filter_keeps_relevant_release_surface() -> None:
    graph = build_ui_experience_graph(FakeConn(), focus="release")

    assert [surface["id"] for surface in graph["surfaces"]] == ["build"]
    assert graph["counts"]["surfaces_returned"] == 1
