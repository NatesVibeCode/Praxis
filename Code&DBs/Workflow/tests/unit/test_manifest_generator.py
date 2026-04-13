from __future__ import annotations

import json
from pathlib import Path

import runtime.manifest_generator as manifest_generator


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def fetchrow(self, query: str, *args):
        return {
            "id": "manifest_123",
            "manifest": {
                "version": 2,
                "grid": "4x4",
                "quadrants": {
                    "A1": {
                        "module": "metric",
                        "span": 1,
                        "config": {"label": "Inbox", "value": "12"},
                    }
                },
            },
            "version": 2,
        }

    def execute(self, query: str, *args):
        self.executed.append((query, args))
        return []


def test_manifest_refine_uses_medium_route_by_default(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_call_llm(prompt: str, conn=None, *, route_slug: str = "auto/planner") -> str:
        captured["prompt"] = prompt
        captured["conn"] = conn
        captured["route_slug"] = route_slug
        return json.dumps(
            {
                "manifest": {
                    "version": 4,
                    "kind": "helm_surface_bundle",
                    "title": "Support Workspace",
                    "default_tab_id": "main",
                    "tabs": [
                        {
                            "id": "main",
                            "label": "Overview",
                            "surface_id": "main",
                            "source_option_ids": ["web_search"],
                        }
                    ],
                    "surfaces": {
                        "main": {
                            "id": "main",
                            "title": "Overview",
                            "kind": "quadrant_manifest",
                            "manifest": {
                                "version": 2,
                                "grid": "4x4",
                                "quadrants": {
                                    "A1": {
                                        "module": "chart",
                                        "span": 1,
                                        "config": {"endpoint": "/api/support", "type": "bar"},
                                    }
                                },
                            },
                        }
                    }
                }
            }
        )

    monkeypatch.delenv("WORKFLOW_REFINE_AGENT_ROUTE", raising=False)
    monkeypatch.setattr(manifest_generator, "_call_llm", _fake_call_llm)

    generator = manifest_generator.ManifestGenerator(_FakeConn())
    result = generator.refine("manifest_123", "Replace the KPI with a chart")

    assert captured["route_slug"] == "auto/medium"
    assert result.manifest["kind"] == "helm_surface_bundle"
    assert result.manifest["surfaces"]["main"]["manifest"]["quadrants"]["A1"]["module"] == "chart"
    assert result.version == 3


def test_manifest_refine_route_respects_override(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_REFINE_AGENT_ROUTE", "auto/review")

    assert manifest_generator._manifest_refine_agent_route() == "auto/review"


def test_manifest_generator_does_not_embed_manifest_or_object_type_write_sql() -> None:
    source = Path(manifest_generator.__file__).read_text(encoding="utf-8")

    forbidden_sql_snippets = (
        "INSERT INTO app_manifests",
        "UPDATE app_manifests",
        "INSERT INTO app_manifest_history",
        "INSERT INTO object_types",
        "UPDATE object_types",
    )
    leaked = [snippet for snippet in forbidden_sql_snippets if snippet in source]
    assert leaked == [], f"manifest_generator.py still owns canonical write SQL: {leaked}"
