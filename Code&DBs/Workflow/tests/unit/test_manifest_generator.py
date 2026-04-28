from __future__ import annotations

import json
from pathlib import Path

import pytest

import runtime.feedback_authority as feedback_authority
import runtime.manifest_generator as manifest_generator


def _valid_refine_response_json(*, module: str = "chart") -> str:
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
                                    "module": module,
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
        return _valid_refine_response_json(module="chart")

    monkeypatch.delenv("WORKFLOW_REFINE_AGENT_ROUTE", raising=False)
    monkeypatch.setattr(manifest_generator, "_call_llm", _fake_call_llm)
    monkeypatch.setattr(manifest_generator, "format_block_catalog_for_prompt", lambda: "- chart")
    monkeypatch.setattr(manifest_generator, "block_ids", lambda: ("chart", "metric"))
    monkeypatch.setattr(
        feedback_authority,
        "record_feedback_event",
        lambda *_args, **_kwargs: {"status": "recorded"},
    )

    generator = manifest_generator.ManifestGenerator(_FakeConn())
    result = generator.refine("manifest_123", "Replace the KPI with a chart")

    assert captured["route_slug"] == "auto/medium"
    assert result.manifest["kind"] == "helm_surface_bundle"
    assert result.manifest["surfaces"]["main"]["manifest"]["quadrants"]["A1"]["module"] == "chart"
    assert result.version == 3


def test_manifest_refine_records_feedback_before_llm(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _record_feedback(conn, command):
        captured["conn"] = conn
        captured["command"] = command
        return {"status": "recorded"}

    def _fake_call_llm(*_args, **_kwargs) -> str:
        return _valid_refine_response_json(module="chart")

    conn = _FakeConn()
    monkeypatch.setattr(feedback_authority, "record_feedback_event", _record_feedback)
    monkeypatch.setattr(manifest_generator, "_call_llm", _fake_call_llm)
    monkeypatch.setattr(manifest_generator, "format_block_catalog_for_prompt", lambda: "- chart")
    monkeypatch.setattr(manifest_generator, "block_ids", lambda: ("chart", "metric"))

    manifest_generator.ManifestGenerator(conn).refine("manifest_123", "tighten the view")

    assert captured["conn"] is conn
    command = captured["command"]
    assert command.feedback_stream_ref == "feedback.manifest_refinement"
    assert command.target_ref == "manifest_123"
    assert command.signal_payload == {"instruction": "tighten the view"}


def test_manifest_refine_fails_closed_when_feedback_authority_fails(monkeypatch) -> None:
    def _record_feedback(*_args, **_kwargs):
        raise RuntimeError("feedback db down")

    monkeypatch.setattr(feedback_authority, "record_feedback_event", _record_feedback)
    monkeypatch.setattr(
        manifest_generator,
        "_call_llm",
        lambda *_args, **_kwargs: pytest.fail("LLM call must not run without feedback authority"),
    )

    generator = manifest_generator.ManifestGenerator(_FakeConn())
    with pytest.raises(RuntimeError, match="manifest_refinement.feedback_authority_failed"):
        generator.refine("manifest_123", "Replace the KPI with a chart")


def test_manifest_refine_route_respects_override(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_REFINE_AGENT_ROUTE", "auto/review")

    assert manifest_generator._manifest_refine_agent_route() == "auto/review"


def test_manifest_llm_call_requires_db_route_authority(monkeypatch) -> None:
    monkeypatch.setattr(
        manifest_generator.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("direct CLI fallback must not run"),
    )

    with pytest.raises(RuntimeError, match="route_authority_unavailable"):
        manifest_generator._call_llm("Build the workspace", conn=None)


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
