from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import runtime.canonical_manifests as canonical_manifests
from runtime.manifest_generator import GeneratedManifest


class _MatcherStub:
    def __init__(self, coverage_score: float = 0.0) -> None:
        self.coverage_score = coverage_score
        self.calls: list[str] = []

    def match(self, intent: str):
        self.calls.append(intent)
        return SimpleNamespace(coverage_score=self.coverage_score)


def test_generate_manifest_persists_manifest_history_and_object_types_through_storage_owner() -> None:
    matcher = _MatcherStub(coverage_score=0.6)
    generator = SimpleNamespace(
        generate=lambda intent, matches: GeneratedManifest(
            manifest_id="manifest_123",
            manifest={"kind": "helm_surface_bundle"},
            version=4,
            confidence=0.8,
            explanation="Generated manifest",
            object_types=(
                {
                    "type_id": "ticket",
                    "name": "Ticket",
                    "schema": {"title": {"type": "string"}},
                },
            ),
        )
    )

    with patch.object(canonical_manifests, "ensure_object_type_record") as ensure_mock, patch.object(
        canonical_manifests,
        "create_app_manifest",
        return_value={"id": "manifest_123"},
    ) as create_mock, patch.object(
        canonical_manifests,
        "record_app_manifest_history",
        return_value={"id": "history_123"},
    ) as history_mock:
        result = canonical_manifests.generate_manifest(
            object(),
            matcher=matcher,
            generator=generator,
            intent="Build support dashboard",
        )

    assert matcher.calls == ["Build support dashboard"]
    ensure_mock.assert_called_once()
    assert ensure_mock.call_args.kwargs["type_id"] == "ticket"
    create_mock.assert_called_once()
    history_mock.assert_called_once()
    assert result.manifest_id == "manifest_123"


def test_generate_manifest_quick_clones_template_through_storage_owner() -> None:
    conn = SimpleNamespace(
        fetchrow=lambda query, *args: {
            "id": "template_123",
            "name": "Support Workspace",
            "description": "Template",
            "manifest": {"version": 2, "grid": "4x4", "quadrants": {"A1": {"module": "metric"}}},
        }
    )

    with patch.object(
        canonical_manifests,
        "create_app_manifest",
        return_value={"id": "clone_123"},
    ) as create_mock:
        payload = canonical_manifests.generate_manifest_quick(
            conn,
            matcher=SimpleNamespace(match=lambda _intent: (_ for _ in ()).throw(AssertionError("matcher unused"))),
            generator=object(),
            intent="Build support dashboard",
            template_id="template_123",
        )

    create_mock.assert_called_once()
    assert payload["method"] == "clone"
    assert payload["cloned_from"] == "template_123"
    assert payload["manifest"]["kind"] == "helm_surface_bundle"


def test_refine_manifest_persists_revision_through_storage_owner() -> None:
    generator = SimpleNamespace(
        refine=lambda manifest_id, instruction: GeneratedManifest(
            manifest_id=manifest_id,
            manifest={"kind": "helm_surface_bundle", "title": "Refined"},
            version=5,
            confidence=1.0,
            explanation="Added chart",
            changelog="Added chart",
            object_types=(
                {
                    "type_id": "ticket",
                    "name": "Ticket",
                    "property_definitions": {"title": {"type": "string"}},
                },
            ),
        )
    )

    with patch.object(
        canonical_manifests,
        "load_app_manifest_record",
        return_value={"id": "manifest_123", "name": "Support Workspace", "description": "Template"},
    ), patch.object(canonical_manifests, "ensure_object_type_record") as ensure_mock, patch.object(
        canonical_manifests,
        "record_app_manifest_history",
        return_value={"id": "history_123"},
    ) as history_mock, patch.object(
        canonical_manifests,
        "upsert_app_manifest",
        return_value={"id": "manifest_123"},
    ) as upsert_mock:
        result = canonical_manifests.refine_manifest(
            object(),
            generator=generator,
            manifest_id="manifest_123",
            instruction="Add a trend chart",
        )

    ensure_mock.assert_called_once()
    history_mock.assert_called_once()
    upsert_mock.assert_called_once()
    assert upsert_mock.call_args.kwargs["version"] == 5
    assert result.version == 5


def test_save_manifest_as_creates_manifest_through_storage_owner() -> None:
    with patch.object(
        canonical_manifests,
        "create_app_manifest",
        return_value={
            "id": "support-workspace-123abc",
            "name": "Support Workspace",
            "description": "Workspace",
            "manifest": {"kind": "helm_surface_bundle"},
        },
    ) as create_mock:
        saved = canonical_manifests.save_manifest_as(
            object(),
            name="Support Workspace",
            description="Workspace",
            manifest={"version": 2, "grid": "4x4", "quadrants": {"A1": {"module": "metric"}}},
        )

    create_mock.assert_called_once()
    assert saved["name"] == "Support Workspace"
