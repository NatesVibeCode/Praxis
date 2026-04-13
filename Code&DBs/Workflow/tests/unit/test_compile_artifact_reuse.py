from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import pathlib

import runtime.compile_index as compile_index
import runtime.compiler as compiler
import runtime.operating_model_planner as planner

_REPO_ROOT = str(pathlib.Path(__file__).resolve().parents[4])


class _ArtifactConn:
    def __init__(self) -> None:
        self.compile_artifact_rows: list[dict[str, object]] = []

    def execute(self, query: str, *args):
        if "INSERT INTO compile_artifacts" in query:
            self.compile_artifact_rows.append(
                {
                    "compile_artifact_id": args[0],
                    "artifact_kind": args[1],
                    "artifact_ref": args[2],
                    "revision_ref": args[3],
                    "parent_artifact_ref": args[4],
                    "input_fingerprint": args[5],
                    "content_hash": args[6],
                    "authority_refs": json.loads(args[7]),
                    "payload": json.loads(args[8]),
                    "decision_ref": args[9],
                }
            )
            return []
        if "FROM compile_artifacts" in query:
            artifact_kind = args[0]
            input_fingerprint = args[1]
            return [
                row
                for row in self.compile_artifact_rows
                if row["artifact_kind"] == artifact_kind and row["input_fingerprint"] == input_fingerprint
            ]
        return []


class _StubMatcher:
    def __init__(self, conn, embedder=None) -> None:
        self._conn = conn
        self._embedder = embedder

    def match(self, intent: str, limit: int = 10):
        return SimpleNamespace(
            ui_components=(),
            calculations=(),
            workflows=(),
        )

    def compose(self, intent: str, matches):
        return SimpleNamespace(
            components=(),
            calculations=(),
            workflows=(),
            bindings=(),
            layout_suggestion="",
            confidence=0.0,
        )


def _compile_index_snapshot() -> compile_index.CompileIndexSnapshot:
    repo_info = {
        "repo_root": _REPO_ROOT,
        "git_head": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "git_branch": "main",
        "git_dirty": False,
        "git_status_hash": "0123456789abcdef",
        "repo_fingerprint": "compile-index-fingerprint",
    }
    payload = {
        "schema_version": 1,
        "repo_info": repo_info,
        "surface_manifest": {
            "repo_root": _REPO_ROOT,
            "surface_name": "compiler",
            "surface_revision": "surface_compiler_manifest_test",
            "tracked_files": ["Code&DBs/Workflow/runtime/compiler.py"],
            "file_fingerprints": {
                "Code&DBs/Workflow/runtime/compiler.py": "surface-file-fingerprint"
            },
        },
        "source_fingerprints": {
            "reference_catalog": "catalog-fingerprint",
            "integration_registry": "integration-fingerprint",
            "object_types": "object-type-fingerprint",
            "compiler_route_hints": "route-hint-fingerprint",
            "capability_catalog": "capability-fingerprint",
        },
        "source_counts": {
            "reference_catalog": 0,
            "integration_registry": 0,
            "object_types": 0,
            "compiler_route_hints": 0,
            "capability_catalog": 0,
        },
        "reference_catalog": [],
        "integration_registry": [],
        "object_types": [],
        "compiler_route_hints": [],
        "capability_catalog": [],
    }
    return compile_index.CompileIndexSnapshot(
        schema_version=1,
        compile_index_ref="compile_index.compiler.test",
        compile_surface_revision="compile_surface.compiler.test",
        compile_surface_name="compiler",
        repo_root=_REPO_ROOT,
        repo_fingerprint="compile-index-fingerprint",
        repo_info=repo_info,
        surface_manifest=payload["surface_manifest"],
        source_fingerprints=payload["source_fingerprints"],
        source_counts=payload["source_counts"],
        decision_ref="decision.compile.index.refresh.test",
        refresh_count=1,
        refreshed_at=datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc),
        stale_after_at=datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc),
        freshness_state="fresh",
        freshness_reason=None,
        reference_catalog=(),
        integration_registry=(),
        object_types=(),
        compiler_route_hints=(),
        capability_catalog=(),
        payload=payload,
    )


def test_compile_prose_reuses_definition_artifact_on_exact_input_match(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _ArtifactConn()
    snapshot = _compile_index_snapshot()
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)

    first = compiler.compile_prose(
        "Review the workflow output carefully.",
        conn=conn,
        compile_index_snapshot=snapshot,
    )

    assert first["reuse_provenance"]["decision"] == "compiled"

    monkeypatch.setattr(
        "runtime.intent_matcher.IntentMatcher",
        lambda *args, **kwargs: pytest.fail("definition compile should have been reused before intent matching"),
    )
    second = compiler.compile_prose(
        "Review the workflow output carefully.",
        conn=conn,
        compile_index_snapshot=snapshot,
    )

    assert second["reuse_provenance"]["decision"] == "reused"
    assert second["definition"]["definition_revision"] == first["definition"]["definition_revision"]


def test_compile_prose_skips_invalid_reusable_definition_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _ArtifactConn()
    snapshot = _compile_index_snapshot()
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)
    compiler.compile_prose(
        "Review the workflow output carefully.",
        conn=conn,
        compile_index_snapshot=snapshot,
    )
    conn.compile_artifact_rows[0]["content_hash"] = "not-the-real-hash"

    result = compiler.compile_prose(
        "Review the workflow output carefully.",
        conn=conn,
        compile_index_snapshot=snapshot,
    )

    assert result["reuse_provenance"]["decision"] == "compiled"
    assert result["error"] is None


def test_compile_prose_reuses_definition_artifact_when_db_returns_json_strings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _ArtifactConn()
    snapshot = _compile_index_snapshot()
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)

    first = compiler.compile_prose(
        "Review the workflow output carefully.",
        conn=conn,
        compile_index_snapshot=snapshot,
    )
    conn.compile_artifact_rows[0]["authority_refs"] = json.dumps(conn.compile_artifact_rows[0]["authority_refs"])
    conn.compile_artifact_rows[0]["payload"] = json.dumps(conn.compile_artifact_rows[0]["payload"])

    monkeypatch.setattr(
        "runtime.intent_matcher.IntentMatcher",
        lambda *args, **kwargs: pytest.fail("definition compile should reuse JSON-serialized DB artifacts"),
    )
    second = compiler.compile_prose(
        "Review the workflow output carefully.",
        conn=conn,
        compile_index_snapshot=snapshot,
    )

    assert second["reuse_provenance"]["decision"] == "reused"
    assert second["definition"]["definition_revision"] == first["definition"]["definition_revision"]


def test_plan_definition_reuses_exact_artifact_without_replanning(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _ArtifactConn()
    definition = {
        "source_prose": "Build a thing",
        "compiled_prose": "Build a thing",
        "definition_revision": "def_1234abcd",
        "references": [],
        "narrative_blocks": [],
        "draft_flow": [],
        "trigger_intent": [],
    }

    first = planner.plan_definition(definition, title="Alpha", conn=conn)
    assert first["reuse_provenance"]["decision"] == "compiled"

    monkeypatch.setattr(
        planner,
        "_plan_jobs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("planner should reuse exact artifact")),
    )
    second = planner.plan_definition(definition, title="Alpha", conn=conn)

    assert second["reuse_provenance"]["decision"] == "reused"
    assert second["compiled_spec"]["plan_revision"] == first["compiled_spec"]["plan_revision"]


def test_plan_definition_skips_invalid_reusable_artifact() -> None:
    conn = _ArtifactConn()
    definition = {
        "source_prose": "Build a thing",
        "compiled_prose": "Build a thing",
        "definition_revision": "def_1234abcd",
        "references": [],
        "narrative_blocks": [],
        "draft_flow": [],
        "trigger_intent": [],
    }
    planner.plan_definition(definition, title="Alpha", conn=conn)
    conn.compile_artifact_rows[0]["payload"]["plan_revision"] = "plan_tampered"

    result = planner.plan_definition(definition, title="Alpha", conn=conn)

    assert result["reuse_provenance"]["decision"] == "compiled"
