"""Quick smoke test for the operating model compiler."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest


WORKFLOW_ROOT = Path(__file__).resolve().parents[1]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

import runtime.materializer as compiler_module
from runtime.materializer import materialize_prose


def _compile_index_snapshot() -> compiler_module.MaterializeIndexSnapshot:
    now = datetime(2026, 4, 23, 0, 0, tzinfo=timezone.utc)
    repo_root = str(WORKFLOW_ROOT.parents[1])
    source_counts = {
        "reference_catalog": 1,
        "integration_registry": 1,
        "object_types": 1,
        "materializer_route_hints": 1,
        "capability_catalog": 1,
    }
    return compiler_module.MaterializeIndexSnapshot(
        schema_version=1,
        materialize_index_ref="materialize_index.materializer.smoke_test",
        compile_surface_revision="materialize_surface.materializer.smoke_test",
        materialize_surface_name="compiler",
        repo_root=repo_root,
        repo_fingerprint="compiler-smoke-test-authority",
        repo_info={"repo_root": repo_root, "repo_fingerprint": "compiler-smoke-test-authority"},
        surface_manifest={"surface_revision": "compiler-smoke-test"},
        source_fingerprints={key: f"{key}.fingerprint" for key in source_counts},
        source_counts=source_counts,
        decision_ref="decision.materializer.smoke_test",
        refresh_count=1,
        refreshed_at=now,
        stale_after_at=now,
        freshness_state="fresh",
        freshness_reason=None,
        reference_catalog=(
            {
                "slug": "@gmail/inbox",
                "ref_type": "integration",
                "display_name": "Gmail Inbox",
                "resolved_id": "gmail",
                "resolved_table": "integration_registry",
            },
        ),
        integration_registry=(
            {
                "id": "gmail",
                "name": "Gmail",
                "provider": "google",
                "auth_status": "connected",
                "capabilities": [{"action": "search", "description": "Search inbox"}],
            },
        ),
        object_types=(
            {"type_id": "invoice", "name": "Invoice", "description": "Invoice status"},
        ),
        materializer_route_hints=(("review", "auto/review"),),
        capability_catalog=(
            {"slug": "capability.gmail.search", "summary": "Search Gmail inboxes"},
        ),
        payload={"authority": "materialize_index.smoke_test"},
    )


@pytest.fixture(autouse=True)
def _stub_external_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep external services bounded while preserving compile-index authority shape."""

    class _FakeConn:
        def execute(self, *_args, **_kwargs):
            return []

    class _FakeArtifactStore:
        def __init__(self, _conn) -> None:
            pass

        def load_reusable_artifact(self, **_kwargs):
            return None

        def record_definition(self, **_kwargs):
            return None

    def _fake_llm_compile(prose: str, context: str, *, conn=None) -> dict:
        return {
            "title": compiler_module._derive_title(prose, prose),
            "prose": prose,
            "authority": "",
            "sla": {},
        }

    monkeypatch.setattr(compiler_module, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(
        compiler_module,
        "_load_compile_index_snapshot_with_auto_refresh",
        lambda *_args, **_kwargs: _compile_index_snapshot(),
    )
    monkeypatch.setattr(
        compiler_module,
        "_resolve_compiler_embedder",
        lambda: (None, {"mode": "degraded", "reason": "compiler smoke test"}),
    )
    monkeypatch.setattr(compiler_module, "MaterializeArtifactStore", _FakeArtifactStore)
    monkeypatch.setattr(compiler_module, "_call_llm_compile", _fake_llm_compile)


def _assert_compiler_shape(result: dict) -> None:
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert "definition" in result, f"Missing definition: {result.keys()}"
    assert "unresolved" in result, f"Missing unresolved: {result.keys()}"
    if result["materialize_index"] is not None:
        assert result["materialize_index"]["freshness_state"] == "fresh"
        assert result["materialize_index"]["materialize_index_ref"] != "materialize_index:fallback"
    assert result["error"] is None or isinstance(
        result["error"], str
    ), f"Unexpected error type: {type(result.get('error'))}"


def test_basic_compilation() -> None:
    result = materialize_prose(
        "When a bug is filed, analyze severity and notify the team if critical"
    )
    _assert_compiler_shape(result)
    definition = result["definition"]
    assert isinstance(definition["materialized_prose"], str)
    assert isinstance(definition["references"], list)
    assert isinstance(definition["draft_flow"], list)
    assert isinstance(definition["execution_setup"], dict)
    assert isinstance(definition["surface_manifest"], dict)
    assert isinstance(definition["build_receipt"], dict)
    print(f'✓ Compiled: {definition["materialized_prose"][:100]}...')
    print(f'  References: {len(definition["references"])} found')
    print(f'  Draft steps: {len(definition["draft_flow"])} planned')
    print(f'  Unresolved: {result["unresolved"]}')
    if result.get("error"):
        print(f'  Non-fatal error: {result["error"]}')


def test_empty_prose() -> None:
    result = materialize_prose("")
    _assert_compiler_shape(result)
    assert result.get("error") is not None or result["definition"].get("materialized_prose") == ""
    print("✓ Empty prose handled")


def test_with_references() -> None:
    result = materialize_prose(
        "Search @gmail/inbox for invoices and update #invoice/status to paid"
    )
    _assert_compiler_shape(result)
    refs = result["definition"].get("references", [])
    assert isinstance(refs, list)
    assert any(ref.get("slug") == "@gmail/inbox" for ref in refs), refs
    assert any(ref.get("slug") == "#invoice/status" for ref in refs), refs
    print(f"✓ Reference test: {len(refs)} references found")
    for ref in refs:
        print(
            f'  - {ref.get("slug", "?")} ({ref.get("type", "?")}) '
            f'resolved={ref.get("resolved_to") is not None}'
        )


if __name__ == "__main__":
    print("=== Compiler Smoke Tests ===")
    test_basic_compilation()
    test_empty_prose()
    test_with_references()
    print("\n=== All tests passed ===")
