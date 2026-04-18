"""Quick smoke test for the operating model compiler."""

from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest


WORKFLOW_ROOT = Path(__file__).resolve().parents[1]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from _pg_test_conn import ensure_test_database_ready

os.environ.setdefault("WORKFLOW_DATABASE_URL", ensure_test_database_ready())

import runtime.compiler as compiler_module
from runtime.compiler import compile_prose


@pytest.fixture(autouse=True)
def _stub_external_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the smoke test bounded when DB or planner infrastructure is unavailable."""

    class _FakeConn:
        def execute(self, *_args, **_kwargs):
            return []

    class _FakeArtifactStore:
        def __init__(self, _conn) -> None:
            pass

        def load_reusable_artifact(self, **_kwargs):
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
        lambda *_args, **_kwargs: compiler_module._fallback_compile_index_snapshot(
            reason="compiler smoke test",
        ),
    )
    monkeypatch.setattr(
        compiler_module,
        "_resolve_compiler_embedder",
        lambda: (None, {"mode": "degraded", "reason": "compiler smoke test"}),
    )
    monkeypatch.setattr(compiler_module, "CompileArtifactStore", _FakeArtifactStore)
    monkeypatch.setattr(compiler_module, "_call_llm_compile", _fake_llm_compile)


def _assert_compiler_shape(result: dict) -> None:
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert "definition" in result, f"Missing definition: {result.keys()}"
    assert "unresolved" in result, f"Missing unresolved: {result.keys()}"
    assert result["error"] is None or isinstance(
        result["error"], str
    ), f"Unexpected error type: {type(result.get('error'))}"


def test_basic_compilation() -> None:
    result = compile_prose(
        "When a bug is filed, analyze severity and notify the team if critical"
    )
    _assert_compiler_shape(result)
    definition = result["definition"]
    assert isinstance(definition["compiled_prose"], str)
    assert isinstance(definition["references"], list)
    assert isinstance(definition["draft_flow"], list)
    assert isinstance(definition["execution_setup"], dict)
    assert isinstance(definition["surface_manifest"], dict)
    assert isinstance(definition["build_receipt"], dict)
    print(f'✓ Compiled: {definition["compiled_prose"][:100]}...')
    print(f'  References: {len(definition["references"])} found')
    print(f'  Draft steps: {len(definition["draft_flow"])} planned')
    print(f'  Unresolved: {result["unresolved"]}')
    if result.get("error"):
        print(f'  Non-fatal error: {result["error"]}')


def test_empty_prose() -> None:
    result = compile_prose("")
    _assert_compiler_shape(result)
    assert result.get("error") is not None or result["definition"].get("compiled_prose") == ""
    print("✓ Empty prose handled")


def test_with_references() -> None:
    result = compile_prose(
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
