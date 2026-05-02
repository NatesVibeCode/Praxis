from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

import runtime.materialize_index as compile_index

_REPO_ROOT = str(Path(__file__).resolve().parents[4])


class _CompileIndexConn:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.inserted_row: dict[str, Any] | None = None

    def execute(self, query: str, *params: Any) -> list[dict[str, Any]]:
        self.executed.append((query, params))
        if "INSERT INTO compile_index_snapshots" in query:
            self.inserted_row = {
                "compile_index_ref": params[0],
                "compile_surface_revision": params[1],
                "compile_surface_name": params[2],
                "schema_version": params[3],
                "repo_root": params[4],
                "repo_fingerprint": params[5],
                "source_fingerprints": params[6],
                "source_counts": params[7],
                "payload": params[8],
                "decision_ref": params[9],
                "refreshed_at": params[10],
                "stale_after_at": params[11],
                "refresh_count": 1,
            }
            return []
        if "FROM compile_index_snapshots" in query:
            if self.inserted_row is not None:
                return [dict(self.inserted_row)]
            return self.rows
        return []


def _source_rows() -> dict[str, list[dict[str, Any]]]:
    catalog = [
        {
            "slug": "@gmail/search",
            "ref_type": "integration",
            "display_name": "Gmail Search",
            "resolved_id": "gmail",
            "resolved_table": "integration_registry",
            "description": "Search connected Gmail accounts",
        }
    ]
    integrations = [
        {
            "id": "gmail",
            "name": "Gmail",
            "provider": "google",
            "auth_status": "connected",
            "description": "Mail provider",
            "icon": "mail",
            "mcp_server_id": "mcp.gmail",
            "capabilities": [{"action": "search", "description": "Search inbox"}],
        }
    ]
    object_types = [
        {
            "type_id": "ticket",
            "name": "Ticket",
            "description": "Support ticket",
            "icon": "ticket",
            "fields": [
                {"name": "status", "label": "Status", "type": "string", "description": "Workflow state"}
            ],
        }
    ]
    route_hints = [("review", "auto/review")]
    capabilities = [
        {
            "id": "cap-research-local-knowledge",
            "slug": "research/local-knowledge",
            "kind": "memory",
            "title": "Local knowledge recall",
            "summary": "Search prior findings and saved research before going outbound.",
            "description": "Uses praxis_research and the local research runtime to search existing findings and compile briefs before new work starts.",
            "route": "praxis_research",
            "engines": ["praxis_research", "memory.research_runtime"],
            "signals": ["research"],
            "reference_slugs": [],
            "enabled": True,
            "binding_revision": "binding.capability_catalog.bootstrap.20260408",
            "decision_ref": "decision.capability_catalog.bootstrap.20260408",
        }
    ]
    return {
        "catalog": catalog,
        "integrations": integrations,
        "object_types": object_types,
        "route_hints": route_hints,
        "capabilities": capabilities,
    }


def _snapshot_row(
    *,
    compile_index_ref: str = "compile_index.compiler.test",
    compile_surface_revision: str = "compile_surface.compiler.test",
    compile_surface_name: str = "compiler",
    refreshed_at: datetime | None = None,
    stale_after_at: datetime | None = None,
    repo_root: str = _REPO_ROOT,
    repo_fingerprint: str = "repo-fingerprint",
) -> dict[str, Any]:
    refreshed_at = refreshed_at or datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    stale_after_at = stale_after_at or datetime(2030, 1, 1, 0, 0, tzinfo=timezone.utc)
    payload = {
        "schema_version": 1,
        "repo_info": {
            "repo_root": repo_root,
            "git_head": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "git_branch": "main",
            "git_dirty": False,
            "git_status_hash": "0123456789abcdef",
            "repo_fingerprint": repo_fingerprint,
        },
        "surface_manifest": {
            "repo_root": repo_root,
            "surface_name": "compiler",
            "surface_revision": "surface_compiler_manifest_test",
            "tracked_files": ["Code&DBs/Workflow/runtime/compile_index.py"],
            "file_fingerprints": {
                "Code&DBs/Workflow/runtime/compile_index.py": "surface-file-fingerprint"
            },
        },
        "source_fingerprints": {
            "reference_catalog": "catalog-fingerprint",
            "integration_registry": "integration-fingerprint",
            "object_types": "object-type-fingerprint",
            "materializer_route_hints": "route-hint-fingerprint",
            "capability_catalog": "capability-fingerprint",
        },
        "source_counts": {
            "reference_catalog": 1,
            "integration_registry": 1,
            "object_types": 1,
            "materializer_route_hints": 1,
            "capability_catalog": 1,
        },
        "reference_catalog": _source_rows()["catalog"],
        "integration_registry": _source_rows()["integrations"],
        "object_types": _source_rows()["object_types"],
        "materializer_route_hints": [
            {"hint_text": hint, "route_slug": route}
            for hint, route in _source_rows()["route_hints"]
        ],
        "capability_catalog": _source_rows()["capabilities"],
    }
    return {
        "compile_index_ref": compile_index_ref,
        "compile_surface_revision": compile_surface_revision,
        "compile_surface_name": compile_surface_name,
        "schema_version": 1,
        "repo_root": repo_root,
        "repo_fingerprint": repo_fingerprint,
        "source_fingerprints": payload["source_fingerprints"],
        "source_counts": payload["source_counts"],
        "payload": payload,
        "decision_ref": "decision.compile.index.refresh.test",
        "refreshed_at": refreshed_at,
        "stale_after_at": stale_after_at,
        "refresh_count": 1,
    }


def test_current_compile_surface_manifest_tracks_live_authority_modules() -> None:
    manifest = compile_index.current_compile_surface_manifest(Path(_REPO_ROOT))
    tracked_files = set(manifest["tracked_files"])

    assert "Code&DBs/Workflow/runtime/compile_index.py" in tracked_files
    assert "Code&DBs/Workflow/runtime/compile_reuse.py" in tracked_files
    assert "Code&DBs/Workflow/runtime/compiler.py" in tracked_files
    assert "Code&DBs/Workflow/runtime/definition_compile_kernel.py" in tracked_files
    assert "Code&DBs/Workflow/runtime/capability_catalog.py" in tracked_files
    assert "Code&DBs/Workflow/registry/reference_catalog_sync.py" in tracked_files
    assert "Code&DBs/Workflow/registry/integration_registry_sync.py" in tracked_files
    assert "Code&DBs/Workflow/runtime/reference_catalog.py" not in tracked_files
    assert "Code&DBs/Workflow/runtime/integration_registry_sync.py" not in tracked_files
    assert manifest["surface_revision"].startswith("surface_")


def test_refresh_compile_index_materializes_durable_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _CompileIndexConn()
    source_rows = _source_rows()

    monkeypatch.setattr(compile_index, "sync_integration_registry", lambda _conn: 1)
    monkeypatch.setattr(
        compile_index,
        "sync_reference_catalog",
        lambda _conn, **kwargs: 1,
    )
    monkeypatch.setattr(compile_index, "sync_capability_catalog", lambda _conn, **kwargs: 1)
    monkeypatch.setattr(compile_index, "_load_integrations", lambda _conn: source_rows["integrations"])
    monkeypatch.setattr(compile_index, "_load_object_types", lambda _conn: source_rows["object_types"])
    monkeypatch.setattr(compile_index, "_load_reference_catalog", lambda _conn: source_rows["catalog"])
    monkeypatch.setattr(compile_index, "_load_compiler_route_hints", lambda _conn: source_rows["route_hints"])
    monkeypatch.setattr(compile_index, "load_capability_catalog", lambda _conn: source_rows["capabilities"])
    monkeypatch.setattr(
        compile_index,
        "current_repo_fingerprint",
        lambda repo_root=None: {
            "repo_root": str(Path(repo_root).resolve()) if repo_root is not None else _REPO_ROOT,
            "git_head": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "git_branch": "main",
            "git_dirty": False,
            "git_status_hash": "0123456789abcdef",
            "repo_fingerprint": "repo-fingerprint",
        },
    )
    monkeypatch.setattr(
        compile_index,
        "current_compile_surface_manifest",
        lambda repo_root=None: {
            "repo_root": str(Path(repo_root).resolve()) if repo_root is not None else _REPO_ROOT,
            "surface_name": "compiler",
            "surface_revision": "surface_compiler_manifest_test",
            "tracked_files": ["Code&DBs/Workflow/runtime/compile_index.py"],
            "file_fingerprints": {
                "Code&DBs/Workflow/runtime/compile_index.py": "surface-file-fingerprint"
            },
        },
    )

    snapshot = compile_index.refresh_compile_index(
        conn,
        repo_root=Path(_REPO_ROOT),
        stale_after_seconds=120,
        decision_ref="decision.compile.index.refresh.test",
    )

    insert_queries = [
        query
        for query, _ in conn.executed
        if "INSERT INTO compile_index_snapshots" in query
    ]
    assert len(insert_queries) == 1
    assert "$6::jsonb" not in insert_queries[0]
    assert "$9::jsonb" in insert_queries[0]
    assert snapshot.compile_index_ref.startswith("compile_index.compiler.")
    assert snapshot.compile_surface_revision.startswith("compile_surface.compiler.")
    assert snapshot.compile_surface_name == "compiler"
    assert snapshot.repo_fingerprint == "repo-fingerprint"
    assert snapshot.surface_manifest["surface_revision"] == "surface_compiler_manifest_test"
    assert snapshot.refresh_count == 1
    assert snapshot.source_counts == {
        "reference_catalog": 1,
        "integration_registry": 1,
        "object_types": 1,
        "materializer_route_hints": 1,
        "capability_catalog": 1,
    }
    assert snapshot.summary()["route_hint_count"] == 1
    assert snapshot.to_compile_context()["route_hints"] == [("review", "auto/review")]
    assert snapshot.connected_integrations()[0]["id"] == "gmail"
    assert snapshot.route_hint_cache() == (("review", "auto/review"),)
    assert any(
        query.strip().startswith("SELECT compile_index_ref")
        for query, _ in conn.executed
    )


def test_load_compile_index_snapshot_returns_latest_fresh_snapshot() -> None:
    conn = _CompileIndexConn(rows=[_snapshot_row()])

    snapshot = compile_index.load_compile_index_snapshot(
        conn,
        surface_name="compiler",
        require_fresh=True,
        repo_root=None,
    )

    assert snapshot.compile_index_ref == "compile_index.compiler.test"
    assert snapshot.compile_surface_revision == "compile_surface.compiler.test"
    assert snapshot.freshness_state == "fresh"
    assert snapshot.freshness_reason is None
    assert snapshot.summary()["freshness_state"] == "fresh"
    assert conn.executed[0][0].strip().startswith("SELECT compile_index_ref")


def test_load_compile_index_snapshot_missing_fails_closed() -> None:
    conn = _CompileIndexConn(rows=[])

    with pytest.raises(compile_index.MaterializeIndexAuthorityError) as exc_info:
        compile_index.load_compile_index_snapshot(
            conn,
            surface_name="compiler",
            require_fresh=True,
            repo_root=None,
        )

    assert exc_info.value.reason_code == "compile_index.snapshot_missing"


def test_load_compile_index_snapshot_stale_fails_closed() -> None:
    conn = _CompileIndexConn(
        rows=[
            _snapshot_row(
                stale_after_at=datetime(2026, 4, 8, 18, 0, tzinfo=timezone.utc),
            )
        ]
    )

    with pytest.raises(compile_index.MaterializeIndexAuthorityError) as exc_info:
        compile_index.load_compile_index_snapshot(
            conn,
            surface_name="compiler",
            require_fresh=True,
            repo_root=None,
        )

    assert exc_info.value.reason_code == "compile_index.snapshot_stale"
    assert exc_info.value.details["freshness_state"] == "stale"


def test_load_compile_index_snapshot_ignores_unrelated_repo_dirt_when_surface_manifest_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _CompileIndexConn(rows=[_snapshot_row(repo_fingerprint="old-repo-fingerprint")])
    monkeypatch.setattr(
        compile_index,
        "current_compile_surface_manifest",
        lambda repo_root=None: {
            "repo_root": str(Path(repo_root).resolve()) if repo_root is not None else _REPO_ROOT,
            "surface_name": "compiler",
            "surface_revision": "surface_compiler_manifest_test",
            "tracked_files": ["Code&DBs/Workflow/runtime/compile_index.py"],
            "file_fingerprints": {
                "Code&DBs/Workflow/runtime/compile_index.py": "surface-file-fingerprint"
            },
        },
    )
    monkeypatch.setattr(
        compile_index,
        "current_repo_fingerprint",
        lambda repo_root=None: {
            "repo_root": str(Path(repo_root).resolve()) if repo_root is not None else _REPO_ROOT,
            "git_head": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "git_branch": "main",
            "git_dirty": True,
            "git_status_hash": "ffffffffffffffff",
            "repo_fingerprint": "new-repo-fingerprint",
        },
    )

    snapshot = compile_index.load_compile_index_snapshot(
        conn,
        surface_name="compiler",
        require_fresh=True,
        repo_root=Path(_REPO_ROOT),
    )

    assert snapshot.freshness_state == "fresh"


def test_load_compile_index_snapshot_stales_when_surface_manifest_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _CompileIndexConn(rows=[_snapshot_row()])
    monkeypatch.setattr(
        compile_index,
        "current_compile_surface_manifest",
        lambda repo_root=None: {
            "repo_root": str(Path(repo_root).resolve()) if repo_root is not None else _REPO_ROOT,
            "surface_name": "compiler",
            "surface_revision": "surface_compiler_manifest_changed",
            "tracked_files": ["Code&DBs/Workflow/runtime/compile_index.py"],
            "file_fingerprints": {
                "Code&DBs/Workflow/runtime/compile_index.py": "surface-file-fingerprint-changed"
            },
        },
    )

    with pytest.raises(compile_index.MaterializeIndexAuthorityError) as exc_info:
        compile_index.load_compile_index_snapshot(
            conn,
            surface_name="compiler",
            require_fresh=True,
            repo_root=Path(_REPO_ROOT),
        )

    assert exc_info.value.reason_code == "compile_index.snapshot_stale"
    assert exc_info.value.details["reason"] == "surface_manifest_mismatch"
