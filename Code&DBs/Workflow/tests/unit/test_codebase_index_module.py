from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
_REPO_ROOT = str(Path(__file__).resolve().parents[4])
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import runtime.codebase_index_module as codebase_index_module
from runtime.codebase_index_module import CodebaseIndexModule
from surfaces.mcp.tools.discover import tool_praxis_discover


def test_ensure_conn_wraps_database_url_with_authoritative_connection(monkeypatch) -> None:
    expected = object()
    captured: list[dict[str, str] | None] = []

    def _fake_ensure_postgres_available(env=None):
        captured.append(env)
        return expected

    monkeypatch.setattr(
        "storage.postgres.connection.ensure_postgres_available",
        _fake_ensure_postgres_available,
    )

    module = CodebaseIndexModule(
        conn="postgresql://test@localhost:5432/praxis_test",
        repo_root=_REPO_ROOT,
    )

    assert module._ensure_conn() is expected
    assert captured == [{"WORKFLOW_DATABASE_URL": "postgresql://test@localhost:5432/praxis_test"}]


def test_ensure_conn_preserves_connection_like_object() -> None:
    class _Conn:
        def execute(self, query: str, *args):
            return []

    conn = _Conn()
    module = CodebaseIndexModule(conn=conn, repo_root=_REPO_ROOT)

    assert module._ensure_conn() is conn


def test_run_fails_when_indexer_reports_degraded_observability(monkeypatch) -> None:
    class _Conn:
        def execute(self, query: str, *args):
            return []

    class _FakeIndexer:
        def __init__(self, *, conn, repo_root):
            self.conn = conn
            self.repo_root = repo_root

        def index_codebase(self):
            return {
                "indexed": 1,
                "skipped": 0,
                "total": 2,
                "observability_state": "degraded",
                "errors": (
                    {
                        "scope": "index_codebase",
                        "code": "module_indexer.index_failed",
                        "module_path": "src/writer.py",
                        "kind": "module",
                        "name": "writer",
                        "error_type": "RuntimeError",
                        "error_message": "write lane offline",
                    },
                ),
            }

    monkeypatch.setattr("runtime.module_indexer.ModuleIndexer", _FakeIndexer)

    module = CodebaseIndexModule(conn=_Conn(), repo_root=_REPO_ROOT)
    result = module.run()

    assert result.ok is False
    assert result.error == "discovery index degraded: src/writer.py/module/writer: write lane offline"


def test_run_skips_vector_index_when_disabled(monkeypatch) -> None:
    class _Conn:
        def execute(self, query: str, *args):
            return []

    class _FakeKnowledgeGraph:
        def __init__(self) -> None:
            self.ingests: list[tuple[str, str, str]] = []

        def ingest(self, *, kind: str, content: str, source: str, metadata=None):
            self.ingests.append((kind, content, source))
            return {"kind": kind, "content": content, "source": source}

    def _unexpected_indexer(*_args, **_kwargs):
        raise AssertionError("ModuleIndexer should not be constructed when indexing is disabled")

    monkeypatch.setattr("runtime.module_indexer.ModuleIndexer", _unexpected_indexer)
    monkeypatch.setattr(
        codebase_index_module,
        "_extract_dependency_map",
        lambda _workflow_root: ({"runtime": {"runtime/module.py": {"classes": [], "deps": []}}}, []),
    )
    monkeypatch.setattr(
        codebase_index_module,
        "_build_subsystem_doc",
        lambda name, mods, edges: f"{name}:{len(mods)}:{len(edges)}",
    )

    module = CodebaseIndexModule(
        conn=_Conn(),
        repo_root=_REPO_ROOT,
        knowledge_graph=_FakeKnowledgeGraph(),
        index_codebase_enabled=False,
    )

    result = module.run()

    assert result.ok is True
    assert result.error is None


class _ProgressEmitter:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def log(self, message: str) -> None:
        self.messages.append(message)

    def emit(self, *, progress: int, total: int, message: str) -> None:
        self.messages.append(message)


def test_discover_reindex_reports_degraded_progress_when_indexer_has_partial_failures(
    monkeypatch,
) -> None:
    class _FakeIndexer:
        def index_codebase(self, *, subdirs=None, force=False):
            assert subdirs is None
            assert force is False
            return {
                "indexed": 1,
                "skipped": 0,
                "total": 2,
                "observability_state": "degraded",
                "errors": (
                    {
                        "scope": "index_codebase",
                        "code": "module_indexer.index_failed",
                        "module_path": "src/writer.py",
                        "kind": "module",
                        "name": "writer",
                        "error_type": "RuntimeError",
                        "error_message": "write lane offline",
                    },
                ),
            }

    monkeypatch.setattr(
        "surfaces.mcp.tools.discover._subs.get_module_indexer",
        lambda: _FakeIndexer(),
    )

    progress = _ProgressEmitter()
    result = tool_praxis_discover({"action": "reindex"}, progress)

    assert result["action"] == "reindex"
    assert result["result"]["observability_state"] == "degraded"
    assert progress.messages[-1] == "Degraded — 1 entities indexed, 1 errors"
