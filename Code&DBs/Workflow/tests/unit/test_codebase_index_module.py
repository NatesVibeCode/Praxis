from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
_REPO_ROOT = str(Path(__file__).resolve().parents[4])
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.codebase_index_module import CodebaseIndexModule


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
