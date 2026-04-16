from __future__ import annotations

from pathlib import Path

from surfaces.cli import _db


def test_cli_sync_conn_uses_shared_surface_database_authority(monkeypatch) -> None:
    captured: dict[str, object] = {}
    fake_conn = object()

    monkeypatch.setattr(
        _db,
        "workflow_database_env_for_repo",
        lambda repo_root: {
            "WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow",
            "PATH": "",
        },
    )

    def _fake_get_workflow_pool(env=None):
        captured["env"] = env
        return "pool"

    def _fake_sync_postgres_connection(pool):
        captured["pool"] = pool
        return fake_conn

    monkeypatch.setattr(_db, "get_workflow_pool", _fake_get_workflow_pool)
    monkeypatch.setattr(_db, "SyncPostgresConnection", _fake_sync_postgres_connection)

    resolved = _db.cli_sync_conn()

    assert _db.cli_repo_root() == Path(__file__).resolve().parents[4]
    assert resolved is fake_conn
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow",
        "PATH": "",
    }
    assert captured["pool"] == "pool"
