from __future__ import annotations

from surfaces.api.handlers import webhook_ingest


def test_webhook_conn_uses_shared_surface_database_authority(monkeypatch) -> None:
    captured: dict[str, object] = {}
    fake_conn = object()

    monkeypatch.setattr(
        webhook_ingest,
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

    monkeypatch.setattr(webhook_ingest, "get_workflow_pool", _fake_get_workflow_pool)
    monkeypatch.setattr(webhook_ingest, "SyncPostgresConnection", _fake_sync_postgres_connection)

    resolved = webhook_ingest._webhook_conn()

    assert resolved is fake_conn
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow",
        "PATH": "",
    }
    assert captured["pool"] == "pool"
