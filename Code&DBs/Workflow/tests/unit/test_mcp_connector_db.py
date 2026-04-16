from __future__ import annotations

from surfaces.mcp.tools import connector as connector_tool


def test_connector_conn_uses_shared_mcp_database_authority(monkeypatch) -> None:
    captured: dict[str, object] = {}
    fake_conn = object()

    monkeypatch.setattr(
        connector_tool,
        "workflow_database_env",
        lambda: {
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

    monkeypatch.setattr("storage.postgres.get_workflow_pool", _fake_get_workflow_pool)
    monkeypatch.setattr("storage.postgres.connection.SyncPostgresConnection", _fake_sync_postgres_connection)

    resolved = connector_tool._connector_conn()

    assert resolved is fake_conn
    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow",
        "PATH": "",
    }
    assert captured["pool"] == "pool"
