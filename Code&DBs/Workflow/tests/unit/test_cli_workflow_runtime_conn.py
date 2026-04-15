from __future__ import annotations

from surfaces.cli.commands import workflow as workflow_commands


def test_workflow_runtime_conn_uses_runtime_database_authority(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "runtime._workflow_database.resolve_runtime_database_url",
        lambda required=True: "postgresql://postgres@127.0.0.1:5432/praxis",
    )
    monkeypatch.setattr(
        "storage.postgres.connection.get_workflow_pool",
        lambda env=None: captured.setdefault("env", env) or object(),
    )
    monkeypatch.setattr(
        "storage.postgres.connection.SyncPostgresConnection",
        lambda pool: {"pool": pool},
    )

    conn = workflow_commands._workflow_runtime_conn()

    assert captured["env"] == {
        "WORKFLOW_DATABASE_URL": "postgresql://postgres@127.0.0.1:5432/praxis",
    }
    assert conn == {"pool": captured["env"]}
