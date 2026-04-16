from __future__ import annotations

from surfaces.cli.commands import workflow as workflow_commands


def test_workflow_runtime_conn_uses_runtime_database_authority(monkeypatch) -> None:
    sentinel = object()
    monkeypatch.setattr(workflow_commands, "cli_sync_conn", lambda: sentinel)

    conn = workflow_commands._workflow_runtime_conn()

    assert conn is sentinel
