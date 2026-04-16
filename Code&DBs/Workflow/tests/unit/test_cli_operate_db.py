from __future__ import annotations

from io import StringIO

from surfaces.cli.commands import operate as operate_commands


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, *params):
        self.calls.append((sql, params))
        return []


def test_notifications_command_uses_cli_db_authority_for_persisted_rows(monkeypatch) -> None:
    fake_conn = _FakeConn()
    monkeypatch.setattr(operate_commands, "cli_sync_conn", lambda: fake_conn)

    stdout = StringIO()
    exit_code = operate_commands._notifications_command(["tail", "5"], stdout=stdout)

    assert exit_code == 0
    assert fake_conn.calls
    _, params = fake_conn.calls[0]
    assert params == (5,)
    assert stdout.getvalue().strip() == "no notifications found"
