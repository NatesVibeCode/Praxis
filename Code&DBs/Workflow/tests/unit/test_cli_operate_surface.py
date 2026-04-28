from __future__ import annotations

from io import StringIO

from surfaces.cli.commands import operate as operate_commands
import surfaces.api.rest as rest


def test_operate_call_normalizes_hyphenated_mode(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_execute(request, **_kwargs):
        captured["mode"] = request.mode
        captured["operation"] = request.operation
        return 200, {"ok": True, "result": {"accepted": True}}

    monkeypatch.setattr(rest, "execute_operate_request", _fake_execute)

    stdout = StringIO()
    rc = operate_commands._operate_command(
        [
            "call",
            "operator.next",
            "--mode",
            "ComManD",
            "--input-json",
            '{"limit":5}',
            "--json",
        ],
        stdout=stdout,
    )

    assert rc == 0
    assert captured == {"mode": "command", "operation": "operator.next"}


def test_operate_rejects_invalid_mode_before_api_call(monkeypatch) -> None:
    called = {"value": False}

    def _fake_execute(_request, **_kwargs):
        called["value"] = True
        return 200, {"ok": True}

    monkeypatch.setattr(rest, "execute_operate_request", _fake_execute)

    stdout = StringIO()
    rc = operate_commands._operate_command(
        [
            "call",
            "operator.next",
            "--mode",
            "launch",
            "--input-json",
            '{"limit":5}',
        ],
        stdout=stdout,
    )

    assert rc == 2
    assert called["value"] is False
    assert "mode must be one of: call, query, command" in stdout.getvalue()


def test_operate_query_rejects_conflicting_mode(monkeypatch) -> None:
    called = {"value": False}

    def _fake_execute(_request, **_kwargs):
        called["value"] = True
        return 200, {"ok": True}

    monkeypatch.setattr(rest, "execute_operate_request", _fake_execute)

    stdout = StringIO()
    rc = operate_commands._operate_command(
        [
            "query",
            "operator.next",
            "--mode",
            "command",
            "--input-json",
            '{"limit":5}',
        ],
        stdout=stdout,
    )

    assert rc == 2
    assert called["value"] is False
    assert "--mode must match the selected operate subcommand (`query`)" in stdout.getvalue()
