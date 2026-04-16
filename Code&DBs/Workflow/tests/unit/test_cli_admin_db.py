from __future__ import annotations

import json
from io import StringIO
from types import SimpleNamespace

from surfaces.cli.commands import admin as admin_commands


def test_compile_command_uses_cli_db_authority_when_available(monkeypatch) -> None:
    sentinel = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(admin_commands, "cli_sync_conn", lambda: sentinel)

    def _fake_compile_spec(intent_dict, *, conn=None):
        captured["intent_dict"] = intent_dict
        captured["conn"] = conn
        return (
            SimpleNamespace(
                to_dispatch_spec_dict=lambda: {"name": "compiled-spec", "jobs": []}
            ),
            [],
        )

    monkeypatch.setattr("runtime.spec_compiler.compile_spec", _fake_compile_spec)

    stdout = StringIO()
    exit_code = admin_commands._compile_command(
        [
            "--description",
            "Add retry logic",
            "--write",
            "runtime/workflow/unified.py",
            "--stage",
            "build",
        ],
        stdout=stdout,
    )

    assert exit_code == 0
    assert captured["conn"] is sentinel
    assert captured["intent_dict"]["description"] == "Add retry logic"
    payload = json.loads(stdout.getvalue())
    assert payload["name"] == "compiled-spec"
