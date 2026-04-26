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


def test_compile_command_previews_prose_without_legacy_compile_shape(monkeypatch) -> None:
    sentinel = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(admin_commands, "cli_sync_conn", lambda: sentinel)

    class _Preview:
        def to_dict(self):
            return {
                "kind": "compile_preview",
                "cqrs_role": "query",
                "scope_packet": {"suggested_steps": [{"label": "discover"}]},
            }

    def _fake_preview(intent, *, conn):
        captured["intent"] = intent
        captured["conn"] = conn
        return _Preview()

    monkeypatch.setattr("runtime.compile_cqrs.preview_compile", _fake_preview)

    stdout = StringIO()
    exit_code = admin_commands._compile_command(
        ["--description", "Build a custom Gmail integration workflow"],
        stdout=stdout,
    )

    assert exit_code == 0
    assert captured == {
        "intent": "Build a custom Gmail integration workflow",
        "conn": sentinel,
    }
    payload = json.loads(stdout.getvalue())
    assert payload["kind"] == "compile_preview"
    assert payload["cqrs_role"] == "query"


def test_compile_command_materializes_prose_when_requested(monkeypatch) -> None:
    sentinel = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(admin_commands, "cli_sync_conn", lambda: sentinel)

    def _fake_materialize(intent, *, conn, workflow_id, title, enable_llm):
        captured.update(
            {
                "intent": intent,
                "conn": conn,
                "workflow_id": workflow_id,
                "title": title,
                "enable_llm": enable_llm,
            }
        )
        return {"kind": "compile_materialization", "workflow_id": workflow_id}

    monkeypatch.setattr("runtime.compile_cqrs.materialize_workflow", _fake_materialize)

    stdout = StringIO()
    exit_code = admin_commands._compile_command(
        [
            "--description",
            "Build a custom Gmail integration workflow",
            "--action",
            "materialize",
            "--workflow-id",
            "wf_cli_compile",
            "--title",
            "Gmail workflow",
            "--no-llm",
        ],
        stdout=stdout,
    )

    assert exit_code == 0
    assert captured == {
        "intent": "Build a custom Gmail integration workflow",
        "conn": sentinel,
        "workflow_id": "wf_cli_compile",
        "title": "Gmail workflow",
        "enable_llm": False,
    }
    payload = json.loads(stdout.getvalue())
    assert payload["kind"] == "compile_materialization"
