from __future__ import annotations

import json
from io import StringIO

from surfaces.cli.commands import admin as admin_commands


def test_generate_plan_uses_cli_db_authority_when_available(monkeypatch) -> None:
    sentinel = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(admin_commands, "cli_sync_conn", lambda: sentinel)

    def _fake_execute(subsystems, *, operation_name, payload):
        captured["operation_name"] = operation_name
        captured["conn"] = subsystems.get_pg_conn()
        captured["payload"] = payload
        return {
            "kind": "compile_preview",
            "cqrs_role": "query",
            "scope_packet": {"suggested_steps": [{"label": "discover"}]},
        }

    monkeypatch.setattr("runtime.operation_catalog_gateway.execute_operation_from_subsystems", _fake_execute)

    stdout = StringIO()
    exit_code = admin_commands._generate_plan_command(
        [
            "--description",
            "Add retry logic",
            "--match-limit",
            "8",
        ],
        stdout=stdout,
    )

    assert exit_code == 0
    assert captured == {
        "operation_name": "compile_preview",
        "conn": sentinel,
        "payload": {"intent": "Add retry logic", "match_limit": 8},
    }
    payload = json.loads(stdout.getvalue())
    assert payload["kind"] == "compile_preview"


def test_generate_plan_previews_prose(monkeypatch) -> None:
    sentinel = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(admin_commands, "cli_sync_conn", lambda: sentinel)

    def _fake_execute(subsystems, *, operation_name, payload):
        captured["operation_name"] = operation_name
        captured["conn"] = subsystems.get_pg_conn()
        captured["payload"] = payload
        return {
            "kind": "compile_preview",
            "cqrs_role": "query",
            "scope_packet": {"suggested_steps": [{"label": "discover"}]},
        }

    monkeypatch.setattr("runtime.operation_catalog_gateway.execute_operation_from_subsystems", _fake_execute)

    stdout = StringIO()
    exit_code = admin_commands._generate_plan_command(
        ["--description", "Build a custom Gmail integration workflow"],
        stdout=stdout,
    )

    assert exit_code == 0
    assert captured == {
        "operation_name": "compile_preview",
        "conn": sentinel,
        "payload": {"intent": "Build a custom Gmail integration workflow", "match_limit": 5},
    }
    payload = json.loads(stdout.getvalue())
    assert payload["kind"] == "compile_preview"
    assert payload["cqrs_role"] == "query"


def test_generate_plan_rejects_old_intent_file_shape() -> None:
    stdout = StringIO()
    exit_code = admin_commands._generate_plan_command(["intent.json"], stdout=stdout)

    assert exit_code == 1
    assert "intent-file plan generation is not a user-facing surface" in stdout.getvalue()


def test_generate_plan_rejects_old_write_stage_shape() -> None:
    stdout = StringIO()
    exit_code = admin_commands._generate_plan_command(
        [
            "--description",
            "Build a custom Gmail integration workflow",
            "--write",
            "runtime/workflow/unified.py",
            "--stage",
            "build",
        ],
        stdout=stdout,
    )

    assert exit_code == 1
    assert "unknown option: --write" in stdout.getvalue()


def test_materialize_plan_materializes_prose(monkeypatch) -> None:
    sentinel = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(admin_commands, "cli_sync_conn", lambda: sentinel)

    def _fake_execute(subsystems, *, operation_name, payload):
        captured.update(
            {
                "operation_name": operation_name,
                "conn": subsystems.get_pg_conn(),
                "payload": payload,
            }
        )
        return {"kind": "compile_materialization", "workflow_id": payload.get("workflow_id")}

    monkeypatch.setattr("runtime.operation_catalog_gateway.execute_operation_from_subsystems", _fake_execute)

    stdout = StringIO()
    exit_code = admin_commands._materialize_plan_command(
        [
            "--description",
            "Build a custom Gmail integration workflow",
            "--workflow-id",
            "wf_cli_plan",
            "--title",
            "Gmail workflow",
            "--no-llm",
        ],
        stdout=stdout,
    )

    assert exit_code == 0
    assert captured == {
        "operation_name": "compile_materialize",
        "conn": sentinel,
        "payload": {
            "intent": "Build a custom Gmail integration workflow",
            "workflow_id": "wf_cli_plan",
            "title": "Gmail workflow",
            "enable_llm": False,
        },
    }
    payload = json.loads(stdout.getvalue())
    assert payload["kind"] == "compile_materialization"
