from __future__ import annotations

import json
import os
from io import StringIO

import pytest

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://postgres@localhost:5432/praxis")

from surfaces.cli.commands import authority as authority_commands
from surfaces.cli.main import main as workflow_cli_main


def test_top_level_help_mentions_authority_frontdoors() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow schema|registry|object-type|object|catalog|files|reload|reconcile" in rendered
    assert "workflow handoff <latest|lineage|status|history>" in rendered


def test_schema_help_is_available() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["schema", "--help"], stdout=stdout) == 2
    rendered = stdout.getvalue()
    assert "workflow schema status" in rendered
    assert "workflow schema describe <object-name|migration.sql>" in rendered


def test_schema_status_renders_json(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_schema_status_payload(*, scope: str) -> dict[str, object]:
        assert scope == "workflow"
        return {
            "scope": "workflow",
            "bootstrapped": True,
            "expected_count": 10,
            "missing_objects": [],
            "missing_by_migration": {},
        }

    monkeypatch.setattr(authority_commands, "_schema_status_payload", _fake_schema_status_payload)
    stdout = StringIO()

    assert workflow_cli_main(["schema", "status", "--json"], stdout=stdout) == 0
    payload = json.loads(stdout.getvalue())
    assert payload["bootstrapped"] is True


def test_registry_list_delegates_to_runtime_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(authority_commands, "_sync_conn", lambda: object())
    monkeypatch.setattr(
        authority_commands,
        "list_app_manifests",
        lambda conn, **kwargs: [{"id": "manifest-1", "status": "draft", "kind": "x", "manifest_type": "y"}],
    )
    stdout = StringIO()

    assert workflow_cli_main(["registry", "list", "--json"], stdout=stdout) == 0
    payload = json.loads(stdout.getvalue())
    assert payload["manifests"][0]["id"] == "manifest-1"


def test_registry_upsert_requires_confirmation() -> None:
    stdout = StringIO()

    assert (
        workflow_cli_main(
            ["registry", "upsert", "--id", "manifest-1", "--manifest-json", '{"kind":"x"}'],
            stdout=stdout,
        )
        == 2
    )
    assert "confirmation required" in stdout.getvalue()


def test_object_type_upsert_calls_runtime_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(authority_commands, "_sync_conn", lambda: object())
    monkeypatch.setattr(
        authority_commands,
        "upsert_object_type",
        lambda conn, **kwargs: {"type_id": kwargs["type_id"], "name": kwargs["name"]},
    )
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "object-type",
                "upsert",
                "--type-id",
                "ticket",
                "--name",
                "Ticket",
                "--property-definitions-json",
                "[]",
                "--yes",
                "--json",
            ],
            stdout=stdout,
        )
        == 0
    )
    payload = json.loads(stdout.getvalue())
    assert payload["type"]["type_id"] == "ticket"


def test_object_upsert_routes_to_create_when_object_id_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(authority_commands, "_sync_conn", lambda: object())
    monkeypatch.setattr(
        authority_commands,
        "create_object",
        lambda conn, **kwargs: {"object_id": "obj-1", "type_id": kwargs["type_id"], "status": "active"},
    )
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "object",
                "upsert",
                "--type-id",
                "ticket",
                "--properties-json",
                '{"title":"Bug"}',
                "--yes",
                "--json",
            ],
            stdout=stdout,
        )
        == 0
    )
    payload = json.loads(stdout.getvalue())
    assert payload["object"]["type_id"] == "ticket"


def test_catalog_upsert_requires_confirmation() -> None:
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "catalog",
                "upsert",
                "--item-json",
                '{"catalog_item_id":"x","surface_name":"moon","label":"X","icon":"x","family":"think","status":"ready","drop_kind":"node","action_value":"auto/x","description":"","truth_category":"runtime","truth_badge":"ok","truth_detail":"ok","surface_tier":"primary","surface_badge":"ok","surface_detail":"ok","enabled":true,"display_order":1,"binding_revision":"rev","decision_ref":"decision"}',
            ],
            stdout=stdout,
        )
        == 2
    )
    assert "confirmation required" in stdout.getvalue()


def test_reload_command_uses_reload_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(authority_commands, "tool_praxis_reload", lambda _params: {"reloaded": ["mcp_catalog"]})
    stdout = StringIO()

    assert workflow_cli_main(["reload"], stdout=stdout) == 0
    assert "mcp_catalog" in stdout.getvalue()


def test_reconcile_alias_delegates_to_data_command(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_data_command(args: list[str], *, stdout):
        captured["args"] = list(args)
        stdout.write("ok\n")
        return 0

    monkeypatch.setattr(authority_commands, "_data_command", _fake_data_command)
    stdout = StringIO()

    assert workflow_cli_main(["reconcile", "--job-file", "job.json"], stdout=stdout) == 0
    assert captured["args"] == ["reconcile", "--job-file", "job.json"]
