from __future__ import annotations

import json
import os
from io import StringIO
from pathlib import Path

import pytest

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://postgres@localhost:5432/praxis")

from surfaces.cli.commands import authority as authority_commands
from surfaces.cli.main import main as workflow_cli_main


def _write_fake_workflow_tree(tmp_path: Path, *, filenames: list[str]) -> Path:
    workflow_root = tmp_path / "Code&DBs" / "Workflow"
    migration_root = tmp_path / "Code&DBs" / "Databases" / "migrations" / "workflow"
    authority_root = workflow_root / "system_authority"
    migration_root.mkdir(parents=True)
    authority_root.mkdir(parents=True)
    for name in filenames:
        (migration_root / name).write_text("-- test\n", encoding="utf-8")
    spec = {
        "canonical_manifest": filenames,
        "policy_buckets": {"canonical": filenames, "bootstrap_only": [], "deprecated": [], "dead": []},
        "expected_objects": {name: [] for name in filenames},
        "tie_break_order": {},
    }
    (authority_root / "workflow_migration_authority.json").write_text(
        json.dumps(spec, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return workflow_root


def test_top_level_help_mentions_authority_frontdoors() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow schema|registry|object-type|object-field|object|catalog|files|reload|reconcile" in rendered
    assert "workflow handoff <latest|lineage|status|history>" in rendered
    assert "workflow maintenance backfill-failure-categories --yes" in rendered


def test_schema_help_is_available() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["schema", "--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow schema status" in rendered
    assert "workflow schema describe <object-name|migration.sql>" in rendered
    assert "workflow schema next-migration <slug>" in rendered
    assert "workflow schema renumber-migrations" in rendered


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


def test_schema_next_migration_renders_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        authority_commands,
        "_schema_next_migration_payload",
        lambda *, slug: {
            "scope": "workflow",
            "next_prefix": 328,
            "requested_slug": slug,
            "normalized_slug": "repo_policy_onboarding",
            "proposed_filename": "328_repo_policy_onboarding.sql",
            "renumber_applied": False,
            "renumber_actions": [],
            "operator_messages": [],
            "managed_duplicate_prefixes": {"324": ["324_a.sql", "324_b.sql"]},
            "unmanaged_duplicate_prefixes": {},
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["schema", "next-migration", "repo policy onboarding", "--json"], stdout=stdout) == 0
    payload = json.loads(stdout.getvalue())
    assert payload["next_prefix"] == 328
    assert payload["proposed_filename"] == "328_repo_policy_onboarding.sql"


def test_schema_next_migration_renders_auto_renumber_notice(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        authority_commands,
        "_schema_next_migration_payload",
        lambda *, slug: {
            "scope": "workflow",
            "next_prefix": 340,
            "requested_slug": slug,
            "normalized_slug": "next_policy",
            "proposed_filename": "340_next_policy.sql",
            "renumber_applied": True,
            "renumber_actions": [
                {
                    "old_filename": "338_conflict.sql",
                    "new_filename": "339_conflict.sql",
                    "reason": "unmanaged duplicate prefix 338; kept 338_existing.sql",
                }
            ],
            "operator_messages": [
                "Automatically renumbered unmanaged duplicate migration prefixes before allocating the next migration: 338_conflict.sql -> 339_conflict.sql."
            ],
            "managed_duplicate_prefixes": {},
            "unmanaged_duplicate_prefixes": {},
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["schema", "next-migration", "next policy"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "Automatically renumbered unmanaged duplicate migration prefixes" in rendered
    assert "338_conflict.sql -> 339_conflict.sql" in rendered
    assert "proposed_filename=340_next_policy.sql" in rendered


def test_schema_next_migration_payload_auto_repairs_duplicate_prefixes(tmp_path: Path) -> None:
    workflow_root = _write_fake_workflow_tree(
        tmp_path,
        filenames=["100_alpha.sql", "100_beta.sql", "101_next.sql"],
    )

    payload = authority_commands._schema_next_migration_payload(
        slug="operator notice proof",
        workflow_root=workflow_root,
    )

    assert payload["proposed_filename"] == "103_operator_notice_proof.sql"
    assert payload["renumber_applied"] is True
    assert payload["renumber_actions"] == [
        {
            "old_filename": "100_beta.sql",
            "new_filename": "102_beta.sql",
            "reason": "unmanaged duplicate prefix 100; kept 100_alpha.sql",
        }
    ]
    assert payload["operator_messages"] == [
        "Automatically renumbered unmanaged duplicate migration prefixes before "
        "allocating the next migration: 100_beta.sql -> 102_beta.sql."
    ]
    assert payload["unmanaged_duplicate_prefixes"] == {}


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
    captured: dict[str, object] = {}

    def _execute(*, env, operation_name: str, payload):
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "type": {"type_id": payload["type_id"], "name": payload["name"]},
            "operation_receipt": {"operation_name": operation_name},
        }

    monkeypatch.setattr(authority_commands.operation_catalog_gateway, "execute_operation_from_env", _execute)
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
                "--fields-json",
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
    assert captured["operation_name"] == "object_schema.type_upsert_by_id"
    assert captured["payload"]["fields"] == []


def test_object_field_upsert_calls_runtime_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _execute(*, env, operation_name: str, payload):
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {
            "type_id": payload["type_id"],
            "field": {"name": payload["field_name"], "type": payload["field_kind"]},
            "operation_receipt": {"operation_name": operation_name},
        }

    monkeypatch.setattr(authority_commands.operation_catalog_gateway, "execute_operation_from_env", _execute)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "object-field",
                "upsert",
                "--type-id",
                "ticket",
                "--field-name",
                "status",
                "--field-kind",
                "enum",
                "--options-json",
                '["open","closed"]',
                "--yes",
                "--json",
            ],
            stdout=stdout,
        )
        == 0
    )
    payload = json.loads(stdout.getvalue())
    assert payload["field"]["name"] == "status"
    assert captured["operation_name"] == "object_schema.field_upsert"
    assert captured["payload"]["options"] == ["open", "closed"]


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
                '{"catalog_item_id":"x","surface_name":"canvas","label":"X","icon":"x","family":"think","status":"ready","drop_kind":"node","action_value":"auto/x","description":"","truth_category":"runtime","truth_badge":"ok","truth_detail":"ok","surface_tier":"primary","surface_badge":"ok","surface_detail":"ok","enabled":true,"display_order":1,"binding_revision":"rev","decision_ref":"decision"}',
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
