from __future__ import annotations

import json
from io import StringIO

import pytest

from surfaces.cli.commands import praxis_authoring as authoring_commands
from surfaces.cli.praxis import main as praxis_main


def test_praxis_help_lists_namespaces() -> None:
    stdout = StringIO()

    assert praxis_main(["--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "praxis workflow <command>" in rendered
    assert "praxis db <status|plan|apply|describe>" in rendered
    assert "praxis page scaffold" in rendered


def test_praxis_workflow_namespace_delegates_to_workflow_frontdoor() -> None:
    stdout = StringIO()

    assert praxis_main(["workflow", "--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "Most used:" in rendered
    assert "workflow tools list" in rendered


def test_praxis_db_primitive_scaffold_renders_sql() -> None:
    stdout = StringIO()

    assert (
        praxis_main(
            [
                "db",
                "primitive",
                "scaffold",
                "customer",
                "--field",
                "account_name:text:required",
            ],
            stdout=stdout,
        )
        == 0
    )
    payload = json.loads(stdout.getvalue())
    assert payload["primitive"]["primitive_type"] == "customer"
    assert "CREATE TABLE IF NOT EXISTS customer_primitives" in payload["sql"]
    assert "account_name TEXT NOT NULL" in payload["sql"]


def test_praxis_data_shape_plan_renders_materialization_sql() -> None:
    stdout = StringIO()

    assert (
        praxis_main(
            [
                "data",
                "shape",
                "plan",
                "--spec-json",
                json.dumps(
                    {
                        "target_name": "customer_360",
                        "sources": [
                            {
                                "name": "hubspot",
                                "relation": "hubspot_companies",
                                "field_map": {
                                    "company_id": "canonical_key",
                                    "company_name": "title",
                                },
                            }
                        ],
                    }
                ),
            ],
            stdout=stdout,
        )
        == 0
    )
    payload = json.loads(stdout.getvalue())
    assert payload["target_name"] == "customer_360"
    assert payload["materialization"]["view_name"] == "customer_360_canonical_v"
    assert "FROM hubspot_companies" in payload["materialization"]["sql"]


def test_praxis_object_type_scaffold_apply_uses_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    stdout = StringIO()

    monkeypatch.setattr(authoring_commands, "_sync_conn", lambda: object())
    monkeypatch.setattr(
        authoring_commands,
        "upsert_object_type",
        lambda conn, **kwargs: {"saved": True, **kwargs},
    )

    assert (
        praxis_main(
            [
                "object-type",
                "scaffold",
                "customer",
                "--field",
                "customer_id:text:required",
                "--apply",
                "--yes",
            ],
            stdout=stdout,
        )
        == 0
    )
    payload = json.loads(stdout.getvalue())
    assert payload["object_type"]["type_id"] == "customer"
    assert payload["saved_object_type"]["saved"] is True


def test_praxis_page_scaffold_apply_persists_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    stdout = StringIO()

    monkeypatch.setattr(authoring_commands, "_sync_conn", lambda: object())
    monkeypatch.setattr(authoring_commands, "upsert_object_type", lambda conn, **kwargs: kwargs)
    monkeypatch.setattr(
        authoring_commands,
        "save_manifest",
        lambda conn, **kwargs: {"id": kwargs["manifest_id"], "saved": True},
    )

    assert (
        praxis_main(
            [
                "page",
                "scaffold",
                "customer health dashboard",
                "--apply",
                "--yes",
            ],
            stdout=stdout,
        )
        == 0
    )
    payload = json.loads(stdout.getvalue())
    assert payload["bindings"]["primary_type"] == "record"
    assert payload["saved_manifest"]["saved"] is True


def test_praxis_unknown_namespace_suggests_known_roots() -> None:
    stdout = StringIO()

    assert praxis_main(["regstry"], stdout=stdout) == 2
    rendered = stdout.getvalue()
    assert "unknown namespace: regstry" in rendered
    assert "praxis registry" in rendered
