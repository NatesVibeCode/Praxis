from __future__ import annotations

import json
import os
from io import StringIO
from pathlib import Path

import pytest

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://postgres@localhost:5432/praxis")

from surfaces.cli.commands import data as data_commands
from surfaces.cli.main import main as workflow_cli_main
from surfaces.mcp.catalog import get_tool_catalog


def test_data_help_is_available() -> None:
    stdout = StringIO()

    rc = workflow_cli_main(["data", "--help"], stdout=stdout)

    assert rc == 2
    rendered = stdout.getvalue()
    assert "workflow data profile <input-file>" in rendered
    assert "workflow data join --job-file <job.json>" in rendered
    assert "workflow data merge --job-file <job.json>" in rendered
    assert "workflow data split --job-file <job.json>" in rendered
    assert "workflow data sync --job-file <job.json>" in rendered
    assert "workflow data launch --job-file <job.json>" in rendered


def test_data_profile_alias_uses_catalog_backed_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True, "stats": {"row_count": 1, "field_count": 1, "fields": []}}

    monkeypatch.setattr(data_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["data", "profile", "artifacts/data/users.csv"], stdout=stdout) == 0
    assert captured == {
        "tool_name": "praxis_data",
        "params": {"action": "profile", "input_path": "artifacts/data/users.csv"},
    }


def test_data_join_alias_uses_catalog_backed_runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    job_path = tmp_path / "join.json"
    job_path.write_text(
        json.dumps(
            {
                "operation": "join",
                "records": [{"user_id": "u1"}],
                "secondary_records": [{"user_id": "u1", "amount": 10}],
                "keys": ["user_id"],
                "right_prefix": "order_",
            }
        ),
        encoding="utf-8",
    )

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True, "record_count": 1, "records_preview": [{}], "records_truncated": False}

    monkeypatch.setattr(data_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["data", "join", "--job-file", str(job_path)], stdout=stdout) == 0
    assert captured["tool_name"] == "praxis_data"
    assert captured["params"] == {
        "action": "join",
        "job": {
            "operation": "join",
            "records": [{"user_id": "u1"}],
            "secondary_records": [{"user_id": "u1", "amount": 10}],
            "keys": ["user_id"],
            "right_prefix": "order_",
        },
    }


def test_data_export_alias_uses_catalog_backed_runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    job_path = tmp_path / "export.json"
    job_path.write_text(
        json.dumps(
            {
                "operation": "export",
                "records": [{"id": "u1", "email": "alice@example.com", "status": "active"}],
                "fields": ["id", "email"],
                "field_map": {"email": "user_email"},
            }
        ),
        encoding="utf-8",
    )

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True, "record_count": 1, "records_preview": [{}], "records_truncated": False}

    monkeypatch.setattr(data_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["data", "export", "--job-file", str(job_path)], stdout=stdout) == 0
    assert captured["tool_name"] == "praxis_data"
    assert captured["params"] == {
        "action": "export",
        "job": {
            "operation": "export",
            "records": [{"id": "u1", "email": "alice@example.com", "status": "active"}],
            "fields": ["id", "email"],
            "field_map": {"email": "user_email"},
        },
    }


def test_data_parse_requires_yes_when_output_file_requested() -> None:
    stdout = StringIO()

    rc = workflow_cli_main(
        [
            "data",
            "parse",
            "artifacts/data/users.csv",
            "--output-file",
            "artifacts/data/users.json",
        ],
        stdout=stdout,
    )

    assert rc == 2
    rendered = stdout.getvalue()
    assert "tool: praxis_data" in rendered
    assert "confirmation required" in rendered


def test_data_launch_requires_yes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    job_path = tmp_path / "dedupe.json"
    job_path.write_text(
        json.dumps(
            {
                "operation": "dedupe",
                "records": [{"email": "a@example.com"}],
                "keys": ["email"],
            }
        ),
        encoding="utf-8",
    )
    stdout = StringIO()

    rc = workflow_cli_main(["data", "launch", "--job-file", str(job_path)], stdout=stdout)

    assert rc == 2
    rendered = stdout.getvalue()
    assert "risk: dispatch" in rendered
    assert "confirmation required" in rendered


def test_tools_catalog_exposes_data_entrypoint() -> None:
    definition = get_tool_catalog()["praxis_data"]

    assert definition.cli_entrypoint == "workflow data"
    assert definition.example_input()["action"] == "profile"
    assert "join" in definition.selector_enum
    assert "merge" in definition.selector_enum
    assert "aggregate" in definition.selector_enum
    assert "split" in definition.selector_enum
    assert "export" in definition.selector_enum
    assert "sync" in definition.selector_enum
