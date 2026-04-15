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
from surfaces.mcp.tools import data as data_tools


def test_data_help_is_available() -> None:
    stdout = StringIO()

    rc = workflow_cli_main(["data", "--help"], stdout=stdout)

    assert rc == 2
    rendered = stdout.getvalue()
    assert "workflow data profile <input-file>" in rendered
    assert "workflow data join --job-file <job.json>" in rendered
    assert "workflow data repair --job-file <job.json>" in rendered
    assert "workflow data repair_loop --job-file <job.json>" in rendered
    assert "workflow data checkpoint --job-file <job.json>" in rendered
    assert "workflow data approve --job-file <job.json>" in rendered
    assert "workflow data apply --job-file <job.json>" in rendered
    assert "--checkpoint-manifest-id <id>" in rendered
    assert "--plan-manifest-id <id>" in rendered
    assert "--approval-manifest-id <id>" in rendered
    assert "workflow data approve --plan-manifest-id plan_abc123" in rendered
    assert "workflow data apply --plan-manifest-id plan_abc123 --approval-manifest-id approval_def456" in rendered
    assert "workflow data merge --job-file <job.json>" in rendered
    assert "workflow data split --job-file <job.json>" in rendered
    assert "workflow data dead_letter --job-file <job.json>" in rendered
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


def test_data_checkpoint_alias_uses_catalog_backed_runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    job_path = tmp_path / "checkpoint.json"
    job_path.write_text(
        json.dumps(
            {
                "operation": "checkpoint",
                "records": [{"id": "1", "updated_at": "2025-01-01T00:00:00Z"}],
                "keys": ["id"],
                "cursor_field": "updated_at",
            }
        ),
        encoding="utf-8",
    )

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True, "checkpoint": {"row_count": 1, "content_hash": "abc123"}}

    monkeypatch.setattr(data_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["data", "checkpoint", "--job-file", str(job_path)], stdout=stdout) == 0
    assert captured["tool_name"] == "praxis_data"
    assert captured["params"] == {
        "action": "checkpoint",
        "job": {
            "operation": "checkpoint",
            "records": [{"id": "1", "updated_at": "2025-01-01T00:00:00Z"}],
            "keys": ["id"],
            "cursor_field": "updated_at",
        },
    }


def test_data_replay_alias_supports_checkpoint_manifest_id(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True, "record_count": 1, "records_preview": [{"id": "3"}], "records_truncated": False}

    monkeypatch.setattr(data_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "data",
                "replay",
                "--checkpoint-manifest-id",
                "checkpoint_xyz789",
                "--input-file",
                "artifacts/data/events.json",
                "--cursor-field",
                "updated_at",
            ],
            stdout=stdout,
        )
        == 0
    )
    assert captured["tool_name"] == "praxis_data"
    assert captured["params"] == {
        "action": "replay",
        "checkpoint_manifest_id": "checkpoint_xyz789",
        "input_path": "artifacts/data/events.json",
        "cursor_field": "updated_at",
    }


def test_data_approve_alias_uses_catalog_backed_runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    job_path = tmp_path / "approve.json"
    job_path.write_text(
        json.dumps(
            {
                "operation": "approve",
                "plan": {"create": [], "update": [], "delete": [], "noop": [], "conflicts": []},
                "approved_by": "ops",
                "approval_reason": "Reviewed diff and counts",
            }
        ),
        encoding="utf-8",
    )

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True, "approval": {"approved_by": "ops", "plan_digest": "abc123"}}

    monkeypatch.setattr(data_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["data", "approve", "--job-file", str(job_path), "--yes"], stdout=stdout) == 0
    assert captured["tool_name"] == "praxis_data"
    assert captured["params"] == {
        "action": "approve",
        "job": {
            "operation": "approve",
            "plan": {"create": [], "update": [], "delete": [], "noop": [], "conflicts": []},
            "approved_by": "ops",
            "approval_reason": "Reviewed diff and counts",
        },
    }


def test_data_approve_alias_supports_plan_manifest_id_and_renders_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {
            "ok": True,
            "approval": {"approved_by": "ops", "plan_digest": "abc123"},
            "plan_manifest_id": "plan_abc123",
        }

    monkeypatch.setattr(data_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "data",
                "approve",
                "--plan-manifest-id",
                "plan_abc123",
                "--approved-by",
                "ops",
                "--approval-reason",
                "Reviewed diff and counts",
                "--yes",
            ],
            stdout=stdout,
        )
        == 0
    )
    assert captured["tool_name"] == "praxis_data"
    assert captured["params"] == {
        "action": "approve",
        "plan_manifest_id": "plan_abc123",
        "approved_by": "ops",
        "approval_reason": "Reviewed diff and counts",
    }
    rendered = stdout.getvalue()
    assert "approved_by=ops" in rendered
    assert "plan=abc123" in rendered
    assert "plan_manifest=plan_abc123" in rendered


def test_data_apply_alias_supports_manifest_ids_and_renders_refs(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {
            "ok": True,
            "record_count": 1,
            "plan_digest": "abc123",
            "plan_manifest_id": "plan_abc123",
            "approval_manifest_id": "approval_def456",
            "records_preview": [{"id": "u1"}],
            "records_truncated": False,
        }

    monkeypatch.setattr(data_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "data",
                "apply",
                "--plan-manifest-id",
                "plan_abc123",
                "--approval-manifest-id",
                "approval_def456",
                "--secondary-input-file",
                "artifacts/data/target.json",
                "--keys",
                "id",
                "--yes",
            ],
            stdout=stdout,
        )
        == 0
    )
    assert captured["tool_name"] == "praxis_data"
    assert captured["params"] == {
        "action": "apply",
        "plan_manifest_id": "plan_abc123",
        "approval_manifest_id": "approval_def456",
        "secondary_input_path": "artifacts/data/target.json",
        "keys": ["id"],
    }
    rendered = stdout.getvalue()
    assert "records: 1" in rendered
    assert "plan: abc123" in rendered
    assert "plan_manifest=plan_abc123" in rendered
    assert "approval_manifest=approval_def456" in rendered


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


def test_mcp_data_tool_resolves_manifest_ids_before_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    plan_manifest = {
        "kind": "praxis_control_manifest",
        "manifest_family": "control_plane",
        "manifest_type": "data_plan",
        "plan": {"create": [], "update": [], "delete": [], "noop": [], "conflicts": []},
        "plan_digest": "abc123",
    }
    approval_manifest = {
        "kind": "praxis_control_manifest",
        "manifest_family": "control_plane",
        "manifest_type": "data_approval",
        "approval": {"approved_by": "ops", "approval_reason": "Reviewed diff and counts"},
    }

    class _FakePg:
        def fetchrow(self, query: str, manifest_id: str):
            if manifest_id == "plan_abc123":
                return {
                    "id": "plan_abc123",
                    "name": "Plan manifest",
                    "description": "Plan manifest description",
                    "status": "draft",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "manifest": plan_manifest,
                }
            if manifest_id == "approval_def456":
                return {
                    "id": "approval_def456",
                    "name": "Approval manifest",
                    "description": "Approval manifest description",
                    "status": "approved",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "manifest": approval_manifest,
                }
            return None

    captured: dict[str, object] = {}

    monkeypatch.setattr(data_tools._subs, "get_pg_conn", lambda: _FakePg())

    def _execute_data_job(job: dict[str, object], **kwargs: object):
        captured["job"] = dict(job)
        captured["kwargs"] = dict(kwargs)
        return {
            "ok": True,
            "record_count": 1,
            "records_preview": [{"id": "u1"}],
            "records_truncated": False,
            "plan_digest": "abc123",
        }

    monkeypatch.setattr(data_tools, "execute_data_job", _execute_data_job)

    result = data_tools.tool_praxis_data(
        {
            "action": "apply",
            "plan_manifest_id": "plan_abc123",
            "approval_manifest_id": "approval_def456",
            "secondary_input": {"records": [{"id": "u1"}]},
            "keys": ["id"],
        }
    )

    assert captured["kwargs"]["default_operation"] == "apply"
    assert captured["kwargs"]["pg_conn"].__class__.__name__ == "_FakePg"
    assert captured["job"]["plan_manifest_id"] == "plan_abc123"
    assert captured["job"]["approval_manifest_id"] == "approval_def456"
    assert result["plan_manifest_id"] == "plan_abc123"
    assert result["approval_manifest_id"] == "approval_def456"
    assert result["plan_manifest"]["manifest_id"] == "plan_abc123"
    assert result["approval_manifest"]["manifest_id"] == "approval_def456"


def test_tools_catalog_exposes_data_entrypoint() -> None:
    definition = get_tool_catalog()["praxis_data"]

    assert definition.cli_entrypoint == "workflow data"
    assert definition.example_input()["action"] == "profile"
    assert "repair" in definition.selector_enum
    assert "repair_loop" in definition.selector_enum
    assert "checkpoint" in definition.selector_enum
    assert "approve" in definition.selector_enum
    assert "apply" in definition.selector_enum
    assert "join" in definition.selector_enum
    assert "merge" in definition.selector_enum
    assert "aggregate" in definition.selector_enum
    assert "split" in definition.selector_enum
    assert "export" in definition.selector_enum
    assert "dead_letter" in definition.selector_enum
    assert "sync" in definition.selector_enum
