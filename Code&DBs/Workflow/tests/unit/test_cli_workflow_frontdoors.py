from __future__ import annotations

import json
import os
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://localhost:5432/praxis")

from surfaces.cli.main import main as workflow_cli_main
from surfaces.cli.commands import operate as operate_commands
from surfaces.cli.commands import workflow as workflow_commands


class _FakeSubsystems:
    def __init__(self, conn: object = object()) -> None:
        self._conn = conn

    def get_pg_conn(self):
        return self._conn

    def get_intent_matcher(self):
        return "matcher"

    def get_manifest_generator(self):
        return "generator"


def test_run_status_frontdoor_supports_idle_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_workflow_tool(params: dict[str, object]):
        captured.update(params)
        return {"run_id": "workflow_123", "status": "running", "health": {"state": "degraded"}}

    monkeypatch.setattr(workflow_commands, "_workflow_tool", _fake_workflow_tool)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "run-status",
                "workflow_123",
                "--kill-if-idle",
                "--idle-threshold-seconds",
                "900",
            ],
            stdout=stdout,
        )
        == 0
    )
    assert captured == {
        "action": "status",
        "run_id": "workflow_123",
        "kill_if_idle": True,
        "idle_threshold_seconds": 900,
    }
    payload = json.loads(stdout.getvalue())
    assert payload["run_id"] == "workflow_123"
    assert payload["status"] == "running"


def test_inspect_job_frontdoor_accepts_optional_label(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_workflow_tool(params: dict[str, object]):
        captured.update(params)
        return {"run_id": "workflow_456", "jobs": [{"label": "build_a"}]}

    monkeypatch.setattr(workflow_commands, "_workflow_tool", _fake_workflow_tool)
    stdout = StringIO()

    assert workflow_cli_main(["inspect-job", "workflow_456", "build_a"], stdout=stdout) == 0
    assert captured == {"action": "inspect", "run_id": "workflow_456", "label": "build_a"}
    payload = json.loads(stdout.getvalue())
    assert payload["jobs"][0]["label"] == "build_a"


def test_notifications_drain_frontdoor_uses_live_notification_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        operate_commands,
        "_workflow_tool",
        lambda params: {
            "notifications": f"drained via {params['action']}",
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["notifications", "drain"], stdout=stdout) == 0
    assert stdout.getvalue().strip() == "drained via notifications"


def test_triggers_frontdoor_supports_list_and_create(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Conn:
        def execute(self, query: str, *params: object):
            normalized = " ".join(query.split())
            if normalized.startswith("SELECT t.*, w.name AS workflow_name"):
                return [
                    {
                        "id": "trg_1",
                        "workflow_id": "wf_1",
                        "workflow_name": "Workflow One",
                        "event_type": "system.event",
                        "filter": {"kind": "match"},
                        "enabled": True,
                        "cron_expression": None,
                        "created_at": None,
                        "last_fired_at": None,
                        "fire_count": 0,
                    }
                ]
            raise AssertionError(f"unexpected query: {query}")

    fake_query_mod = SimpleNamespace(
        _trigger_to_dict=lambda row: {
            "id": row["id"],
            "workflow_id": row["workflow_id"],
            "workflow_name": row["workflow_name"],
            "event_type": row["event_type"],
            "filter": row["filter"],
            "enabled": row["enabled"],
            "cron_expression": row["cron_expression"],
            "created_at": row["created_at"],
            "last_fired_at": row["last_fired_at"],
            "fire_count": row["fire_count"],
        },
        _validate_trigger_body=lambda body, **_kwargs: None,
    )
    monkeypatch.setattr(workflow_commands, "_workflow_subsystems", lambda: _FakeSubsystems(_Conn()))
    monkeypatch.setattr(workflow_commands, "_workflow_query_mod", lambda: fake_query_mod)
    import runtime.canonical_workflows as canonical_workflows

    monkeypatch.setattr(
        canonical_workflows,
        "save_workflow_trigger",
        lambda _conn, *, body: {
            "id": "trg_new",
            "workflow_id": body["workflow_id"],
            "workflow_name": "Workflow One",
            "event_type": body["event_type"],
            "filter": body.get("filter", {}),
            "enabled": body.get("enabled", True),
            "cron_expression": body.get("cron_expression"),
            "created_at": None,
            "last_fired_at": None,
            "fire_count": 0,
        },
    )

    stdout = StringIO()
    assert workflow_cli_main(["triggers", "list"], stdout=stdout) == 0
    listed = json.loads(stdout.getvalue())
    assert listed["count"] == 1
    assert listed["triggers"][0]["id"] == "trg_1"

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "triggers",
                "create",
                "--input-json",
                '{"workflow_id":"wf_1","event_type":"system.event","filter":{"kind":"match"}}',
            ],
            stdout=stdout,
        )
        == 0
    )
    created = json.loads(stdout.getvalue())
    assert created["trigger"]["id"] == "trg_new"
    assert created["trigger"]["workflow_id"] == "wf_1"


def test_manifest_frontdoor_supports_generate_and_save_as(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(workflow_commands, "_workflow_subsystems", lambda: _FakeSubsystems(object()))
    import runtime.canonical_manifests as canonical_manifests

    monkeypatch.setattr(
        canonical_manifests,
        "generate_manifest",
        lambda _conn, *, matcher, generator, intent: SimpleNamespace(
            manifest_id="manifest_123",
            manifest={"id": "manifest_123", "name": "Generated"},
            version=4,
            confidence=0.88,
            explanation=f"generated for {intent} with {matcher}/{generator}",
        ),
    )
    monkeypatch.setattr(
        canonical_manifests,
        "save_manifest_as",
        lambda _conn, *, name, description="", manifest: {
            "id": "saved_456",
            "name": name,
            "description": description,
            "manifest": dict(manifest),
            "version": 99,
        },
    )

    stdout = StringIO()
    assert workflow_cli_main(["manifest", "generate", "moon", "dashboard"], stdout=stdout) == 0
    generated = json.loads(stdout.getvalue())
    assert generated["manifest_id"] == "manifest_123"
    assert generated["confidence"] == 0.88

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"title": "Saved Title", "widgets": []}), encoding="utf-8")
    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "manifest",
                "save-as",
                "--name",
                "Saved Copy",
                "--description",
                "copy for cli",
                "--input-file",
                str(manifest_path),
            ],
            stdout=stdout,
        )
        == 0
    )
    saved = json.loads(stdout.getvalue())
    assert saved["manifest_id"] == "saved_456"
    assert saved["name"] == "Saved Copy"
    assert saved["description"] == "copy for cli"
