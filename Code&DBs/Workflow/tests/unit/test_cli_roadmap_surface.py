from __future__ import annotations

import json
from io import StringIO

import pytest

from surfaces.cli.commands import roadmap as roadmap_commands
from surfaces.cli.main import main as workflow_cli_main


def test_roadmap_view_routes_to_read_model_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        captured["workflow_token"] = workflow_token
        return 0, {"ok": True}

    monkeypatch.setattr(roadmap_commands, "run_cli_tool", _fake_run_cli_tool)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "roadmap",
                "view",
                "--root",
                "roadmap_item.phase_program.build_closure.phase_002",
                "--semantic-neighbor-limit",
                "8",
            ],
            stdout=stdout,
        )
        == 0
    )

    assert captured["tool_name"] == "praxis_operator_roadmap_view"
    assert captured["params"] == {
        "root_roadmap_item_id": "roadmap_item.phase_program.build_closure.phase_002",
        "semantic_neighbor_limit": 8,
    }


def test_roadmap_write_commit_routes_to_command_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True, "action": params.get("action")}

    monkeypatch.setattr(roadmap_commands, "run_cli_tool", _fake_run_cli_tool)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "roadmap",
                "write",
                "commit",
                "--title",
                "Phase 002 Build Closure",
                "--intent-brief",
                "Promote closure work with explicit proof gates",
                "--parent",
                "roadmap_item.phase_program.build_closure",
                "--depends-on",
                "roadmap_item.phase_program.build_closure.phase_001",
                "--phase-ready",
            ],
            stdout=stdout,
        )
        == 0
    )

    assert captured["tool_name"] == "praxis_operator_write"
    assert captured["params"] == {
        "action": "commit",
        "title": "Phase 002 Build Closure",
        "intent_brief": "Promote closure work with explicit proof gates",
        "parent_roadmap_item_id": "roadmap_item.phase_program.build_closure",
        "depends_on": ["roadmap_item.phase_program.build_closure.phase_001"],
        "phase_ready": True,
    }
    payload = json.loads(stdout.getvalue())
    assert payload["action"] == "commit"


def test_roadmap_closeout_preview_routes_to_command_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True}

    monkeypatch.setattr(roadmap_commands, "run_cli_tool", _fake_run_cli_tool)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "roadmap",
                "closeout",
                "preview",
                "--bug-id",
                "bug.workflow.123",
                "--roadmap-item-id",
                "roadmap_item.phase_program.build_closure.phase_001",
            ],
            stdout=stdout,
        )
        == 0
    )

    assert captured["tool_name"] == "praxis_operator_closeout"
    assert captured["params"] == {
        "action": "preview",
        "bug_ids": ["bug.workflow.123"],
        "roadmap_item_ids": ["roadmap_item.phase_program.build_closure.phase_001"],
    }


def test_roadmap_help_topic_and_root_help_include_new_surface() -> None:
    topic_stdout = StringIO()
    root_stdout = StringIO()

    assert workflow_cli_main(["help", "roadmap"], stdout=topic_stdout) == 0
    assert workflow_cli_main(["--help"], stdout=root_stdout) == 0

    assert "workflow roadmap view" in topic_stdout.getvalue()
    assert "workflow roadmap write <preview|validate|commit>" in topic_stdout.getvalue()
    assert "workflow roadmap view|status|scoreboard|graph|write|closeout" in root_stdout.getvalue()


def test_roadmap_status_without_run_id_prints_run_scoped_guidance() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["roadmap", "status"], stdout=stdout) == 2
    rendered = stdout.getvalue()
    assert "usage: workflow roadmap status --run-id <run_id>" in rendered
    assert "note: this command is run-scoped." in rendered
    assert "workflow roadmap view" in rendered
