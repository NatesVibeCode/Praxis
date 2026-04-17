from __future__ import annotations

import json
import importlib
from io import StringIO

from surfaces.cli import workflow_cli
from surfaces.cli.commands import query as workflow_query

workflow_main = importlib.import_module("surfaces.cli.main")


def test_commands_prints_command_index(capsys) -> None:
    rc = workflow_cli.main(["commands"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "usage: workflow_cli.py <command> [args]" in rendered
    assert "commands / help" in rendered
    assert "workflow_cli.py diagnose <run_id>" in rendered
    assert "workflow_cli.py routes" in rendered
    assert "workflow_cli.py tools" in rendered
    assert "workflow_cli.py run <spec.json>" in rendered


def test_help_topic_prints_command_usage(capsys) -> None:
    rc = workflow_cli.main(["help", "run"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "usage: workflow_cli.py run <spec.json>" in rendered
    assert "Tip: `workflow_cli.py commands` shows the full command index." in rendered


def test_help_topic_routes_points_at_modern_discovery_frontdoor(capsys) -> None:
    rc = workflow_cli.main(["help", "routes"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "workflow api routes" in rendered
    assert "workflow routes --tag workflow --json" in rendered
    assert "workflow_cli.py routes" in rendered
    assert "workflow help api" in rendered


def test_help_topic_api_is_an_alias_for_routes(capsys) -> None:
    rc = workflow_cli.main(["help", "api"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "workflow api routes" in rendered
    assert "workflow routes --tag workflow --json" in rendered
    assert "workflow help api" in rendered


def test_help_topic_tools_points_at_modern_discovery_frontdoor(capsys) -> None:
    rc = workflow_cli.main(["help", "tools"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "workflow tools list" in rendered
    assert "workflow tools describe <tool|alias>" in rendered
    assert "workflow_cli.py tools" in rendered
    assert "workflow mcp" in rendered


def test_help_topic_mcp_is_an_alias_for_tools(capsys) -> None:
    rc = workflow_cli.main(["help", "mcp"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "workflow tools list" in rendered
    assert "workflow tools describe <tool|alias>" in rendered
    assert "workflow mcp" in rendered


def test_help_topic_diagnose_prints_command_usage(capsys) -> None:
    rc = workflow_cli.main(["help", "diagnose"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "usage: workflow_cli.py diagnose <run_id>" in rendered
    assert (
        "Tip: run `workflow_cli.py diagnose <run_id>` to inspect one workflow receipt "
        "and provider-health context."
    ) in rendered


def test_help_topic_bugs_exposes_the_full_bug_surface(capsys) -> None:
    rc = workflow_main.main(["help", "bugs"], stdout=None)

    assert rc == 0
    rendered = capsys.readouterr().out
    assert (
        "workflow bugs "
        "[list|search <query>|stats|file|history|packet|replay|backfill_replay|attach_evidence|patch_resume|resolve]"
    ) in rendered
    assert "file               File a new bug" in rendered
    assert "attach_evidence    Attach canonical evidence to a bug" in rendered
    assert "resolve            Mark an existing bug fixed, deferred, or won't-fix; FIXED may run verifier proof" in rendered


def test_top_level_help_aliases_return_command_index(capsys) -> None:
    rc = workflow_cli.main(["--help"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "workflow_cli.py repair <run_id>" in rendered


def test_modern_help_index_mentions_bug_tracker_surface() -> None:
    stdout = StringIO()

    rc = workflow_main.main(["commands"], stdout=stdout)

    assert rc == 0
    rendered = stdout.getvalue()
    assert "Derived search, analysis, and bug-tracker surfaces" in rendered


def test_bugs_command_resolve_dispatches_mutation_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        captured["workflow_token"] = workflow_token
        return 0, {"resolved": True, "bug": {"bug_id": "BUG-1234", "status": "FIXED"}}

    monkeypatch.setattr(workflow_query, "run_cli_tool", _fake_run_cli_tool)

    stdout = StringIO()
    rc = workflow_query._bugs_command(
        ["resolve", "--bug-id", "BUG-1234", "--status", "FIXED"],
        stdout=stdout,
    )

    assert rc == 0
    assert captured["tool_name"] == "praxis_bugs"
    assert captured["workflow_token"] == ""
    assert captured["params"]["action"] == "resolve"
    assert captured["params"]["bug_id"] == "BUG-1234"
    assert captured["params"]["status"] == "FIXED"
    assert json.loads(stdout.getvalue()) == {
        "resolved": True,
        "bug": {"bug_id": "BUG-1234", "status": "FIXED"},
    }


def test_bugs_command_resolve_fixed_forwards_verifier_inputs(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        captured["workflow_token"] = workflow_token
        return 0, {
            "resolved": True,
            "bug": {"bug_id": "BUG-1234", "status": "FIXED"},
            "verification": {"verification_run_id": "verification_run:test"},
        }

    monkeypatch.setattr(workflow_query, "run_cli_tool", _fake_run_cli_tool)

    stdout = StringIO()
    rc = workflow_query._bugs_command(
        [
            "resolve",
            "--bug-id",
            "BUG-1234",
            "--status",
            "FIXED",
            "--verifier-ref",
            "verifier.job.python.pytest_file",
            "--inputs-json",
            '{"path":"Code&DBs/Workflow/tests/unit/test_bug_surface_contract.py"}',
            "--target-kind",
            "bug",
            "--target-ref",
            "BUG-1234",
        ],
        stdout=stdout,
    )

    assert rc == 0
    assert captured["tool_name"] == "praxis_bugs"
    assert captured["params"]["action"] == "resolve"
    assert captured["params"]["verifier_ref"] == "verifier.job.python.pytest_file"
    assert captured["params"]["inputs"] == {
        "path": "Code&DBs/Workflow/tests/unit/test_bug_surface_contract.py",
    }
    assert captured["params"]["target_kind"] == "bug"
    assert captured["params"]["target_ref"] == "BUG-1234"


def test_bugs_command_search_forwards_status_and_severity_filters(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        captured["workflow_token"] = workflow_token
        return 0, {"bugs": [], "count": 0, "returned_count": 0}

    monkeypatch.setattr(workflow_query, "run_cli_tool", _fake_run_cli_tool)

    stdout = StringIO()
    rc = workflow_query._bugs_command(
        ["search", "timeout", "--status", "OPEN", "--severity", "P1", "--json"],
        stdout=stdout,
    )

    assert rc == 0
    assert captured["tool_name"] == "praxis_bugs"
    assert captured["params"]["action"] == "search"
    assert captured["params"]["title"] == "timeout"
    assert captured["params"]["status"] == "OPEN"
    assert captured["params"]["severity"] == "P1"
    assert json.loads(stdout.getvalue()) == {"bugs": [], "count": 0, "returned_count": 0}


def test_bugs_command_file_parses_resume_context_json(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        captured["workflow_token"] = workflow_token
        return 0, {"bug": {"bug_id": "BUG-1234"}}

    monkeypatch.setattr(workflow_query, "run_cli_tool", _fake_run_cli_tool)

    stdout = StringIO()
    rc = workflow_query._bugs_command(
        [
            "file",
            "--title",
            "tmp probe",
            "--description",
            "tmp",
            "--severity",
            "P3",
            "--category",
            "OTHER",
            "--filed-by",
            "codex",
            "--source-kind",
            "manual",
            "--resume-context-json",
            '{"hypothesis":"x"}',
        ],
        stdout=stdout,
    )

    assert rc == 0
    assert captured["tool_name"] == "praxis_bugs"
    assert captured["params"]["action"] == "file"
    assert captured["params"]["resume_context"] == {"hypothesis": "x"}
    assert json.loads(stdout.getvalue()) == {"bug": {"bug_id": "BUG-1234"}}


def test_recall_command_surfaces_unavailable_backend_state(monkeypatch) -> None:
    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        return 1, {
            "status": "unavailable",
            "error_type": "RuntimeError",
            "error_message": "knowledge graph offline",
            "results": [],
        }

    monkeypatch.setattr(workflow_query, "run_cli_tool", _fake_run_cli_tool)

    stdout = StringIO()
    rc = workflow_query._recall_command(["provider routing"], stdout=stdout)

    assert rc == 1
    rendered = stdout.getvalue()
    assert "recall unavailable" in rendered
    assert "knowledge graph offline" in rendered
    assert "no results found" not in rendered


def test_routes_command_delegates_to_modern_frontdoor(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_main(argv, *, stdout=None):
        captured["argv"] = list(argv)
        captured["stdout"] = stdout
        stdout.write("routes delegated\n")
        return 0

    monkeypatch.setattr(workflow_main, "main", _fake_main)

    rc = workflow_cli.main(["routes", "--json"])

    assert rc == 0
    assert captured["argv"] == ["routes", "--json"]
    assert captured["stdout"] is not None
    assert "routes delegated" in capsys.readouterr().out


def test_tools_command_delegates_to_modern_frontdoor(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_main(argv, *, stdout=None):
        captured["argv"] = list(argv)
        captured["stdout"] = stdout
        stdout.write("tools delegated\n")
        return 0

    monkeypatch.setattr(workflow_main, "main", _fake_main)

    rc = workflow_cli.main(["tools", "list"])

    assert rc == 0
    assert captured["argv"] == ["tools", "list"]
    assert captured["stdout"] is not None
    assert "tools delegated" in capsys.readouterr().out


def test_diagnose_command_delegates_to_modern_frontdoor(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_main(argv, *, stdout=None):
        captured["argv"] = list(argv)
        captured["stdout"] = stdout
        stdout.write("diagnose delegated\n")
        return 0

    monkeypatch.setattr(workflow_main, "main", _fake_main)

    rc = workflow_cli.main(["diagnose", "run_123"])

    assert rc == 0
    assert captured["argv"] == ["diagnose", "run_123"]
    assert captured["stdout"] is not None
    assert "diagnose delegated" in capsys.readouterr().out
