from __future__ import annotations

import json
import importlib
from io import StringIO

from surfaces.cli import workflow_cli
from surfaces.cli.commands import query as workflow_query

workflow_main = importlib.import_module("surfaces.cli.main")


def _run_legacy(argv: list[str], capsys) -> tuple[int, str]:
    rc = workflow_cli.main(argv)
    return rc, capsys.readouterr().out


def _run_modern(argv: list[str]) -> tuple[int, str]:
    stdout = StringIO()
    rc = workflow_main.main(argv, stdout=stdout)
    return rc, stdout.getvalue()


def test_commands_delegates_to_modern_command_index(capsys) -> None:
    legacy_rc, legacy_rendered = _run_legacy(["commands"], capsys)
    modern_rc, modern_rendered = _run_modern(["commands"])

    assert legacy_rc == modern_rc == 0
    assert legacy_rendered == modern_rendered
    assert "workflow commands" in legacy_rendered
    assert "workflow tools [list|search|describe|call|help]" in legacy_rendered
    assert "workflow decompose <objective...>" in legacy_rendered


def test_help_topic_delegates_to_modern_command_usage(capsys) -> None:
    legacy_rc, legacy_rendered = _run_legacy(["help", "run"], capsys)
    modern_rc, modern_rendered = _run_modern(["help", "run"])

    assert legacy_rc == modern_rc == 0
    assert legacy_rendered == modern_rendered
    assert "workflow run <spec.json>" in legacy_rendered
    assert "workflow run -p <prompt>" in legacy_rendered


def test_help_topic_routes_delegates_to_modern_discovery_frontdoor(capsys) -> None:
    legacy_rc, legacy_rendered = _run_legacy(["help", "routes"], capsys)
    modern_rc, modern_rendered = _run_modern(["help", "routes"])

    assert legacy_rc == modern_rc == 0
    assert legacy_rendered == modern_rendered
    assert "workflow api [routes|integrations|data-dictionary|--host HOST|--port PORT]" in legacy_rendered
    assert "workflow routes --json" in legacy_rendered
    assert "workflow help routes" in legacy_rendered


def test_help_topic_api_is_an_alias_for_routes(capsys) -> None:
    legacy_rc, legacy_rendered = _run_legacy(["help", "api"], capsys)
    modern_rc, modern_rendered = _run_modern(["help", "api"])

    assert legacy_rc == modern_rc == 0
    assert legacy_rendered == modern_rendered
    assert "workflow api [routes|integrations|data-dictionary|--host HOST|--port PORT]" in legacy_rendered
    assert "Flat alias: workflow routes" in legacy_rendered


def test_help_topic_tools_delegates_to_modern_discovery_frontdoor(capsys) -> None:
    legacy_rc, legacy_rendered = _run_legacy(["help", "tools"], capsys)
    modern_rc, modern_rendered = _run_modern(["help", "tools"])

    assert legacy_rc == modern_rc == 0
    assert legacy_rendered == modern_rendered
    assert "workflow tools list" in legacy_rendered
    assert "workflow tools describe <tool|alias>" in legacy_rendered
    assert "workflow tools call <tool|alias> --input-json '<json>' --yes" in legacy_rendered


def test_help_topic_mcp_is_an_alias_for_tools(capsys) -> None:
    legacy_rc, legacy_rendered = _run_legacy(["help", "mcp"], capsys)
    modern_rc, modern_rendered = _run_modern(["help", "mcp"])

    assert legacy_rc == modern_rc == 0
    assert legacy_rendered == modern_rendered
    assert "usage: workflow mcp [list|search|describe|call|help]" in legacy_rendered
    assert "Alias for workflow tools discovery." in legacy_rendered
    assert "workflow tools list" in legacy_rendered
    assert "workflow tools search <topic> [--exact]" in legacy_rendered


def test_help_topic_diagnose_delegates_to_modern_usage(capsys) -> None:
    legacy_rc, legacy_rendered = _run_legacy(["help", "diagnose"], capsys)
    modern_rc, modern_rendered = _run_modern(["help", "diagnose"])

    assert legacy_rc == modern_rc == 0
    assert legacy_rendered == modern_rendered
    assert "usage: workflow diagnose <run_id>" in legacy_rendered


def test_help_topic_decompose_delegates_to_modern_usage(capsys) -> None:
    legacy_rc, legacy_rendered = _run_legacy(["help", "decompose"], capsys)
    modern_rc, modern_rendered = _run_modern(["help", "decompose"])

    assert legacy_rc == modern_rc == 0
    assert legacy_rendered == modern_rendered
    assert "usage: workflow decompose <objective...> [--scope-files a,b,c] [--json]" in legacy_rendered
    assert "--scope-files" in legacy_rendered


def test_help_topic_bugs_exposes_the_full_bug_surface(capsys) -> None:
    rc = workflow_main.main(["help", "bugs"], stdout=None)

    assert rc == 0
    rendered = capsys.readouterr().out
    assert (
        "workflow bugs "
        "[list|search <query>|duplicate_check <query>|stats|file|history|packet|replay|backfill_replay|attach_evidence|patch_resume|resolve]"
    ) in rendered
    assert "file               File a new bug" in rendered
    assert "duplicate_check <query>" in rendered
    assert "attach_evidence    Attach canonical evidence to a bug" in rendered
    assert "resolve            Mark an existing bug fixed, deferred, or won't-fix; FIXED may run verifier proof" in rendered
    assert "--category S       Filing category: SCOPE, VERIFY, IMPORT, WIRING, ARCHITECTURE, RUNTIME, TEST, OTHER" in rendered


def test_bugs_subcommand_help_after_action_is_success() -> None:
    stdout = StringIO()

    rc = workflow_query._bugs_command(["file", "--help"], stdout=stdout)

    assert rc == 0
    assert "usage: workflow bugs" in stdout.getvalue()


def test_bugs_duplicate_check_accepts_body_context(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"bugs": [], "count": 0, "returned_count": 0}

    monkeypatch.setattr(workflow_query, "run_cli_tool", _fake_run_cli_tool)

    stdout = StringIO()
    rc = workflow_query._bugs_command(
        [
            "duplicate-check",
            "--title",
            "operator write failed",
            "--body",
            "missing intent brief at runtime",
            "--json",
        ],
        stdout=stdout,
    )

    assert rc == 0
    assert captured["tool_name"] == "praxis_bugs"
    assert captured["params"]["action"] == "duplicate_check"
    assert captured["params"]["title_like"] == "operator write failed"
    assert captured["params"]["description"] == "missing intent brief at runtime"


def test_roadmap_nested_action_help_is_success() -> None:
    write_stdout = StringIO()
    closeout_stdout = StringIO()

    write_rc = workflow_main.main(
        ["roadmap", "write", "preview", "--help"],
        stdout=write_stdout,
    )
    closeout_rc = workflow_main.main(
        ["roadmap", "closeout", "preview", "--help"],
        stdout=closeout_stdout,
    )

    assert write_rc == 0
    assert "usage: workflow roadmap write" in write_stdout.getvalue()
    assert closeout_rc == 0
    assert "usage: workflow roadmap closeout" in closeout_stdout.getvalue()


def test_top_level_help_delegates_to_modern_command_index(capsys) -> None:
    legacy_rc, legacy_rendered = _run_legacy(["--help"], capsys)
    modern_rc, modern_rendered = _run_modern(["--help"])

    assert legacy_rc == modern_rc == 0
    assert legacy_rendered == modern_rendered
    assert "workflow help commands" in legacy_rendered
    assert "workflow mcp [list|search|describe|call|help]" in legacy_rendered


def test_advertised_root_commands_have_successful_help() -> None:
    failures: list[tuple[list[str], int, str]] = []
    probed: set[tuple[str, ...]] = set()

    for entry in workflow_main._COMMAND_INDEX_ENTRIES:
        command = str(entry.get("command") or "")
        if not command.startswith("workflow "):
            continue
        parts = command.split()
        if len(parts) < 2:
            continue
        root = parts[1]
        if any(character in root for character in "|[<"):
            continue
        argv = ["help", "--help"] if root == "help" else [root, "--help"]
        argv_key = tuple(argv)
        if argv_key in probed:
            continue
        probed.add(argv_key)

        stdout = StringIO()
        rc = workflow_main.main(argv, stdout=stdout)
        rendered = stdout.getvalue()
        if rc != 0 or not rendered.strip():
            failures.append((argv, rc, rendered))

    assert not failures


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
