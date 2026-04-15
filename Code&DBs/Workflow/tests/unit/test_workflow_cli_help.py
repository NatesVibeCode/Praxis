from __future__ import annotations

import importlib

from surfaces.cli import workflow_cli

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


def test_top_level_help_aliases_return_command_index(capsys) -> None:
    rc = workflow_cli.main(["--help"])

    assert rc == 0
    rendered = capsys.readouterr().out
    assert "workflow_cli.py repair <run_id>" in rendered


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
