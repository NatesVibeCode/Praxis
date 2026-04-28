from __future__ import annotations

import importlib.util
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

from surfaces.cli.commands import heartbeat as heartbeat_command
from surfaces.mcp.tools import heartbeat as heartbeat_tool


REPO_ROOT = Path(__file__).resolve().parents[4]
WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
DAILY_HEARTBEAT_SCRIPT = REPO_ROOT / "scripts" / "daily_heartbeat.py"
CLI_HEARTBEAT_SOURCE = WORKFLOW_ROOT / "surfaces" / "cli" / "commands" / "heartbeat.py"
MCP_HEARTBEAT_SOURCE = WORKFLOW_ROOT / "surfaces" / "mcp" / "tools" / "heartbeat.py"


def _load_daily_heartbeat_script():
    spec = importlib.util.spec_from_file_location(
        "praxis_daily_heartbeat_script",
        DAILY_HEARTBEAT_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_daily_heartbeat_tool_dispatches_cqrs_operation(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_execute_catalog_tool(*, operation_name: str, payload: dict) -> dict:
        captured["operation_name"] = operation_name
        captured["payload"] = payload
        return {"ok": True, "status": "succeeded"}

    monkeypatch.setattr(heartbeat_tool, "_execute_catalog_tool", _fake_execute_catalog_tool)

    result = heartbeat_tool.tool_praxis_daily_heartbeat(
        {"scope": "providers", "triggered_by": "http"}
    )

    assert result == {"ok": True, "status": "succeeded"}
    assert captured["operation_name"] == "operator.daily_heartbeat_refresh"
    assert captured["payload"] == {
        "scope": "providers",
        "triggered_by": "http",
    }


def test_heartbeat_cli_command_uses_catalog_tool(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict):
        captured["tool_name"] = tool_name
        captured["params"] = params
        return 0, {"ok": True, "status": "succeeded", "heartbeat_run_id": "heartbeat.1"}

    monkeypatch.setattr(heartbeat_command, "run_cli_tool", _fake_run_cli_tool)

    stdout = StringIO()
    exit_code = heartbeat_command._heartbeat_command(
        ["--scope", "credentials"],
        stdout=stdout,
    )

    assert exit_code == 0
    assert captured["tool_name"] == "praxis_daily_heartbeat"
    assert captured["params"] == {
        "scope": "credentials",
        "triggered_by": "cli",
    }


def test_daily_heartbeat_script_uses_catalog_tool(monkeypatch, capsys) -> None:
    module = _load_daily_heartbeat_script()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        module,
        "_parse_args",
        lambda: SimpleNamespace(scope="mcp", triggered_by="launchd"),
    )

    def _fake_run_cli_tool(tool_name: str, params: dict):
        captured["tool_name"] = tool_name
        captured["params"] = params
        return 0, {"ok": True, "status": "succeeded", "heartbeat_run_id": "heartbeat.2"}

    monkeypatch.setattr(module, "run_cli_tool", _fake_run_cli_tool)

    exit_code = module.main()

    assert exit_code == 0
    assert captured["tool_name"] == "praxis_daily_heartbeat"
    assert captured["params"] == {
        "scope": "mcp",
        "triggered_by": "launchd",
    }
    assert "heartbeat.2" in capsys.readouterr().out


def test_daily_heartbeat_frontdoors_do_not_import_runtime_writer_directly() -> None:
    script_source = DAILY_HEARTBEAT_SCRIPT.read_text(encoding="utf-8")
    cli_source = CLI_HEARTBEAT_SOURCE.read_text(encoding="utf-8")
    mcp_source = MCP_HEARTBEAT_SOURCE.read_text(encoding="utf-8")

    assert "from runtime.daily_heartbeat import run_daily_heartbeat" not in script_source
    assert "from runtime.daily_heartbeat import run_daily_heartbeat" not in cli_source
    assert "from runtime.daily_heartbeat import run_daily_heartbeat" not in mcp_source
    assert "run_cli_tool(" in script_source
