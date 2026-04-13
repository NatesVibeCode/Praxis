from __future__ import annotations

import json
import os
from io import StringIO

import pytest

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://localhost:5432/praxis")

from surfaces.cli.main import main as workflow_cli_main
from surfaces.cli.commands import operate as operate_commands
from surfaces.cli.commands import query as query_commands


def test_tools_list_json_exposes_catalog() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "list", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    names = {row["name"] for row in payload}
    assert len(payload) == 42
    assert {"praxis_query", "praxis_discover", "praxis_session_context"} <= names


def test_tools_describe_surfaces_cli_metadata() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "describe", "praxis_query"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "badges: stable, query, alias:query" in rendered
    assert "entrypoint: workflow query" in rendered
    assert "describe_command: workflow tools describe praxis_query" in rendered
    assert "workflow_token_required: no" in rendered
    assert '"question"' in rendered


def test_tools_call_requires_yes_for_write_or_dispatch() -> None:
    stdout = StringIO()

    rc = workflow_cli_main(
        [
            "tools",
            "call",
            "praxis_ingest",
            "--input-json",
            '{"kind":"document","source":"catalog/runtime","content":"hello"}',
        ],
        stdout=stdout,
    )

    assert rc == 2
    rendered = stdout.getvalue()
    assert "risk: write" in rendered
    assert "confirmation required" in rendered


def test_discover_reindex_requires_yes(monkeypatch: pytest.MonkeyPatch) -> None:
    stdout = StringIO()

    rc = workflow_cli_main(["discover", "reindex"], stdout=stdout)

    assert rc == 2
    rendered = stdout.getvalue()
    assert "tool: praxis_discover" in rendered
    assert "risk: write" in rendered
    assert "confirmation required" in rendered

    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True}

    monkeypatch.setattr(query_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["discover", "reindex", "--yes"], stdout=stdout) == 0
    assert captured == {
        "tool_name": "praxis_discover",
        "params": {"action": "reindex"},
    }


def test_tools_call_requires_token_for_session_tools() -> None:
    stdout = StringIO()

    rc = workflow_cli_main(
        [
            "tools",
            "call",
            "praxis_session_context",
            "--input-json",
            '{"action":"read"}',
        ],
        stdout=stdout,
    )

    assert rc == 2
    assert "workflow token required" in stdout.getvalue()


def test_query_alias_uses_catalog_backed_tool_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True}

    monkeypatch.setattr(query_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["query", "what", "failed"], stdout=stdout) == 0
    assert captured == {
        "tool_name": "praxis_query",
        "params": {"question": "what failed"},
    }


def test_health_alias_uses_catalog_backed_tool_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {
            "preflight": {"overall": "ok", "checks": []},
            "operator_snapshot": {},
            "lane_recommendation": {"recommended_posture": "green", "reasons": []},
        }

    monkeypatch.setattr(operate_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["health"], stdout=stdout) == 0
    assert captured == {
        "tool_name": "praxis_health",
        "params": {},
    }
