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


def test_help_text_explains_search_semantics() -> None:
    discover_stdout = StringIO()
    recall_stdout = StringIO()
    query_stdout = StringIO()

    assert workflow_cli_main(["discover", "--help"], stdout=discover_stdout) == 2
    assert workflow_cli_main(["recall", "--help"], stdout=recall_stdout) == 2
    assert workflow_cli_main(["query", "--help"], stdout=query_stdout) == 2

    assert "hybrid retrieval" in discover_stdout.getvalue()
    assert "graph traversal" in recall_stdout.getvalue()
    assert "Best first stop" in query_stdout.getvalue()


def test_architecture_scan_alias_renders_exact_static_findings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        query_commands,
        "_scan_architecture",
        lambda scope: {
            "scope": scope,
            "summary": {
                "scanned_files": 12,
                "sql_literals_outside_storage": 2,
                "frontdoor_runtime_imports": 1,
                "frontdoor_storage_postgres_imports": 1,
                "total_violations": 4,
            },
            "violations": {
                "sql_literals_outside_storage": [
                    {"path": "surfaces/api/rest.py", "line": 656, "excerpt": "SELECT * FROM workflow_runs WHERE run_id = $1"},
                ],
                "frontdoor_imports": [
                    {
                        "path": "surfaces/api/rest.py",
                        "line": 12,
                        "rule": "surfaces_imports_runtime",
                        "import": "runtime.receipt_store",
                    },
                ],
            },
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["architecture", "scan", "--scope", "surfaces"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "Architecture scan (surfaces)" in rendered
    assert "raw SQL outside storage: 2" in rendered
    assert "surfaces/api/rest.py:656" in rendered
    assert "surfaces_imports_runtime" in rendered


def test_architecture_scan_alias_supports_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        query_commands,
        "_scan_architecture",
        lambda scope: {
            "scope": scope,
            "summary": {
                "scanned_files": 3,
                "sql_literals_outside_storage": 0,
                "frontdoor_runtime_imports": 0,
                "frontdoor_storage_postgres_imports": 0,
                "total_violations": 0,
            },
            "violations": {
                "sql_literals_outside_storage": [],
                "frontdoor_imports": [],
            },
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["architecture", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert payload["scope"] == "all"
    assert payload["summary"]["total_violations"] == 0


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
