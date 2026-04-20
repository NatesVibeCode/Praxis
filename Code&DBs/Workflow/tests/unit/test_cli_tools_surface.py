from __future__ import annotations

import json
import os
from io import StringIO

import pytest

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://postgres@localhost:5432/praxis")

from surfaces.cli.main import main as workflow_cli_main
from surfaces.cli.commands import operate as operate_commands
from surfaces.cli.commands import query as query_commands
from surfaces.cli.commands import tools as tools_commands
from surfaces.mcp.catalog import McpToolDefinition, get_tool_catalog


def _tool_definition(
    name: str,
    *,
    description: str,
    recommended_alias: str | None = None,
) -> McpToolDefinition:
    metadata: dict[str, object] = {"description": description}
    if recommended_alias is not None:
        metadata["cli"] = {"recommended_alias": recommended_alias}
    return McpToolDefinition(
        name=name,
        module_name="surfaces.mcp.tools.test",
        handler_name="handler",
        metadata=metadata,
        selector_defaults={},
    )


def test_tools_list_json_exposes_catalog() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "list", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    names = {row["name"] for row in payload}
    assert len(payload) == len(get_tool_catalog())
    assert {"praxis_query", "praxis_discover", "praxis_session_context"} <= names
    assert any(row["entrypoint"] == "workflow query" for row in payload)


def test_tools_root_shows_quickstart() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "Tool discovery quickstart:" in rendered
    assert "workflow tools search <topic> [--exact] [--surface <surface>] [--tier <tier>] [--risk <risk>]" in rendered
    assert "workflow tools help <list|search|describe|call>" in rendered
    assert "search results are relevance-ranked" in rendered.lower()
    assert "unique prefix" in rendered.lower()
    assert "workflow mcp" in rendered
    assert "Common direct entrypoints:" in rendered
    assert "workflow diagnose" in rendered


def test_tools_describe_daily_heartbeat_smoke() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "describe", "praxis_daily_heartbeat"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "praxis_daily_heartbeat" in rendered
    assert "entrypoint: workflow heartbeat" in rendered
    assert "alias:heartbeat" in rendered


def test_commands_index_includes_daily_heartbeat() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["commands"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "workflow heartbeat [--scope <scope>] [--pretty]" in rendered
    assert "Run the daily external-health heartbeat" in rendered


def test_tools_search_prioritizes_exact_alias_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tools_commands,
        "get_tool_catalog",
        lambda: {
            "praxis_alpha": _tool_definition(
                "praxis_alpha",
                description="General help that mentions query for broader discovery.",
            ),
            "praxis_query": _tool_definition(
                "praxis_query",
                description="Primary query surface for operator questions.",
                recommended_alias="query",
            ),
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["tools", "search", "query", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert [row["name"] for row in payload] == ["praxis_query", "praxis_alpha"]


def test_tools_search_exact_mode_returns_only_direct_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tools_commands,
        "get_tool_catalog",
        lambda: {
            "praxis_alpha": _tool_definition(
                "praxis_alpha",
                description="General help that mentions query for broader discovery.",
            ),
            "praxis_query": _tool_definition(
                "praxis_query",
                description="Primary query surface for operator questions.",
                recommended_alias="query",
            ),
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["tools", "search", "query", "--exact", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert [row["name"] for row in payload] == ["praxis_query"]
    assert payload[0]["describe_command"] == "workflow tools describe praxis_query"

    stdout = StringIO()
    assert workflow_cli_main(["tools", "search", "workflow query", "--exact", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert [row["name"] for row in payload] == ["praxis_query"]


def test_tools_search_single_match_prints_next_step(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tools_commands,
        "get_tool_catalog",
        lambda: {
            "praxis_query": _tool_definition(
                "praxis_query",
                description="Primary query surface for operator questions.",
                recommended_alias="query",
            ),
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["tools", "search", "query", "--exact"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "Best next step:" in rendered
    assert "workflow tools describe praxis_query" in rendered
    assert "workflow query" in rendered


def test_tools_search_highlights_top_exact_match_even_with_competing_hits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        tools_commands,
        "get_tool_catalog",
        lambda: {
            "praxis_alpha": _tool_definition(
                "praxis_alpha",
                description="Alpha tool that mentions query in passing.",
                recommended_alias="alpha",
            ),
            "praxis_query": _tool_definition(
                "praxis_query",
                description="Primary query surface for operator questions.",
                recommended_alias="query",
            ),
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["tools", "search", "query"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert rendered.index("praxis_query") < rendered.index("praxis_alpha")
    assert "Best next step:" in rendered
    assert "workflow tools describe praxis_query" in rendered
    assert "workflow query" in rendered


def test_tools_search_reports_no_matches_with_broadening_hints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tools_commands,
        "get_tool_catalog",
        lambda: {
            "praxis_query": _tool_definition(
                "praxis_query",
                description="Primary query surface for operator questions.",
                recommended_alias="query",
            ),
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["tools", "search", "needle"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "no tools matched 'needle'" in rendered
    assert "tips:" in rendered
    assert "workflow tools list --json" in rendered
    assert "workflow tools search <broader text>" in rendered
    assert "0 tool(s)" in rendered


def test_tools_search_reports_no_exact_matches_with_broadening_hints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tools_commands,
        "get_tool_catalog",
        lambda: {
            "praxis_query": _tool_definition(
                "praxis_query",
                description="Primary query surface for operator questions.",
                recommended_alias="query",
            ),
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["tools", "search", "needle", "--exact"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "no exact matches found for needle" in rendered
    assert "tips:" in rendered
    assert "add --exact only when you already know the alias, tool name, or entrypoint" in rendered
    assert "0 tool(s)" in rendered


def test_tools_describe_surfaces_cli_metadata() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "describe", "query"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "badges: stable, query, alias:query" in rendered
    assert "entrypoint: workflow query" in rendered
    assert "describe_command: workflow tools describe praxis_query" in rendered
    assert "workflow_token_required: no" in rendered
    assert '"question"' in rendered


def test_tools_describe_praxis_bugs_smoke() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "describe", "praxis_bugs"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "praxis_bugs" in rendered
    assert "entrypoint: workflow bugs" in rendered
    assert "describe_command: workflow tools describe praxis_bugs" in rendered
    assert "resolve+verifier_ref" in rendered


def test_tools_describe_accepts_unique_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools_commands, "get_definition", lambda tool_name: None)
    monkeypatch.setattr(
        tools_commands,
        "get_tool_catalog",
        lambda: {
            "praxis_query": _tool_definition(
                "praxis_query",
                description="Primary query surface for operator questions.",
                recommended_alias="query",
            ),
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["tools", "describe", "que"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "praxis_query" in rendered
    assert "entrypoint: workflow query" in rendered
    assert "describe_command: workflow tools describe praxis_query" in rendered


def test_tools_list_plain_output_highlights_entrypoints() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "list"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "ENTRYPOINT" in rendered
    assert "ALIAS" in rendered
    assert "workflow query" in rendered
    assert "workflow integration" in rendered
    assert "query" in rendered
    assert "workflow tools call praxis_connector" in rendered


def test_integration_tool_has_direct_entrypoint() -> None:
    definition = get_tool_catalog()["praxis_integration"]

    assert definition.cli_entrypoint == "workflow integration"
    assert definition.cli_recommended_alias == "integration"


def test_tools_describe_reports_ambiguous_prefixes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tools_commands, "get_definition", lambda tool_name: None)
    monkeypatch.setattr(
        tools_commands,
        "get_tool_catalog",
        lambda: {
            "praxis_alpha": _tool_definition(
                "praxis_alpha",
                description="Alpha tool for broad use.",
                recommended_alias="alpha",
            ),
            "praxis_alpine": _tool_definition(
                "praxis_alpine",
                description="Alpine tool for a nearby use case.",
                recommended_alias="alpine",
            ),
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["tools", "describe", "al"], stdout=stdout) == 2

    rendered = stdout.getvalue()
    assert "ambiguous tool name: al" in rendered
    assert "did you mean:" in rendered
    assert "praxis_alpha" in rendered
    assert "praxis_alpine" in rendered
    assert "workflow tools search <text>" in rendered


def test_tools_search_supports_filter_only_browsing() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "search", "--surface", "query", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert payload
    assert all(row["surface"] == "query" for row in payload)
    assert all("entrypoint" in row for row in payload)
    assert any(row["entrypoint"] == "workflow query" for row in payload)


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


def test_tools_call_accepts_unique_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        captured["workflow_token"] = workflow_token
        return 0, {"ok": True}

    monkeypatch.setattr(tools_commands, "get_definition", lambda tool_name: None)
    monkeypatch.setattr(
        tools_commands,
        "get_tool_catalog",
        lambda: {
            "praxis_query": _tool_definition(
                "praxis_query",
                description="Primary query surface for operator questions.",
                recommended_alias="query",
            ),
        },
    )
    monkeypatch.setattr(tools_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(
        [
            "tools",
            "call",
            "que",
            "--input-json",
            '{"question":"what failed"}',
        ],
        stdout=stdout,
    ) == 0

    assert captured == {
        "tool_name": "praxis_query",
        "params": {"question": "what failed"},
        "workflow_token": "",
    }


def test_tools_call_accepts_recommended_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        captured["workflow_token"] = workflow_token
        return 0, {"ok": True}

    monkeypatch.setattr(tools_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(
        [
            "tools",
            "call",
            "query",
            "--input-json",
            '{"question":"what failed"}',
        ],
        stdout=stdout,
    ) == 0

    assert captured == {
        "tool_name": "praxis_query",
        "params": {"question": "what failed"},
        "workflow_token": "",
    }


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


def test_tools_root_help_mentions_alias_support() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "--help"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "workflow tools describe <tool|alias>" in rendered
    assert "workflow tools call <tool|alias>" in rendered


def test_tools_help_search_shows_targeted_usage() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "help", "search"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "usage: workflow tools search <text>" in rendered
    assert "Search by topic, alias, entrypoint, or describe-command text." in rendered
    assert "add --exact only when you already know the alias, tool name, or entrypoint" in rendered


def test_mcp_help_topic_routes_to_tools_help() -> None:
    tools_stdout = StringIO()
    mcp_stdout = StringIO()

    assert workflow_cli_main(["tools", "help", "call"], stdout=tools_stdout) == 0
    assert workflow_cli_main(["mcp", "help", "call"], stdout=mcp_stdout) == 0

    assert mcp_stdout.getvalue() == tools_stdout.getvalue()


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


def test_health_alias_renders_trend_observability(monkeypatch: pytest.MonkeyPatch) -> None:
    def _run_cli_tool(tool_name: str, params: dict[str, object], *, workflow_token: str = ""):
        assert tool_name == "praxis_health"
        assert params == {}
        return 0, {
            "preflight": {"overall": "ok", "checks": []},
            "operator_snapshot": {},
            "lane_recommendation": {"recommended_posture": "green", "reasons": []},
            "trend_observability": {
                "summary": {
                    "total_trends": 1,
                    "critical_trends": 0,
                    "warning_trends": 1,
                    "info_trends": 0,
                    "degrading_trends": 1,
                    "accelerating_trends": 0,
                    "improving_trends": 0,
                },
                "trend_digest": "WARNING:\n  cost_trend anthropic\nSummary: 0 critical, 1 warnings, 0 info",
            },
        }

    monkeypatch.setattr(operate_commands, "run_cli_tool", _run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["health"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "Trends" in rendered
    assert "total_trends: 1" in rendered
    assert "digest: WARNING:" in rendered
