from __future__ import annotations

import importlib
import json
import os
import subprocess
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://postgres@localhost:5432/praxis")

import surfaces.api.rest as rest
from surfaces.cli import workflow_cli as legacy_workflow_cli
from surfaces.cli.main import main as workflow_cli_main
from surfaces.cli.commands import authority as authority_commands
from surfaces.cli.commands import query as query_commands
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


def test_run_status_frontdoor_accepts_json_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_workflow_tool(params: dict[str, object]):
        captured.update(params)
        return {"run_id": "workflow_123", "status": "running"}

    monkeypatch.setattr(workflow_commands, "_workflow_tool", _fake_workflow_tool)
    stdout = StringIO()

    assert workflow_cli_main(["run-status", "workflow_123", "--json"], stdout=stdout) == 0

    assert captured == {"action": "status", "run_id": "workflow_123"}
    payload = json.loads(stdout.getvalue())
    assert payload["run_id"] == "workflow_123"


def test_run_status_frontdoor_summary_projects_agent_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_workflow_tool(params: dict[str, object]):
        captured.update(params)
        return {
            "run_id": "workflow_123",
            "status": "failed",
            "spec_name": "Agent Affordance CLI Oracle Scout",
            "packet_inspection": {"large": "payload"},
            "health": {
                "state": "critical",
                "likely_failed": True,
                "elapsed_seconds": 944.8,
                "completed_jobs": 2,
                "running_or_claimed": 0,
                "terminal_jobs": 2,
                "resource_telemetry": {"heartbeat_freshness": "fresh"},
                "signals": [
                    {
                        "type": "first_failed_node",
                        "severity": "high",
                        "message": "worker failed",
                        "node_id": "scout",
                        "failure_code": "worker_exception",
                        "hint": "inspect this node",
                    }
                ],
            },
            "recovery": {
                "mode": "inspect",
                "reason": "Inspect before retrying.",
                "recommended_tool": {
                    "name": "praxis_workflow",
                    "arguments": {"action": "inspect", "run_id": "workflow_123"},
                    "extra": "not needed",
                },
            },
            "jobs": [
                {
                    "job_label": "cursor_cli_error_surface_scout",
                    "status": "failed",
                    "agent_slug": "cursor/composer-2",
                    "attempt": 3,
                    "error_code": "sandbox_error",
                    "reason_code": "worker_exception",
                    "workspace": {"too": "large"},
                },
                {
                    "job_label": "mini_friction_pattern_scout",
                    "status": "failed",
                    "agent_slug": "openai/gpt-5.4-mini",
                    "attempt": 3,
                    "error_code": "sandbox_error",
                    "reason_code": "worker_exception",
                },
            ],
        }

    monkeypatch.setattr(workflow_commands, "_workflow_tool", _fake_workflow_tool)
    stdout = StringIO()

    assert workflow_cli_main(["run-status", "workflow_123", "--summary"], stdout=stdout) == 0

    assert captured == {"action": "status", "run_id": "workflow_123"}
    payload = json.loads(stdout.getvalue())
    assert payload == {
        "run_id": "workflow_123",
        "status": "failed",
        "spec_name": "Agent Affordance CLI Oracle Scout",
        "total_jobs": 2,
        "job_status_counts": {"failed": 2},
        "health": {
            "state": "critical",
            "likely_failed": True,
            "elapsed_seconds": 944.8,
            "completed_jobs": 2,
            "running_or_claimed": 0,
            "terminal_jobs": 2,
            "signals": [
                {
                    "type": "first_failed_node",
                    "severity": "high",
                    "message": "worker failed",
                    "node_id": "scout",
                    "failure_code": "worker_exception",
                    "hint": "inspect this node",
                }
            ],
        },
        "recovery": {
            "mode": "inspect",
            "reason": "Inspect before retrying.",
            "recommended_tool": {
                "name": "praxis_workflow",
                "arguments": {"action": "inspect", "run_id": "workflow_123"},
            },
        },
        "jobs": [
            {
                "job_label": "cursor_cli_error_surface_scout",
                "status": "failed",
                "agent_slug": "cursor/composer-2",
                "attempt": 3,
                "error_code": "sandbox_error",
                "reason_code": "worker_exception",
            },
            {
                "job_label": "mini_friction_pattern_scout",
                "status": "failed",
                "agent_slug": "openai/gpt-5.4-mini",
                "attempt": 3,
                "error_code": "sandbox_error",
                "reason_code": "worker_exception",
            },
        ],
    }
    assert "packet_inspection" not in payload


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


def test_top_level_help_mentions_routes_alias() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow help commands" in rendered
    assert "workflow help mcp" in rendered
    assert "workflow routes" in rendered
    assert "workflow api help" in rendered
    assert "workflow integrations" in rendered
    assert "workflow integration list" in rendered
    assert "workflow integration help" in rendered
    assert "workflow research 'API auth drift'" in rendered
    assert "workflow decompose 'build real-time notifications'" in rendered
    assert "workflow authority-index" in rendered
    assert "workflow records" not in rendered
    assert "workflow defs <create|update>" not in rendered


def test_commands_index_mentions_routes_alias() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["commands"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow routes" in rendered
    assert "workflow help commands" in rendered
    assert "workflow help mcp" in rendered
    assert "Alias for workflow API route discovery" in rendered
    assert "workflow api help [routes|integrations|data-dictionary]" in rendered
    assert "workflow integrations" in rendered
    assert "workflow api integrations" in rendered
    assert "workflow api data-dictionary" in rendered
    assert "workflow integration [list|describe|health|test|call|create|secret|reload|help]" in rendered
    assert "workflow research [list|<topic>] [--workers N] [--agent SLUG] [--threshold N] [--json]" in rendered
    assert "workflow decompose <objective...>" in rendered
    assert "workflow authority-index" in rendered
    assert "workflow dictionary <list|describe|set-override|clear-override|reproject>" in rendered
    assert "workflow authority-memory refresh" in rendered
    assert "workflow records <list|get|create|update|rename>" in rendered
    assert "workflow defs <create|update>" not in rendered


def test_instances_check_uses_fast_orient_and_compares_cli_mcp_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import surfaces._workflow_database as workflow_database
    import surfaces.mcp.subsystems as mcp_subsystems
    import runtime.setup_wizard as setup_wizard

    captured: dict[str, object] = {}

    def _fake_setup_payload_for_cli(mode, *, repo_root=None, apply=False):
        assert mode == "doctor"
        return {
            "ok": True,
            "runtime_target": {
                "api_authority": "http://localhost:8420",
                "db_authority": "postgresql://***@host.docker.internal:5432/praxis",
            },
            "native_instance": {
                "praxis_instance_name": "praxis",
                "praxis_runtime_profile": "praxis",
                "repo_root": "/repo",
            },
        }

    def _fake_run_cli_tool(tool_name, params):
        captured["tool_name"] = tool_name
        captured["params"] = params
        return 0, {
            "authority_envelope": {
                "primitive_contracts": {
                    "runtime_binding": {
                        "http_endpoints": {"api_base_url": "http://localhost:8420"},
                        "database": {
                            "redacted_url": "postgresql://postgres:***@localhost:5432/praxis",
                        },
                    }
                }
            },
            "native_instance": {"status": "skipped", "reason": "orient_fast_path"},
            "cli_surface": {"tool_count": 42},
        }

    monkeypatch.setattr(setup_wizard, "setup_payload_for_cli", _fake_setup_payload_for_cli)
    monkeypatch.setattr(operate_commands, "run_cli_tool", _fake_run_cli_tool)
    monkeypatch.setattr(
        workflow_database,
        "workflow_database_authority_for_repo",
        lambda repo_root, env: SimpleNamespace(
            database_url="postgresql://postgres:secret@localhost:5432/praxis",
            source="test",
        ),
    )
    monkeypatch.setattr(
        mcp_subsystems,
        "workflow_database_env",
        lambda: {
            "WORKFLOW_DATABASE_URL": "postgresql://postgres:secret@localhost:5432/praxis",
            "WORKFLOW_DATABASE_AUTHORITY_SOURCE": "test",
        },
    )

    stdout = StringIO()
    assert workflow_cli_main(["instances", "check", "--json"], stdout=stdout) == 0
    payload = json.loads(stdout.getvalue())

    assert captured["tool_name"] == "praxis_orient"
    assert captured["params"] == {
        "fast": True,
        "skip_engineering_observability": True,
        "compact": True,
    }
    assert payload["route_catalog"]["included"] is False
    assert payload["route_catalog"]["status"] == "skipped"
    assert payload["instances"]["cli_db"] == "postgresql://postgres:***@localhost:5432/praxis"
    assert payload["instances"]["mcp_db"] == "postgresql://postgres:***@localhost:5432/praxis"
    assert payload["instances"]["db_signatures"]["setup"] == (
        "postgresql://local-runtime-db-host:5432/praxis"
    )
    assert payload["instances"]["db_signatures"]["orient"] == (
        "postgresql://local-runtime-db-host:5432/praxis"
    )
    assert payload["checks"]["cli_mcp_db_match"] is True
    assert payload["checks"]["cli_orient_db_match"] is True
    assert payload["checks"]["db_match"] is True


def test_instances_check_fails_on_mcp_db_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    import surfaces._workflow_database as workflow_database
    import surfaces.mcp.subsystems as mcp_subsystems
    import runtime.setup_wizard as setup_wizard

    monkeypatch.setattr(
        setup_wizard,
        "setup_payload_for_cli",
        lambda mode, *, repo_root=None, apply=False: {
            "ok": True,
            "runtime_target": {
                "api_authority": "http://127.0.0.1:8420",
                "db_authority": "postgresql://postgres:***@db.example/praxis",
            },
            "native_instance": {
                "praxis_instance_name": "praxis",
                "praxis_runtime_profile": "praxis",
                "repo_root": "/repo",
            },
        },
    )
    monkeypatch.setattr(
        operate_commands,
        "run_cli_tool",
        lambda tool_name, params: (
            0,
            {
                "authority_envelope": {
                    "primitive_contracts": {
                        "runtime_binding": {
                            "http_endpoints": {"api_base_url": "http://localhost:8420"},
                            "database": {
                                "redacted_url": "postgresql://postgres:***@db.example/praxis",
                            },
                        }
                    }
                },
                "native_instance": {"status": "skipped", "reason": "orient_fast_path"},
            },
        ),
    )
    monkeypatch.setattr(
        workflow_database,
        "workflow_database_authority_for_repo",
        lambda repo_root, env: SimpleNamespace(
            database_url="postgresql://postgres:secret@db.example/praxis",
            source="test",
        ),
    )
    monkeypatch.setattr(
        mcp_subsystems,
        "workflow_database_env",
        lambda: {
            "WORKFLOW_DATABASE_URL": "postgresql://postgres:secret@other.example/praxis",
            "WORKFLOW_DATABASE_AUTHORITY_SOURCE": "test-other",
        },
    )

    stdout = StringIO()
    assert workflow_cli_main(["instances", "check", "--json"], stdout=stdout) == 1
    payload = json.loads(stdout.getvalue())

    assert payload["checks"]["cli_mcp_db_match"] is False
    assert payload["checks"]["db_match"] is False
    assert any("cli=" in error and "mcp=" in error for error in payload["checks"]["errors"])


def test_global_launcher_resolution_flags_other_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operate_commands.shutil, "which", lambda name: "/usr/local/bin/praxis")

    def _fake_run(*_args, **_kwargs):
        return subprocess.CompletedProcess(
            args=["praxis", "launcher", "resolve", "--json"],
            returncode=0,
            stdout=json.dumps(
                {
                    "ok": True,
                    "resolution": {
                        "repo_root": "/repo/other",
                        "workdir": "/repo/other",
                        "executable_path": "/repo/other/scripts/praxis",
                        "authority_source": "database",
                    },
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(operate_commands.subprocess, "run", _fake_run)

    payload = operate_commands._global_launcher_resolution(Path("/repo/current"))

    assert payload["status"] == "ok"
    assert payload["matches_current_repo"] is False
    assert payload["repo_root"] == "/repo/other"


def test_authority_command_reports_launcher_alignment(monkeypatch: pytest.MonkeyPatch) -> None:
    from runtime import instance as instance_module
    from surfaces import _workflow_database as workflow_database_module
    from surfaces.mcp import subsystems as mcp_subsystems

    monkeypatch.setattr(operate_commands, "_workspace_repo_root", lambda: Path("/repo/current"))
    monkeypatch.setattr(
        operate_commands,
        "_resolve_authority_env",
        lambda: ({"WORKFLOW_DATABASE_URL": "postgresql://user:pw@db.local:5432/praxis"}, True),
    )
    monkeypatch.setattr(
        operate_commands,
        "_global_launcher_resolution",
        lambda _repo_root: {
            "available": True,
            "status": "ok",
            "binary": "/usr/local/bin/praxis",
            "matches_current_repo": False,
            "repo_root": "/repo/other",
            "workdir": "/repo/other",
            "executable_path": "/repo/other/scripts/praxis",
            "authority_source": "database",
        },
    )
    monkeypatch.setattr(
        workflow_database_module,
        "workflow_database_authority_for_repo",
        lambda _repo_root, env: SimpleNamespace(
            database_url="postgresql://user:pw@db.local:5432/praxis",
            source="test_env",
        ),
    )
    monkeypatch.setattr(
        mcp_subsystems,
        "workflow_database_env",
        lambda: {
            "WORKFLOW_DATABASE_URL": "postgresql://user:pw@db.local:5432/praxis",
            "WORKFLOW_DATABASE_AUTHORITY_SOURCE": "test_env",
        },
    )
    monkeypatch.setattr(
        instance_module,
        "native_instance_contract",
        lambda env: {
            "praxis_instance_name": "praxis",
            "praxis_runtime_profile": "praxis",
            "repo_root": "/repo/current",
            "workdir": "/repo/current",
        },
    )
    monkeypatch.setattr(
        operate_commands,
        "_build_runtime_binding",
        lambda _env, native_instance: {
            "database": {"redacted_url": "postgresql://user:***@db.local:5432/praxis"},
            "workspace": {"runtime_profile": "praxis"},
            "http_endpoints": {"api_base_url": "http://127.0.0.1:8420"},
        },
    )
    monkeypatch.setattr(
        operate_commands,
        "run_cli_tool",
        lambda _tool, _params: (
            0,
            {
                "primitive_contracts": {
                    "runtime_binding": {
                        "database": {
                            "redacted_url": "postgresql://user:***@db.local:5432/praxis",
                        },
                        "http_endpoints": {"api_base_url": "http://127.0.0.1:8420"},
                        "workspace": {"runtime_profile": "praxis"},
                    }
                }
            },
        ),
    )

    stdout = StringIO()
    assert operate_commands._authority_command(["--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert payload["global_launcher"]["repo_root"] == "/repo/other"
    assert payload["alignment_checks"]["global_launcher_matches_repo"] is False


def test_integration_help_subcommand_is_discoverable() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["integration", "help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "usage: workflow integration [list|describe|health|test|call|create|secret|reload|help] [args]" in rendered
    assert "workflow integration help" in rendered
    assert "workflow integration call <integration_id> <integration_action>" in rendered


def test_dictionary_describe_missing_object_returns_not_found_packet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        authority_commands,
        "tool_praxis_data_dictionary",
        lambda _params: {
            "error": "data dictionary: unknown object_kind 'workflow_records'",
            "status_code": 404,
        },
    )

    stdout = StringIO()
    assert workflow_cli_main(["dictionary", "describe", "workflow_records", "--json"], stdout=stdout) == 1

    payload = json.loads(stdout.getvalue())
    assert payload["status"] == "not_found"
    assert payload["reason_code"] == "data_dictionary.object_not_found"
    assert payload["object_kind"] == "workflow_records"


def test_object_list_help_is_available_after_subcommand() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["object", "list", "--help"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "usage: workflow object <list|get|upsert|delete>" in rendered
    assert "workflow object list --type-id TYPE" in rendered


def test_authority_memory_refresh_frontdoor_uses_projection_refresher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    class _FakeResult:
        def to_json(self) -> dict[str, object]:
            return {
                "projection_id": "authority_memory_projection",
                "total_upserted": 9,
                "total_deactivated": 1,
            }

    async def _fake_refresh_authority_memory_projection():
        nonlocal called
        called = True
        return _FakeResult()

    monkeypatch.setattr(
        authority_commands,
        "refresh_authority_memory_projection",
        _fake_refresh_authority_memory_projection,
    )
    stdout = StringIO()

    assert workflow_cli_main(["authority-memory", "refresh"], stdout=stdout) == 0
    assert called is True
    assert stdout.getvalue().strip() == "projection_id=authority_memory_projection upserted=9 deactivated=1"


def test_data_dictionary_frontdoor_uses_canonical_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_data_dictionary_tool(params: dict[str, object]):
        captured.update(params)
        return {
            "action": "list",
            "count": 1,
            "objects": [
                {
                    "object_kind": "integration",
                    "label": "Integration",
                    "category": "integration",
                    "summary": "External systems",
                }
            ],
        }

    monkeypatch.setattr(authority_commands, "tool_praxis_data_dictionary", _fake_data_dictionary_tool)
    stdout = StringIO()

    assert workflow_cli_main(["dictionary", "list"], stdout=stdout) == 0
    assert captured == {"action": "list", "category": None}
    rendered = stdout.getvalue()
    assert "1 object kind(s)" in rendered
    assert "integration" in rendered


def test_dashboard_frontdoor_uses_backend_dashboard_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_backend_dashboard_payload():
        captured["called"] = True
        return {
            "summary": {
                "health": {"label": "Healthy"},
                "workflow_counts": {"total": 12, "live": 3, "saved": 5, "draft": 4},
                "runs_24h": 7,
                "active_runs": 2,
                "pass_rate_24h": 0.875,
                "total_cost_24h": 1.2345,
                "top_agent": "anthropic/claude-test",
                "models_online": 4,
                "queue": {
                    "depth": 4,
                    "status": "ok",
                    "utilization_pct": 50.0,
                    "pending": 1,
                    "ready": 2,
                    "claimed": 0,
                    "running": 1,
                    "error": None,
                },
            },
            "sections": [
                {"key": "live", "count": 3},
                {"key": "saved", "count": 5},
                {"key": "draft", "count": 4},
            ],
            "leaderboard": [
                {
                    "provider_slug": "anthropic",
                    "model_slug": "claude-test",
                    "pass_rate": 0.95,
                    "total_workflows": 10,
                }
            ],
            "recent_runs": [
                {
                    "run_id": "run-1",
                    "status": "running",
                    "completed_jobs": 1,
                    "total_jobs": 3,
                    "total_cost": 0.125,
                }
            ],
        }

    monkeypatch.setattr(operate_commands, "_backend_dashboard_payload", _fake_backend_dashboard_payload)

    stdout = StringIO()
    assert workflow_cli_main(["dashboard"], stdout=stdout) == 0

    assert captured == {"called": True}
    rendered = stdout.getvalue()
    assert "dashboard_summary:" in rendered
    assert "health=Healthy" in rendered
    assert "workflows=total=12 live=3 saved=5 draft=4" in rendered
    assert "queue: depth=4 status=ok utilization_pct=50.0 pending=1 ready=2 claimed=0 running=1" in rendered
    assert "leaderboard_top:" in rendered
    assert "anthropic/claude-test pass_rate_pct=95.0 total_workflows=10" in rendered
    assert "recent_runs:" in rendered
    assert "run-1 running jobs=1/3 cost_usd=0.1250" in rendered


def test_integration_frontdoor_lists_registered_integrations(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], **_kwargs):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {
            "integrations": [
                {
                    "id": "ipify",
                    "auth_status": "connected",
                    "provider": "http",
                    "capabilities": [{"action": "get_ip"}],
                }
            ],
            "count": 1,
        }

    monkeypatch.setattr(operate_commands, "run_cli_tool", _fake_run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(["integration", "list"], stdout=stdout) == 0
    assert captured == {"tool_name": "praxis_integration", "params": {"action": "list"}}
    rendered = stdout.getvalue()
    assert "INTEGRATION" in rendered
    assert "ipify" in rendered
    assert "get_ip" in rendered


def test_research_frontdoor_launches_parallel_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], **_kwargs):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {
            "action": "run",
            "topic": params.get("topic"),
            "slug": "api_auth_drift",
            "workers": params.get("workers"),
            "agent": params.get("agent"),
            "workflow": {"run_id": "run_123"},
        }

    monkeypatch.setattr(query_commands, "run_cli_tool", _fake_run_cli_tool)
    stdout = StringIO()

    assert workflow_cli_main(
        ["research", "API", "auth", "drift", "--workers", "12", "--agent", "deepseek/deepseek-r3"],
        stdout=stdout,
    ) == 0
    assert captured == {
        "tool_name": "praxis_research_workflow",
        "params": {
            "action": "run",
            "topic": "API auth drift",
            "workers": 12,
            "agent": "deepseek/deepseek-r3",
        },
    }
    payload = json.loads(stdout.getvalue())
    assert payload["action"] == "run"
    assert payload["workflow"]["run_id"] == "run_123"


def test_decompose_frontdoor_routes_to_sprint_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], **_kwargs):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {
            "total_sprints": 2,
            "total_estimate_minutes": 75,
            "critical_path": ["plan", "build"],
            "sprints": [
                {"label": "plan", "complexity": "low", "estimate_minutes": 15},
                {"label": "build", "complexity": "medium", "estimate_minutes": 60},
            ],
        }

    monkeypatch.setattr(query_commands, "run_cli_tool", _fake_run_cli_tool)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "decompose",
                "Build",
                "real-time",
                "notifications",
                "--scope-files",
                "src/alpha.py,src/beta.py",
                "--json",
            ],
            stdout=stdout,
        )
        == 0
    )
    assert captured == {
        "tool_name": "praxis_decompose",
        "params": {
            "objective": "Build real-time notifications",
            "scope_files": ["src/alpha.py", "src/beta.py"],
        },
    }
    assert json.loads(stdout.getvalue()) == {
        "total_sprints": 2,
        "total_estimate_minutes": 75,
        "critical_path": ["plan", "build"],
        "sprints": [
            {"label": "plan", "complexity": "low", "estimate_minutes": 15},
            {"label": "build", "complexity": "medium", "estimate_minutes": 60},
        ],
    }


def test_integration_create_requires_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], **_kwargs):
        nonlocal called
        called = True
        return 0, {"tool_name": tool_name, "params": dict(params)}

    monkeypatch.setattr(operate_commands, "run_cli_tool", _fake_run_cli_tool)
    stdout = StringIO()

    rc = workflow_cli_main(
        [
            "integration",
            "create",
            "--id",
            "ipify",
            "--name",
            "IPify",
            "--capabilities-json",
            '[{"action":"get_ip","method":"GET","path":"https://api.ipify.org/?format=json"}]',
        ],
        stdout=stdout,
    )

    assert rc == 2
    assert called is False
    rendered = stdout.getvalue()
    assert "tool: praxis_integration" in rendered
    assert "risk: write" in rendered
    assert "confirmation required" in rendered


def test_api_help_mentions_route_discovery() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["api", "--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow api [help|routes|integrations|data-dictionary|--host HOST|--port PORT]" in rendered
    assert "routes        show and filter the live HTTP route catalog without starting the server" in rendered
    assert "integrations  show and filter the /api/integrations route scope without starting the server" in rendered
    assert "data-dictionary show and filter the /api/data-dictionary route scope without starting the server" in rendered
    assert "Flat alias: workflow routes" in rendered
    assert "workflow integrations" in rendered
    assert "workflow api data-dictionary" in rendered
    assert "Discovery shortcuts:" in rendered
    assert "workflow help routes" in rendered


def test_routes_help_alias_mentions_route_discovery() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help", "routes"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow api [help|routes|integrations|data-dictionary|--host HOST|--port PORT]" in rendered
    assert "Flat alias: workflow routes" in rendered
    assert "workflow integrations" in rendered
    assert "workflow tools list" in rendered


def test_api_routes_help_is_a_successful_discovery_command() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["api", "routes", "--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow routes --json" in rendered
    assert "Discovery shortcuts:" in rendered
    assert "workflow help routes" in rendered


@pytest.mark.parametrize(
    ("argv", "path_prefix"),
    [
        (["integrations", "--search", "health", "--json"], "/api/integrations"),
        (["api", "integrations", "--search", "health", "--json"], "/api/integrations"),
        (["api", "data-dictionary", "--search", "health", "--json"], "/api/data-dictionary"),
    ],
)
def test_scoped_api_route_aliases_forward_the_expected_path_prefix(
    argv: list[str],
    path_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_list_api_routes(**kwargs):
        captured.update(kwargs)
        return {
            "count": 1,
            "docs_url": "/docs",
            "openapi_url": "/openapi.json",
            "redoc_url": "/redoc",
            "filters": {key: value for key, value in kwargs.items() if value is not None},
            "routes": [
                {
                    "path": path_prefix,
                    "methods": ["GET"],
                    "summary": "Scoped route discovery",
                    "description": "Scoped route discovery",
                }
            ],
        }

    monkeypatch.setattr(rest, "list_api_routes", _fake_list_api_routes)
    stdout = StringIO()

    assert workflow_cli_main(argv, stdout=stdout) == 0
    assert captured == {
        "search": "health",
        "method": None,
        "tag": None,
        "path_prefix": path_prefix,
        "visibility": "public",
    }
    payload = json.loads(stdout.getvalue())
    assert payload["filters"]["path_prefix"] == path_prefix


def test_api_routes_frontdoor_supports_discovery_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_list_api_routes(**kwargs):
        captured.update(kwargs)
        return {
            "count": 1,
            "docs_url": "/docs",
            "openapi_url": "/openapi.json",
            "redoc_url": "/redoc",
            "filters": {key: value for key, value in kwargs.items() if value is not None},
            "routes": [
                {
                    "path": "/api/health",
                    "methods": ["GET"],
                    "summary": "Platform health from Postgres",
                    "description": "Platform health from Postgres",
                }
            ],
        }

    monkeypatch.setattr(rest, "list_api_routes", _fake_list_api_routes)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "api",
                "routes",
                "--search",
                "health",
                "--method",
                "GET",
                "--tag",
                "platform",
                "--path-prefix",
                "/api",
                "--json",
            ],
            stdout=stdout,
        )
        == 0
    )

    assert captured == {
        "search": "health",
        "method": "GET",
        "tag": "platform",
        "path_prefix": "/api",
        "visibility": "public",
    }
    payload = json.loads(stdout.getvalue())
    assert payload["filters"] == {
        "search": "health",
        "method": "GET",
        "tag": "platform",
        "path_prefix": "/api",
        "visibility": "public",
    }


def test_routes_alias_frontdoor_supports_discovery_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_list_api_routes(**kwargs):
        captured.update(kwargs)
        return {
            "count": 1,
            "docs_url": "/docs",
            "openapi_url": "/openapi.json",
            "redoc_url": "/redoc",
            "filters": {key: value for key, value in kwargs.items() if value is not None},
            "routes": [
                {
                    "path": "/api/health",
                    "methods": ["GET"],
                    "summary": "Platform health from Postgres",
                    "description": "Platform health from Postgres",
                }
            ],
        }

    monkeypatch.setattr(rest, "list_api_routes", _fake_list_api_routes)
    stdout = StringIO()

    assert (
        workflow_cli_main(
            [
                "routes",
                "--search",
                "health",
                "--method",
                "GET",
                "--tag",
                "platform",
                "--path-prefix",
                "/api",
                "--json",
            ],
            stdout=stdout,
        )
        == 0
    )

    assert captured == {
        "search": "health",
        "method": "GET",
        "tag": "platform",
        "path_prefix": "/api",
        "visibility": "public",
    }
    payload = json.loads(stdout.getvalue())
    assert payload["filters"] == {
        "search": "health",
        "method": "GET",
        "tag": "platform",
        "path_prefix": "/api",
        "visibility": "public",
    }


def test_api_routes_frontdoor_supports_visibility_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_list_api_routes(**kwargs):
        captured.update(kwargs)
        return {
            "count": 1,
            "docs_url": "/docs",
            "openapi_url": "/openapi.json",
            "redoc_url": "/redoc",
            "filters": {key: value for key, value in kwargs.items() if value is not None},
            "routes": [{"path": "/api/routes", "methods": ["GET"], "summary": "Internal route catalog"}],
        }

    monkeypatch.setattr(rest, "list_api_routes", _fake_list_api_routes)
    stdout = StringIO()

    assert workflow_cli_main(["api", "routes", "--visibility", "all", "--json"], stdout=stdout) == 0

    assert captured == {
        "search": None,
        "method": None,
        "tag": None,
        "path_prefix": None,
        "visibility": "all",
    }


def test_api_routes_frontdoor_lists_the_live_http_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        rest,
        "list_api_routes",
        lambda **_kwargs: {
            "count": 2,
            "docs_url": "/docs",
            "openapi_url": "/openapi.json",
            "redoc_url": "/redoc",
            "summary": {
                "route_count": 2,
                "methods": [
                    {"method": "GET", "count": 2},
                ],
                "tags": [
                    {"tag": "workflow", "count": 2},
                ],
                "suggested_filters": {"tag": "workflow", "method": "GET"},
            },
            "routes": [
                {
                    "path": "/api/health",
                    "methods": ["GET"],
                    "summary": "Platform health from Postgres",
                    "description": "Platform health from Postgres",
                },
                {
                    "path": "/api/routes",
                    "methods": ["GET"],
                    "summary": "Route catalog",
                    "description": "Route catalog",
                },
            ],
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["api", "routes", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert payload["count"] == 2
    assert payload["routes"][0]["path"] == "/api/health"
    assert payload["routes"][1]["path"] == "/api/routes"
    assert payload["summary"]["route_count"] == 2


def test_api_routes_frontdoor_renders_route_facets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        rest,
        "list_api_routes",
        lambda **_kwargs: {
            "count": 2,
            "docs_url": "/docs",
            "openapi_url": "/openapi.json",
            "redoc_url": "/redoc",
            "summary": {
                "route_count": 2,
                "methods": [
                    {"method": "GET", "count": 2},
                    {"method": "POST", "count": 1},
                ],
                "tags": [
                    {"tag": "workflow", "count": 2},
                    {"tag": "operator", "count": 1},
                ],
                "suggested_filters": {"tag": "workflow", "method": "GET"},
            },
            "routes": [
                {
                    "path": "/api/health",
                    "methods": ["GET"],
                    "summary": "Platform health from Postgres",
                    "description": "Platform health from Postgres",
                },
                {
                    "path": "/api/routes",
                    "methods": ["GET", "POST"],
                    "summary": "Route catalog",
                    "description": "Route catalog",
                },
            ],
        },
    )
    stdout = StringIO()

    assert workflow_cli_main(["api", "routes"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "methods: GET=2, POST=1" in rendered
    assert "tags:    workflow=2, operator=1" in rendered
    assert "try:    workflow api routes --tag workflow --method GET" in rendered
    assert "workflow routes is the flat alias" in rendered
    assert "workflow routes --json" in rendered


def test_work_frontdoor_claim_and_acknowledge(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object], **_kwargs):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {"ok": True, "tool": tool_name, "params": dict(params)}

    monkeypatch.setattr(workflow_commands, "run_cli_tool", _fake_run_cli_tool)

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "work",
                "claim",
                "--subscription-id",
                "dispatch:worker:bridge",
                "--run-id",
                "dispatch_001",
                "--limit",
                "7",
            ],
            stdout=stdout,
        )
        == 0
    )
    assert captured["tool_name"] == "praxis_workflow"
    assert captured["params"] == {
        "action": "claim",
        "subscription_id": "dispatch:worker:bridge",
        "run_id": "dispatch_001",
        "last_acked_evidence_seq": None,
        "limit": 7,
    }

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "work",
                "acknowledge",
                "--work-json",
                '{"claimable": true, "inbox_batch": {"cursor": {"subscription_id": "dispatch:worker:bridge", "run_id": "dispatch_001"}, "next_cursor": {"subscription_id": "dispatch:worker:bridge", "run_id": "dispatch_001"}, "facts": [], "has_more": false}, "route_snapshot": {"run_id": "dispatch_001", "workflow_id": "workflow_1", "request_id": "request_1", "current_state": "claim_accepted", "claim_id": "claim_1", "lease_id": null, "proposal_id": null, "attempt_no": 1, "transition_seq": 1, "sandbox_group_id": null, "sandbox_session_id": null, "share_mode": "exclusive", "reuse_reason_code": null, "last_event_id": null}}',
                "--through-evidence-seq",
                "2",
                "--yes",
            ],
            stdout=stdout,
        )
        == 0
    )
    assert captured["params"]["action"] == "acknowledge"
    assert captured["params"]["through_evidence_seq"] == 2
    assert captured["params"]["work"]["claimable"] is True


def test_workflow_run_prompt_frontdoor_uses_prompt_compiler(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _PromptLaunchSpec:
        name = "prompt launch"
        workflow_id = "workflow_cli_prompt"
        phase = "execute"
        jobs = [{"label": "run"}]

        def to_inline_spec_dict(self) -> dict[str, object]:
            return {
                "name": self.name,
                "workflow_id": self.workflow_id,
                "phase": self.phase,
                "jobs": self.jobs,
            }

    def _fake_compile_prompt_launch_spec(**kwargs):
        captured["compile_kwargs"] = kwargs
        return _PromptLaunchSpec()

    def _fake_submit_workflow_launch(**kwargs):
        captured["launch_kwargs"] = kwargs
        return 0

    monkeypatch.setattr(
        workflow_commands,
        "_default_prompt_provider_slug",
        lambda: "openai",
    )
    monkeypatch.setattr(workflow_commands, "compile_prompt_launch_spec", _fake_compile_prompt_launch_spec)
    monkeypatch.setattr(
        workflow_commands,
        "_workflow_cli",
        lambda: SimpleNamespace(_submit_workflow_launch=_fake_submit_workflow_launch),
    )
    stdout = StringIO()

    assert workflow_commands._run_command(
        [
            "-p",
            "fix the failing prompt path",
            "--write",
            "runtime/example.py",
            "--workdir",
            ".",
            "--foreground-submit",
        ],
        stdout=stdout,
    ) == 0

    assert captured["compile_kwargs"] == {
        "prompt": "fix the failing prompt path",
        "provider_slug": "openai",
        "model_slug": None,
        "tier": None,
        "adapter_type": None,
        "scope_write": ["runtime/example.py"],
        "workdir": ".",
        "context_files": None,
        "timeout": 300,
        "task_type": None,
        "system_prompt": "You are a code editor. Return ONLY valid JSON structured output.",
        "workspace_ref": None,
        "runtime_profile_ref": None,
    }
    assert captured["launch_kwargs"]["prompt_launch_spec"].workflow_id == "workflow_cli_prompt"
    assert captured["launch_kwargs"]["requested_by_ref"] == "workflow.run.prompt"


def test_workflow_run_prompt_frontdoor_routes_scratch_lane(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _PromptLaunchSpec:
        name = "prompt launch"
        workflow_id = "workflow_cli_prompt"
        phase = "execute"
        jobs = [{"label": "run"}]

        def to_inline_spec_dict(self) -> dict[str, object]:
            return {
                "name": self.name,
                "workflow_id": self.workflow_id,
                "phase": self.phase,
                "jobs": self.jobs,
            }

    def _fake_compile_prompt_launch_spec(**kwargs):
        captured["compile_kwargs"] = kwargs
        return _PromptLaunchSpec()

    monkeypatch.setattr(workflow_commands, "_default_prompt_provider_slug", lambda: "openai")
    monkeypatch.setattr(workflow_commands, "compile_prompt_launch_spec", _fake_compile_prompt_launch_spec)
    monkeypatch.setattr(
        workflow_commands,
        "_workflow_cli",
        lambda: SimpleNamespace(_submit_workflow_launch=lambda **_kwargs: 0),
    )
    stdout = StringIO()

    assert workflow_commands._run_command(
        [
            "-p",
            "parse this public PDF and report the tables",
            "--scratch",
            "--foreground-submit",
        ],
        stdout=stdout,
    ) == 0

    assert captured["compile_kwargs"]["workspace_ref"] == "scratch_agent"
    assert captured["compile_kwargs"]["runtime_profile_ref"] == "scratch_agent"


def test_workflow_run_prompt_frontdoor_accepts_runtime_profile_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _PromptLaunchSpec:
        name = "prompt launch"
        workflow_id = "workflow_cli_prompt"
        phase = "execute"
        jobs = [{"label": "run"}]

        def to_inline_spec_dict(self) -> dict[str, object]:
            return {
                "name": self.name,
                "workflow_id": self.workflow_id,
                "phase": self.phase,
                "jobs": self.jobs,
            }

    def _fake_compile_prompt_launch_spec(**kwargs):
        captured["compile_kwargs"] = kwargs
        return _PromptLaunchSpec()

    monkeypatch.setattr(workflow_commands, "_default_prompt_provider_slug", lambda: "openai")
    monkeypatch.setattr(workflow_commands, "compile_prompt_launch_spec", _fake_compile_prompt_launch_spec)
    monkeypatch.setattr(
        workflow_commands,
        "_workflow_cli",
        lambda: SimpleNamespace(_submit_workflow_launch=lambda **_kwargs: 0),
    )
    stdout = StringIO()

    assert workflow_commands._run_command(
        [
            "-p",
            "inspect a temporary artifact",
            "--runtime-profile",
            "scratch_agent",
            "--workspace",
            "scratch_agent",
            "--foreground-submit",
        ],
        stdout=stdout,
    ) == 0

    assert captured["compile_kwargs"]["workspace_ref"] == "scratch_agent"
    assert captured["compile_kwargs"]["runtime_profile_ref"] == "scratch_agent"


def test_workflow_run_prompt_frontdoor_reports_unadmitted_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        workflow_commands,
        "_default_prompt_provider_slug",
        lambda: "openai",
    )
    monkeypatch.setattr(
        workflow_commands,
        "compile_prompt_launch_spec",
        lambda **_kwargs: (_ for _ in ()).throw(
            ValueError(
                "provider 'cursor' is not admitted for llm_task; "
                "reason: Prompt probe did not complete successfully for cursor/composer-2; "
                "decision_ref: decision.provider-onboarding.cursor.20260415T165657Z; "
                "known providers: cursor, google, openai"
            )
        ),
    )
    monkeypatch.setattr(
        workflow_commands,
        "_workflow_cli",
        lambda: SimpleNamespace(
            _submit_workflow_launch=lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("submit should not run")
            )
        ),
    )
    stdout = StringIO()

    assert workflow_commands._run_command(
        [
            "-p",
            "fix the failing prompt path",
            "--provider",
            "cursor",
            "--model",
            "composer-2",
            "--foreground-submit",
        ],
        stdout=stdout,
    ) == 2

    assert "error: provider 'cursor' is not admitted for llm_task" in stdout.getvalue()
    assert "Prompt probe did not complete successfully for cursor/composer-2" in stdout.getvalue()


def test_workflow_run_prompt_frontdoor_skips_default_provider_lookup_when_provider_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _PromptLaunchSpec:
        name = "prompt launch"
        workflow_id = "workflow_cli_prompt"
        phase = "execute"
        jobs = [{"label": "run"}]

        def to_inline_spec_dict(self) -> dict[str, object]:
            return {
                "name": self.name,
                "workflow_id": self.workflow_id,
                "phase": self.phase,
                "jobs": self.jobs,
            }

    monkeypatch.setattr(
        workflow_commands,
        "_default_prompt_provider_slug",
        lambda: (_ for _ in ()).throw(AssertionError("default provider should not be consulted")),
    )
    def _fake_compile_prompt_launch_spec(**kwargs):
        captured["compile_kwargs"] = kwargs
        return _PromptLaunchSpec()

    def _fake_submit_workflow_launch(**kwargs):
        captured["launch_kwargs"] = kwargs
        return 0

    monkeypatch.setattr(workflow_commands, "compile_prompt_launch_spec", _fake_compile_prompt_launch_spec)
    monkeypatch.setattr(
        workflow_commands,
        "_workflow_cli",
        lambda: SimpleNamespace(_submit_workflow_launch=_fake_submit_workflow_launch),
    )
    stdout = StringIO()

    assert workflow_commands._run_command(
        [
            "-p",
            "inspect the preview payload",
            "--provider",
            "openai",
            "--model",
            "gpt-5.4-mini",
            "--foreground-submit",
        ],
        stdout=stdout,
    ) == 0

    assert captured["compile_kwargs"]["provider_slug"] == "openai"
    assert captured["compile_kwargs"]["model_slug"] == "gpt-5.4-mini"


def test_workflow_run_prompt_frontdoor_passes_preview_execution_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _PromptLaunchSpec:
        name = "prompt launch"
        workflow_id = "workflow_cli_prompt"
        phase = "execute"
        jobs = [{"label": "run"}]

        def to_inline_spec_dict(self) -> dict[str, object]:
            return {
                "name": self.name,
                "workflow_id": self.workflow_id,
                "phase": self.phase,
                "jobs": self.jobs,
            }

    monkeypatch.setattr(workflow_commands, "_default_prompt_provider_slug", lambda: "openai")
    monkeypatch.setattr(workflow_commands, "compile_prompt_launch_spec", lambda **_kwargs: _PromptLaunchSpec())
    def _fake_submit_workflow_launch(**kwargs):
        captured["launch_kwargs"] = kwargs
        return 0

    monkeypatch.setattr(
        workflow_commands,
        "_workflow_cli",
        lambda: SimpleNamespace(_submit_workflow_launch=_fake_submit_workflow_launch),
    )
    stdout = StringIO()

    assert workflow_commands._run_command(
        [
            "-p",
            "inspect the preview payload",
            "--preview-execution",
        ],
        stdout=stdout,
    ) == 0

    assert captured["launch_kwargs"]["preview_execution"] is True


def test_prompt_provider_help_lists_registered_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        workflow_commands,
        "_prompt_provider_choices",
        lambda: ("cursor", "google", "openai"),
    )
    monkeypatch.setattr(
        workflow_commands,
        "_default_prompt_provider_slug",
        lambda: "openai",
    )

    assert (
        workflow_commands._prompt_provider_help_line()
        == "  --provider <slug>    Registered provider: cursor, google, openai (default: openai)\n"
    )


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


def test_records_frontdoor_supports_create_update_and_rename(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_query_mod = SimpleNamespace(
        _validate_workflow_body=lambda body, **_kwargs: None,
        _workflow_to_dict=lambda row, include_definition=False: {
            "id": row["id"],
            "name": row["name"],
            "definition": row["definition"] if include_definition else None,
            "compiled_spec": row["compiled_spec"],
        },
    )
    monkeypatch.setattr(workflow_commands, "_workflow_subsystems", lambda: _FakeSubsystems(object()))
    monkeypatch.setattr(workflow_commands, "_workflow_query_mod", lambda: fake_query_mod)
    import runtime.canonical_workflows as canonical_workflows

    monkeypatch.setattr(
        canonical_workflows,
        "save_workflow",
        lambda _conn, workflow_id=None, body=None: {
            "id": workflow_id or body.get("id") or "wf_probe",
            "name": body["name"],
            "definition": body["definition"],
            "compiled_spec": body.get("compiled_spec"),
        },
    )
    monkeypatch.setattr(
        canonical_workflows,
        "rename_workflow",
        lambda _conn, *, workflow_id, new_workflow_id, name=None, operator_surface="workflow records": {
            "id": new_workflow_id,
            "name": name or "Runtime Regression Probe",
            "definition": {"definition_revision": "def_runtime_regression_probe"},
            "compiled_spec": {"definition_revision": "def_runtime_regression_probe"},
            "workflow_id": workflow_id,
        },
    )

    create_payload = {
        "id": "runtime_regression_probe",
        "name": "Runtime Regression Probe",
        "definition": {"definition_revision": "def_runtime_regression_probe"},
        "compiled_spec": {
            "definition_revision": "def_runtime_regression_probe",
            "jobs": [{"label": "seed_contract"}],
        },
    }
    stdout = StringIO()
    assert (
        workflow_cli_main(
            ["records", "create", "--input-json", json.dumps(create_payload)],
            stdout=stdout,
        )
        == 0
    )
    created = json.loads(stdout.getvalue())
    assert created["workflow"]["id"] == "runtime_regression_probe"
    assert created["workflow"]["compiled_spec"]["jobs"][0]["label"] == "seed_contract"

    update_payload = {
        "name": "Runtime Regression Probe v2",
        "definition": {"definition_revision": "def_runtime_regression_probe_v2"},
    }
    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "records",
                "update",
                "runtime_regression_probe",
                "--input-json",
                json.dumps(update_payload),
            ],
            stdout=stdout,
        )
        == 0
    )
    updated = json.loads(stdout.getvalue())
    assert updated["workflow"]["id"] == "runtime_regression_probe"
    assert updated["workflow"]["name"] == "Runtime Regression Probe v2"

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "records",
                "rename",
                "runtime_regression_probe",
                "--to",
                "runtime_regression_probe_v2",
                "--name",
                "Runtime Regression Probe v2",
            ],
            stdout=stdout,
        )
        == 0
    )
    renamed = json.loads(stdout.getvalue())
    assert renamed["workflow"]["id"] == "runtime_regression_probe_v2"
    assert renamed["workflow"]["name"] == "Runtime Regression Probe v2"


def test_records_frontdoor_supports_list_get_and_never_run(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_query_mod = SimpleNamespace(
        _workflow_to_dict=lambda row, include_definition=False: {
            "id": row["id"],
            "name": row["name"],
            "has_spec": row.get("compiled_spec") is not None,
            "invocation_count": row.get("invocation_count", 0),
            "definition": row.get("definition") if include_definition else None,
        },
    )
    conn = object()
    monkeypatch.setattr(workflow_commands, "_workflow_subsystems", lambda: _FakeSubsystems(conn))
    monkeypatch.setattr(workflow_commands, "_workflow_query_mod", lambda: fake_query_mod)

    import storage.postgres.workflow_runtime_repository as workflow_repo

    captured: dict[str, object] = {}

    def _list_records(_conn, *, never_run=False, limit=100, **_kwargs):
        captured["conn"] = _conn
        captured["never_run"] = never_run
        captured["limit"] = limit
        return [
            {
                "id": "wf_draft",
                "name": "Draft Flow",
                "definition": {"type": "pipeline"},
                "compiled_spec": None,
                "invocation_count": 0,
            }
        ]

    monkeypatch.setattr(workflow_repo, "list_workflow_records", _list_records)
    monkeypatch.setattr(
        workflow_repo,
        "load_workflow_record",
        lambda _conn, *, workflow_id: {
            "id": workflow_id,
            "name": "Draft Flow",
            "definition": {"type": "pipeline"},
            "compiled_spec": None,
            "invocation_count": 0,
        },
    )

    stdout = StringIO()
    assert (
        workflow_cli_main(
            ["records", "list", "--never-run", "--limit", "5", "--json"],
            stdout=stdout,
        )
        == 0
    )
    listed = json.loads(stdout.getvalue())
    assert listed["count"] == 1
    assert listed["filters"]["never_run"] is True
    assert listed["workflows"][0]["id"] == "wf_draft"
    assert captured == {"conn": conn, "never_run": True, "limit": 5}

    stdout = StringIO()
    assert workflow_cli_main(["records", "get", "wf_draft", "--include-definition"], stdout=stdout) == 0
    fetched = json.loads(stdout.getvalue())
    assert fetched["workflow"]["id"] == "wf_draft"
    assert fetched["workflow"]["definition"] == {"type": "pipeline"}


def test_records_frontdoor_returns_typed_db_authority_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _DbUnavailable(Exception):
        pass

    _DbUnavailable.__name__ = "PostgresConfigurationError"

    class _UnavailableSubsystems:
        def get_pg_conn(self):
            raise _DbUnavailable("db down")

    fake_query_mod = SimpleNamespace(_workflow_to_dict=lambda row, include_definition=False: row)
    monkeypatch.setattr(workflow_commands, "_workflow_query_mod", lambda: fake_query_mod)
    monkeypatch.setattr(workflow_commands, "_workflow_subsystems", _UnavailableSubsystems)

    stdout = StringIO()
    assert workflow_cli_main(["records", "list", "--never-run"], stdout=stdout) == 1

    payload = json.loads(stdout.getvalue())
    assert payload["status"] == "error"
    assert payload["reason_code"] == "workflow_records.db_authority_unavailable"
    assert payload["source_authority"] == "public.workflows"


def test_records_nested_help_is_success() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["records", "list", "--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "usage: workflow records list" in rendered
    assert "[--json]" in rendered


def test_status_frontdoor_rejects_unsupported_since_hours() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["status", "--since-hours", "24000"], stdout=stdout) == 2

    rendered = stdout.getvalue()
    assert "workflow status does not support arguments" in rendered
    assert "time-window filtering is not implemented" in rendered
    assert "--since-hours" not in str(legacy_workflow_cli.__doc__)


def test_failed_cli_command_records_friction(monkeypatch: pytest.MonkeyPatch) -> None:
    workflow_main_module = importlib.import_module("surfaces.cli.main")
    captured: dict[str, object] = {}

    def _fake_record_cli_command_failure(**kwargs: object) -> bool:
        captured.update(kwargs)
        return True

    monkeypatch.setattr(
        workflow_main_module,
        "record_cli_command_failure",
        _fake_record_cli_command_failure,
    )

    stdout = StringIO()
    assert workflow_cli_main(["status", "--since-hours", "24000"], stdout=stdout) == 2

    assert captured["args"] == ["status", "--since-hours", "24000"]
    assert captured["exit_code"] == 2
    assert "workflow status does not support arguments" in str(captured["output_text"])
    assert "Agent hint:" in str(captured["output_text"])
    assert captured["output_truncated"] is False


def test_successful_cli_command_does_not_record_friction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_main_module = importlib.import_module("surfaces.cli.main")

    def _unexpected_record_cli_command_failure(**_kwargs: object) -> bool:
        raise AssertionError("successful commands should not record CLI friction")

    monkeypatch.setattr(
        workflow_main_module,
        "record_cli_command_failure",
        _unexpected_record_cli_command_failure,
    )

    stdout = StringIO()
    assert workflow_cli_main(["status", "--help"], stdout=stdout) == 0


def test_status_help_is_success() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["status", "--help"], stdout=stdout) == 0
    assert stdout.getvalue() == "usage: workflow status [--json]\n"


def test_verify_platform_returns_typed_db_authority_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _DbUnavailable(Exception):
        pass

    _DbUnavailable.__name__ = "PostgresConfigurationError"

    import runtime.verifier_authority as verifier_authority

    monkeypatch.setattr(
        verifier_authority,
        "registry_snapshot",
        lambda: (_ for _ in ()).throw(_DbUnavailable("db down")),
    )

    stdout = StringIO()
    assert workflow_cli_main(["verify-platform"], stdout=stdout) == 1

    payload = json.loads(stdout.getvalue())
    assert payload["status"] == "error"
    assert payload["reason_code"] == "verifier.db_authority_unavailable"
    assert payload["source_authority"] == "verifier_registry"


def test_legacy_defs_and_workflows_aliases_fail_fast_with_rename_hint() -> None:
    stdout = StringIO()
    assert (
        workflow_cli_main(["defs", "create", "--input-json", "{}"], stdout=stdout)
        == 2
    )
    assert "workflow records" in stdout.getvalue()

    stdout = StringIO()
    assert (
        workflow_cli_main(["workflows", "create", "--input-json", "{}"], stdout=stdout)
        == 2
    )
    assert "unknown command: workflows" in stdout.getvalue()
    assert "Agent hint: `workflow workflows` is not a front door" in stdout.getvalue()


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


def test_main_accepts_optional_workflow_namespace_token(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    workflow_main_module = importlib.import_module("surfaces.cli.main")

    def _fake_run(args: list[str], *, stdout):
        captured["args"] = list(args)
        return 0

    monkeypatch.setitem(workflow_main_module._ARG_COMMANDS, "run", _fake_run)

    assert workflow_cli_main(["workflow", "run", "spec.queue.json"], stdout=StringIO()) == 0
    assert captured["args"] == ["spec.queue.json"]


def test_root_help_is_discoverable() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "Most used:" in rendered
    assert "workflow run <spec.json>" in rendered
    assert "workflow run-status <run_id>" in rendered
    assert "workflow mcp [list|search|describe|call|help]" in rendered
    assert "workflow help mcp" in rendered
    assert "workflow integrations" in rendered
    assert "workflow commands --json" in rendered
    assert "workflow native-operator instance" in rendered
    assert "workflow help api" in rendered
    assert "workflow help commands" in rendered
    assert "workflow help <command>" in rendered


def test_commands_root_and_help_topic_show_the_command_index() -> None:
    commands_stdout = StringIO()
    help_stdout = StringIO()

    assert workflow_cli_main(["commands"], stdout=commands_stdout) == 0
    assert workflow_cli_main(["help", "commands"], stdout=help_stdout) == 0

    commands_rendered = commands_stdout.getvalue()
    help_rendered = help_stdout.getvalue()
    assert commands_rendered == help_rendered
    assert "Command index:" in commands_rendered
    assert "workflow commands" in commands_rendered
    assert "workflow help commands" in commands_rendered
    assert "workflow run-status <run_id>" in commands_rendered
    assert "workflow mcp [list|search|describe|call|help]" in commands_rendered
    assert "workflow help mcp" in commands_rendered
    assert "workflow research [list|<topic>] [--workers N] [--agent SLUG] [--threshold N] [--json]" in commands_rendered
    assert "workflow tools [list|search|describe|call|help]" in commands_rendered


def test_commands_root_json_exposes_machine_readable_index() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["commands", "--json"], stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert payload["usage"] == "workflow commands"
    assert any(entry["command"] == "workflow commands" for entry in payload["entries"])
    assert any(entry["command"] == "workflow help commands" for entry in payload["entries"])
    assert any(
        entry["command"] == "workflow api help [routes|integrations|data-dictionary]"
        for entry in payload["entries"]
    )
    assert any(
        entry["command"] == "workflow tools [list|search|describe|call|help]"
        for entry in payload["entries"]
    )
    assert any(
        entry["command"] == "workflow research [list|<topic>] [--workers N] [--agent SLUG] [--threshold N] [--json]"
        for entry in payload["entries"]
    )
    assert any(entry["command"] == "workflow help mcp" for entry in payload["entries"])
    assert "run `workflow commands --json` for machine-readable discovery" in payload["tips"]
    assert "run `workflow api help` or `workflow help api` for HTTP route discovery" in payload["tips"]


def test_help_can_show_command_specific_usage() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help", "run"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "workflow run <spec.json>" in rendered
    assert "workflow run -p <prompt>" in rendered


def test_api_help_alias_is_discoverable() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["api", "help"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "usage: workflow api [help|routes|integrations|data-dictionary|--host HOST|--port PORT]" in rendered
    assert "workflow api help routes" in rendered
    assert "workflow api help integrations" in rendered
    assert "workflow api help data-dictionary" in rendered


def test_preview_frontdoor_delegates_to_run_with_preview_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    workflow_main_module = importlib.import_module("surfaces.cli.main")

    def _fake_workflow_command_handler(command_name: str):
        assert command_name == "_run_command"

        def _fake_run(args: list[str], *, stdout) -> int:
            captured["args"] = list(args)
            return 0

        return _fake_run

    monkeypatch.setattr(
        workflow_main_module,
        "_workflow_command_handler",
        _fake_workflow_command_handler,
    )

    assert workflow_cli_main(["preview", "spec.queue.json"], stdout=StringIO()) == 0
    assert captured == {
        "args": ["spec.queue.json", "--preview-execution"],
    }


def test_commands_root_does_not_import_workflow_command_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_main_module = importlib.import_module("surfaces.cli.main")
    original_import_module = workflow_main_module.importlib.import_module

    def _guarded_import(name: str, package: str | None = None):
        if name == ".commands.workflow" and package == "surfaces.cli":
            raise AssertionError("commands root should not import workflow command handlers")
        return original_import_module(name, package)

    monkeypatch.setattr(workflow_main_module.importlib, "import_module", _guarded_import)

    stdout = StringIO()
    assert workflow_cli_main(["commands"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow commands" in rendered


def test_active_frontdoor_uses_operator_status_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_run_cli_tool(tool_name: str, params: dict[str, object]):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params)
        return 0, {
            "since_hours": 24,
            "pass_rate": 0.5,
            "adjusted_pass_rate": 0.75,
            "observability_state": "ready",
            "queue_depth": 4,
            "queue_depth_status": "ok",
            "queue_depth_pending": 3,
            "queue_depth_ready": 0,
            "queue_depth_claimed": 0,
            "queue_depth_running": 1,
            "queue_depth_total": 4,
            "in_flight_workflows": [
                {
                    "run_id": "workflow_live",
                    "workflow_name": "Live Workflow",
                    "total_jobs": 2,
                    "completed_jobs": 1,
                }
            ],
        }

    monkeypatch.setattr(workflow_commands, "run_cli_tool", _fake_run_cli_tool)

    stdout = StringIO()
    assert workflow_cli_main(["active"], stdout=stdout) == 0

    assert captured == {
        "tool_name": "praxis_status_snapshot",
        "params": {"since_hours": 24},
    }
    payload = json.loads(stdout.getvalue())
    assert payload["source"] == "praxis_status_snapshot"
    assert payload["active_runs"] == ["workflow_live"]
    assert payload["count"] == 1
    assert payload["runs"][0]["workflow_name"] == "Live Workflow"
    assert payload["queue"]["running"] == 1


def test_generate_frontdoor_uses_direct_compat_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_generate(args) -> int:
        captured["manifest_file"] = args.manifest_file
        captured["output"] = args.output
        captured["strict"] = args.strict
        captured["merge"] = args.merge
        return 0

    monkeypatch.setattr(legacy_workflow_cli, "cmd_generate", _fake_generate)

    assert workflow_cli_main(["generate", "manifest.json", "spec.queue.json"], stdout=StringIO()) == 0
    assert captured == {
        "manifest_file": "manifest.json",
        "output": "spec.queue.json",
        "strict": False,
        "merge": False,
    }


def test_legacy_workflow_cli_run_delegates_to_modern_frontdoor(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}
    workflow_main_module = importlib.import_module("surfaces.cli.main")

    def _fake_main(argv, *, stdout=None):
        captured["argv"] = list(argv)
        stdout.write("run delegated\n")
        return 0

    monkeypatch.setattr(workflow_main_module, "main", _fake_main)

    assert legacy_workflow_cli.main(["run", "spec.queue.json"]) == 0
    assert captured["argv"] == ["run", "spec.queue.json"]
    assert "run delegated" in capsys.readouterr().out


def test_legacy_workflow_cli_generate_delegates_to_modern_frontdoor(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}
    workflow_main_module = importlib.import_module("surfaces.cli.main")

    def _fake_main(argv, *, stdout=None):
        captured["argv"] = list(argv)
        stdout.write("generate delegated\n")
        return 0

    monkeypatch.setattr(workflow_main_module, "main", _fake_main)

    assert legacy_workflow_cli.main(["generate", "manifest.json", "spec.queue.json"]) == 0
    assert captured["argv"] == ["generate", "manifest.json", "spec.queue.json"]
    assert "generate delegated" in capsys.readouterr().out


def test_help_can_show_circuits_usage() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help", "circuits"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "workflow circuits open <provider_slug>" in rendered
    assert "workflow circuits history [provider_slug]" in rendered
    assert "workflow circuits reset <provider_slug>" in rendered


def test_help_can_show_native_operator_usage() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help", "native-operator"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "workflow native-operator instance" in rendered
    assert "workflow native-operator route-disable" in rendered
    assert "workflow native-operator operator-decision" in rendered
    assert "start was removed" in rendered


def test_help_can_show_api_usage() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help", "api"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "workflow api [help|routes|integrations|data-dictionary|--host HOST|--port PORT]" in rendered
    assert "routes        show and filter the live HTTP route catalog without starting the server" in rendered


def test_native_operator_help_entrypoint_is_available() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["native-operator", "--help"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "workflow native-operator status" in rendered
    assert "workflow native-operator operator-decision" in rendered
    assert "workflow native-operator provider-onboard" in rendered


def test_circuits_command_routes_to_catalog_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object] | None = None, *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params or {})
        captured["workflow_token"] = workflow_token
        return 0, {"ok": True, "params": params or {}}

    monkeypatch.setattr(operate_commands, "run_cli_tool", _run_cli_tool)

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "circuits",
                "open",
                "openai",
                "--reason",
                "provider_outage",
                "--rationale",
                "Provider outage",
                "--decided-by",
                "ops",
            ],
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert captured["tool_name"] == "praxis_circuits"
    assert captured["params"] == {
        "action": "open",
        "provider_slug": "openai",
        "reason_code": "provider_outage",
        "rationale": "Provider outage",
        "decided_by": "ops",
    }
    assert payload["ok"] is True


def test_circuits_history_routes_to_catalog_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _run_cli_tool(tool_name: str, params: dict[str, object] | None = None, *, workflow_token: str = ""):
        captured["tool_name"] = tool_name
        captured["params"] = dict(params or {})
        return 0, {"history": []}

    monkeypatch.setattr(operate_commands, "run_cli_tool", _run_cli_tool)

    stdout = StringIO()
    assert workflow_cli_main(["circuits", "history", "openai"], stdout=stdout) == 0

    assert captured["tool_name"] == "praxis_circuits"
    assert captured["params"] == {
        "action": "history",
        "provider_slug": "openai",
    }


def test_tools_search_finds_circuits_tool() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["tools", "search", "circuit"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "praxis_circuits" in rendered
    assert "workflow circuits" in rendered


def test_help_can_show_command_group_usage() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help", "tools"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "Tool discovery quickstart:" in rendered
    assert "workflow tools describe <tool|alias>" in rendered
    assert "unique prefix" in rendered.lower()


def test_help_can_show_mcp_usage() -> None:
    tools_stdout = StringIO()
    mcp_stdout = StringIO()

    assert workflow_cli_main(["tools"], stdout=tools_stdout) == 0
    assert workflow_cli_main(["help", "mcp"], stdout=mcp_stdout) == 0

    tools_rendered = tools_stdout.getvalue()
    mcp_rendered = mcp_stdout.getvalue()
    assert "Tool discovery quickstart:" in tools_rendered
    assert "Tool discovery quickstart:" in mcp_rendered
    assert "usage: workflow mcp [list|search|describe|call|help]" in mcp_rendered
    assert "Alias for workflow tools discovery." in mcp_rendered
    assert "workflow tools list" in mcp_rendered
    assert "workflow tools describe <tool|alias>" in mcp_rendered
    assert tools_rendered in mcp_rendered


def test_mcp_root_alias_routes_to_tools_quickstart() -> None:
    tools_stdout = StringIO()
    mcp_stdout = StringIO()

    assert workflow_cli_main(["tools"], stdout=tools_stdout) == 0
    assert workflow_cli_main(["mcp"], stdout=mcp_stdout) == 0

    assert mcp_stdout.getvalue() == tools_stdout.getvalue()


def test_help_rejects_unknown_topic() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help", "nope"], stdout=stdout) == 2

    rendered = stdout.getvalue()
    assert "unknown help topic: nope" in rendered
    assert "did you mean:" in rendered
    assert "workflow help run" in rendered


def test_unknown_root_command_suggests_help() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["nope"], stdout=stdout) == 2

    rendered = stdout.getvalue()
    assert "unknown command: nope" in rendered
    assert "did you mean:" in rendered
    assert "workflow help <command>" in rendered
    assert "workflow help api" in rendered


def test_defs_alias_is_no_longer_exposed_as_a_root_command() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["defs"], stdout=stdout) == 2

    rendered = stdout.getvalue()
    assert "workflow defs has been removed" in rendered
    assert "workflow records" in rendered


def test_run_frontdoor_forwards_fresh_launch_intent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "spec.queue.json"
    spec_path.write_text(
        json.dumps(
            {
                "name": "frontdoor run",
                "workflow_id": "frontdoor_run",
                "phase": "test",
                "jobs": [{"label": "job-a", "agent": "openai/gpt-5.4-mini", "prompt": "Run it"}],
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def _fake_cmd_run(args) -> int:
        captured["spec"] = args.spec
        captured["dry_run"] = args.dry_run
        captured["fresh"] = args.fresh
        captured["job_id"] = args.job_id
        captured["run_id"] = args.run_id
        captured["result_file"] = args.result_file
        return 0

    monkeypatch.setattr(
        workflow_commands,
        "_workflow_cli",
        lambda: SimpleNamespace(cmd_run=_fake_cmd_run),
    )

    assert (
        workflow_cli_main(
            [
                "run",
                str(spec_path),
                "--fresh",
                "--dry-run",
                "--job-id",
                "job-77",
                "--result-file",
                str(tmp_path / "result.json"),
            ],
            stdout=StringIO(),
        )
        == 0
    )
    assert captured == {
        "spec": str(spec_path),
        "dry_run": True,
        "fresh": True,
        "job_id": "job-77",
        "run_id": None,
        "result_file": str(tmp_path / "result.json"),
    }


def test_run_frontdoor_uses_detached_launcher_for_async_submit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "spec.queue.json"
    spec_path.write_text(
        json.dumps(
            {
                "name": "frontdoor detached run",
                "workflow_id": "frontdoor_detached_run",
                "phase": "test",
                "jobs": [{"label": "job-a", "agent": "openai/gpt-5.4-mini", "prompt": "Run it"}],
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        workflow_commands,
        "_launch_detached_frontdoor",
        lambda **kwargs: captured.update(kwargs) or 0,
    )
    monkeypatch.setattr(
        workflow_commands,
        "_workflow_cli",
        lambda: SimpleNamespace(
            cmd_run=lambda _args: (_ for _ in ()).throw(AssertionError("cmd_run should not be used"))
        ),
    )

    assert workflow_cli_main(["run", str(spec_path)], stdout=StringIO()) == 0
    assert captured == {
        "command_name": "run",
        "args": [str(spec_path)],
        "stdout": captured["stdout"],
        "result_file_base": "workflow_run_result",
        "success_prefix": "Workflow submitted",
        "emit_parent": False,
    }


def test_spawn_frontdoor_uses_detached_launcher_for_async_submit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "spec.queue.json"
    spec_path.write_text(
        json.dumps(
            {
                "name": "frontdoor detached spawn",
                "workflow_id": "frontdoor_detached_spawn",
                "phase": "test",
                "jobs": [{"label": "job-a", "agent": "openai/gpt-5.4-mini", "prompt": "Run it"}],
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        workflow_commands,
        "_launch_detached_frontdoor",
        lambda **kwargs: captured.update(kwargs) or 0,
    )
    monkeypatch.setattr(
        workflow_commands,
        "_workflow_cli",
        lambda: SimpleNamespace(
            cmd_spawn=lambda _args: (_ for _ in ()).throw(AssertionError("cmd_spawn should not be used"))
        ),
    )

    assert (
        workflow_cli_main(
            ["spawn", "workflow_parent_123", str(spec_path), "--reason", "phase.review"],
            stdout=StringIO(),
        )
        == 0
    )
    assert captured == {
        "command_name": "spawn",
        "args": [
            "workflow_parent_123",
            str(spec_path),
            "--reason",
            "phase.review",
        ],
        "stdout": captured["stdout"],
        "result_file_base": "workflow_spawn_result",
        "success_prefix": "Child workflow spawned",
        "emit_parent": True,
    }


# ---------------------------------------------------------------------------
# BUG-61881910: `praxis workflow chain` must accept a coordination-program JSON
# ---------------------------------------------------------------------------


def test_chain_frontdoor_detects_coordination_program(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A single coordination-program JSON (with top-level 'waves') must route
    to the durable WorkflowChainProgram submit path — not the legacy
    'requires 2+ specs' error."""

    # Minimal coordination-program shape.
    coord = {
        "program": "test_program",
        "why": "regression",
        "validate_order": ["config/cascade/specs/s1.json"],
        "waves": [
            {"wave_id": "w1", "specs": ["config/cascade/specs/s1.json"]}
        ],
    }
    coord_path = tmp_path / "test_chain.json"
    coord_path.write_text(json.dumps(coord))

    captured: dict[str, object] = {}

    def _fake_request(pg_conn, **kwargs):  # noqa: ARG001
        captured.update(kwargs)
        return SimpleNamespace(command_id="cmd_test", command_type="workflow.chain.submit")

    def _fake_render(pg_conn, command, **kwargs):  # noqa: ARG001
        return {"status": "submitted", "chain_id": "chain_test_123"}

    # Patch the control_commands module imports lazily used inside the handler.
    import runtime.control_commands as _cc
    monkeypatch.setattr(_cc, "request_workflow_chain_submit_command", _fake_request)
    monkeypatch.setattr(_cc, "render_workflow_chain_submit_response", _fake_render)

    # Provide a fake pg_conn through _subs.
    import surfaces.mcp.subsystems as _subs_mod
    monkeypatch.setattr(_subs_mod, "_subs", _FakeSubsystems())

    stdout = StringIO()
    exit_code = workflow_cli_main(["chain", str(coord_path)], stdout=stdout)
    assert exit_code == 0, f"unexpected failure: {stdout.getvalue()!r}"
    payload = json.loads(stdout.getvalue())
    assert payload == {"status": "submitted", "chain_id": "chain_test_123"}
    assert captured["coordination_path"] == str(coord_path)
    assert captured["requested_by_kind"] == "cli"
    assert captured["adopt_active"] is True


def test_chain_frontdoor_no_adopt_active_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """--no-adopt-active must flow through to the submit command."""

    coord = {
        "program": "test_program",
        "validate_order": [],
        "waves": [{"wave_id": "w1", "specs": []}],
    }
    coord_path = tmp_path / "test_chain.json"
    coord_path.write_text(json.dumps(coord))

    captured: dict[str, object] = {}

    def _fake_request(pg_conn, **kwargs):  # noqa: ARG001
        captured.update(kwargs)
        return SimpleNamespace(command_id="cmd_test", command_type="workflow.chain.submit")

    def _fake_render(pg_conn, command, **kwargs):  # noqa: ARG001
        return {"status": "submitted", "chain_id": "chain_test_456"}

    import runtime.control_commands as _cc
    monkeypatch.setattr(_cc, "request_workflow_chain_submit_command", _fake_request)
    monkeypatch.setattr(_cc, "render_workflow_chain_submit_response", _fake_render)

    import surfaces.mcp.subsystems as _subs_mod
    monkeypatch.setattr(_subs_mod, "_subs", _FakeSubsystems())

    stdout = StringIO()
    exit_code = workflow_cli_main(
        ["chain", str(coord_path), "--no-adopt-active"], stdout=stdout
    )
    assert exit_code == 0
    assert captured["adopt_active"] is False


def test_chain_frontdoor_legacy_mode_still_requires_two_specs(
    tmp_path: Path,
) -> None:
    """Backward-compat: a single non-coordination JSON (no 'waves' key) still
    gets the legacy error hint."""

    legacy_single = tmp_path / "only_one_spec.json"
    legacy_single.write_text(json.dumps({"name": "solo", "jobs": []}))

    stdout = StringIO()
    exit_code = workflow_cli_main(["chain", str(legacy_single)], stdout=stdout)
    assert exit_code == 2
    assert "coordination-program JSON" in stdout.getvalue()


def test_chain_frontdoor_help_mentions_coordination_and_legacy_modes() -> None:
    stdout = StringIO()
    exit_code = workflow_cli_main(["chain", "--help"], stdout=stdout)
    assert exit_code == 0
    out = stdout.getvalue()
    assert "coordination.json" in out.lower() or "coordination-program" in out.lower()
    assert "spec1.json" in out  # legacy mode still documented
    assert "--no-adopt-active" in out
