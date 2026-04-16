from __future__ import annotations

import importlib
import json
import os
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://postgres@localhost:5432/praxis")

import surfaces.api.rest as rest
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


def test_top_level_help_mentions_routes_alias() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow routes" in rendered


def test_commands_index_mentions_routes_alias() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["commands"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow routes" in rendered
    assert "Alias for workflow API route discovery" in rendered


def test_api_help_mentions_route_discovery() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["api", "--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow api [routes|--host HOST|--port PORT]" in rendered
    assert "routes        show and filter the live HTTP route catalog without starting the server" in rendered
    assert "Flat alias: workflow routes" in rendered
    assert "Discovery shortcuts:" in rendered
    assert "workflow help routes" in rendered


def test_routes_help_alias_mentions_route_discovery() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help", "routes"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow api [routes|--host HOST|--port PORT]" in rendered
    assert "Flat alias: workflow routes" in rendered
    assert "workflow tools list" in rendered


def test_api_routes_help_is_a_successful_discovery_command() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["api", "routes", "--help"], stdout=stdout) == 0
    rendered = stdout.getvalue()
    assert "workflow routes --json" in rendered
    assert "Discovery shortcuts:" in rendered
    assert "workflow help routes" in rendered


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
    }
    assert captured["launch_kwargs"]["prompt_launch_spec"].workflow_id == "workflow_cli_prompt"
    assert captured["launch_kwargs"]["requested_by_ref"] == "workflow.run.prompt"


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
    assert "workflow mcp" in rendered
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
    assert "workflow mcp [list|search|describe|call]" in commands_rendered
    assert "workflow tools [list|search|describe|call]" in commands_rendered


def test_help_can_show_command_specific_usage() -> None:
    stdout = StringIO()

    assert workflow_cli_main(["help", "run"], stdout=stdout) == 0

    rendered = stdout.getvalue()
    assert "workflow run <spec.json>" in rendered
    assert "workflow run -p <prompt>" in rendered


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
    assert "workflow api [routes|--host HOST|--port PORT]" in rendered
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

    assert mcp_stdout.getvalue() == tools_stdout.getvalue()


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
