"""Startup wiring tests for the FastAPI REST surface."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import surfaces.api.rest as rest
from surfaces.api import handlers as api_handlers
from surfaces.api.handlers import _query_bugs
from surfaces.api.handlers import _surface_usage as surface_usage_mod
from surfaces.api.handlers import workflow_query


def test_rest_startup_initializes_shared_subsystems_once(monkeypatch) -> None:
    created: list[object] = []

    class _FakeSubsystems:
        def __init__(self) -> None:
            created.append(self)

    monkeypatch.setattr(rest, "_Subsystems", _FakeSubsystems)
    if hasattr(rest.app.state, "shared_subsystems"):
        delattr(rest.app.state, "shared_subsystems")

    try:
        first = rest._ensure_shared_subsystems(rest.app)
        second = rest._ensure_shared_subsystems(rest.app)
        assert first is second
        assert created == [first]
        assert rest.app.state.shared_subsystems is first
    finally:
        if hasattr(rest.app.state, "shared_subsystems"):
            delattr(rest.app.state, "shared_subsystems")


def test_rest_startup_boots_shared_subsystems_when_enabled(monkeypatch) -> None:
    created: list[object] = []
    booted: list[object] = []

    class _FakeSubsystems:
        def __init__(self) -> None:
            created.append(self)

        def boot(self) -> dict[str, object]:
            booted.append(self)
            return {"booted": True}

    monkeypatch.setattr(rest, "_Subsystems", _FakeSubsystems)
    monkeypatch.setattr(rest, "_should_boot_shared_subsystems", lambda: True)
    if hasattr(rest.app.state, "shared_subsystems"):
        delattr(rest.app.state, "shared_subsystems")

    try:
        first = rest._boot_shared_subsystems(rest.app)
        second = rest._boot_shared_subsystems(rest.app)
        assert first is second
        assert created == [first]
        assert booted == [first, first]
    finally:
        if hasattr(rest.app.state, "shared_subsystems"):
            delattr(rest.app.state, "shared_subsystems")


def test_rest_startup_degrades_when_capability_mount_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        rest,
        "mount_capabilities",
        lambda _app: (_ for _ in ()).throw(RuntimeError("db unavailable")),
    )
    monkeypatch.setattr(
        rest.launcher_handlers,
        "launcher_status_payload",
        lambda: {"ok": True, "ready": False},
    )

    with TestClient(rest.app) as client:
        response = client.get("/api/launcher/status")

    assert response.status_code == 200
    assert response.json() == {"ok": True, "ready": False}


def test_launcher_status_endpoint_delegates_to_handler(monkeypatch) -> None:
    expected = {
        "ok": True,
        "ready": False,
        "platform_state": "degraded",
        "launch_url": "https://praxis.example/app",
        "dashboard_url": "https://praxis.example/app",
        "api_docs_url": "https://praxis.example/docs",
        "doctor": {"api_server_ready": False},
        "dependency_truth": {"ok": True},
        "services": [],
        "service_summary": {"total": 0},
    }
    monkeypatch.setattr(rest.launcher_handlers, "launcher_status_payload", lambda: expected)

    with TestClient(rest.app) as client:
        response = client.get("/api/launcher/status")

    assert response.status_code == 200
    assert response.json() == expected


def test_launcher_resolve_endpoint_returns_workspace_authority(monkeypatch) -> None:
    class _FakeConn:
        def execute(self, query: str, *args: object):
            assert "registry_workspace_base_path_authority" in query
            assert args == ("praxis", "default")
            return [
                {
                    "workspace_ref": "praxis",
                    "host_ref": "default",
                    "base_path_ref": "workspace_base.praxis.default",
                    "base_path": "${PRAXIS_WORKSPACE_BASE_PATH}",
                    "repo_root_path": ".",
                    "workdir_path": ".",
                }
            ]

    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: _FakeConn())

    with TestClient(rest.app) as client:
        response = client.get(
            "/api/launcher/resolve",
            params={"workspace_ref": "praxis", "host_ref": "default"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["resolution"]["base_path_ref"] == "workspace_base.praxis.default"
    assert payload["resolution"]["base_path"] == "${PRAXIS_WORKSPACE_BASE_PATH}"


def test_agent_sessions_surface_is_mounted(monkeypatch, tmp_path) -> None:
    class _FakeAgentSessionConn:
        def execute(self, sql: str, *args):
            if "FROM agent_sessions" in sql and "ORDER BY last_activity_at" in sql:
                return []
            return []

    monkeypatch.setattr(rest.agent_sessions_app, "AGENTS_DIR", tmp_path / "agents")
    monkeypatch.setattr(
        rest.agent_sessions_app.app.state,
        "pg_conn_factory",
        lambda: _FakeAgentSessionConn(),
        raising=False,
    )
    monkeypatch.setenv("PRAXIS_API_TOKEN", "session-token")

    with TestClient(rest.app) as client:
        index_response = client.get("/api/agent-sessions")
        agents_response = client.get(
            "/api/agent-sessions/agents",
            headers={"Authorization": "Bearer session-token"},
        )

    assert index_response.status_code == 200
    assert index_response.json()["service"] == "agent_sessions"
    assert index_response.json()["base_path"] == "/api/agent-sessions"
    assert "/api/agent-sessions/workflows/launch" in index_response.json()["routes"]
    assert "/api/agent-sessions/workflows/commands/{command_id}/approve" in index_response.json()["routes"]
    assert agents_response.status_code == 200
    assert agents_response.json() == []


def test_agent_sessions_cwd_is_repo_relative_by_default(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("PRAXIS_AGENT_CWD", raising=False)
    monkeypatch.delenv("PRAXIS_REPO_ROOT", raising=False)
    assert rest.agent_sessions_app._claude_cwd() == rest.agent_sessions_app.PRAXIS_ROOT

    monkeypatch.setenv("PRAXIS_REPO_ROOT", str(tmp_path))
    assert rest.agent_sessions_app._claude_cwd() == tmp_path.resolve()


def test_launcher_serves_pwa_manifest_and_service_worker(monkeypatch, tmp_path) -> None:
    (tmp_path / "manifest.webmanifest").write_text('{"name":"Praxis"}', encoding="utf-8")
    (tmp_path / "sw.js").write_text("self.skipWaiting();", encoding="utf-8")
    monkeypatch.setattr(rest, "_APP_DIST_DIR", tmp_path)

    with TestClient(rest.app) as client:
        manifest_response = client.get("/app/manifest.webmanifest")
        sw_response = client.get("/sw.js")

    assert manifest_response.status_code == 200
    assert manifest_response.headers["content-type"].startswith("application/manifest+json")
    assert manifest_response.json()["name"] == "Praxis"
    assert sw_response.status_code == 200
    assert "self.skipWaiting" in sw_response.text


def test_mobile_app_routes_are_inline_and_installable() -> None:
    with TestClient(rest.app) as client:
        app_response = client.get("/mobile")
        manifest_response = client.get("/mobile/manifest.webmanifest")
        sw_response = client.get("/mobile/sw.js")

    assert app_response.status_code == 200
    assert app_response.headers["content-type"].startswith("text/html")
    assert "Praxis" in app_response.text
    assert "/api/agent-sessions/agents" in app_response.text
    assert "/api/agent-sessions/workflows/launch" in app_response.text
    assert "/api/agent-sessions/workflows/commands/" in app_response.text
    assert "providerMeta" in app_response.text
    assert "waitForWorkflow" in app_response.text
    assert manifest_response.status_code == 200
    assert manifest_response.headers["content-type"].startswith("application/manifest+json")
    assert manifest_response.json()["start_url"] == "/mobile"
    assert sw_response.status_code == 200
    assert "praxis-mobile" in sw_response.text


def test_mobile_bootstrap_issue_is_host_only_and_db_backed(monkeypatch) -> None:
    class _FakeConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, sql: str, *args: object):
            self.calls.append((sql, args))
            if "INSERT INTO mobile_bootstrap_tokens" in sql:
                return [
                    {
                        "token_id": "00000000-0000-4000-8000-000000000010",
                        "principal_ref": args[0],
                        "token_hash": args[1],
                        "issued_at": datetime(2026, 4, 23, tzinfo=timezone.utc),
                        "expires_at": datetime(2026, 4, 23, 0, 10, tzinfo=timezone.utc),
                    }
                ]
            return []

    fake_conn = _FakeConn()
    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: fake_conn)

    with TestClient(rest.app, client=("127.0.0.1", 50000)) as client:
        response = client.post(
            "/api/mobile/bootstrap-token",
            json={"principal_ref": "operator:nate", "ttl_s": 300},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pairing_code"]
    assert payload["principal_ref"] == "operator:nate"
    assert fake_conn.calls
    assert "sha256:" in str(fake_conn.calls[0][1][1])

    with TestClient(rest.app, client=("192.0.2.10", 50000)) as client:
        denied = client.post("/api/mobile/bootstrap-token")

    assert denied.status_code == 403
    assert denied.json()["error_code"] == "mobile.bootstrap_issue_host_only"


def test_launcher_recover_endpoint_returns_structured_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        rest.launcher_handlers,
        "launcher_recover_payload",
        lambda *, action, service, run_id, open_browser: (
            200,
            {
                "ok": True,
                "action": action,
                "service": service,
                "run_id": run_id,
                "open_browser": open_browser,
            },
        ),
    )

    with TestClient(rest.app) as client:
        response = client.post(
            "/api/launcher/recover",
            json={"action": "restart_service", "service": "workflow-api"},
        )

    assert response.status_code == 200
    assert response.json()["action"] == "restart_service"
    assert response.json()["service"] == "workflow-api"


def test_service_lifecycle_catalog_http_routes_execute_handlers(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    def _handler(name: str):
        def _capture(command, _subsystems):
            calls.append((name, command))
            return {"status": name}

        return _capture

    monkeypatch.setattr(rest, "handle_register_runtime_target", _handler("target_registered"))
    monkeypatch.setattr(rest, "handle_list_runtime_targets", _handler("targets_listed"))
    monkeypatch.setattr(rest, "handle_register_service_definition", _handler("service_registered"))
    monkeypatch.setattr(rest, "handle_declare_service_desired_state", _handler("desired_state_declared"))
    monkeypatch.setattr(rest, "handle_record_service_lifecycle_event", _handler("event_recorded"))
    monkeypatch.setattr(rest, "handle_query_service_projection", _handler("projection_found"))

    with TestClient(rest.app) as client:
        responses = [
            client.post(
                "/api/service-lifecycle/targets",
                json={"runtime_target_ref": "target.alpha", "substrate_kind": "container"},
            ),
            client.get("/api/service-lifecycle/targets?workspace_ref=praxis&limit=5"),
            client.post(
                "/api/service-lifecycle/services",
                json={"service_ref": "service.api", "service_kind": "http_api"},
            ),
            client.post(
                "/api/service-lifecycle/desired-state",
                json={
                    "service_ref": "service.api",
                    "runtime_target_ref": "target.alpha",
                    "desired_status": "running",
                },
            ),
            client.post(
                "/api/service-lifecycle/events",
                json={
                    "service_ref": "service.api",
                    "runtime_target_ref": "target.alpha",
                    "event_type": "health_check_passed",
                },
            ),
            client.get("/api/service-lifecycle/projection/service.api/target.alpha"),
        ]

    assert [response.status_code for response in responses] == [200, 200, 200, 200, 200, 200]
    assert [response.json()["status"] for response in responses] == [
        "target_registered",
        "targets_listed",
        "service_registered",
        "desired_state_declared",
        "event_recorded",
        "projection_found",
    ]
    assert [name for name, _command in calls] == [
        "target_registered",
        "targets_listed",
        "service_registered",
        "desired_state_declared",
        "event_recorded",
        "projection_found",
    ]


def test_api_routes_endpoint_lists_the_live_http_surface() -> None:
    with TestClient(rest.app) as client:
        response = client.get("/api/routes")

    assert response.status_code == 200
    payload = response.json()

    assert payload["count"] == len(payload["routes"])
    assert payload["docs_url"] == "/docs"
    assert payload["openapi_url"] == "/openapi.json"
    assert payload["summary"]["route_count"] == payload["count"]
    assert isinstance(payload["summary"]["methods"], list)
    assert isinstance(payload["summary"]["tags"], list)

    route_paths = {row["path"] for row in payload["routes"]}
    assert "/v1/catalog" in route_paths
    assert "/v1/events" in route_paths
    assert "/v1/runs" in route_paths
    assert "/api/health" not in route_paths

    runs_route = next(row for row in payload["routes"] if row["path"] == "/v1/runs")
    assert runs_route["include_in_schema"] is True
    assert runs_route["visibility"] == "public"
    assert runs_route["operation_id"]


def test_api_routes_endpoint_filters_the_live_http_surface() -> None:
    with TestClient(rest.app) as client:
        response = client.get("/api/routes", params={"path_prefix": "/v1/catalog"})

    assert response.status_code == 200
    payload = response.json()

    assert payload["count"] == 1
    assert payload["filters"] == {"path_prefix": "/v1/catalog"}
    assert [row["path"] for row in payload["routes"]] == ["/v1/catalog"]


def test_api_routes_endpoint_can_include_internal_routes() -> None:
    with TestClient(rest.app) as client:
        response = client.get("/api/routes", params={"visibility": "all", "path_prefix": "/api"})

    assert response.status_code == 200
    payload = response.json()

    route_paths = {row["path"] for row in payload["routes"]}
    assert "/api/health" in route_paths
    assert "/api/routes" in route_paths
    routes_route = next(row for row in payload["routes"] if row["path"] == "/api/routes")
    assert routes_route["visibility"] == "internal"


def test_api_bugs_route_forwards_full_query_contract_through_http(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_handle_bugs(subsystems, body):
        captured["subsystems"] = subsystems
        captured["body"] = body
        return {"bugs": [], "count": 0, "returned_count": 0}

    fake_subsystems = object()
    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: fake_subsystems)
    monkeypatch.setattr(_query_bugs, "_handle_bugs", _fake_handle_bugs)

    with TestClient(rest.app) as client:
        response = client.get(
            "/api/bugs",
            params=[
                ("limit", "12"),
                ("status", "open"),
                ("severity", "p1"),
                ("category", "runtime"),
                ("title_like", "authority drift"),
                ("tags", "alpha"),
                ("tags", "beta"),
                ("exclude_tags", "legacy"),
                ("source_issue_id", "issue-123"),
                ("include_replay_state", "1"),
                ("replay_ready_only", "on"),
                ("open_only", "yes"),
            ],
        )

    assert response.status_code == 200
    assert response.json() == {"bugs": [], "count": 0, "returned_count": 0}
    assert captured["subsystems"] is fake_subsystems
    assert captured["body"] == {
        "action": "list",
        "limit": 12,
        "status": "open",
        "severity": "p1",
        "category": "runtime",
        "title_like": "authority drift",
        "tags": ("alpha", "beta"),
        "exclude_tags": ("legacy",),
        "source_issue_id": "issue-123",
        "include_replay_state": True,
        "replay_ready_only": True,
        "open_only": True,
    }


def test_api_handoff_routes_are_registered_on_the_fastapi_app() -> None:
    paths = {route.path for route in rest.app.routes if getattr(route, "path", None)}

    assert "/api/handoff/latest" in paths
    assert "/api/handoff/lineage" in paths
    assert "/api/handoff/status" in paths
    assert "/api/handoff/history" in paths


def test_run_status_authority_failure_surfaces_503(monkeypatch) -> None:
    def _boom(conn, run_id: str):
        raise RuntimeError("status authority offline")

    monkeypatch.setattr("runtime.workflow.unified.get_run_status", _boom)

    with pytest.raises(HTTPException) as exc_info:
        rest._load_run_jobs_from_status_authority(object(), "run-123")

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["reason_code"] == "run_detail.status_authority_query_failed"
    assert exc_info.value.detail["run_id"] == "run-123"
    assert exc_info.value.detail["error_message"] == "status authority offline"


def test_query_route_is_gone_from_rest_surface(monkeypatch) -> None:
    recorded: list[dict[str, Any]] = []
    fake_subsystems = object()

    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: fake_subsystems)
    monkeypatch.setattr(
        workflow_query,
        "_record_api_route_usage",
        lambda _subs, **kwargs: recorded.append(kwargs),
    )

    with TestClient(rest.app) as client:
        response = client.post("/query", json={"question": "status"})

    assert response.status_code == 404
    assert recorded == []


def test_surface_usage_metrics_endpoint_returns_serialized_rows(monkeypatch) -> None:
    surface_usage_mod._reset_surface_usage_recorder_health_for_tests()

    class _FakeRepo:
        def __init__(self, conn) -> None:
            assert conn == "surface-usage-pg"

        def list_usage_rollup(self, *, days: int, entrypoint_name: str | None = None):
            assert days == 14
            assert entrypoint_name == "/api/workflows"
            return [
                {
                    "surface_kind": "api",
                    "transport_kind": "http",
                    "entrypoint_kind": "route",
                    "entrypoint_name": "/api/workflows",
                    "caller_kind": "direct",
                    "http_method": "POST",
                    "invocation_count": 7,
                    "success_count": 6,
                    "client_error_count": 1,
                    "server_error_count": 0,
                    "first_invoked_at": datetime(2026, 4, 14, 8, 0, tzinfo=timezone.utc),
                    "last_invoked_at": datetime(2026, 4, 15, 9, 0, tzinfo=timezone.utc),
                }
            ]

        def list_usage_daily(self, *, days: int, entrypoint_name: str | None = None):
            assert days == 14
            assert entrypoint_name == "/api/workflows"
            return [
                {
                    "usage_date": datetime(2026, 4, 15, tzinfo=timezone.utc).date(),
                    "surface_kind": "api",
                    "transport_kind": "http",
                    "entrypoint_kind": "route",
                    "entrypoint_name": "/api/workflows",
                    "caller_kind": "direct",
                    "http_method": "POST",
                    "invocation_count": 3,
                    "success_count": 3,
                    "client_error_count": 0,
                    "server_error_count": 0,
                    "first_invoked_at": datetime(2026, 4, 15, 8, 0, tzinfo=timezone.utc),
                    "last_invoked_at": datetime(2026, 4, 15, 9, 0, tzinfo=timezone.utc),
                }
            ]

        def list_usage_events(self, *, days: int, entrypoint_name: str | None = None, limit: int = 50):
            assert days == 14
            assert entrypoint_name == "/api/workflows"
            assert limit == 5
            return [
                {
                    "event_id": 7,
                    "occurred_at": datetime(2026, 4, 15, 9, 0, tzinfo=timezone.utc),
                    "surface_kind": "api",
                    "transport_kind": "http",
                    "entrypoint_kind": "route",
                    "entrypoint_name": "/api/workflows",
                    "caller_kind": "direct",
                    "http_method": "POST",
                    "status_code": 200,
                    "result_state": "ok",
                    "reason_code": "",
                    "routed_to": "",
                    "workflow_id": "",
                    "run_id": "",
                    "job_label": "",
                    "request_id": "req-123",
                    "client_version": "moon-ui/1.2.3",
                    "payload_size_bytes": 120,
                    "response_size_bytes": 240,
                    "prose_chars": 19,
                    "query_chars": 0,
                    "result_count": 0,
                    "unresolved_count": 0,
                    "capability_count": 2,
                    "reference_count": 1,
                    "compiled_job_count": 0,
                    "trigger_count": 0,
                    "definition_hash": "defhash",
                    "definition_revision": "def_123",
                    "task_class": "research",
                    "planner_required": True,
                    "llm_used": False,
                    "has_current_plan": False,
                    "metadata": {"compile_index_ref": "compile_index.alpha"},
                }
            ]

        def summarize_query_routing(self, *, days: int):
            assert days == 14
            return [
                {
                    "entrypoint_name": "praxis_query",
                    "caller_kind": "workflow_session",
                    "routed_to": "knowledge_graph",
                    "result_state": "ok",
                    "reason_code": "",
                    "invocation_count": 4,
                    "success_count": 4,
                    "average_query_chars": 18.5,
                    "total_result_count": 12,
                }
            ]

        def summarize_builder_funnels(self, *, days: int):
            assert days == 14
            return {
                "definition_count": 3,
                "compile_lane_count": 3,
                "refine_lane_count": 1,
                "plan_count": 2,
                "commit_count": 2,
                "run_count": 1,
                "compile_to_plan_count": 2,
                "plan_to_commit_count": 2,
                "commit_to_run_count": 1,
                "full_path_count": 1,
            }

    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: "surface-usage-pg")
    monkeypatch.setattr(rest, "PostgresWorkflowSurfaceUsageRepository", _FakeRepo)

    with TestClient(rest.app) as client:
        response = client.get(
            "/api/metrics/surface-usage",
            params={"days": 14, "entrypoint": "/api/workflows", "event_limit": 5},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["authority_ready"] is True
    assert payload["observability_state"] == "ready"
    assert payload["recorder_health"]["authority_ready"] is True
    assert payload["filters"] == {"entrypoint": "/api/workflows", "event_limit": 5}
    assert payload["totals"] == {
        "entry_count": 1,
        "invocation_count": 7,
        "success_count": 6,
        "client_error_count": 1,
        "server_error_count": 0,
        "event_count": 1,
    }
    assert payload["entries"][0]["first_invoked_at"] == "2026-04-14T08:00:00+00:00"
    assert payload["daily"][0]["usage_date"] == "2026-04-15"
    assert payload["recent_events"][0]["request_id"] == "req-123"
    assert payload["query_routing_quality"][0]["total_result_count"] == 12
    assert payload["builder_funnels"]["full_path_count"] == 1


def test_surface_usage_metrics_endpoint_reports_db_authority_failure(monkeypatch) -> None:
    surface_usage_mod._reset_surface_usage_recorder_health_for_tests()

    class _FailingRepo:
        def __init__(self, _conn) -> None:
            pass

        def list_usage_rollup(self, **_kwargs):
            raise RuntimeError("surface usage db offline")

    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: "surface-usage-pg")
    monkeypatch.setattr(rest, "PostgresWorkflowSurfaceUsageRepository", _FailingRepo)

    with TestClient(rest.app) as client:
        response = client.get("/api/metrics/surface-usage")

    assert response.status_code == 200
    payload = response.json()
    assert payload["authority_ready"] is False
    assert payload["observability_state"] == "degraded"
    assert payload["reason_code"] == "surface_usage.authority_unavailable"
    assert payload["error"] == "RuntimeError: surface usage db offline"
    assert payload["totals"]["invocation_count"] == 0
    assert payload["entries"] == []


def test_reviews_endpoint_reports_authority_failure(monkeypatch) -> None:
    import runtime.review_tracker as review_tracker_mod

    def _fail_tracker():
        raise RuntimeError("review db unavailable")

    monkeypatch.setattr(review_tracker_mod, "get_review_tracker", _fail_tracker)

    payload = rest.get_reviews()

    assert payload["authority_ready"] is False
    assert payload["observability_state"] == "degraded"
    assert payload["reason_code"] == "review_tracker.authority_unavailable"
    assert payload["error"] == "RuntimeError: review db unavailable"
    assert payload["authors"] == []
    assert payload["total_reviews"] == 0


def test_costs_endpoint_reports_authority_failure(monkeypatch) -> None:
    import runtime.cost_tracker as cost_tracker_mod

    class _FailingCostTracker:
        def summary(self):
            return {
                "record_count": 0,
                "total_cost_usd": 0.0,
                "by_provider": {},
                "authority_ready": False,
                "observability_state": "degraded",
                "reason_code": "cost_tracker.authority_unavailable",
                "error": "RuntimeError: cost db unavailable",
            }

    monkeypatch.setattr(cost_tracker_mod, "get_cost_tracker", lambda: _FailingCostTracker())

    payload = rest.get_costs()

    assert payload["authority_ready"] is False
    assert payload["observability_state"] == "degraded"
    assert payload["reason_code"] == "cost_tracker.authority_unavailable"
    assert payload["error"] == "RuntimeError: cost db unavailable"
    assert payload["record_count"] == 0
    assert payload["total_cost_usd"] == 0.0


def test_trust_endpoint_rebuilds_from_receipt_authority(monkeypatch) -> None:
    import runtime.trust_scoring as trust_scoring_mod

    class _FakeScorer:
        def __init__(self) -> None:
            self.rebuilt = False

        def compute_from_receipts(self) -> None:
            self.rebuilt = True

        def all_scores(self):
            assert self.rebuilt is True
            return []

    scorer = _FakeScorer()
    monkeypatch.setattr(trust_scoring_mod, "get_trust_scorer", lambda: scorer)

    assert rest.get_trust() == []


def test_launcher_app_serves_index_from_dist(monkeypatch, tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    (dist_dir / "index.html").write_text("<!doctype html><html><body><div id='root'></div></body></html>", encoding="utf-8")
    monkeypatch.setattr(rest, "_APP_DIST_DIR", dist_dir)

    with TestClient(rest.app) as client:
        response = client.get("/app")

    assert response.status_code == 200
    assert "root" in response.text


def test_legacy_ui_routes_are_absent() -> None:
    with TestClient(rest.app) as client:
        response = client.get("/ui", follow_redirects=False)

    assert response.status_code == 404


def test_legacy_atlas_artifact_route_redirects_to_live_app() -> None:
    with TestClient(rest.app) as client:
        response = client.get("/api/atlas.html", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/app/atlas"
    assert "no-store" in response.headers["cache-control"]


def test_launcher_app_reports_missing_build(monkeypatch, tmp_path: Path) -> None:
    dist_dir = tmp_path / "missing-dist"
    monkeypatch.setattr(rest, "_APP_DIST_DIR", dist_dir)

    with TestClient(rest.app) as client:
        response = client.get("/app")

    assert response.status_code == 503
    payload = response.json()
    assert payload["error"] == "launcher_build_missing"
    assert payload["launch_url"] is None


def test_launcher_app_reports_unreadable_build(monkeypatch, tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    index_path = dist_dir / "index.html"
    index_path.write_text("<!doctype html><html><body><div id='root'></div></body></html>", encoding="utf-8")
    monkeypatch.setattr(rest, "_APP_DIST_DIR", dist_dir)

    original_read_text = Path.read_text

    def _raise_for_index(path: Path, *args: Any, **kwargs: Any) -> str:
        if path == index_path:
            raise OSError(23, "Too many open files in system")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _raise_for_index)

    with TestClient(rest.app) as client:
        response = client.get("/app")

    assert response.status_code == 503
    payload = response.json()
    assert payload["error"] == "launcher_build_unreadable"
    assert payload["error_type"] == "OSError"
    assert payload["launch_url"] is None
    assert "Too many open files" in payload["error_message"]


class _FakeRunConn:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []

    @staticmethod
    def _normalize(query: str) -> str:
        return " ".join(query.split())

    def execute(self, query: str, *params: Any):
        self.execute_calls.append((query, params))
        normalized = self._normalize(query)

        if normalized == "SELECT 1":
            return [{"ok": 1}]

        if normalized.startswith("SELECT r.run_id,"):
            if "GROUP BY r.run_id, r.workflow_id, r.request_envelope, r.current_state, r.requested_at, r.finished_at" in normalized:
                if params == ("run-2",):
                    return [
                        {
                            "run_id": "run-2",
                            "workflow_id": "wf_spec_two",
                            "spec_name": "Spec Two",
                            "status": "running",
                            "total_jobs": 4,
                            "created_at": datetime(2026, 4, 11, 12, 0, tzinfo=timezone.utc),
                            "finished_at": None,
                            "completed_jobs": 2,
                            "total_cost": 12.75,
                            "total_duration_ms": 2200,
                        }
                    ]
                if params == (2,):
                    return [
                        {
                            "run_id": "run-2",
                            "workflow_id": "wf_spec_two",
                            "spec_name": "Spec Two",
                            "status": "running",
                            "total_jobs": 4,
                            "created_at": datetime(2026, 4, 11, 12, 0, tzinfo=timezone.utc),
                            "finished_at": None,
                            "completed_jobs": 2,
                            "total_cost": 12.75,
                            "total_duration_ms": 2200,
                        }
                    ]
                return [
                    {
                        "run_id": "run-1",
                        "workflow_id": "wf_spec_one",
                        "spec_name": "Spec One",
                        "status": "queued",
                        "total_jobs": 3,
                        "created_at": datetime(2026, 4, 11, 11, 30, tzinfo=timezone.utc),
                        "finished_at": None,
                        "completed_jobs": 1,
                        "total_cost": 3.5,
                        "total_duration_ms": 1000,
                    }
                ]

        if normalized.startswith("SELECT id, label, status, job_type, phase, agent_slug, resolved_agent,"):
            if params == ("run-2",):
                return [
                    {
                        "id": 21,
                        "label": "prepare",
                        "status": "succeeded",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "agent.prepare",
                        "resolved_agent": "agent.prepare",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 1200,
                        "cost_usd": 1.25,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "prepared",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 12, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 12, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 12, 1, tzinfo=timezone.utc),
                    },
                    {
                        "id": 22,
                        "label": "run",
                        "status": "running",
                        "job_type": "workflow",
                        "phase": "execute",
                        "agent_slug": "agent.run",
                        "resolved_agent": "agent.run",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 0,
                        "cost_usd": 0.0,
                        "exit_code": None,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 12, 1, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 12, 1, tzinfo=timezone.utc),
                        "finished_at": None,
                    },
                ]
            if params == ("run-graph",):
                return [
                    {
                        "id": 31,
                        "label": "seed",
                        "status": "succeeded",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "deterministic_task",
                        "resolved_agent": "deterministic_task",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 10,
                        "cost_usd": 0.0,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "{\"go\": true}",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                    },
                    {
                        "id": 32,
                        "label": "gate",
                        "status": "succeeded",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "control_operator",
                        "resolved_agent": "control_operator",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 5,
                        "cost_usd": 0.0,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                    },
                    {
                        "id": 33,
                        "label": "gate__then__then_path",
                        "status": "succeeded",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "deterministic_task",
                        "resolved_agent": "deterministic_task",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 5,
                        "cost_usd": 0.0,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                    },
                    {
                        "id": 34,
                        "label": "gate__else__else_path",
                        "status": "skipped",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "deterministic_task",
                        "resolved_agent": "deterministic_task",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 0,
                        "cost_usd": 0.0,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                    },
                    {
                        "id": 35,
                        "label": "after_gate",
                        "status": "succeeded",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "deterministic_task",
                        "resolved_agent": "deterministic_task",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 5,
                        "cost_usd": 0.0,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                    },
                    {
                        "id": 36,
                        "label": "route_mode",
                        "status": "succeeded",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "control_operator",
                        "resolved_agent": "control_operator",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 5,
                        "cost_usd": 0.0,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                    },
                    {
                        "id": 37,
                        "label": "route_mode__manual__manual_review",
                        "status": "succeeded",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "deterministic_task",
                        "resolved_agent": "deterministic_task",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 5,
                        "cost_usd": 0.0,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                    },
                    {
                        "id": 38,
                        "label": "route_mode__auto__auto_path",
                        "status": "skipped",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "deterministic_task",
                        "resolved_agent": "deterministic_task",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 0,
                        "cost_usd": 0.0,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                    },
                    {
                        "id": 39,
                        "label": "after_route",
                        "status": "succeeded",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "deterministic_task",
                        "resolved_agent": "deterministic_task",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 5,
                        "cost_usd": 0.0,
                        "exit_code": 0,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                    },
                    {
                        "id": 40,
                        "label": "loop_checks",
                        "status": "running",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "control_operator",
                        "resolved_agent": "control_operator",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 0,
                        "cost_usd": 0.0,
                        "exit_code": None,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "finished_at": None,
                    },
                    {
                        "id": 41,
                        "label": "finalize",
                        "status": "pending",
                        "job_type": "workflow",
                        "phase": "build",
                        "agent_slug": "deterministic_task",
                        "resolved_agent": "deterministic_task",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 0,
                        "duration_ms": 0,
                        "cost_usd": 0.0,
                        "exit_code": None,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "created_at": datetime(2026, 4, 11, 13, 0, tzinfo=timezone.utc),
                        "started_at": None,
                        "finished_at": None,
                    },
                ]
            return [
                {
                    "id": 11,
                    "label": "step-1",
                    "status": "succeeded",
                    "job_type": "workflow",
                    "phase": "build",
                    "agent_slug": "agent.step-1",
                    "resolved_agent": "agent.step-1",
                    "integration_id": None,
                    "integration_action": None,
                    "integration_args": None,
                    "attempt": 1,
                    "duration_ms": 1000,
                    "cost_usd": 1.25,
                    "exit_code": 0,
                    "last_error_code": None,
                    "stdout_preview": "hello",
                    "output_path": None,
                    "receipt_id": "receipt-11",
                    "created_at": datetime(2026, 4, 11, 11, 30, tzinfo=timezone.utc),
                    "started_at": datetime(2026, 4, 11, 11, 31, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 4, 11, 11, 32, tzinfo=timezone.utc),
                }
            ]

        if normalized.startswith("SELECT COALESCE(SUM(cost_usd),0) as total_cost"):
            if params == ("run-2",):
                return [
                    {
                        "total_cost": 12.75,
                        "total_tokens_in": 100,
                        "total_tokens_out": 200,
                        "total_duration_ms": 2200,
                    }
                ]
            return [
                {
                    "total_cost": 3.5,
                    "total_tokens_in": 50,
                    "total_tokens_out": 75,
                    "total_duration_ms": 1000,
                    }
                ]

        if normalized == "SELECT count(*) as cnt FROM workflow_jobs WHERE heartbeat_at > now() - interval '5 minutes'":
            return [{"cnt": 2}]

        if normalized == "SELECT count(*) as cnt FROM workflow_jobs WHERE claimed_at > now() - interval '10 minutes'":
            return [{"cnt": 1}]

        if normalized == "SELECT count(*) as cnt FROM workflow_jobs WHERE status = 'ready'":
            return [{"cnt": 0}]

        if normalized.startswith(
            "SELECT count(*) as total, count(*) FILTER (WHERE status = 'succeeded') as passed, count(*) FILTER (WHERE status IN ('failed', 'dead_letter')) as failed FROM workflow_jobs WHERE created_at > now() - interval '24 hours'"
        ):
            return [{"total": 2, "passed": 2, "failed": 0}]

        if normalized.startswith("SELECT s.job_label, s.summary FROM workflow_job_submissions s"):
            return []

        if normalized == "SELECT request_envelope->'spec_snapshot' AS spec_snapshot FROM workflow_runs WHERE run_id = $1":
            if params == ("run-graph",):
                return [
                    {
                        "spec_snapshot": {
                            "name": "Graph Demo",
                            "workflow_id": "graph_demo",
                            "workspace_ref": "praxis",
                            "runtime_profile_ref": "praxis",
                            "jobs": [
                                {
                                    "label": "seed",
                                    "expected_outputs": {
                                        "go": True,
                                        "mode": "manual",
                                        "items": ["lint", "smoke", "ui"],
                                    },
                                },
                                {
                                    "label": "gate",
                                    "adapter_type": "control_operator",
                                    "depends_on": ["seed"],
                                    "operator": {
                                        "kind": "if",
                                        "predicate": {"field": "go", "op": "equals", "value": True},
                                    },
                                    "branches": {
                                        "then": [{"label": "then_path", "expected_outputs": {"path": "then"}}],
                                        "else": [{"label": "else_path", "expected_outputs": {"path": "else"}}],
                                    },
                                },
                                {
                                    "label": "after_gate",
                                    "depends_on": ["gate"],
                                    "expected_outputs": {"after_gate": True},
                                },
                                {
                                    "label": "route_mode",
                                    "adapter_type": "control_operator",
                                    "depends_on": ["after_gate"],
                                    "operator": {
                                        "kind": "switch",
                                        "field": "mode",
                                        "cases": [
                                            {"branch": "manual", "value": "manual"},
                                            {"branch": "auto", "value": "auto"},
                                        ],
                                    },
                                    "branches": {
                                        "manual": [{"label": "manual_review", "expected_outputs": {"selected": "manual"}}],
                                        "auto": [{"label": "auto_path", "expected_outputs": {"selected": "auto"}}],
                                    },
                                },
                                {
                                    "label": "after_route",
                                    "depends_on": ["route_mode"],
                                    "expected_outputs": {"after_route": True},
                                },
                                {
                                    "label": "loop_checks",
                                    "adapter_type": "control_operator",
                                    "depends_on": ["after_route"],
                                    "operator": {
                                        "kind": "foreach",
                                        "source_ref": {"from_node_id": "seed", "output_key": "items"},
                                        "max_items": 10,
                                        "max_parallel": 2,
                                        "aggregate_mode": "ordered_results",
                                        "result_key": "results",
                                    },
                                    "template_jobs": [{"label": "run_check", "expected_outputs": {"ok": True}}],
                                },
                                {
                                    "label": "finalize",
                                    "depends_on": ["loop_checks"],
                                    "expected_outputs": {"done": True},
                                },
                            ],
                        }
                    }
                ]
            return []

        if normalized == "SELECT operator_frame_id, node_id, frame_state FROM run_operator_frames WHERE run_id = $1 ORDER BY node_id, operator_frame_id":
            if params == ("run-graph",):
                return [
                    {"operator_frame_id": "frame-1", "node_id": "loop_checks", "frame_state": "succeeded"},
                    {"operator_frame_id": "frame-2", "node_id": "loop_checks", "frame_state": "succeeded"},
                    {"operator_frame_id": "frame-3", "node_id": "loop_checks", "frame_state": "running"},
                ]
            return []

        if normalized.startswith("SELECT id, run_id, label, status, job_type, phase, agent_slug, resolved_agent,"):
            if params == ("run-2", 22):
                return [
                    {
                        "id": 22,
                        "run_id": "run-2",
                        "label": "run",
                        "status": "running",
                        "job_type": "workflow",
                        "phase": "execute",
                        "agent_slug": "agent.run",
                        "resolved_agent": "agent.run",
                        "integration_id": None,
                        "integration_action": None,
                        "integration_args": None,
                        "attempt": 1,
                        "duration_ms": 0,
                        "cost_usd": 0.0,
                        "exit_code": None,
                        "last_error_code": None,
                        "stdout_preview": "",
                        "output_path": None,
                        "receipt_id": "receipt-22",
                        "created_at": datetime(2026, 4, 11, 12, 1, tzinfo=timezone.utc),
                        "started_at": datetime(2026, 4, 11, 12, 1, tzinfo=timezone.utc),
                        "finished_at": None,
                    }
                ]
            return []

        raise AssertionError(f"unexpected query: {query}")


class _FakeRunSubsystems:
    def __init__(self, conn: _FakeRunConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _FakeRunConn:
        return self._conn


def test_run_routes_use_shared_pg_authority_and_match_contract(monkeypatch) -> None:
    conn = _FakeRunConn()
    subsystems = _FakeRunSubsystems(conn)
    captured_health_inputs: list[dict[str, Any]] = []

    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: subsystems)
    monkeypatch.setattr(
        rest,
        "_health_db_snapshot",
        lambda: (
            [
                {"name": "postgres", "ok": True},
                {"name": "worker", "ok": True, "active_jobs": 0, "ready_jobs": 0},
                {"name": "workflow", "ok": True, "total": 1, "passed": 1, "failed": 0, "pass_rate": 1.0},
            ],
            "healthy",
        ),
    )
    monkeypatch.setattr(
        rest,
        "summarize_run_health",
        lambda run_data, now: (
            captured_health_inputs.append(run_data)
            or {
                "state": "healthy",
                "likely_failed": False,
                "signals": [],
                "elapsed_seconds": 42.1,
                "completed_jobs": 2,
                "running_or_claimed": 1,
                "terminal_jobs": 1,
                "resource_telemetry": {
                    "seconds_since_last_activity": 12.3,
                },
                "non_retryable_failed_jobs": [],
            }
        ),
    )

    with TestClient(rest.app) as client:
        recent_response = client.get("/api/runs/recent?limit=2")
        detail_response = client.get("/api/runs/run-2")
        job_response = client.get("/api/runs/run-2/jobs/22")
        health_response = client.get("/api/health")

    assert recent_response.status_code == 200
    assert recent_response.json() == [
        {
            "run_id": "run-2",
            "workflow_id": "wf_spec_two",
            "spec_name": "Spec Two",
            "status": "running",
            "total_jobs": 4,
            "completed_jobs": 2,
            "total_cost": 12.75,
            "created_at": "2026-04-11T12:00:00+00:00",
            "finished_at": None,
        }
    ]

    assert detail_response.status_code == 200
    assert detail_response.json()["run_id"] == "run-2"
    assert detail_response.json()["workflow_id"] == "wf_spec_two"
    assert detail_response.json()["spec_name"] == "Spec Two"
    assert detail_response.json()["total_jobs"] == 4
    assert detail_response.json()["completed_jobs"] == 2
    assert detail_response.json()["total_cost"] == 12.75
    assert detail_response.json()["total_duration_ms"] == 2200
    assert detail_response.json()["jobs"][0]["has_output"] is True
    assert detail_response.json()["jobs"][0]["created_at"] == "2026-04-11T12:00:00+00:00"
    assert detail_response.json()["summary"] == "All 2 steps completed successfully."
    assert detail_response.json()["health"] == {
        "state": "healthy",
        "likely_failed": False,
        "signals": [],
        "elapsed_seconds": 42.1,
        "completed_jobs": 2,
        "running_or_claimed": 1,
        "terminal_jobs": 1,
        "resource_telemetry": {
            "seconds_since_last_activity": 12.3,
        },
        "non_retryable_failed_jobs": [],
    }
    assert captured_health_inputs and captured_health_inputs[0]["run_id"] == "run-2"
    assert len(captured_health_inputs[0]["jobs"]) == 2

    assert job_response.status_code == 200
    assert job_response.json()["id"] == 22
    assert job_response.json()["receipt_id"] == "receipt-22"
    assert job_response.json()["output"] == ""
    assert job_response.json()["output_source"] == "preview"

    assert health_response.status_code == 200
    assert health_response.json()["status"] == "healthy"


def test_run_detail_graph_shows_control_gates_and_operator_frames(monkeypatch) -> None:
    conn = _FakeRunConn()
    subsystems = _FakeRunSubsystems(conn)

    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: subsystems)

    with TestClient(rest.app) as client:
        response = client.get("/api/runs/run-graph")

    assert response.status_code == 200
    graph = response.json()["graph"]
    nodes = {node["id"]: node for node in graph["nodes"]}
    edges = {edge["id"]: edge for edge in graph["edges"]}

    assert "gate" in nodes
    assert "route_mode" in nodes
    assert nodes["loop_checks"]["loop"] == {
        "count": 3,
        "succeeded": 2,
        "failed": 0,
        "running": 1,
    }
    assert nodes["gate__then__then_path"]["status"] == "succeeded"
    assert nodes["gate__else__else_path"]["status"] == "skipped"
    assert nodes["route_mode__manual__manual_review"]["status"] == "succeeded"
    assert nodes["route_mode__auto__auto_path"]["status"] == "skipped"
    assert edges["edge_4"]["type"] == "after_success"
    assert edges["edge_0"]["type"] == "conditional"
    assert edges["edge_0"]["condition"] == {
        "field": "go",
        "op": "equals",
        "value": True,
    }
    assert edges["edge_1"]["condition"] == {
        "field": "go",
        "op": "not_equals",
        "value": True,
    }
    assert edges["edge_2"]["condition"] == {
        "field": "mode",
        "op": "equals",
        "value": "manual",
    }


def test_build_run_graph_projects_completion_contract_from_spec_snapshot() -> None:
    class _Conn:
        def execute(self, query: str, *params: object) -> list[dict[str, object]]:
            normalized = " ".join(query.split())
            if normalized == "SELECT request_envelope->'spec_snapshot' AS spec_snapshot FROM workflow_runs WHERE run_id = $1":
                assert params == ("run-contract",)
                return [
                    {
                        "spec_snapshot": {
                            "jobs": [
                                {
                                    "label": "enter_data",
                                    "agent": "openai/gpt-5.4",
                                    "task_type": "data_entry",
                                    "description": "Enter applicant data into the target system.",
                                    "outcome_goal": "Applicant record is populated in the CRM.",
                                    "prompt": "Open the CRM tool and enter the supplied applicant data.",
                                    "completion_contract": {
                                        "result_kind": "artifact_bundle",
                                        "submit_tool_names": ["praxis_submit_artifact_bundle"],
                                        "submission_required": True,
                                        "verification_required": False,
                                    },
                                },
                                {
                                    "label": "review",
                                    "depends_on": ["enter_data"],
                                    "agent": "openai/gpt-5.4",
                                },
                            ]
                        }
                    }
                ]
            raise AssertionError(f"unexpected query: {query}")

    jobs = [
        {
            "label": "enter_data",
            "status": "succeeded",
            "cost_usd": 0.25,
            "duration_ms": 1200,
            "resolved_agent": "openai/gpt-5.4",
            "agent_slug": "openai/gpt-5.4",
            "attempt": 1,
            "last_error_code": None,
        },
        {
            "label": "review",
            "status": "pending",
            "cost_usd": 0,
            "duration_ms": 0,
            "resolved_agent": "openai/gpt-5.4",
            "agent_slug": "openai/gpt-5.4",
            "attempt": 0,
            "last_error_code": None,
        },
    ]

    graph = rest._build_run_graph(_Conn(), "run-contract", jobs)

    assert graph is not None
    nodes = {node["id"]: node for node in graph["nodes"]}
    assert nodes["enter_data"]["task_type"] == "data_entry"
    assert nodes["enter_data"]["outcome_goal"] == "Applicant record is populated in the CRM."
    assert nodes["enter_data"]["prompt"] == "Open the CRM tool and enter the supplied applicant data."
    assert nodes["enter_data"]["completion_contract"] == {
        "result_kind": "artifact_bundle",
        "submit_tool_names": ["praxis_submit_artifact_bundle"],
        "submission_required": True,
        "verification_required": False,
    }


def test_rest_openapi_operation_ids_are_unique() -> None:
    rest.app.openapi_schema = None
    schema = rest.app.openapi()

    operation_ids: list[str] = []
    for path_item in schema.get("paths", {}).values():
        if not isinstance(path_item, dict):
            continue
        for operation in path_item.values():
            if isinstance(operation, dict) and "operationId" in operation:
                operation_ids.append(str(operation["operationId"]))

    assert operation_ids
    assert len(operation_ids) == len(set(operation_ids))


def test_rest_openapi_only_exposes_public_v1_contract() -> None:
    rest.app.openapi_schema = None
    schema = rest.app.openapi()

    paths = set(schema.get("paths", {}).keys())
    assert "/v1/runs" in paths
    assert "/v1/catalog" in paths
    assert "/api/health" not in paths
    assert "/api/routes" not in paths


def test_rest_legacy_api_bridge_routes_unknown_api_paths_to_handler(monkeypatch) -> None:
    seen: list[tuple[str, str]] = []

    async def _fake_dispatch(request):
        seen.append((request.method, request.url.path))
        return rest.JSONResponse(
            status_code=200,
            content={"ok": True, "path": request.url.path, "method": request.method},
        )

    monkeypatch.setattr(rest, "_route_to_handler", _fake_dispatch)

    with TestClient(rest.app) as client:
        get_response = client.get("/api/models")
        post_response = client.post("/api/workflows", json={"goal": "test"})

    assert get_response.status_code == 200
    assert get_response.json() == {"ok": True, "path": "/api/models", "method": "GET"}
    assert post_response.status_code == 200
    assert post_response.json() == {"ok": True, "path": "/api/workflows", "method": "POST"}
    assert seen == [("GET", "/api/models"), ("POST", "/api/workflows")]


def test_dashboard_route_delegates_to_handler_bridge(monkeypatch) -> None:
    seen: list[tuple[str, str]] = []

    async def _fake_dispatch(request):
        seen.append((request.method, request.url.path))
        return rest.JSONResponse(
            status_code=200,
            content={
                "generated_at": "2026-04-14T12:00:00+00:00",
                "summary": {"workflow_counts": {"total": 0, "live": 0, "saved": 0, "draft": 0}},
            },
        )

    monkeypatch.setattr(rest, "_route_to_handler", _fake_dispatch)

    with TestClient(rest.app) as client:
        response = client.get("/api/dashboard")

    assert response.status_code == 200
    assert response.json()["summary"]["workflow_counts"] == {"total": 0, "live": 0, "saved": 0, "draft": 0}
    assert seen == [("GET", "/api/dashboard")]
