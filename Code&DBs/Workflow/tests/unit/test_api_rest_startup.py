"""Startup wiring tests for the FastAPI REST surface."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import surfaces.api.rest as rest
from surfaces.api import handlers as api_handlers


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


def test_launcher_status_endpoint_delegates_to_handler(monkeypatch) -> None:
    expected = {
        "ok": True,
        "ready": False,
        "platform_state": "degraded",
        "launch_url": "http://127.0.0.1:8420/app",
        "dashboard_url": "http://127.0.0.1:8420/app",
        "api_docs_url": "http://127.0.0.1:8420/docs",
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


def test_query_route_records_surface_usage(monkeypatch) -> None:
    recorded: list[dict[str, Any]] = []
    fake_subsystems = object()

    monkeypatch.setattr(rest, "_ensure_shared_subsystems", lambda _app: fake_subsystems)
    monkeypatch.setitem(
        api_handlers.ROUTES,
        "/query",
        lambda _subs, body: {"ok": True, "question": body["question"]},
    )
    monkeypatch.setattr(
        rest,
        "_record_api_route_usage",
        lambda _subs, **kwargs: recorded.append(kwargs),
    )

    with TestClient(rest.app) as client:
        response = client.post("/query", json={"question": "status"})

    assert response.status_code == 200
    assert response.json() == {"ok": True, "question": "status"}
    assert len(recorded) == 1
    assert recorded[0]["path"] == "/query"
    assert recorded[0]["method"] == "POST"
    assert recorded[0]["status_code"] == 200
    assert recorded[0]["request_body"] == {"question": "status"}
    assert recorded[0]["response_payload"] == {"ok": True, "question": "status"}
    assert recorded[0]["headers"] is not None


def test_surface_usage_metrics_endpoint_returns_serialized_rows(monkeypatch) -> None:
    class _FakeRepo:
        def __init__(self, conn) -> None:
            assert conn == "surface-usage-pg"

        def list_usage_rollup(self, *, days: int, entrypoint_name: str | None = None):
            assert days == 14
            assert entrypoint_name == "/api/compile"
            return [
                {
                    "surface_kind": "api",
                    "transport_kind": "http",
                    "entrypoint_kind": "route",
                    "entrypoint_name": "/api/compile",
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
            assert entrypoint_name == "/api/compile"
            return [
                {
                    "usage_date": datetime(2026, 4, 15, tzinfo=timezone.utc).date(),
                    "surface_kind": "api",
                    "transport_kind": "http",
                    "entrypoint_kind": "route",
                    "entrypoint_name": "/api/compile",
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
            assert entrypoint_name == "/api/compile"
            assert limit == 5
            return [
                {
                    "event_id": 7,
                    "occurred_at": datetime(2026, 4, 15, 9, 0, tzinfo=timezone.utc),
                    "surface_kind": "api",
                    "transport_kind": "http",
                    "entrypoint_kind": "route",
                    "entrypoint_name": "/api/compile",
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
            params={"days": 14, "entrypoint": "/api/compile", "event_limit": 5},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"] == {"entrypoint": "/api/compile", "event_limit": 5}
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


def test_launcher_app_serves_index_from_dist(monkeypatch, tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    (dist_dir / "index.html").write_text("<!doctype html><html><body><div id='root'></div></body></html>", encoding="utf-8")
    monkeypatch.setattr(rest, "_APP_DIST_DIR", dist_dir)

    with TestClient(rest.app) as client:
        response = client.get("/app")

    assert response.status_code == 200
    assert "root" in response.text


def test_legacy_ui_redirects_to_launcher_app() -> None:
    with TestClient(rest.app) as client:
        response = client.get("/ui", follow_redirects=False)

    assert response.status_code in {307, 308}
    assert response.headers["location"] == "/app"


def test_launcher_app_reports_missing_build(monkeypatch, tmp_path: Path) -> None:
    dist_dir = tmp_path / "missing-dist"
    monkeypatch.setattr(rest, "_APP_DIST_DIR", dist_dir)

    with TestClient(rest.app) as client:
        response = client.get("/app")

    assert response.status_code == 503
    assert response.json()["error"] == "launcher_build_missing"


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
    assert any(query.strip() == "SELECT 1" for query, _params in conn.execute_calls)


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
    assert nodes["loop_checks"]["fan_out"] == {
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
        post_response = client.post("/api/compile", json={"goal": "test"})

    assert get_response.status_code == 200
    assert get_response.json() == {"ok": True, "path": "/api/models", "method": "GET"}
    assert post_response.status_code == 200
    assert post_response.json() == {"ok": True, "path": "/api/compile", "method": "POST"}
    assert seen == [("GET", "/api/models"), ("POST", "/api/compile")]


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
