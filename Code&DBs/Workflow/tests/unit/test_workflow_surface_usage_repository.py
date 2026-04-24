from __future__ import annotations

from datetime import date, datetime, timezone

import storage.postgres as postgres_mod
import surfaces.api.handlers._surface_usage as surface_usage_mod
from surfaces.api.handlers._surface_usage import record_api_route_usage
from storage.postgres.workflow_surface_usage_repository import (
    PostgresWorkflowSurfaceUsageRepository,
)


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        self.calls.append((normalized, args))
        if normalized.startswith("SELECT surface_kind, transport_kind, entrypoint_kind, entrypoint_name"):
            return [
                {
                    "surface_kind": "api",
                    "transport_kind": "http",
                    "entrypoint_kind": "route",
                    "entrypoint_name": "/api/compile",
                    "caller_kind": "direct",
                    "http_method": "POST",
                    "invocation_count": 3,
                    "success_count": 2,
                    "client_error_count": 1,
                    "server_error_count": 0,
                    "first_invoked_at": datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
                    "last_invoked_at": datetime(2026, 4, 15, 9, 30, tzinfo=timezone.utc),
                }
            ]
        if normalized.startswith("SELECT usage_date, surface_kind, transport_kind, entrypoint_kind, entrypoint_name"):
            return [
                {
                    "usage_date": date(2026, 4, 15),
                    "surface_kind": "mcp",
                    "transport_kind": "mcp",
                    "entrypoint_kind": "tool",
                    "entrypoint_name": "praxis_query",
                    "caller_kind": "workflow_session",
                    "http_method": "",
                    "invocation_count": 5,
                    "success_count": 4,
                    "client_error_count": 1,
                    "server_error_count": 0,
                    "first_invoked_at": datetime(2026, 4, 15, 8, 0, tzinfo=timezone.utc),
                    "last_invoked_at": datetime(2026, 4, 15, 9, 0, tzinfo=timezone.utc),
                }
            ]
        if normalized.startswith("SELECT event_id, occurred_at, surface_kind"):
            return [
                {
                    "event_id": 7,
                    "occurred_at": datetime(2026, 4, 15, 9, 5, tzinfo=timezone.utc),
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
        if "FROM workflow_surface_usage_events" in normalized and "entrypoint_name IN ('/query', 'praxis_query')" in normalized:
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
        if normalized.startswith("WITH stage_by_definition AS"):
            return [
                {
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
            ]
        return []


def test_surface_usage_repository_records_one_invocation() -> None:
    conn = _FakeConn()
    repo = PostgresWorkflowSurfaceUsageRepository(conn)

    repo.record_invocation(
        surface_kind="api",
        transport_kind="http",
        entrypoint_kind="route",
        entrypoint_name="/api/compile",
        caller_kind="direct",
        http_method="POST",
        status_code=409,
        occurred_at=datetime(2026, 4, 15, 8, 15, tzinfo=timezone.utc),
    )

    assert len(conn.calls) == 1
    sql, args = conn.calls[0]
    assert sql.startswith("INSERT INTO workflow_surface_usage_daily (")
    assert "ON CONFLICT (" in sql
    assert args[0] == date(2026, 4, 15)
    assert args[1] == "api"
    assert args[4] == "/api/compile"
    assert args[7] == 0
    assert args[8] == 1
    assert args[9] == 0


def test_surface_usage_repository_records_event_and_daily_rollup() -> None:
    conn = _FakeConn()
    repo = PostgresWorkflowSurfaceUsageRepository(conn)

    repo.record_event(
        surface_kind="api",
        transport_kind="http",
        entrypoint_kind="route",
        entrypoint_name="/api/compile",
        caller_kind="direct",
        http_method="POST",
        status_code=200,
        result_state="ok",
        request_id="req-123",
        client_version="moon-ui/1.2.3",
        prose_chars=19,
        capability_count=2,
        reference_count=1,
        definition_hash="defhash",
        definition_revision="def_123",
        task_class="research",
        planner_required=True,
        metadata={"compile_index_ref": "compile_index.alpha"},
        occurred_at=datetime(2026, 4, 15, 9, 5, tzinfo=timezone.utc),
    )

    assert len(conn.calls) == 2
    event_sql, event_args = conn.calls[0]
    daily_sql, _daily_args = conn.calls[1]
    assert event_sql.startswith("INSERT INTO workflow_surface_usage_events (")
    assert event_args[0] == datetime(2026, 4, 15, 9, 5, tzinfo=timezone.utc)
    assert event_args[4] == "/api/compile"
    assert event_args[8] == "ok"
    assert event_args[14] == "req-123"
    assert event_args[15] == "moon-ui/1.2.3"
    assert event_args[26] == "defhash"
    assert event_args[27] == "def_123"
    assert event_args[28] == "research"
    assert event_args[29] is True
    assert daily_sql.startswith("INSERT INTO workflow_surface_usage_daily (")


def test_record_api_route_usage_tracks_orient_frontdoor() -> None:
    surface_usage_mod._reset_surface_usage_recorder_health_for_tests()

    class _FakeSubsystems:
        def __init__(self, conn: _FakeConn) -> None:
            self._conn = conn

        def get_pg_conn(self) -> _FakeConn:
            return self._conn

    conn = _FakeConn()
    record_api_route_usage(
        _FakeSubsystems(conn),
        path="/orient",
        method="POST",
        status_code=200,
        response_payload={"kind": "orient_authority_envelope"},
    )

    assert len(conn.calls) == 2
    event_sql, event_args = conn.calls[0]
    daily_sql, _daily_args = conn.calls[1]
    assert event_sql.startswith("INSERT INTO workflow_surface_usage_events (")
    assert event_args[4] == "/orient"
    assert event_args[8] == "ok"
    assert daily_sql.startswith("INSERT INTO workflow_surface_usage_daily (")


def test_record_api_route_usage_reports_recorder_failure(monkeypatch) -> None:
    surface_usage_mod._reset_surface_usage_recorder_health_for_tests()

    class _FailingRepo:
        def __init__(self, _conn) -> None:
            pass

        def record_event(self, **_kwargs) -> None:
            raise RuntimeError("surface usage table unavailable")

    class _FakeSubsystems:
        def __init__(self) -> None:
            self.conn = _FakeConn()

        def get_pg_conn(self) -> object:
            return self.conn

    monkeypatch.setattr(postgres_mod, "PostgresWorkflowSurfaceUsageRepository", _FailingRepo)

    subsystems = _FakeSubsystems()
    record_api_route_usage(
        subsystems,
        path="/orient",
        method="POST",
        status_code=200,
        response_payload={"kind": "orient_authority_envelope"},
    )

    health = surface_usage_mod.surface_usage_recorder_health()
    assert health["authority_ready"] is False
    assert health["observability_state"] == "degraded"
    assert health["dropped_event_count"] == 1
    assert health["durable_event_count"] == 1
    assert health["durable_error_count"] == 0
    assert health["backup_authority_ready"] is True
    assert health["last_entrypoint"] == "/orient"
    assert health["last_surface_kind"] == "api"
    assert health["last_error"] == "RuntimeError: surface usage table unavailable"
    assert health["last_friction_event_id"]
    assert health["last_durable_error"] is None
    assert any(
        call[0].startswith("INSERT INTO friction_events")
        for call in subsystems.conn.calls
    )
    surface_usage_mod._reset_surface_usage_recorder_health_for_tests()


def test_record_api_route_usage_reports_unbacked_recorder_failure_without_connection() -> None:
    surface_usage_mod._reset_surface_usage_recorder_health_for_tests()

    record_api_route_usage(
        None,
        path="/orient",
        method="POST",
        status_code=200,
        response_payload={"kind": "orient_authority_envelope"},
    )

    health = surface_usage_mod.surface_usage_recorder_health()
    assert health["authority_ready"] is False
    assert health["observability_state"] == "degraded"
    assert health["dropped_event_count"] == 1
    assert health["durable_event_count"] == 0
    assert health["durable_error_count"] == 1
    assert health["backup_authority_ready"] is False
    assert health["last_entrypoint"] == "/orient"
    assert health["last_surface_kind"] == "api"
    assert health["last_error"] == "RuntimeError: surface usage postgres connection unavailable"
    assert health["last_friction_event_id"] is None
    assert health["last_durable_error"] == "surface usage failure had no Postgres connection for friction ledger"
    surface_usage_mod._reset_surface_usage_recorder_health_for_tests()


def test_surface_usage_repository_lists_rollup_daily_events_and_summaries() -> None:
    conn = _FakeConn()
    repo = PostgresWorkflowSurfaceUsageRepository(conn)

    rollup = repo.list_usage_rollup(days=14, entrypoint_name="/api/compile")
    daily = repo.list_usage_daily(days=7, entrypoint_name="praxis_query")
    events = repo.list_usage_events(days=7, entrypoint_name="/api/compile", limit=10)
    query_summary = repo.summarize_query_routing(days=7)
    builder_funnels = repo.summarize_builder_funnels(days=7)

    assert rollup[0]["entrypoint_name"] == "/api/compile"
    assert daily[0]["entrypoint_name"] == "praxis_query"
    assert events[0]["request_id"] == "req-123"
    assert query_summary[0]["entrypoint_name"] == "praxis_query"
    assert query_summary[0]["total_result_count"] == 12
    assert builder_funnels["full_path_count"] == 1

    assert conn.calls[0][1][1] == "/api/compile"
    assert conn.calls[1][1][1] == "praxis_query"
    assert conn.calls[2][1][1] == "/api/compile"
