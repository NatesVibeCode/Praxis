from __future__ import annotations

import io
from datetime import datetime, timezone
from types import SimpleNamespace

import runtime.dashboard as dashboard_mod
from surfaces.cli.commands.operate import _metrics_command


class _FakeHistory:
    def summary(self) -> dict[str, object]:
        return {
            "total_workflows": 2,
            "succeeded": 1,
            "failed": 1,
            "pass_rate": 0.5,
            "workflow_history_source": "metrics",
            "workflow_history_status": "complete",
            "workflow_history_error": None,
        }

    def recent_workflows(self, limit: int = 20):  # noqa: ARG002
        return [
            SimpleNamespace(
                status="failed",
                run_id="run-1",
                provider_slug="anthropic",
                model_slug="claude-test",
                failure_code="TIMEOUT",
                latency_ms=123,
                finished_at=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
            )
        ]


class _HealthyHistory:
    def summary(self) -> dict[str, object]:
        return {
            "total_workflows": 10,
            "succeeded": 10,
            "failed": 0,
            "pass_rate": 1.0,
            "workflow_history_source": "metrics",
            "workflow_history_status": "complete",
            "workflow_history_error": None,
        }

    def recent_workflows(self, limit: int = 20):  # noqa: ARG002
        return []


class _FakeCostTracker:
    def summary(self) -> dict[str, object]:
        return {
            "total_cost_usd": 1.25,
            "total_input_tokens": 100,
            "total_output_tokens": 200,
            "cost_by_agent": {"anthropic/claude-test": 1.25},
        }


class _FakeCircuits:
    def all_states(self) -> dict[str, dict[str, object]]:
        return {"anthropic": {"state": "OPEN", "success_count": 0, "failure_count": 1}}


class _ClosedCircuits:
    def all_states(self) -> dict[str, dict[str, object]]:
        return {
            "anthropic": {"state": "CLOSED", "success_count": 10, "failure_count": 0},
            "openai": {"state": "CLOSED", "success_count": 10, "failure_count": 0},
        }


class _FakeRoutes:
    def __init__(self) -> None:
        self._providers = ("anthropic",)

    def consecutive_failures(self, provider_slug: str) -> int:  # noqa: ARG002
        return 2

    def provider_slugs(self) -> tuple[str, ...]:
        return self._providers


class _MixedRouteHealth:
    def consecutive_failures(self, provider_slug: str) -> int:
        return 2 if provider_slug == "anthropic" else 0

    def provider_slugs(self) -> tuple[str, ...]:
        return ("anthropic", "openai")


class _FakeMetricsView:
    def pass_rate_by_model(self, days: int = 7):  # noqa: ARG002
        return [{"provider_slug": "anthropic", "model_slug": "claude-test", "total_workflows": 2, "pass_rate": 50.0}]

    def cost_by_agent(self, days: int = 7):  # noqa: ARG002
        return [{"provider_slug": "anthropic", "total_cost_usd": 1.25, "num_workflows": 2, "avg_cost_per_workflow": 0.625}]

    def latency_percentiles(self, days: int = 7):  # noqa: ARG002
        return {"p50": 100, "p95": 200, "p99": 300}

    def efficiency_summary(self, days: int = 7):  # noqa: ARG002
        return {
            "total_workflows": 2,
            "succeeded": 1,
            "failed": 1,
            "first_pass_success_rate": 0.5,
            "retry_success_rate": 0.0,
            "cost_per_success_usd": 1.25,
            "tokens_per_success": 300.0,
            "avg_latency_ms": 123.0,
            "avg_tool_uses": 2.0,
        }

    def failure_category_breakdown(self, days: int = 7):  # noqa: ARG002
        return [{"failure_category": "timeout", "failure_zone": "external", "count": 1, "pct": 100.0}]

    def hourly_workflow_volume(self, days: int = 7):  # noqa: ARG002
        return [{"hour": "2026-04-10T12:00:00+00:00", "count": 2}]

    def capability_distribution(self, days: int = 7):  # noqa: ARG002
        return [{"capability": "ops", "count": 2}]


class _ExplodingRoutes:
    def consecutive_failures(self, provider_slug: str) -> int:  # noqa: ARG002
        raise AssertionError("unreachable")

    def provider_slugs(self) -> tuple[str, ...]:
        raise RuntimeError("route metrics offline")


def test_build_dashboard_surfaces_observability_block(monkeypatch):
    monkeypatch.setattr(dashboard_mod, "get_workflow_history", lambda: _FakeHistory())
    monkeypatch.setattr(dashboard_mod, "get_cost_tracker", lambda: _FakeCostTracker())
    monkeypatch.setattr(dashboard_mod, "get_circuit_breakers", lambda: _FakeCircuits())
    monkeypatch.setattr(dashboard_mod, "get_route_outcomes", lambda: _FakeRoutes())
    monkeypatch.setattr(dashboard_mod, "build_leaderboard", lambda: [])
    monkeypatch.setattr(dashboard_mod, "get_workflow_metrics_view", lambda: _FakeMetricsView())

    data = dashboard_mod.build_dashboard()

    assert "observability" in data
    assert data["observability"]["efficiency_summary"]["first_pass_success_rate"] == 0.5
    assert data["observability"]["failure_category_breakdown"][0]["failure_category"] == "timeout"
    assert data["observability"]["hourly_workflow_volume"][0]["count"] == 2

    rendered = dashboard_mod.format_dashboard(data)
    assert "dashboard_summary:" in rendered
    assert "workflow_history_source=metrics" in rendered
    assert "workflow_history_status=complete" in rendered
    assert "observability_digest:" in rendered
    assert "failure_mix:" in rendered
    assert "route_health:" in rendered
    assert "leaderboard_top:" in rendered


def test_metrics_command_prints_observability_sections(monkeypatch):
    monkeypatch.setattr("runtime.observability.get_workflow_metrics_view", lambda: _FakeMetricsView())

    stdout = io.StringIO()
    exit_code = _metrics_command([], stdout=stdout)

    output = stdout.getvalue()
    assert exit_code == 0
    assert "metrics_summary:" in output
    assert "observability_digest:" in output
    assert "failure_mix:" in output


def test_build_dashboard_surfaces_route_health_failures_explicitly(monkeypatch):
    monkeypatch.setattr(dashboard_mod, "get_workflow_history", lambda: _FakeHistory())
    monkeypatch.setattr(dashboard_mod, "get_cost_tracker", lambda: _FakeCostTracker())
    monkeypatch.setattr(dashboard_mod, "get_circuit_breakers", lambda: _FakeCircuits())
    monkeypatch.setattr(dashboard_mod, "get_route_outcomes", lambda: _ExplodingRoutes())
    monkeypatch.setattr(dashboard_mod, "build_leaderboard", lambda: [])
    monkeypatch.setattr(dashboard_mod, "get_workflow_metrics_view", lambda: _FakeMetricsView())

    data = dashboard_mod.build_dashboard()

    assert data["route_health"] == {}
    assert data["route_health_error"] == "RuntimeError: route metrics offline"
    assert data["system_health"] == "degraded"

    rendered = dashboard_mod.format_dashboard(data)
    assert "route_health: unavailable error=RuntimeError: route metrics offline" in rendered


def test_build_dashboard_degrades_when_route_failure_counts_are_present(monkeypatch):
    monkeypatch.setattr(dashboard_mod, "get_workflow_history", lambda: _HealthyHistory())
    monkeypatch.setattr(dashboard_mod, "get_cost_tracker", lambda: _FakeCostTracker())
    monkeypatch.setattr(dashboard_mod, "get_circuit_breakers", lambda: _ClosedCircuits())
    monkeypatch.setattr(dashboard_mod, "get_route_outcomes", lambda: _MixedRouteHealth())
    monkeypatch.setattr(dashboard_mod, "build_leaderboard", lambda: [])
    monkeypatch.setattr(dashboard_mod, "get_workflow_metrics_view", lambda: _FakeMetricsView())

    data = dashboard_mod.build_dashboard()

    assert data["route_health"] == {"anthropic": 2, "openai": 0}
    assert data["degraded_route_health"] == {"anthropic": 2}
    assert data["route_health_error"] is None
    assert data["system_health"] == "degraded"


def test_build_dashboard_surfaces_leaderboard_failures_explicitly(monkeypatch):
    monkeypatch.setattr(dashboard_mod, "get_workflow_history", lambda: _FakeHistory())
    monkeypatch.setattr(dashboard_mod, "get_cost_tracker", lambda: _FakeCostTracker())
    monkeypatch.setattr(dashboard_mod, "get_circuit_breakers", lambda: _FakeCircuits())
    monkeypatch.setattr(dashboard_mod, "get_route_outcomes", lambda: _FakeRoutes())
    monkeypatch.setattr(
        dashboard_mod,
        "build_leaderboard",
        lambda: (_ for _ in ()).throw(RuntimeError("receipt authority offline")),
    )
    monkeypatch.setattr(dashboard_mod, "get_workflow_metrics_view", lambda: _FakeMetricsView())

    data = dashboard_mod.build_dashboard()

    assert data["leaderboard"] == []
    assert data["leaderboard_error"] == "RuntimeError: receipt authority offline"
    assert data["system_health"] == "degraded"

    rendered = dashboard_mod.format_dashboard(data)
    assert "leaderboard_top:" in rendered
    assert "unavailable error=RuntimeError: receipt authority offline" in rendered
