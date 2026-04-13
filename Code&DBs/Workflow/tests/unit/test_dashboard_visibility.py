from __future__ import annotations

import io
import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import runtime.dashboard as dashboard_mod
from surfaces.cli.commands.operate import _metrics_command


class _FakeHistory:
    def summary(self) -> dict[str, float | int]:
        return {"total_workflows": 2, "succeeded": 1, "failed": 1, "pass_rate": 0.5}

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


class _FakeRoutes:
    def __init__(self) -> None:
        self._buffers = {"anthropic": [1, 2]}
        self._lock = threading.Lock()

    def consecutive_failures(self, provider_slug: str) -> int:  # noqa: ARG002
        return 2


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
    assert "observability_digest:" in rendered
    assert "failure_mix:" in rendered
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
