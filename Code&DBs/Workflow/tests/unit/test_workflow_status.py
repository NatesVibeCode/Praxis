from __future__ import annotations

from datetime import datetime, timezone


def test_workflow_status_falls_back_to_workflow_runs_when_metrics_are_empty(
    monkeypatch,
) -> None:
    from runtime import workflow_status
    import storage.postgres as postgres

    class _MetricsView:
        def recent_workflows(self, *, limit: int = 20, days: int | None = None):  # noqa: ARG002
            return []

    class _Conn:
        def execute(self, query: str, *args):
            assert "FROM public.workflow_runs" in query
            # The args depend on whether days is present.
            # In summary(), days is None by default.
            assert args == (20,)
            return [
                {
                    "run_id": "workflow_123",
                    "workflow_id": "workflow.test",
                    "current_state": "succeeded",
                    "terminal_reason_code": None,
                    "requested_at": datetime(2026, 4, 23, 10, 0, tzinfo=timezone.utc),
                    "started_at": datetime(2026, 4, 23, 10, 1, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 4, 23, 10, 2, tzinfo=timezone.utc),
                    "spec_name": "Test Workflow",
                    "parent_run_id": None,
                }
            ]

    monkeypatch.setattr(workflow_status, "get_workflow_metrics_view", lambda: _MetricsView())
    monkeypatch.setattr(postgres, "ensure_postgres_available", lambda: _Conn())

    summary = workflow_status.WorkflowHistory(max_size=20).summary()

    assert summary["total_workflows"] == 1
    assert summary["succeeded"] == 1
    assert summary["workflow_history_source"] == "workflow_runs"
    assert summary["workflow_history_status"] == "complete"
    assert summary["last_5"][0]["run_id"] == "workflow_123"


def test_workflow_summary_supports_time_window(monkeypatch) -> None:
    from runtime import workflow_status
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    recent_date = now - timedelta(hours=1)
    old_date = now - timedelta(days=2)

    class _MetricsView:
        def recent_workflows(self, *, limit: int = 20, days: int | None = None):
            all_rows = [
                {
                    "run_id": "recent_1",
                    "status": "succeeded",
                    "created_at": recent_date,
                    "latency_ms": 100,
                    "provider_slug": "test",
                },
                {
                    "run_id": "old_1",
                    "status": "succeeded",
                    "created_at": old_date,
                    "latency_ms": 100,
                    "provider_slug": "test",
                },
            ]
            if days is not None:
                cutoff = now - timedelta(days=days)
                return [r for r in all_rows if r["created_at"] >= cutoff]
            return all_rows[:limit]

    monkeypatch.setattr(workflow_status, "get_workflow_metrics_view", lambda: _MetricsView())

    history = workflow_status.WorkflowHistory(max_size=20)

    # Test without filter
    summary_all = history.summary()
    assert summary_all["total_workflows"] == 2

    # Test with filter
    summary_filtered = history.summary(days=1)
    assert summary_filtered["total_workflows"] == 1
    assert summary_filtered["last_5"][0]["run_id"] == "recent_1"
    assert summary_filtered["time_window_days"] == 1

