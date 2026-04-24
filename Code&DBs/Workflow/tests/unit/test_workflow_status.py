from __future__ import annotations

from datetime import datetime, timezone


def test_workflow_status_falls_back_to_workflow_runs_when_metrics_are_empty(
    monkeypatch,
) -> None:
    from runtime import workflow_status
    import storage.postgres as postgres

    class _MetricsView:
        def recent_workflows(self, *, limit: int = 20):  # noqa: ARG002
            return []

    class _Conn:
        def execute(self, query: str, *args):
            assert "FROM public.workflow_runs" in query
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
