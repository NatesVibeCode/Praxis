from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import runtime.workflow_status as workflow_status
from runtime.workflow.orchestrator import WorkflowResult


def _workflow_result(run_id: str, *, status: str = "succeeded", finished_at: datetime | None = None) -> WorkflowResult:
    finished = finished_at or datetime(2099, 1, 4, 12, 0, tzinfo=timezone.utc)
    return WorkflowResult(
        run_id=run_id,
        status=status,
        reason_code=status,
        completion="done" if status == "succeeded" else None,
        outputs={"cost_usd": 0.5, "total_cost_usd": 0.5},
        evidence_count=1,
        started_at=finished,
        finished_at=finished,
        latency_ms=100,
        provider_slug="anthropic",
        model_slug="claude-3",
        adapter_type="cli_llm",
        failure_code=None if status == "succeeded" else "timeout",
        attempts=1,
        label=None,
        task_type=None,
        capabilities=None,
        author_model="anthropic/claude-3",
        reviews_workflow_id=None,
        review_target_modules=None,
        parent_run_id=None,
        persisted=True,
        sync_status="complete",
    )


def test_workflow_history_reads_from_metrics_view(monkeypatch) -> None:
    fake_rows = [
        {
            "run_id": "run_new",
            "provider_slug": "anthropic",
            "model_slug": "claude-3",
            "status": "failed",
            "failure_code": "timeout",
            "latency_ms": 250,
            "cost_usd": 1.5,
            "input_tokens": 11,
            "output_tokens": 22,
            "attempts": 2,
            "review_target_modules": ["runtime/foo.py"],
            "adapter_type": "cli_llm",
            "created_at": datetime(2099, 1, 4, 12, 0, tzinfo=timezone.utc),
        },
        {
            "run_id": "run_old",
            "provider_slug": "openai",
            "model_slug": "gpt-4o",
            "status": "succeeded",
            "failure_code": None,
            "latency_ms": 100,
            "cost_usd": 0.75,
            "input_tokens": 5,
            "output_tokens": 10,
            "attempts": 1,
            "review_target_modules": None,
            "adapter_type": "api",
            "created_at": datetime(2099, 1, 4, 11, 0, tzinfo=timezone.utc),
        },
    ]

    monkeypatch.setattr(
        workflow_status,
        "get_workflow_metrics_view",
        lambda: SimpleNamespace(recent_workflows=lambda limit=20, days=None: fake_rows[:limit]),
    )

    history = workflow_status.WorkflowHistory(max_size=10)

    recent = history.recent_workflows(limit=2)
    assert [result.run_id for result in recent] == ["run_new", "run_old"]
    assert recent[0].status == "failed"
    assert recent[0].outputs["cost_usd"] == 1.5
    assert recent[0].review_target_modules == ["runtime/foo.py"]

    summary = history.summary()
    assert summary["total_workflows"] == 2
    assert summary["succeeded"] == 1
    assert summary["failed"] == 1
    assert summary["pass_rate"] == 0.5
    assert summary["total_cost_usd"] == 2.25
    assert summary["last_5"][0]["run_id"] == "run_new"
    assert summary["workflow_history_source"] == "metrics"
    assert summary["workflow_history_status"] == "complete"
    assert summary["workflow_history_error"] is None


def test_workflow_history_reports_degraded_source_when_metrics_view_fails(monkeypatch) -> None:
    history = workflow_status.WorkflowHistory(max_size=10)
    history.record_workflow(_workflow_result("run_fallback", status="failed"))

    def _raise(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("metrics view offline")

    monkeypatch.setattr(
        workflow_status,
        "get_workflow_metrics_view",
        lambda: SimpleNamespace(recent_workflows=_raise),
    )
    monkeypatch.setattr(
        workflow_status.WorkflowHistory,
        "_recent_workflows_from_runs",
        lambda self, limit, days=None: _raise(),
    )

    summary = history.summary()
    assert summary["total_workflows"] == 1
    assert summary["workflow_history_source"] == "fallback"
    assert summary["workflow_history_status"] == "degraded"
    assert summary["workflow_history_error"] == (
        "RuntimeError: metrics view offline; "
        "workflow_runs fallback RuntimeError: metrics view offline"
    )
