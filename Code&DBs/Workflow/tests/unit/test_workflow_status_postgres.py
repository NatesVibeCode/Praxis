from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import runtime.workflow_status as workflow_status


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
        lambda: SimpleNamespace(recent_workflows=lambda limit=20: fake_rows[:limit]),
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
