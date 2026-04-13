from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

import runtime.observability as observability_mod
from runtime.observability import WorkflowMetricsView
from runtime.workflow.orchestrator import WorkflowResult


@pytest.fixture()
def metrics_db_url():
    return os.environ["WORKFLOW_DATABASE_URL"]


def _result(
    *,
    run_id: str,
    status: str = "succeeded",
    parent_run_id: str | None = None,
    reviews_workflow_id: str | None = None,
    review_target_modules: list[str] | None = None,
    attempts: int = 1,
    failure_code: str | None = None,
    cost_usd: float = 1.0,
    input_tokens: int = 10,
    output_tokens: int = 20,
    label: str = "job",
    task_type: str | None = "build",
    capabilities: list[str] | None = None,
) -> WorkflowResult:
    return WorkflowResult(
        run_id=run_id,
        status=status,
        reason_code="ok" if status == "succeeded" else "workflow.failed",
        completion="done" if status == "succeeded" else None,
        outputs={
            "cost_usd": cost_usd,
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            },
            "tool_use_count": 3,
            "cache_read_tokens": 4,
            "cache_creation_tokens": 5,
            "duration_api_ms": 12,
        },
        evidence_count=1,
        started_at=datetime(2099, 1, 1, 12, 0, tzinfo=timezone.utc),
        finished_at=datetime(2099, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
        latency_ms=1000,
        provider_slug="anthropic",
        model_slug="claude-test",
        adapter_type="cli_llm",
        failure_code=failure_code,
        attempts=attempts,
        label=label,
        task_type=task_type,
        capabilities=capabilities or ["ops"],
        author_model="anthropic/claude-test",
        reviews_workflow_id=reviews_workflow_id,
        review_target_modules=review_target_modules,
        parent_run_id=parent_run_id,
    )


def test_failure_breakdown_and_lineage_use_new_metadata(monkeypatch, metrics_db_url):
    fixed_now = datetime(2099, 1, 2, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(observability_mod, "_utc_now", lambda: fixed_now)

    parent_id = f"run_parent_{uuid.uuid4().hex[:8]}"
    child_id = f"run_child_{uuid.uuid4().hex[:8]}"

    async def _run() -> None:
        view = WorkflowMetricsView(db_url=metrics_db_url)
        try:
            conn = await view._get_connection()
            await conn.execute(
                "DELETE FROM workflow_metrics WHERE created_at >= $1",
                datetime(2098, 1, 1, tzinfo=timezone.utc),
            )
            await view.record_workflow_async(_result(run_id=parent_id, label="parent-job"))
            await view.record_workflow_async(
                _result(
                    run_id=child_id,
                    status="failed",
                    parent_run_id=parent_id,
                    failure_code="credential_error",
                    label="child-job",
                    review_target_modules=["runtime/foo.py"],
                )
            )

            breakdown = await view.failure_category_breakdown_async(days=2)
            assert len(breakdown) == 1
            assert breakdown[0]["failure_category"] == "credential_error"
            assert breakdown[0]["failure_zone"] == "config"
            assert breakdown[0]["count"] == 1
            assert float(breakdown[0]["pct"]) == pytest.approx(100.0, rel=1e-3)

            lineage = await view.workflow_lineage_async(run_id=parent_id)
            assert [row["run_id"] for row in lineage] == [parent_id, child_id]
            assert lineage[0]["depth"] == 0
            assert lineage[1]["depth"] == 1
            assert lineage[1]["parent_run_id"] == parent_id
            assert lineage[1]["failure_category"] == "credential_error"
            assert lineage[1]["failure_zone"] == "config"
        finally:
            await view.close()

    import asyncio

    asyncio.run(_run())


def test_efficiency_summary_reports_retries_and_token_usage(monkeypatch, metrics_db_url):
    fixed_now = datetime(2099, 1, 3, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(observability_mod, "_utc_now", lambda: fixed_now)

    async def _run() -> None:
        view = WorkflowMetricsView(db_url=metrics_db_url)
        try:
            conn = await view._get_connection()
            await conn.execute(
                "DELETE FROM workflow_metrics WHERE created_at >= $1",
                datetime(2098, 1, 1, tzinfo=timezone.utc),
            )
            await view.record_workflow_async(_result(run_id=f"run_ok_{uuid.uuid4().hex[:8]}", cost_usd=1.5))
            await view.record_workflow_async(
                _result(
                    run_id=f"run_retry_ok_{uuid.uuid4().hex[:8]}",
                    attempts=2,
                    cost_usd=3.0,
                    input_tokens=15,
                    output_tokens=25,
                )
            )
            await view.record_workflow_async(
                _result(
                    run_id=f"run_failed_{uuid.uuid4().hex[:8]}",
                    status="failed",
                    failure_code="verification_failed",
                    attempts=2,
                    cost_usd=0.25,
                    input_tokens=5,
                    output_tokens=8,
                )
            )

            summary = await view.efficiency_summary_async(days=2)
            assert summary["total_workflows"] == 3
            assert summary["succeeded"] == 2
            assert summary["failed"] == 1
            assert summary["first_pass_success_rate"] == pytest.approx(1 / 3, rel=1e-3)
            assert summary["retry_success_rate"] == pytest.approx(0.5, rel=1e-3)
            assert summary["cost_per_success_usd"] == pytest.approx(2.375, rel=1e-3)
            assert summary["tokens_per_success"] == pytest.approx(41.5, rel=1e-3)
            assert summary["avg_latency_ms"] == pytest.approx(1000.0, rel=1e-3)
            assert summary["avg_tool_uses"] == pytest.approx(3.0, rel=1e-3)
        finally:
            await view.close()

    import asyncio

    asyncio.run(_run())
