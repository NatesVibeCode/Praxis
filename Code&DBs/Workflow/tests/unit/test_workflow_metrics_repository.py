from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from storage.postgres.workflow_metrics_repository import PostgresWorkflowMetricsRepository


class _FakeAsyncConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args):
        self.calls.append((" ".join(query.split()), args))
        return "INSERT 0 1"


def test_workflow_metrics_repository_persists_one_row() -> None:
    async def _run() -> None:
        conn = _FakeAsyncConnection()
        repo = PostgresWorkflowMetricsRepository()

        await repo.upsert_workflow_metric(
            conn,
            run_id="run_123",
            parent_run_id="run_parent",
            reviews_workflow_id=None,
            review_target_modules='["runtime/foo.py"]',
            author_model="anthropic/claude-test",
            provider_slug="anthropic",
            model_slug="claude-test",
            status="failed",
            failure_code="credential_error",
            failure_category="credential_error",
            failure_zone="config",
            is_retryable=True,
            is_transient=False,
            latency_ms=1000,
            cost_usd=1.25,
            input_tokens=10,
            output_tokens=20,
            attempts=2,
            retry_count=1,
            tool_use_count=3,
            cache_read_tokens=4,
            cache_creation_tokens=5,
            duration_api_ms=12,
            task_type="build",
            workflow_label="job-a",
            capabilities='["ops"]',
            label="job-a",
            adapter_type="cli_llm",
            created_at=datetime(2099, 1, 1, 12, 0, tzinfo=timezone.utc),
        )

        assert len(conn.calls) == 1
        sql, args = conn.calls[0]
        assert sql.startswith("INSERT INTO workflow_metrics (")
        assert "ON CONFLICT (run_id) DO NOTHING" in sql
        assert args[0] == "run_123"
        assert args[8] == "credential_error"
        assert args[10] == "config"

    asyncio.run(_run())
