from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import pytest

import runtime.observability as observability_mod
from runtime.observability import WorkflowMetricsView
from runtime.workflow.orchestrator import WorkflowResult


@pytest.fixture()
def metrics_db_url():
    candidate = "postgresql://postgres@localhost:5432/praxis_test"

    async def _probe() -> None:
        conn = await observability_mod.asyncpg.connect(candidate)
        await conn.close()

    try:
        asyncio.run(_probe())
    except Exception as exc:
        pytest.skip(f"workflow metrics integration requires local Postgres: {exc}")
    return candidate


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
            try:
                await conn.execute(
                    "DELETE FROM workflow_metrics WHERE created_at >= $1",
                    datetime(2098, 1, 1, tzinfo=timezone.utc),
                )
            finally:
                await conn.close()
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

    asyncio.run(_run())


def test_efficiency_summary_reports_retries_and_token_usage(monkeypatch, metrics_db_url):
    fixed_now = datetime(2099, 1, 3, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(observability_mod, "_utc_now", lambda: fixed_now)

    async def _run() -> None:
        view = WorkflowMetricsView(db_url=metrics_db_url)
        try:
            conn = await view._get_connection()
            try:
                await conn.execute(
                    "DELETE FROM workflow_metrics WHERE created_at >= $1",
                    datetime(2098, 1, 1, tzinfo=timezone.utc),
                )
            finally:
                await conn.close()
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

    asyncio.run(_run())


def test_sync_metric_wrappers_open_loop_local_connections(monkeypatch) -> None:
    class _LoopBoundConnection:
        def __init__(self) -> None:
            self._loop = asyncio.get_running_loop()

        def _assert_loop(self) -> None:
            if asyncio.get_running_loop() is not self._loop:
                raise RuntimeError("got Future attached to a different loop")

        async def fetchval(self, _sql: str):
            self._assert_loop()
            return True

        async def fetch(self, sql: str, *_args):
            self._assert_loop()
            if "information_schema.columns" in sql:
                return [
                    {"column_name": column}
                    for column in observability_mod._REQUIRED_WORKFLOW_METRICS_COLUMNS
                ]
            if "GROUP BY provider_slug, model_slug" in sql:
                return [
                    {
                        "provider_slug": "openai",
                        "model_slug": "gpt-test",
                        "total_workflows": 1,
                        "succeeded": 1,
                        "failed": 0,
                        "pass_rate": 100.0,
                    }
                ]
            return []

        async def fetchrow(self, _sql: str, *_args):
            self._assert_loop()
            return {"p50": 10, "p95": 20, "p99": 30}

        async def close(self) -> None:
            self._assert_loop()

    class _FakeAsyncPG:
        def __init__(self) -> None:
            self.connect_loops: list[int] = []

        async def connect(self, dsn: str):
            assert dsn == "postgresql://test@localhost:5432/praxis_test"
            self.connect_loops.append(id(asyncio.get_running_loop()))
            return _LoopBoundConnection()

    fake_asyncpg = _FakeAsyncPG()
    monkeypatch.setattr(observability_mod, "asyncpg", fake_asyncpg)

    view = WorkflowMetricsView(db_url="postgresql://test@localhost:5432/praxis_test")

    assert view.pass_rate_by_model(days=7)[0]["provider_slug"] == "openai"
    assert view.latency_percentiles(days=7)["p95"] == 20
    assert len(fake_asyncpg.connect_loops) == 2


def test_recent_workflows_wrapper_reads_postgres_rows(monkeypatch) -> None:
    class _LoopBoundConnection:
        def __init__(self) -> None:
            self._loop = asyncio.get_running_loop()

        def _assert_loop(self) -> None:
            if asyncio.get_running_loop() is not self._loop:
                raise RuntimeError("got Future attached to a different loop")

        async def fetchval(self, _sql: str):
            self._assert_loop()
            return True

        async def fetch(self, sql: str, *_args):
            self._assert_loop()
            if "information_schema.columns" in sql:
                return [
                    {"column_name": column}
                    for column in observability_mod._REQUIRED_WORKFLOW_METRICS_COLUMNS
                ]
            if "FROM workflow_metrics" in sql and "ORDER BY created_at DESC" in sql:
                return [
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
            raise AssertionError(f"unexpected SQL: {sql}")

        async def close(self) -> None:
            self._assert_loop()

    class _FakeAsyncPG:
        async def connect(self, dsn: str):
            assert dsn == "postgresql://test@localhost:5432/praxis_test"
            return _LoopBoundConnection()

    monkeypatch.setattr(observability_mod, "asyncpg", _FakeAsyncPG())

    view = WorkflowMetricsView(db_url="postgresql://test@localhost:5432/praxis_test")

    rows = view.recent_workflows(limit=2)
    assert [row["run_id"] for row in rows] == ["run_new", "run_old"]
    assert rows[0]["failure_code"] == "timeout"
    assert rows[1]["status"] == "succeeded"


def test_capability_distribution_casts_text_capability_payloads(monkeypatch) -> None:
    observed: dict[str, str] = {}

    class _FakeConnection:
        async def fetchval(self, _sql: str):
            return True

        async def fetch(self, sql: str, *_args):
            if "information_schema.columns" in sql:
                return [
                    {"column_name": column}
                    for column in observability_mod._REQUIRED_WORKFLOW_METRICS_COLUMNS
                ]
            observed["sql"] = sql
            return [{"capability": "ops", "count": 2}]

        async def close(self) -> None:
            return None

    class _FakeAsyncPG:
        async def connect(self, dsn: str):
            assert dsn == "postgresql://test@localhost:5432/praxis_test"
            return _FakeConnection()

    monkeypatch.setattr(observability_mod, "asyncpg", _FakeAsyncPG())

    view = WorkflowMetricsView(db_url="postgresql://test@localhost:5432/praxis_test")

    assert view.capability_distribution(days=7) == [{"capability": "ops", "count": 2}]
    assert "capabilities::jsonb" in observed["sql"]
