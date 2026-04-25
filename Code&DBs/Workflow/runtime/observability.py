"""Postgres-backed observability views for workflow metrics.

Records workflow results into a durable Postgres table and provides query
interfaces for metrics, pass rates, cost analysis, latency percentiles, and
failure heatmaps.

Module-level singleton via get_workflow_metrics_view().
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
import sys
import threading
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, TYPE_CHECKING

import asyncpg

from storage.postgres import PostgresWorkflowMetricsRepository

from ._workflow_database import resolve_runtime_database_url
from .failure_projection import project_failure_classification

if TYPE_CHECKING:
    from .workflow import WorkflowResult

_WORKFLOW_METRICS_LOCK = threading.Lock()
_WORKFLOW_METRICS_VIEW: WorkflowMetricsView | None = None
_REQUIRED_WORKFLOW_METRICS_COLUMNS = frozenset(
    {
        "run_id",
        "parent_run_id",
        "reviews_workflow_id",
        "review_target_modules",
        "author_model",
        "provider_slug",
        "model_slug",
        "status",
        "failure_code",
        "failure_category",
        "failure_zone",
        "is_retryable",
        "is_transient",
        "latency_ms",
        "cost_usd",
        "input_tokens",
        "output_tokens",
        "attempts",
        "retry_count",
        "tool_use_count",
        "cache_read_tokens",
        "cache_creation_tokens",
        "duration_api_ms",
        "task_type",
        "workflow_label",
        "capabilities",
        "label",
        "adapter_type",
        "created_at",
    }
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_int(val: Any) -> int:
    """Coerce to int, defaulting to 0 on failure."""
    if val is None:
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _safe_float(val: Any) -> float:
    """Coerce to float, defaulting to 0.0 on failure."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _safe_bool(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    if isinstance(val, str):
        return val.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(val)


def _extract_cost_and_tokens(outputs: Mapping[str, Any]) -> tuple[float, int, int]:
    """Extract (cost_usd, input_tokens, output_tokens) from workflow outputs.

    Handles both Claude CLI and OpenAI-style response shapes.
    """
    cost_usd = 0.0
    input_tokens = 0
    output_tokens = 0

    raw_json = outputs.get("raw_json")
    if isinstance(raw_json, dict):
        cost_usd = _safe_float(raw_json.get("total_cost_usd"))
        usage = raw_json.get("usage")
        if isinstance(usage, dict):
            input_tokens = _safe_int(usage.get("input_tokens"))
            output_tokens = _safe_int(usage.get("output_tokens"))

    # Fallback: OpenAI-style usage at top level
    if input_tokens == 0 and output_tokens == 0:
        usage = outputs.get("usage")
        if isinstance(usage, dict):
            input_tokens = _safe_int(
                usage.get("prompt_tokens") or usage.get("input_tokens")
            )
            output_tokens = _safe_int(
                usage.get("completion_tokens") or usage.get("output_tokens")
            )

    # Fallback: cost at top level
    if cost_usd == 0.0:
        cost_usd = _safe_float(
            outputs.get("cost_usd") or outputs.get("total_cost_usd") or outputs.get("cost")
        )

    return cost_usd, input_tokens, output_tokens


def _failure_zone_for_category(category: str | None) -> str | None:
    normalized = str(category or "").strip()
    if not normalized:
        return None
    return {
        "timeout": "external",
        "rate_limit": "external",
        "provider_error": "external",
        "network_error": "external",
        "infrastructure": "external",
        "credential_error": "config",
        "model_error": "config",
        "input_error": "config",
        "context_overflow": "internal",
        "parse_error": "internal",
        "sandbox_error": "internal",
        "scope_violation": "internal",
        "verification_failed": "internal",
        "unknown": "unknown",
    }.get(normalized, "unknown")


def _tool_use_count(outputs: Mapping[str, Any]) -> int:
    value = outputs.get("tool_use_count")
    if value is not None:
        return _safe_int(value)
    tool_use = outputs.get("tool_use")
    if isinstance(tool_use, list):
        return len(tool_use)
    if isinstance(tool_use, dict):
        return len(tool_use)
    return 0


@dataclass(frozen=True)
class WorkflowMetric:
    """One recorded workflow metric."""

    run_id: str
    parent_run_id: str | None
    reviews_workflow_id: str | None
    review_target_modules: list[str] | None
    author_model: str | None
    provider_slug: str
    model_slug: str | None
    status: str
    failure_code: str | None
    failure_category: str | None
    failure_zone: str | None
    is_retryable: bool | None
    is_transient: bool | None
    latency_ms: int
    cost_usd: float
    input_tokens: int
    output_tokens: int
    attempts: int
    retry_count: int
    tool_use_count: int
    cache_read_tokens: int
    cache_creation_tokens: int
    duration_api_ms: int
    task_type: str | None
    workflow_label: str | None
    capabilities: list[str] | None
    label: str | None
    adapter_type: str
    created_at: datetime


class WorkflowMetricsView:
    """Postgres-backed observability for workflow metrics.

    Provides durable storage and query interfaces for pass rates, costs,
    latencies, failure patterns, and capability distribution.
    """

    def __init__(self, db_url: str | None = None) -> None:
        """Initialize the metrics view.

        Args:
            db_url: Postgres connection URL. If None, reads WORKFLOW_DATABASE_URL.
        """
        self._db_url = resolve_runtime_database_url(db_url, required=False)
        self._schema_initialized = False

    async def _get_connection(self) -> asyncpg.Connection:
        """Open a Postgres connection for the current async operation."""
        if not self._db_url:
            raise RuntimeError(
                "WORKFLOW_DATABASE_URL not set and no db_url provided to WorkflowMetricsView"
            )
        return await asyncpg.connect(self._db_url)

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[asyncpg.Connection]:
        """Yield a loop-local connection and always close it afterwards."""

        conn = await self._get_connection()
        try:
            yield conn
        finally:
            await conn.close()

    async def close(self) -> None:
        """Compatibility no-op now that connections are operation-scoped."""
        return None

    async def _ensure_schema(self, *, conn: asyncpg.Connection | None = None) -> None:
        """Verify the canonical workflow_metrics schema is present."""
        if self._schema_initialized:
            return

        owns_connection = conn is None
        if conn is None:
            conn = await self._get_connection()
        try:
            table_exists = await conn.fetchval(
                "SELECT to_regclass('public.workflow_metrics') IS NOT NULL"
            )
            if not table_exists:
                raise RuntimeError(
                    "workflow_metrics table is missing; bootstrap workflow schema so "
                    "081_observability_lineage_and_metrics.sql can materialize it"
                )

            rows = await conn.fetch(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'workflow_metrics'
                """
            )
            available_columns = {str(row["column_name"]) for row in rows}
            missing_columns = sorted(
                _REQUIRED_WORKFLOW_METRICS_COLUMNS.difference(available_columns)
            )
            if missing_columns:
                raise RuntimeError(
                    "workflow_metrics schema is incomplete; missing columns: "
                    + ", ".join(missing_columns)
                )

            self._schema_initialized = True
        finally:
            if owns_connection:
                await conn.close()

    async def record_workflow_async(self, result: WorkflowResult) -> None:
        """Record a workflow result in the metrics table.

        Args:
            result: The WorkflowResult to record.
        """
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)

            cost_usd, input_tokens, output_tokens = _extract_cost_and_tokens(
                result.outputs
            )

            capabilities_json = (
                json.dumps(result.capabilities) if result.capabilities else None
            )
            review_modules_json = (
                json.dumps(result.review_target_modules)
                if result.review_target_modules
                else None
            )
            failure_category = None
            failure_zone = None
            is_retryable = None
            is_transient = None
            if result.status != "succeeded" and result.failure_code:
                classification = None
                if isinstance(result.outputs, Mapping):
                    raw_classification = result.outputs.get("failure_classification")
                    if isinstance(raw_classification, dict):
                        classification = dict(raw_classification)
                if classification is None:
                    stdout_preview = ""
                    if isinstance(result.outputs, Mapping):
                        stdout_preview = str(result.outputs.get("stderr", "") or "")
                    classification = project_failure_classification(
                        failure_category=result.failure_code,
                        is_transient=_safe_bool(
                            result.outputs.get("is_transient")
                            if isinstance(result.outputs, Mapping)
                            else False
                        ),
                        stdout_preview=stdout_preview,
                    )
                if classification:
                    failure_category = str(
                        classification.get("category") or result.failure_code or None
                    )
                    failure_zone = _failure_zone_for_category(failure_category)
                    is_retryable = classification.get("is_retryable")
                    is_transient = classification.get("is_transient")
            cache_read_tokens = 0
            cache_creation_tokens = 0
            duration_api_ms = 0
            tool_use_count = 0
            if isinstance(result.outputs, Mapping):
                cache_read_tokens = _safe_int(result.outputs.get("cache_read_tokens"))
                cache_creation_tokens = _safe_int(result.outputs.get("cache_creation_tokens"))
                duration_api_ms = _safe_int(
                    result.outputs.get("duration_api_ms")
                    or result.outputs.get("api_duration_ms")
                    or result.outputs.get("duration_ms")
                )
                tool_use_count = _tool_use_count(result.outputs)
                raw_json = result.outputs.get("raw_json")
                if isinstance(raw_json, dict):
                    usage = raw_json.get("usage")
                    if isinstance(usage, dict):
                        cache_read_tokens = cache_read_tokens or _safe_int(usage.get("cache_read_tokens"))
                        cache_creation_tokens = cache_creation_tokens or _safe_int(usage.get("cache_creation_tokens"))
                        duration_api_ms = duration_api_ms or _safe_int(
                            raw_json.get("duration_api_ms") or raw_json.get("duration_ms")
                        )

            author_model = result.author_model
            if author_model is None and result.provider_slug and result.model_slug:
                author_model = f"{result.provider_slug}/{result.model_slug}"

            parent_run_id = result.parent_run_id or result.reviews_workflow_id

            await PostgresWorkflowMetricsRepository().upsert_workflow_metric(
                conn,
                run_id=result.run_id,
                parent_run_id=parent_run_id,
                reviews_workflow_id=result.reviews_workflow_id,
                review_target_modules=review_modules_json,
                author_model=author_model,
                provider_slug=result.provider_slug,
                model_slug=result.model_slug,
                status=result.status,
                failure_code=result.failure_code,
                failure_category=failure_category,
                failure_zone=failure_zone,
                is_retryable=is_retryable,
                is_transient=is_transient,
                latency_ms=result.latency_ms,
                cost_usd=cost_usd,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                attempts=result.attempts,
                retry_count=max(result.attempts - 1, 0),
                tool_use_count=tool_use_count,
                cache_read_tokens=cache_read_tokens,
                cache_creation_tokens=cache_creation_tokens,
                duration_api_ms=duration_api_ms,
                task_type=result.task_type,
                workflow_label=result.label,
                capabilities=capabilities_json,
                label=result.label,
                adapter_type=result.adapter_type,
                created_at=_utc_now(),
            )

    def record_workflow(self, result: WorkflowResult) -> None:
        """Synchronous wrapper for record_workflow_async.

        Runs the async operation in a thread pool.
        """
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self.record_workflow_async(result))
        finally:
            loop.close()

    async def pass_rate_by_model_async(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Get pass rate by (provider_slug, model_slug).

        Args:
            days: Number of days to look back (default 7).

        Returns:
            List of dicts with keys: provider_slug, model_slug, total_workflows,
            succeeded, failed, pass_rate.
        """
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            cutoff = _utc_now() - timedelta(days=days)
            rows = await conn.fetch(
                """
                SELECT
                    provider_slug,
                    model_slug,
                    COUNT(*) as total_workflows,
                    SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) as succeeded,
                    SUM(CASE WHEN status != 'succeeded' THEN 1 ELSE 0 END) as failed,
                    ROUND(
                        100.0 * SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) /
                        NULLIF(COUNT(*), 0),
                        2
                    ) as pass_rate
                FROM workflow_metrics
                WHERE created_at >= $1
                GROUP BY provider_slug, model_slug
                ORDER BY pass_rate DESC, total_workflows DESC
                """,
                cutoff,
            )
            return [dict(row) for row in rows]

    def pass_rate_by_model(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Synchronous wrapper for pass_rate_by_model_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.pass_rate_by_model_async(days=days))
        finally:
            loop.close()

    async def cost_by_agent_async(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Get cost breakdown by provider.

        Args:
            days: Number of days to look back (default 7).

        Returns:
            List of dicts with keys: provider_slug, total_cost_usd, num_workflows,
            avg_cost_per_workflow.
        """
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            cutoff = _utc_now() - timedelta(days=days)
            rows = await conn.fetch(
                """
                SELECT
                    provider_slug,
                    COALESCE(model_slug, 'unknown') as model_slug,
                    provider_slug || '/' || COALESCE(model_slug, 'unknown') as agent_slug,
                    ROUND(SUM(cost_usd)::NUMERIC, 4) as total_cost_usd,
                    COUNT(*) as num_workflows,
                    ROUND((SUM(cost_usd) / NULLIF(COUNT(*), 0))::NUMERIC, 6) as avg_cost_per_workflow
                FROM workflow_metrics
                WHERE created_at >= $1
                GROUP BY provider_slug, model_slug
                ORDER BY total_cost_usd DESC
                """,
                cutoff,
            )
            return [dict(row) for row in rows]

    def cost_by_agent(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Synchronous wrapper for cost_by_agent_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.cost_by_agent_async(days=days))
        finally:
            loop.close()

    async def latency_percentiles_async(
        self,
        *,
        agent_slug: str | None = None,
        provider: str | None = None,
        days: int = 7,
    ) -> dict[str, int]:
        """Get latency percentiles (p50, p95, p99).

        Args:
            agent_slug: Optional "provider/model" slug to filter by.
            provider: Deprecated — use agent_slug instead.
            days: Number of days to look back (default 7).

        Returns:
            Dict with keys: p50, p95, p99 (all in milliseconds).
        """
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            cutoff = _utc_now() - timedelta(days=days)

            query = """
                SELECT
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_ms) as p50,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) as p99
                FROM workflow_metrics
                WHERE created_at >= $1
            """

            params = [cutoff]
            if agent_slug and "/" in agent_slug:
                p, m = agent_slug.split("/", 1)
                query += " AND provider_slug = $2 AND model_slug = $3"
                params.extend([p, m])
            elif provider:
                query += " AND provider_slug = $2"
                params.append(provider)

            row = await conn.fetchrow(query, *params)
            if not row:
                return {"p50": 0, "p95": 0, "p99": 0}

            return {
                "p50": int(row["p50"] or 0),
                "p95": int(row["p95"] or 0),
                "p99": int(row["p99"] or 0),
            }

    def latency_percentiles(
        self,
        *,
        provider: str | None = None,
        days: int = 7,
    ) -> dict[str, int]:
        """Synchronous wrapper for latency_percentiles_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self.latency_percentiles_async(provider=provider, days=days)
            )
        finally:
            loop.close()

    async def failure_heatmap_async(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Get failure heatmap: failure_code x agent_slug (provider/model).

        Args:
            days: Number of days to look back (default 7).

        Returns:
            List of dicts with keys: failure_code, agent_slug, provider_slug, model_slug, count.
        """
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            cutoff = _utc_now() - timedelta(days=days)
            rows = await conn.fetch(
                """
                SELECT
                    COALESCE(failure_code, 'no_error') as failure_code,
                    provider_slug,
                    COALESCE(model_slug, 'unknown') as model_slug,
                    provider_slug || '/' || COALESCE(model_slug, 'unknown') as agent_slug,
                    COUNT(*) as count
                FROM workflow_metrics
                WHERE created_at >= $1 AND status != 'succeeded'
                GROUP BY failure_code, provider_slug, model_slug
                ORDER BY count DESC
                """,
                cutoff,
            )
            return [dict(row) for row in rows]

    def failure_heatmap(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Synchronous wrapper for failure_heatmap_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.failure_heatmap_async(days=days))
        finally:
            loop.close()

    async def failure_category_breakdown_async(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Summarize failures by canonical failure category and zone."""
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            cutoff = _utc_now() - timedelta(days=days)
            rows = await conn.fetch(
                """
                WITH normalized AS (
                    SELECT
                        COALESCE(NULLIF(failure_category, ''), COALESCE(NULLIF(failure_code, ''), 'unknown')) AS failure_category,
                        COALESCE(
                            NULLIF(failure_zone, ''),
                            COALESCE(NULLIF(failure_category, ''), COALESCE(NULLIF(failure_code, ''), 'unknown'))
                        ) AS failure_zone
                    FROM workflow_metrics
                    WHERE created_at >= $1 AND status != 'succeeded'
                )
                SELECT
                    failure_category,
                    failure_zone,
                    COUNT(*) as count,
                    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) as pct
                FROM normalized
                GROUP BY failure_category, failure_zone
                ORDER BY count DESC, failure_category ASC
                """,
                cutoff,
            )
            return [dict(row) for row in rows]

    def failure_category_breakdown(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Synchronous wrapper for failure_category_breakdown_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self.failure_category_breakdown_async(days=days)
            )
        finally:
            loop.close()

    async def efficiency_summary_async(
        self,
        *,
        days: int = 7,
    ) -> dict[str, Any]:
        """Return a compact SLI summary for recent workflow activity."""
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            cutoff = _utc_now() - timedelta(days=days)
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total_workflows,
                    SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
                    SUM(CASE WHEN status != 'succeeded' THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status = 'succeeded' AND attempts = 1 THEN 1 ELSE 0 END) AS first_pass_successes,
                    SUM(CASE WHEN status = 'succeeded' AND attempts > 1 THEN 1 ELSE 0 END) AS retry_successes,
                    SUM(CASE WHEN attempts > 1 THEN 1 ELSE 0 END) AS retried_workflows,
                    SUM(cost_usd) AS total_cost_usd,
                    SUM(latency_ms) AS total_latency_ms,
                    SUM(input_tokens) AS total_input_tokens,
                    SUM(output_tokens) AS total_output_tokens,
                    SUM(tool_use_count) AS total_tool_uses
                FROM workflow_metrics
                WHERE created_at >= $1
                """,
                cutoff,
            )
            if not row:
                return {
                    "total_workflows": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "first_pass_success_rate": 0.0,
                    "retry_success_rate": 0.0,
                    "cost_per_success_usd": 0.0,
                    "tokens_per_success": 0.0,
                    "avg_latency_ms": 0.0,
                    "avg_tool_uses": 0.0,
                }

            total = _safe_int(row["total_workflows"])
            succeeded = _safe_int(row["succeeded"])
            failed = _safe_int(row["failed"])
            first_pass_successes = _safe_int(row["first_pass_successes"])
            retry_successes = _safe_int(row["retry_successes"])
            retried_workflows = _safe_int(row["retried_workflows"])
            total_cost_usd = _safe_float(row["total_cost_usd"])
            total_latency_ms = _safe_float(row["total_latency_ms"])
            total_tokens = _safe_int(row["total_input_tokens"]) + _safe_int(row["total_output_tokens"])
            total_tool_uses = _safe_int(row["total_tool_uses"])

            return {
                "total_workflows": total,
                "succeeded": succeeded,
                "failed": failed,
                "first_pass_success_rate": round(first_pass_successes / total, 4) if total else 0.0,
                "retry_success_rate": round(retry_successes / retried_workflows, 4) if retried_workflows else 0.0,
                "cost_per_success_usd": round(total_cost_usd / succeeded, 6) if succeeded else 0.0,
                "tokens_per_success": round(total_tokens / succeeded, 2) if succeeded else 0.0,
                "avg_latency_ms": round(total_latency_ms / total, 2) if total else 0.0,
                "avg_tool_uses": round(total_tool_uses / total, 2) if total else 0.0,
            }

    def efficiency_summary(
        self,
        *,
        days: int = 7,
    ) -> dict[str, Any]:
        """Synchronous wrapper for efficiency_summary_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.efficiency_summary_async(days=days))
        finally:
            loop.close()

    async def workflow_lineage_async(
        self,
        *,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """Return the run tree rooted at *run_id*."""
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            rows = await conn.fetch(
                """
                WITH RECURSIVE lineage AS (
                    SELECT
                        0 AS depth,
                        run_id, parent_run_id, reviews_workflow_id,
                        review_target_modules, author_model, provider_slug, model_slug,
                        status, failure_code, failure_category, failure_zone,
                        is_retryable, is_transient, latency_ms, cost_usd,
                        input_tokens, output_tokens, attempts, retry_count,
                        tool_use_count, cache_read_tokens, cache_creation_tokens,
                        duration_api_ms, task_type, workflow_label, capabilities,
                        label, adapter_type, created_at
                    FROM workflow_metrics
                    WHERE run_id = $1
                    UNION ALL
                    SELECT
                        parent.depth + 1 AS depth,
                        child.run_id, child.parent_run_id, child.reviews_workflow_id,
                        child.review_target_modules, child.author_model, child.provider_slug,
                        child.model_slug, child.status, child.failure_code,
                        child.failure_category, child.failure_zone, child.is_retryable,
                        child.is_transient, child.latency_ms, child.cost_usd,
                        child.input_tokens, child.output_tokens, child.attempts,
                        child.retry_count, child.tool_use_count,
                        child.cache_read_tokens, child.cache_creation_tokens,
                        child.duration_api_ms, child.task_type, child.workflow_label,
                        child.capabilities, child.label, child.adapter_type,
                        child.created_at
                    FROM workflow_metrics child
                    JOIN lineage parent ON child.parent_run_id = parent.run_id
                )
                SELECT *
                FROM lineage
                ORDER BY depth ASC, created_at ASC
                """,
                run_id,
            )
            return [dict(row) for row in rows]

    def workflow_lineage(
        self,
        *,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """Synchronous wrapper for workflow_lineage_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.workflow_lineage_async(run_id=run_id))
        finally:
            loop.close()

    async def recent_workflows_async(
        self,
        *,
        limit: int = 20,
        days: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return the most recent workflow metrics rows.

        The rows are ordered newest-first and include enough metadata for
        status and dashboard summaries without consulting process-local state.
        """
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            query = """
                SELECT
                    run_id,
                    parent_run_id,
                    reviews_workflow_id,
                    review_target_modules,
                    author_model,
                    provider_slug,
                    model_slug,
                    status,
                    failure_code,
                    failure_category,
                    failure_zone,
                    is_retryable,
                    is_transient,
                    latency_ms,
                    cost_usd,
                    input_tokens,
                    output_tokens,
                    attempts,
                    retry_count,
                    tool_use_count,
                    cache_read_tokens,
                    cache_creation_tokens,
                    duration_api_ms,
                    task_type,
                    workflow_label,
                    capabilities,
                    label,
                    adapter_type,
                    created_at
                FROM workflow_metrics
            """
            params: list[Any] = []
            if days is not None:
                cutoff = _utc_now() - timedelta(days=max(0, days))
                query += " WHERE created_at >= $1"
                params.append(cutoff)

            query += f" ORDER BY created_at DESC, run_id DESC LIMIT ${len(params) + 1}"
            params.append(max(0, int(limit)))

            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    def recent_workflows(
        self,
        *,
        limit: int = 20,
        days: int | None = None,
    ) -> list[dict[str, Any]]:
        """Synchronous wrapper for recent_workflows_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self.recent_workflows_async(limit=limit, days=days)
            )
        finally:
            loop.close()

    async def recent_route_outcomes_async(
        self,
        *,
        provider_slug: str,
        model_slug: str | None = None,
        adapter_type: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return recent workflow metrics rows for one provider route."""
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            query = """
                SELECT
                    run_id,
                    provider_slug,
                    model_slug,
                    adapter_type,
                    status,
                    failure_code,
                    failure_category,
                    latency_ms,
                    created_at
                FROM workflow_metrics
                WHERE provider_slug = $1
            """
            params: list[Any] = [provider_slug]
            if model_slug is not None:
                query += " AND model_slug = $2"
                params.append(model_slug)
                if adapter_type is not None:
                    query += " AND adapter_type = $3"
                    params.append(adapter_type)
            elif adapter_type is not None:
                query += " AND adapter_type = $2"
                params.append(adapter_type)

            query += """
                ORDER BY created_at DESC, run_id DESC
                LIMIT $%d
            """ % (len(params) + 1)
            params.append(max(0, int(limit)))
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    def recent_route_outcomes(
        self,
        *,
        provider_slug: str,
        model_slug: str | None = None,
        adapter_type: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Synchronous wrapper for recent_route_outcomes_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self.recent_route_outcomes_async(
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    adapter_type=adapter_type,
                    limit=limit,
                )
            )
        finally:
            loop.close()

    async def provider_slugs_async(self) -> list[str]:
        """Return distinct provider slugs from workflow_metrics."""
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            rows = await conn.fetch(
                """
                SELECT DISTINCT provider_slug
                  FROM workflow_metrics
                 WHERE provider_slug IS NOT NULL
                   AND provider_slug <> ''
                 ORDER BY provider_slug
                """
            )
            return [str(row["provider_slug"]) for row in rows if row["provider_slug"]]

    def provider_slugs(self) -> list[str]:
        """Synchronous wrapper for provider_slugs_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.provider_slugs_async())
        finally:
            loop.close()

    async def hourly_workflow_volume_async(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Get workflow volume per hour.

        Args:
            days: Number of days to look back (default 7).

        Returns:
            List of dicts with keys: hour, count.
        """
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            cutoff = _utc_now() - timedelta(days=days)
            rows = await conn.fetch(
                """
                SELECT
                    DATE_TRUNC('hour', created_at) as hour,
                    COUNT(*) as count
                FROM workflow_metrics
                WHERE created_at >= $1
                GROUP BY DATE_TRUNC('hour', created_at)
                ORDER BY hour DESC
                """,
                cutoff,
            )
            return [
                {
                    "hour": row["hour"].isoformat() if row["hour"] else None,
                    "count": row["count"],
                }
                for row in rows
            ]

    def hourly_workflow_volume(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Synchronous wrapper for hourly_workflow_volume_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.hourly_workflow_volume_async(days=days))
        finally:
            loop.close()

    async def capability_distribution_async(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Get workflow distribution by capability.

        Args:
            days: Number of days to look back (default 7).

        Returns:
            List of dicts with keys: capability, count.
        """
        async with self._connection() as conn:
            await self._ensure_schema(conn=conn)
            cutoff = _utc_now() - timedelta(days=days)
            rows = await conn.fetch(
                """
                SELECT
                    jsonb_array_elements_text(
                        CASE
                            WHEN NULLIF(BTRIM(capabilities::text), '') IS NULL THEN '[]'::jsonb
                            ELSE capabilities::jsonb
                        END
                    ) as capability,
                    COUNT(*) as count
                FROM workflow_metrics
                WHERE created_at >= $1 AND capabilities IS NOT NULL
                GROUP BY capability
                ORDER BY count DESC
                """,
                cutoff,
            )
            return [dict(row) for row in rows]

    def capability_distribution(
        self,
        *,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Synchronous wrapper for capability_distribution_async."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.capability_distribution_async(days=days))
        finally:
            loop.close()


def get_workflow_metrics_view(*, db_url: str | None = None) -> WorkflowMetricsView:
    """Return the module-level WorkflowMetricsView singleton.

    Creates it on first call. Safe for concurrent use.

    Args:
        db_url: Optional Postgres URL. If provided on first call, sets the
            connection URL. Ignored on subsequent calls.
    """
    global _WORKFLOW_METRICS_VIEW

    if _WORKFLOW_METRICS_VIEW is None:
        with _WORKFLOW_METRICS_LOCK:
            if _WORKFLOW_METRICS_VIEW is None:
                _WORKFLOW_METRICS_VIEW = WorkflowMetricsView(db_url=db_url)

    return _WORKFLOW_METRICS_VIEW
