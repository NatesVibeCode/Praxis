"""Explicit async Postgres repository for workflow metrics persistence."""

from __future__ import annotations

from typing import Any


class PostgresWorkflowMetricsRepository:
    """Owns canonical workflow_metrics inserts."""

    async def upsert_workflow_metric(
        self,
        conn: Any,
        *,
        run_id: str,
        parent_run_id: str | None,
        reviews_workflow_id: str | None,
        review_target_modules: str | None,
        author_model: str | None,
        provider_slug: str,
        model_slug: str | None,
        status: str,
        failure_code: str | None,
        failure_category: str | None,
        failure_zone: str | None,
        is_retryable: bool | None,
        is_transient: bool | None,
        latency_ms: int,
        cost_usd: float,
        input_tokens: int,
        output_tokens: int,
        attempts: int,
        retry_count: int,
        tool_use_count: int,
        cache_read_tokens: int,
        cache_creation_tokens: int,
        duration_api_ms: int,
        task_type: str | None,
        workflow_label: str | None,
        capabilities: str | None,
        label: str | None,
        adapter_type: str,
        created_at: Any,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO workflow_metrics (
                run_id, parent_run_id, reviews_workflow_id, review_target_modules,
                author_model, provider_slug, model_slug,
                status, failure_code, failure_category, failure_zone,
                is_retryable, is_transient, latency_ms, cost_usd,
                input_tokens, output_tokens, attempts, retry_count,
                tool_use_count, cache_read_tokens, cache_creation_tokens,
                duration_api_ms, task_type, workflow_label, capabilities,
                label, adapter_type, created_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
                $27, $28, $29
            )
            ON CONFLICT (run_id) DO NOTHING
            """,
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
            created_at,
        )


__all__ = ["PostgresWorkflowMetricsRepository"]
