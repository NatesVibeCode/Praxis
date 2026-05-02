"""Explicit Postgres repository for frontdoor surface-usage telemetry."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from .validators import (
    _encode_jsonb,
    _optional_text,
    _require_nonnegative_int,
    _require_text,
    _require_utc,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_today() -> date:
    return _utc_now().date()


def _event_window_start(days: int) -> datetime:
    start_date = _utc_today() - timedelta(days=days - 1)
    return datetime.combine(start_date, time.min, tzinfo=timezone.utc)


def _optional_text_or_empty(value: object, *, field_name: str) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return text


class PostgresWorkflowSurfaceUsageRepository:
    """Owns canonical workflow surface-usage reads and writes."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def record_invocation(
        self,
        *,
        surface_kind: str,
        transport_kind: str,
        entrypoint_kind: str,
        entrypoint_name: str,
        caller_kind: str = "direct",
        http_method: str | None = None,
        status_code: int,
        occurred_at: datetime | None = None,
    ) -> None:
        normalized_surface_kind = _require_text(surface_kind, field_name="surface_kind")
        normalized_transport_kind = _require_text(
            transport_kind,
            field_name="transport_kind",
        )
        normalized_entrypoint_kind = _require_text(
            entrypoint_kind,
            field_name="entrypoint_kind",
        )
        normalized_entrypoint_name = _require_text(
            entrypoint_name,
            field_name="entrypoint_name",
        )
        normalized_caller_kind = _require_text(caller_kind, field_name="caller_kind")
        normalized_http_method = _optional_text_or_empty(
            http_method,
            field_name="http_method",
        )
        normalized_status_code = _require_nonnegative_int(
            status_code,
            field_name="status_code",
        )
        occurred_at_utc = _require_utc(occurred_at or _utc_now(), field_name="occurred_at")
        usage_date = occurred_at_utc.date()
        success_count = 1 if 200 <= normalized_status_code < 400 else 0
        client_error_count = 1 if 400 <= normalized_status_code < 500 else 0
        server_error_count = 1 if normalized_status_code >= 500 else 0

        self._conn.execute(
            """
            INSERT INTO workflow_surface_usage_daily (
                usage_date,
                surface_kind,
                transport_kind,
                entrypoint_kind,
                entrypoint_name,
                caller_kind,
                http_method,
                invocation_count,
                success_count,
                client_error_count,
                server_error_count,
                first_invoked_at,
                last_invoked_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, 1, $8, $9, $10, $11, $11
            )
            ON CONFLICT (
                usage_date,
                surface_kind,
                transport_kind,
                entrypoint_kind,
                entrypoint_name,
                caller_kind,
                http_method
            )
            DO UPDATE SET
                invocation_count = workflow_surface_usage_daily.invocation_count + 1,
                success_count = workflow_surface_usage_daily.success_count + EXCLUDED.success_count,
                client_error_count = workflow_surface_usage_daily.client_error_count + EXCLUDED.client_error_count,
                server_error_count = workflow_surface_usage_daily.server_error_count + EXCLUDED.server_error_count,
                first_invoked_at = LEAST(
                    workflow_surface_usage_daily.first_invoked_at,
                    EXCLUDED.first_invoked_at
                ),
                last_invoked_at = GREATEST(
                    workflow_surface_usage_daily.last_invoked_at,
                    EXCLUDED.last_invoked_at
                )
            """,
            usage_date,
            normalized_surface_kind,
            normalized_transport_kind,
            normalized_entrypoint_kind,
            normalized_entrypoint_name,
            normalized_caller_kind,
            normalized_http_method,
            success_count,
            client_error_count,
            server_error_count,
            occurred_at_utc,
        )

    def record_event(
        self,
        *,
        surface_kind: str,
        transport_kind: str,
        entrypoint_kind: str,
        entrypoint_name: str,
        caller_kind: str = "direct",
        http_method: str | None = None,
        status_code: int,
        result_state: str = "ok",
        reason_code: str | None = None,
        routed_to: str | None = None,
        workflow_id: str | None = None,
        run_id: str | None = None,
        job_label: str | None = None,
        request_id: str | None = None,
        client_version: str | None = None,
        payload_size_bytes: int = 0,
        response_size_bytes: int = 0,
        prose_chars: int = 0,
        query_chars: int = 0,
        result_count: int = 0,
        unresolved_count: int = 0,
        capability_count: int = 0,
        reference_count: int = 0,
        materialized_job_count: int = 0,
        trigger_count: int = 0,
        definition_hash: str | None = None,
        definition_revision: str | None = None,
        task_class: str | None = None,
        planner_required: bool = False,
        llm_used: bool = False,
        has_current_plan: bool = False,
        metadata: dict[str, Any] | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        normalized_surface_kind = _require_text(surface_kind, field_name="surface_kind")
        normalized_transport_kind = _require_text(
            transport_kind,
            field_name="transport_kind",
        )
        normalized_entrypoint_kind = _require_text(
            entrypoint_kind,
            field_name="entrypoint_kind",
        )
        normalized_entrypoint_name = _require_text(
            entrypoint_name,
            field_name="entrypoint_name",
        )
        normalized_caller_kind = _require_text(caller_kind, field_name="caller_kind")
        normalized_http_method = _optional_text_or_empty(
            http_method,
            field_name="http_method",
        )
        normalized_status_code = _require_nonnegative_int(
            status_code,
            field_name="status_code",
        )
        normalized_result_state = _require_text(
            result_state,
            field_name="result_state",
        )
        occurred_at_utc = _require_utc(occurred_at or _utc_now(), field_name="occurred_at")

        self._conn.execute(
            """
            INSERT INTO workflow_surface_usage_events (
                occurred_at,
                surface_kind,
                transport_kind,
                entrypoint_kind,
                entrypoint_name,
                caller_kind,
                http_method,
                status_code,
                result_state,
                reason_code,
                routed_to,
                workflow_id,
                run_id,
                job_label,
                request_id,
                client_version,
                payload_size_bytes,
                response_size_bytes,
                prose_chars,
                query_chars,
                result_count,
                unresolved_count,
                capability_count,
                reference_count,
                materialized_job_count,
                trigger_count,
                definition_hash,
                definition_revision,
                task_class,
                planner_required,
                llm_used,
                has_current_plan,
                metadata
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33::jsonb
            )
            """,
            occurred_at_utc,
            normalized_surface_kind,
            normalized_transport_kind,
            normalized_entrypoint_kind,
            normalized_entrypoint_name,
            normalized_caller_kind,
            normalized_http_method,
            normalized_status_code,
            normalized_result_state,
            _optional_text_or_empty(reason_code, field_name="reason_code"),
            _optional_text_or_empty(routed_to, field_name="routed_to"),
            _optional_text_or_empty(workflow_id, field_name="workflow_id"),
            _optional_text_or_empty(run_id, field_name="run_id"),
            _optional_text_or_empty(job_label, field_name="job_label"),
            _optional_text_or_empty(request_id, field_name="request_id"),
            _optional_text_or_empty(client_version, field_name="client_version"),
            _require_nonnegative_int(
                payload_size_bytes,
                field_name="payload_size_bytes",
            ),
            _require_nonnegative_int(
                response_size_bytes,
                field_name="response_size_bytes",
            ),
            _require_nonnegative_int(prose_chars, field_name="prose_chars"),
            _require_nonnegative_int(query_chars, field_name="query_chars"),
            _require_nonnegative_int(result_count, field_name="result_count"),
            _require_nonnegative_int(unresolved_count, field_name="unresolved_count"),
            _require_nonnegative_int(capability_count, field_name="capability_count"),
            _require_nonnegative_int(reference_count, field_name="reference_count"),
            _require_nonnegative_int(materialized_job_count, field_name="materialized_job_count"),
            _require_nonnegative_int(trigger_count, field_name="trigger_count"),
            _optional_text_or_empty(definition_hash, field_name="definition_hash"),
            _optional_text_or_empty(definition_revision, field_name="definition_revision"),
            _optional_text_or_empty(task_class, field_name="task_class"),
            bool(planner_required),
            bool(llm_used),
            bool(has_current_plan),
            _encode_jsonb(metadata or {}, field_name="metadata"),
        )
        self.record_invocation(
            surface_kind=normalized_surface_kind,
            transport_kind=normalized_transport_kind,
            entrypoint_kind=normalized_entrypoint_kind,
            entrypoint_name=normalized_entrypoint_name,
            caller_kind=normalized_caller_kind,
            http_method=normalized_http_method,
            status_code=normalized_status_code,
            occurred_at=occurred_at_utc,
        )

    def list_usage_rollup(
        self,
        *,
        days: int = 30,
        entrypoint_name: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_days = _require_nonnegative_int(days, field_name="days")
        if normalized_days <= 0:
            return []
        normalized_entrypoint_name = _optional_text(
            entrypoint_name,
            field_name="entrypoint_name",
        )
        window_start = _utc_today() - timedelta(days=normalized_days - 1)
        sql = """
            SELECT
                surface_kind,
                transport_kind,
                entrypoint_kind,
                entrypoint_name,
                caller_kind,
                http_method,
                SUM(invocation_count) AS invocation_count,
                SUM(success_count) AS success_count,
                SUM(client_error_count) AS client_error_count,
                SUM(server_error_count) AS server_error_count,
                MIN(first_invoked_at) AS first_invoked_at,
                MAX(last_invoked_at) AS last_invoked_at
            FROM workflow_surface_usage_daily
            WHERE usage_date >= $1
        """
        params: list[Any] = [window_start]
        if normalized_entrypoint_name is not None:
            sql += " AND entrypoint_name = $2"
            params.append(normalized_entrypoint_name)
        sql += """
            GROUP BY
                surface_kind,
                transport_kind,
                entrypoint_kind,
                entrypoint_name,
                caller_kind,
                http_method
            ORDER BY
                SUM(invocation_count) DESC,
                entrypoint_name ASC,
                caller_kind ASC,
                http_method ASC
        """
        rows = self._conn.execute(sql, *params) or []
        return [dict(row) for row in rows]

    def list_usage_daily(
        self,
        *,
        days: int = 30,
        entrypoint_name: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_days = _require_nonnegative_int(days, field_name="days")
        if normalized_days <= 0:
            return []
        normalized_entrypoint_name = _optional_text(
            entrypoint_name,
            field_name="entrypoint_name",
        )
        window_start = _utc_today() - timedelta(days=normalized_days - 1)
        sql = """
            SELECT
                usage_date,
                surface_kind,
                transport_kind,
                entrypoint_kind,
                entrypoint_name,
                caller_kind,
                http_method,
                invocation_count,
                success_count,
                client_error_count,
                server_error_count,
                first_invoked_at,
                last_invoked_at
            FROM workflow_surface_usage_daily
            WHERE usage_date >= $1
        """
        params: list[Any] = [window_start]
        if normalized_entrypoint_name is not None:
            sql += " AND entrypoint_name = $2"
            params.append(normalized_entrypoint_name)
        sql += """
            ORDER BY
                usage_date DESC,
                entrypoint_name ASC,
                caller_kind ASC,
                http_method ASC
        """
        rows = self._conn.execute(sql, *params) or []
        return [dict(row) for row in rows]

    def list_usage_events(
        self,
        *,
        days: int = 30,
        entrypoint_name: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        normalized_days = _require_nonnegative_int(days, field_name="days")
        normalized_limit = _require_nonnegative_int(limit, field_name="limit")
        if normalized_days <= 0 or normalized_limit <= 0:
            return []
        normalized_entrypoint_name = _optional_text(
            entrypoint_name,
            field_name="entrypoint_name",
        )
        window_start = _event_window_start(normalized_days)
        sql = """
            SELECT
                event_id,
                occurred_at,
                surface_kind,
                transport_kind,
                entrypoint_kind,
                entrypoint_name,
                caller_kind,
                http_method,
                status_code,
                result_state,
                reason_code,
                routed_to,
                workflow_id,
                run_id,
                job_label,
                request_id,
                client_version,
                payload_size_bytes,
                response_size_bytes,
                prose_chars,
                query_chars,
                result_count,
                unresolved_count,
                capability_count,
                reference_count,
                materialized_job_count,
                trigger_count,
                definition_hash,
                definition_revision,
                task_class,
                planner_required,
                llm_used,
                has_current_plan,
                metadata
            FROM workflow_surface_usage_events
            WHERE occurred_at >= $1
        """
        params: list[Any] = [window_start]
        if normalized_entrypoint_name is not None:
            sql += " AND entrypoint_name = $2"
            params.append(normalized_entrypoint_name)
        params.append(normalized_limit)
        sql += f"""
            ORDER BY
                occurred_at DESC,
                event_id DESC
            LIMIT ${len(params)}
        """
        rows = self._conn.execute(sql, *params) or []
        return [dict(row) for row in rows]

    def summarize_query_routing(
        self,
        *,
        days: int = 30,
    ) -> list[dict[str, Any]]:
        normalized_days = _require_nonnegative_int(days, field_name="days")
        if normalized_days <= 0:
            return []
        rows = self._conn.execute(
            """
            SELECT
                entrypoint_name,
                caller_kind,
                routed_to,
                result_state,
                reason_code,
                COUNT(*) AS invocation_count,
                SUM(CASE WHEN status_code >= 200 AND status_code < 400 THEN 1 ELSE 0 END) AS success_count,
                AVG(query_chars)::float AS average_query_chars,
                SUM(result_count) AS total_result_count
            FROM workflow_surface_usage_events
            WHERE occurred_at >= $1
              AND entrypoint_name IN ('/query', 'praxis_query')
            GROUP BY
                entrypoint_name,
                caller_kind,
                routed_to,
                result_state,
                reason_code
            ORDER BY
                COUNT(*) DESC,
                entrypoint_name ASC,
                caller_kind ASC,
                routed_to ASC,
                result_state ASC,
                reason_code ASC
            """,
            _event_window_start(normalized_days),
        ) or []
        return [dict(row) for row in rows]

    def summarize_builder_funnels(
        self,
        *,
        days: int = 30,
    ) -> dict[str, Any]:
        normalized_days = _require_nonnegative_int(days, field_name="days")
        if normalized_days <= 0:
            return {
                "definition_count": 0,
                "compile_lane_count": 0,
                "refine_lane_count": 0,
                "plan_count": 0,
                "commit_count": 0,
                "run_count": 0,
                "compile_to_plan_count": 0,
                "plan_to_commit_count": 0,
                "commit_to_run_count": 0,
                "full_path_count": 0,
            }
        rows = self._conn.execute(
            """
            WITH stage_by_definition AS (
                SELECT
                    definition_hash,
                    BOOL_OR(entrypoint_name = '/api/compile') AS compile_seen,
                    BOOL_OR(entrypoint_name = '/api/refine-definition') AS refine_seen,
                    BOOL_OR(entrypoint_name = '/api/plan') AS plan_seen,
                    BOOL_OR(entrypoint_name = '/api/commit') AS commit_seen,
                    BOOL_OR(entrypoint_name = '/api/trigger/:workflow_id') AS run_seen
                FROM workflow_surface_usage_events
                WHERE occurred_at >= $1
                  AND status_code >= 200
                  AND status_code < 400
                  AND definition_hash <> ''
                  AND entrypoint_name IN (
                      '/api/compile',
                      '/api/refine-definition',
                      '/api/plan',
                      '/api/commit',
                      '/api/trigger/:workflow_id'
                  )
                GROUP BY definition_hash
            )
            SELECT
                COUNT(*) AS definition_count,
                SUM(CASE WHEN compile_seen OR refine_seen THEN 1 ELSE 0 END) AS compile_lane_count,
                SUM(CASE WHEN refine_seen THEN 1 ELSE 0 END) AS refine_lane_count,
                SUM(CASE WHEN plan_seen THEN 1 ELSE 0 END) AS plan_count,
                SUM(CASE WHEN commit_seen THEN 1 ELSE 0 END) AS commit_count,
                SUM(CASE WHEN run_seen THEN 1 ELSE 0 END) AS run_count,
                SUM(CASE WHEN (compile_seen OR refine_seen) AND plan_seen THEN 1 ELSE 0 END) AS compile_to_plan_count,
                SUM(CASE WHEN plan_seen AND commit_seen THEN 1 ELSE 0 END) AS plan_to_commit_count,
                SUM(CASE WHEN commit_seen AND run_seen THEN 1 ELSE 0 END) AS commit_to_run_count,
                SUM(CASE WHEN (compile_seen OR refine_seen) AND plan_seen AND commit_seen AND run_seen THEN 1 ELSE 0 END) AS full_path_count
            FROM stage_by_definition
            """,
            _event_window_start(normalized_days),
        ) or []
        row = dict(rows[0]) if rows else {}
        return {
            "definition_count": int(row.get("definition_count") or 0),
            "compile_lane_count": int(row.get("compile_lane_count") or 0),
            "refine_lane_count": int(row.get("refine_lane_count") or 0),
            "plan_count": int(row.get("plan_count") or 0),
            "commit_count": int(row.get("commit_count") or 0),
            "run_count": int(row.get("run_count") or 0),
            "compile_to_plan_count": int(row.get("compile_to_plan_count") or 0),
            "plan_to_commit_count": int(row.get("plan_to_commit_count") or 0),
            "commit_to_run_count": int(row.get("commit_to_run_count") or 0),
            "full_path_count": int(row.get("full_path_count") or 0),
        }


__all__ = ["PostgresWorkflowSurfaceUsageRepository"]
