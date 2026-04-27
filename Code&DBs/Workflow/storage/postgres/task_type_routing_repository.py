"""Explicit Postgres repository for task-type routing state and outcomes."""

from __future__ import annotations

from typing import Any, Mapping, cast

from .validators import _require_text


class PostgresTaskTypeRoutingRepository:
    """Owns durable task routing rows and route-health mutations."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def load_routes_for_task(self, *, task_type: str) -> tuple[Mapping[str, Any], ...]:
        rows = self._conn.execute(
            "SELECT * FROM task_type_routing WHERE task_type = $1",
            _require_text(task_type, field_name="task_type"),
        )
        return tuple(cast(Mapping[str, Any], row) for row in rows or ())

    def load_routes(self, *, task_type: str | None = None) -> tuple[Mapping[str, Any], ...]:
        if task_type:
            rows = self._conn.execute(
                "SELECT * FROM task_type_routing WHERE task_type = $1 ORDER BY rank",
                _require_text(task_type, field_name="task_type"),
            )
        else:
            rows = self._conn.execute("SELECT * FROM task_type_routing ORDER BY task_type, rank")
        return tuple(cast(Mapping[str, Any], row) for row in rows or ())

    def route_exists(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
    ) -> bool:
        return bool(
            self._conn.execute(
                "SELECT 1 FROM task_type_routing WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3 LIMIT 1",
                _require_text(task_type, field_name="task_type"),
                _require_text(provider_slug, field_name="provider_slug"),
                _require_text(model_slug, field_name="model_slug"),
            )
        )

    def load_permitted_task_type_for_model(
        self,
        *,
        provider_slug: str,
        model_slug: str,
    ) -> str | None:
        rows = self._conn.execute(
            "SELECT task_type FROM task_type_routing WHERE provider_slug = $1 AND model_slug = $2 AND permitted = true LIMIT 1",
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
        )
        if not rows:
            return None
        task_type = rows[0].get("task_type")
        return None if task_type is None else str(task_type)

    def load_route_permission(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
    ) -> Mapping[str, Any] | None:
        rows = self._conn.execute(
            "SELECT permitted, rationale FROM task_type_routing WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3 LIMIT 1",
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
        )
        if not rows:
            return None
        return cast(Mapping[str, Any], rows[0])

    def load_routes_with_health(self) -> tuple[Mapping[str, Any], ...]:
        rows = self._conn.execute(
            "SELECT task_type, provider_slug, model_slug, permitted FROM task_type_routing"
        )
        return tuple(cast(Mapping[str, Any], row) for row in rows or ())

    def disable_route(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        rationale: str,
    ) -> None:
        self._conn.execute(
            """UPDATE task_type_routing SET permitted = false,
                      rationale = $4
               WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3""",
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
            _require_text(rationale, field_name="rationale"),
        )

    def upsert_derived_route(
        self,
        *,
        task_type: str,
        model_slug: str,
        provider_slug: str,
        permitted: bool,
        rank: int,
        benchmark_score: float,
        benchmark_name: str,
        cost_per_m_tokens: float,
        rationale: str,
        route_tier: str | None,
        route_tier_rank: int,
        latency_class: str | None,
        latency_rank: int,
        route_health_score: float,
        observed_completed_count: int,
        observed_execution_failure_count: int,
        observed_external_failure_count: int,
        observed_config_failure_count: int,
        observed_downstream_failure_count: int,
        observed_downstream_bug_count: int,
        consecutive_internal_failures: int,
        last_failure_category: str,
        last_failure_zone: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO task_type_routing (
                task_type, sub_task_type, model_slug, provider_slug, transport_type,
                permitted, rank,
                benchmark_score, benchmark_name, cost_per_m_tokens, rationale,
                route_tier, route_tier_rank, latency_class, latency_rank, route_source,
                route_health_score, observed_completed_count, observed_execution_failure_count,
                observed_external_failure_count, observed_config_failure_count,
                observed_downstream_failure_count, observed_downstream_bug_count,
                consecutive_internal_failures, last_failure_category, last_failure_zone, updated_at
            ) VALUES (
                $1, '*', $2, $3, 'CLI', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                'derived', $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, now()
            )
            ON CONFLICT (task_type, sub_task_type, provider_slug, model_slug, transport_type) DO UPDATE SET
                permitted = EXCLUDED.permitted, rank = EXCLUDED.rank,
                benchmark_score = EXCLUDED.benchmark_score, benchmark_name = EXCLUDED.benchmark_name,
                cost_per_m_tokens = EXCLUDED.cost_per_m_tokens, rationale = EXCLUDED.rationale,
                route_tier = EXCLUDED.route_tier, route_tier_rank = EXCLUDED.route_tier_rank,
                latency_class = EXCLUDED.latency_class, latency_rank = EXCLUDED.latency_rank,
                updated_at = now()
            WHERE task_type_routing.route_source = 'derived'
            """,
            _require_text(task_type, field_name="task_type"),
            str(model_slug),
            _require_text(provider_slug, field_name="provider_slug"),
            bool(permitted),
            int(rank),
            float(benchmark_score),
            str(benchmark_name or ""),
            float(cost_per_m_tokens),
            str(rationale or ""),
            route_tier or None,
            int(route_tier_rank),
            latency_class or None,
            int(latency_rank),
            float(route_health_score),
            int(observed_completed_count),
            int(observed_execution_failure_count),
            int(observed_external_failure_count),
            int(observed_config_failure_count),
            int(observed_downstream_failure_count),
            int(observed_downstream_bug_count),
            int(consecutive_internal_failures),
            str(last_failure_category or ""),
            str(last_failure_zone or ""),
        )

    def update_explicit_benchmark_score(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        benchmark_score: float,
        benchmark_name: str,
    ) -> None:
        self._conn.execute(
            """
            UPDATE task_type_routing
            SET benchmark_score = $1,
                benchmark_name  = $2,
                updated_at      = now()
            WHERE task_type    = $3
              AND provider_slug = $4
              AND model_slug    = $5
              AND route_source  = 'explicit'
            """,
            float(benchmark_score),
            _require_text(benchmark_name, field_name="benchmark_name"),
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
        )

    def load_outcome_state(self, *, task_type: str, provider_slug: str, model_slug: str) -> Mapping[str, Any] | None:
        rows = self._conn.execute(
            "SELECT rank, recent_failures FROM task_type_routing WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3",
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
        )
        if not rows:
            return None
        return cast(Mapping[str, Any], rows[0])

    def load_next_permitted_route(self, *, task_type: str, current_rank: int) -> Mapping[str, Any] | None:
        rows = self._conn.execute(
            """SELECT provider_slug, model_slug, rank FROM task_type_routing
               WHERE task_type = $1 AND permitted = true AND rank > $2
               ORDER BY rank ASC LIMIT 1""",
            _require_text(task_type, field_name="task_type"),
            int(current_rank),
        )
        if not rows:
            return None
        return cast(Mapping[str, Any], rows[0])

    def set_route_rank(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        rank: int,
    ) -> None:
        self._conn.execute(
            """UPDATE task_type_routing SET rank = $1, updated_at = now()
               WHERE task_type = $2 AND provider_slug = $3 AND model_slug = $4""",
            int(rank),
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
        )

    def record_success(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        max_route_health: float,
        success_health_bump: float,
    ) -> None:
        self._conn.execute(
            """UPDATE task_type_routing
               SET recent_successes = recent_successes + 1,
                   recent_failures = 0,
                   consecutive_internal_failures = 0,
                   observed_completed_count = observed_completed_count + 1,
                   route_health_score = LEAST($4, route_health_score + $5),
                   last_success_at = now(),
                   last_outcome_at = now(),
                   updated_at = now()
               WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3""",
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
            float(max_route_health),
            float(success_health_bump),
        )

    def record_failure_count_only(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        counter_column: str,
        failure_category: str,
        failure_zone: str,
    ) -> None:
        self._conn.execute(
            f"""UPDATE task_type_routing
                SET {counter_column} = {counter_column} + 1,
                    last_failure_category = $4,
                    last_failure_zone = $5,
                    last_outcome_at = now(),
                    updated_at = now()
                WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3""",
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
            str(failure_category or ""),
            str(failure_zone or ""),
        )

    def record_internal_failure(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        penalty: float,
        failure_category: str,
        failure_zone: str,
        min_route_health: float,
    ) -> None:
        self._conn.execute(
            """UPDATE task_type_routing
               SET recent_failures = recent_failures + 1,
                   recent_successes = 0,
                   consecutive_internal_failures = consecutive_internal_failures + 1,
                   observed_execution_failure_count = observed_execution_failure_count + 1,
                   route_health_score = GREATEST($7, route_health_score - $4),
                   last_failure_at = now(),
                   last_failure_category = $5,
                   last_failure_zone = $6,
                   last_outcome_at = now(),
                   updated_at = now()
               WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3""",
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
            float(penalty),
            str(failure_category or ""),
            str(failure_zone or ""),
            float(min_route_health),
        )

    def record_review_success(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        max_route_health: float,
        review_success_bump: float,
    ) -> None:
        self._conn.execute(
            """UPDATE task_type_routing
               SET route_health_score = LEAST($4, route_health_score + $5),
                   last_reviewed_at = now(),
                   updated_at = now()
               WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3""",
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
            float(max_route_health),
            float(review_success_bump),
        )

    def record_review_failure(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        bug_count: int,
        review_penalty: float,
        min_route_health: float,
    ) -> None:
        self._conn.execute(
            """UPDATE task_type_routing
               SET observed_downstream_failure_count = observed_downstream_failure_count + 1,
                   observed_downstream_bug_count = observed_downstream_bug_count + $4,
                   route_health_score = GREATEST($6, route_health_score - $5),
                   last_reviewed_at = now(),
                   updated_at = now()
               WHERE task_type = $1 AND provider_slug = $2 AND model_slug = $3""",
            _require_text(task_type, field_name="task_type"),
            _require_text(provider_slug, field_name="provider_slug"),
            _require_text(model_slug, field_name="model_slug"),
            int(bug_count),
            float(review_penalty),
            float(min_route_health),
        )


__all__ = ["PostgresTaskTypeRoutingRepository"]
