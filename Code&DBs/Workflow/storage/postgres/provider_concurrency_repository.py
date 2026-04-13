"""Explicit async Postgres repository for provider concurrency authority."""

from __future__ import annotations

from typing import Any

from .validators import PostgresWriteError, _require_text

DEFAULT_PROVIDER_CONCURRENCY_LIMITS: dict[str, int] = {
    "anthropic": 4,
    "openai": 4,
    "google": 8,
}

DEFAULT_PROVIDER_COST_WEIGHT: float = 1.0

_CREATE_PROVIDER_CONCURRENCY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS provider_concurrency (
    provider_slug TEXT PRIMARY KEY,
    max_concurrent INTEGER NOT NULL DEFAULT 4,
    active_slots REAL NOT NULL DEFAULT 0.0,
    cost_weight_default REAL NOT NULL DEFAULT 1.0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def _require_positive_number(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a positive number",
            details={"field": field_name},
        )
    number = float(value)
    if number <= 0:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            f"{field_name} must be a positive number",
            details={"field": field_name},
        )
    return number


def _normalize_stale_after_seconds(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "stale_after_s must be a non-negative number",
            details={"field": "stale_after_s"},
        )
    return max(0, int(float(value)))


class PostgresProviderConcurrencyRepository:
    """Owns provider concurrency bootstrap and slot-state persistence."""

    async def ensure_schema(self, conn: Any) -> None:
        await conn.execute(_CREATE_PROVIDER_CONCURRENCY_TABLE_SQL)

    async def ensure_provider(self, conn: Any, *, provider_slug: str) -> None:
        slug = _require_text(provider_slug, field_name="provider_slug")
        max_concurrent = DEFAULT_PROVIDER_CONCURRENCY_LIMITS.get(slug, 4)
        await conn.execute(
            """
            INSERT INTO provider_concurrency
                (provider_slug, max_concurrent, active_slots, cost_weight_default, updated_at)
            VALUES ($1, $2, 0.0, $3, NOW())
            ON CONFLICT (provider_slug) DO NOTHING
            """,
            slug,
            max_concurrent,
            DEFAULT_PROVIDER_COST_WEIGHT,
        )

    async def ensure_default_providers(self, conn: Any) -> None:
        for provider_slug in DEFAULT_PROVIDER_CONCURRENCY_LIMITS:
            await self.ensure_provider(conn, provider_slug=provider_slug)

    async def reap_stale_slots(
        self,
        conn: Any,
        *,
        provider_slug: str,
        stale_after_s: float,
    ) -> None:
        await conn.execute(
            """
            UPDATE provider_concurrency
            SET active_slots = 0.0,
                updated_at = NOW()
            WHERE provider_slug = $1
              AND updated_at < NOW() - ($2 || ' seconds')::INTERVAL
              AND active_slots > 0
            """,
            _require_text(provider_slug, field_name="provider_slug"),
            str(_normalize_stale_after_seconds(stale_after_s)),
        )

    async def try_acquire_slot(
        self,
        conn: Any,
        *,
        provider_slug: str,
        cost_weight: float,
    ) -> bool:
        slug = _require_text(provider_slug, field_name="provider_slug")
        normalized_cost_weight = _require_positive_number(
            cost_weight,
            field_name="cost_weight",
        )
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT max_concurrent, active_slots
                FROM provider_concurrency
                WHERE provider_slug = $1
                FOR UPDATE NOWAIT
                """,
                slug,
            )
            if row is None:
                return False

            if float(row["active_slots"]) + normalized_cost_weight > float(row["max_concurrent"]):
                return False

            await conn.execute(
                """
                UPDATE provider_concurrency
                SET active_slots = active_slots + $2,
                    updated_at = NOW()
                WHERE provider_slug = $1
                """,
                slug,
                normalized_cost_weight,
            )
            return True

    async def release_slot(
        self,
        conn: Any,
        *,
        provider_slug: str,
        cost_weight: float,
    ) -> None:
        await conn.execute(
            """
            UPDATE provider_concurrency
            SET active_slots = GREATEST(0.0, active_slots - $2),
                updated_at = NOW()
            WHERE provider_slug = $1
            """,
            _require_text(provider_slug, field_name="provider_slug"),
            _require_positive_number(cost_weight, field_name="cost_weight"),
        )

    async def fetch_slot_status(self, conn: Any) -> dict[str, dict[str, float | int | str]]:
        rows = await conn.fetch(
            """
            SELECT provider_slug, max_concurrent, active_slots, cost_weight_default
            FROM provider_concurrency
            ORDER BY provider_slug
            """
        )
        return {
            str(row["provider_slug"]): {
                "provider_slug": str(row["provider_slug"]),
                "max_concurrent": int(row["max_concurrent"]),
                "active_slots": float(row["active_slots"]),
                "cost_weight_default": float(row["cost_weight_default"]),
            }
            for row in rows
        }

    async def has_capacity(self, conn: Any, *, provider_slug: str) -> bool:
        row = await conn.fetchrow(
            """
            SELECT max_concurrent, active_slots
            FROM provider_concurrency
            WHERE provider_slug = $1
            """,
            _require_text(provider_slug, field_name="provider_slug"),
        )
        if row is None:
            return True
        return float(row["active_slots"]) < float(row["max_concurrent"])
