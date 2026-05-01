"""CQRS queries for healer authority — catalog list + run history.

Mirrors verifier_catalog query module. Healer authority is the repair
side of the verifier subsystem: when a verifier fails, a bound healer
can attempt repair (e.g. healer.platform.schema_bootstrap when
verifier.platform.schema_authority fails). These queries surface the
healer registry and past healing_runs without requiring callers to
reach into runtime.verifier_authority directly.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from storage.postgres.verifier_catalog_repository import (
    list_healer_catalog,
    list_healing_runs,
)


class QueryHealerCatalogList(BaseModel):
    """List registered healer authority refs."""

    enabled: bool | None = True
    limit: int = Field(default=100, ge=1, le=500)


def handle_healer_catalog_list(
    query: QueryHealerCatalogList,
    subsystems: Any,
) -> dict[str, Any]:
    items = list_healer_catalog(
        subsystems.get_pg_conn(),
        enabled=query.enabled,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "healer.catalog.list",
        "count": len(items),
        "items": items,
    }


class QueryHealerRunsList(BaseModel):
    """List past healing_runs newest-first, optionally filtered.

    Filters compose with AND. All optional. Use ``since_iso`` for a
    trailing-window read; pass ISO-8601. Returns at most ``limit`` rows
    ordered by ``attempted_at DESC``.
    """

    healer_ref: str | None = None
    verifier_ref: str | None = None
    target_kind: Literal["platform", "receipt", "run", "path"] | None = None
    target_ref: str | None = None
    status: Literal["succeeded", "failed", "skipped", "error"] | None = None
    since_iso: str | None = None
    limit: int = Field(default=100, ge=1, le=500)


def handle_healer_runs_list(
    query: QueryHealerRunsList,
    subsystems: Any,
) -> dict[str, Any]:
    items = list_healing_runs(
        subsystems.get_pg_conn(),
        healer_ref=query.healer_ref,
        verifier_ref=query.verifier_ref,
        target_kind=query.target_kind,
        target_ref=query.target_ref,
        status=query.status,
        since_iso=query.since_iso,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "healer.runs.list",
        "count": len(items),
        "items": items,
    }


__all__ = [
    "QueryHealerCatalogList",
    "QueryHealerRunsList",
    "handle_healer_catalog_list",
    "handle_healer_runs_list",
]
