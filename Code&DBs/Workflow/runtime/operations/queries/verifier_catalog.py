"""CQRS queries for verifier authority — catalog list + run history."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from storage.postgres.verifier_catalog_repository import (
    list_verification_runs,
    list_verifier_catalog,
)


class QueryVerifierCatalogList(BaseModel):
    """List verifier authority refs available to outcome gates."""

    enabled: bool | None = True
    limit: int = Field(default=100, ge=1, le=500)


def handle_verifier_catalog_list(
    query: QueryVerifierCatalogList,
    subsystems: Any,
) -> dict[str, Any]:
    items = list_verifier_catalog(
        subsystems.get_pg_conn(),
        enabled=query.enabled,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "verifier.catalog.list",
        "count": len(items),
        "items": items,
    }


class QueryVerifierRunsList(BaseModel):
    """List past verification_runs newest-first, optionally filtered.

    Filters compose with AND. All are optional. Use ``since_iso`` for a
    trailing-window read; pass ISO-8601 (e.g. ``2026-05-01T00:00:00Z``).
    Returns at most ``limit`` rows ordered by ``attempted_at DESC``.
    """

    verifier_ref: str | None = None
    target_kind: Literal["platform", "receipt", "run", "path"] | None = None
    target_ref: str | None = None
    status: Literal["passed", "failed", "error"] | None = None
    since_iso: str | None = None
    limit: int = Field(default=100, ge=1, le=500)


def handle_verifier_runs_list(
    query: QueryVerifierRunsList,
    subsystems: Any,
) -> dict[str, Any]:
    items = list_verification_runs(
        subsystems.get_pg_conn(),
        verifier_ref=query.verifier_ref,
        target_kind=query.target_kind,
        target_ref=query.target_ref,
        status=query.status,
        since_iso=query.since_iso,
        limit=query.limit,
    )
    return {
        "ok": True,
        "operation": "verifier.runs.list",
        "count": len(items),
        "items": items,
    }


__all__ = [
    "QueryVerifierCatalogList",
    "QueryVerifierRunsList",
    "handle_verifier_catalog_list",
    "handle_verifier_runs_list",
]
