"""CQRS query for registered verifier authority refs."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from storage.postgres.verifier_catalog_repository import list_verifier_catalog


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


__all__ = [
    "QueryVerifierCatalogList",
    "handle_verifier_catalog_list",
]
