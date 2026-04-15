"""Runtime boundary for surface catalog authority reads and writes."""

from __future__ import annotations

from typing import Any

from storage.postgres.surface_catalog_repository import (
    list_surface_catalog_records,
    load_surface_catalog_record,
    retire_surface_catalog_record,
    upsert_surface_catalog_record,
)
from storage.postgres.validators import PostgresWriteError


class SurfaceCatalogBoundaryError(RuntimeError):
    """Raised when surface catalog ownership rejects a request."""

    def __init__(self, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def _raise_storage_boundary(exc: PostgresWriteError) -> None:
    status_code = 400 if exc.reason_code.endswith("invalid_submission") else 500
    raise SurfaceCatalogBoundaryError(str(exc), status_code=status_code) from exc


def list_surface_catalog_items(
    conn: Any,
    *,
    surface_name: str = "moon",
    include_disabled: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    try:
        return list_surface_catalog_records(
            conn,
            surface_name=surface_name,
            include_disabled=include_disabled,
            limit=limit,
        )
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)


def get_surface_catalog_item(conn: Any, *, catalog_item_id: str) -> dict[str, Any]:
    try:
        row = load_surface_catalog_record(conn, catalog_item_id=catalog_item_id)
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    if row is None:
        raise SurfaceCatalogBoundaryError(f"Catalog item not found: {catalog_item_id}", status_code=404)
    return row


def upsert_surface_catalog_item(conn: Any, *, item: dict[str, Any]) -> dict[str, Any]:
    try:
        return upsert_surface_catalog_record(conn, item=item)
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)


def retire_surface_catalog_item(conn: Any, *, catalog_item_id: str) -> dict[str, Any]:
    try:
        row = retire_surface_catalog_record(conn, catalog_item_id=catalog_item_id)
    except PostgresWriteError as exc:
        _raise_storage_boundary(exc)
    if row is None:
        raise SurfaceCatalogBoundaryError(f"Catalog item not found: {catalog_item_id}", status_code=404)
    return row


__all__ = [
    "SurfaceCatalogBoundaryError",
    "get_surface_catalog_item",
    "list_surface_catalog_items",
    "retire_surface_catalog_item",
    "upsert_surface_catalog_item",
]
