"""Gateway-dispatched query wrappers for the primitive_catalog."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ListPrimitivesQuery(BaseModel):
    primitive_kind: str | None = None
    enabled_only: bool = True
    limit: int = 100


class GetPrimitiveQuery(BaseModel):
    primitive_slug: str


class ScanPrimitiveConsistencyQuery(BaseModel):
    primitive_slug: str | None = None


def handle_list_primitives(
    command: ListPrimitivesQuery,
    subsystems: Any,
) -> dict[str, Any]:
    from runtime.primitive_authority import (
        PrimitiveAuthorityError,
        list_primitives,
    )

    conn = subsystems.get_pg_conn()
    try:
        return list_primitives(
            conn,
            primitive_kind=command.primitive_kind,
            enabled_only=command.enabled_only,
            limit=command.limit,
        )
    except PrimitiveAuthorityError as exc:
        return {
            "status": "rejected",
            "error": str(exc),
            "reason_code": exc.reason_code,
            "details": exc.details,
        }


def handle_get_primitive(
    command: GetPrimitiveQuery,
    subsystems: Any,
) -> dict[str, Any]:
    from runtime.primitive_authority import (
        PrimitiveAuthorityError,
        get_primitive,
    )

    conn = subsystems.get_pg_conn()
    try:
        return get_primitive(conn, primitive_slug=command.primitive_slug)
    except PrimitiveAuthorityError as exc:
        return {
            "status": "rejected",
            "error": str(exc),
            "reason_code": exc.reason_code,
            "details": exc.details,
        }


def handle_scan_primitive_consistency(
    command: ScanPrimitiveConsistencyQuery,
    subsystems: Any,
) -> dict[str, Any]:
    """Walk the primitive catalog and report blueprint-vs-code drift."""

    from runtime.primitive_authority import list_primitives, get_primitive
    from runtime.primitive_consistency_scanner import scan_all_primitives

    conn = subsystems.get_pg_conn()
    if command.primitive_slug:
        primitive = get_primitive(conn, primitive_slug=command.primitive_slug)["primitive"]
        primitives = [primitive]
    else:
        primitives = list_primitives(conn, enabled_only=True, limit=500)["primitives"]
    return scan_all_primitives(conn=conn, primitives=primitives)
