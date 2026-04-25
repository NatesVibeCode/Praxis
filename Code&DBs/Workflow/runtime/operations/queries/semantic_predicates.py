"""Gateway-dispatched query wrappers for the semantic predicate catalog."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ListSemanticPredicatesQuery(BaseModel):
    predicate_kind: str | None = None
    applies_to_kind: str | None = None
    applies_to_ref: str | None = None
    enabled_only: bool = True
    limit: int = 100


class GetSemanticPredicateQuery(BaseModel):
    predicate_slug: str


def handle_list_semantic_predicates(
    command: ListSemanticPredicatesQuery,
    subsystems: Any,
) -> dict[str, Any]:
    from runtime.semantic_predicate_authority import (
        SemanticPredicateAuthorityError,
        list_predicates,
    )

    conn = subsystems.get_pg_conn()
    try:
        return list_predicates(
            conn,
            predicate_kind=command.predicate_kind,
            applies_to_kind=command.applies_to_kind,
            applies_to_ref=command.applies_to_ref,
            enabled_only=command.enabled_only,
            limit=command.limit,
        )
    except SemanticPredicateAuthorityError as exc:
        return {
            "status": "rejected",
            "error": str(exc),
            "reason_code": exc.reason_code,
            "details": exc.details,
        }


def handle_get_semantic_predicate(
    command: GetSemanticPredicateQuery,
    subsystems: Any,
) -> dict[str, Any]:
    from runtime.semantic_predicate_authority import (
        SemanticPredicateAuthorityError,
        get_predicate,
    )

    conn = subsystems.get_pg_conn()
    try:
        return get_predicate(conn, predicate_slug=command.predicate_slug)
    except SemanticPredicateAuthorityError as exc:
        return {
            "status": "rejected",
            "error": str(exc),
            "reason_code": exc.reason_code,
            "details": exc.details,
        }
