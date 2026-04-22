from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from runtime.structured_document_semantics import (
    list_context_selection_receipts,
    projection_freshness_payload,
)


class ListStructuredDocumentContextSelectionsQuery(BaseModel):
    query_ref: str | None = None
    section_ref: str | None = None
    selected: bool | None = None
    limit: int = 100


def handle_list_context_selection_receipts(
    command: ListStructuredDocumentContextSelectionsQuery,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_context_selection_receipts(
        subsystems.get_pg_conn(),
        query_ref=command.query_ref,
        section_ref=command.section_ref,
        selected=command.selected,
        limit=command.limit,
    )
    return {
        "status": "listed",
        "context_selection_receipts": rows,
        "count": len(rows),
        "projection_freshness": projection_freshness_payload(
            projection_ref="projection.structured_document.context_selection_receipts",
            status="unknown",
        ),
    }


__all__ = [
    "ListStructuredDocumentContextSelectionsQuery",
    "handle_list_context_selection_receipts",
]
