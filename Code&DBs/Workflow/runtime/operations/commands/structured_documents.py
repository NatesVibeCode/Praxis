from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from runtime.structured_document_semantics import (
    projection_freshness_payload,
    record_context_selection,
)


class RecordStructuredDocumentContextSelectionCommand(BaseModel):
    query_ref: str
    assembler_ref: str
    section_ref: str
    selected: bool = False
    score_total: float = 0
    score_breakdown: dict[str, Any] = Field(default_factory=dict)
    deterministic_reason_codes: list[str] = Field(default_factory=list)
    semantic_assertion_ids: list[str] = Field(default_factory=list)
    source_receipt_id: str | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def handle_record_context_selection(
    command: RecordStructuredDocumentContextSelectionCommand,
    subsystems: Any,
) -> dict[str, Any]:
    result = record_context_selection(subsystems.get_pg_conn(), command)
    result["projection_freshness"] = projection_freshness_payload(
        projection_ref="projection.structured_document.context_selection_receipts",
        status="unknown",
    )
    return result


__all__ = [
    "RecordStructuredDocumentContextSelectionCommand",
    "handle_record_context_selection",
]
