"""Structured document authority queries."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any

from pydantic import BaseModel


class StructuredDocumentQueryError(RuntimeError):
    """Raised when structured document query input is invalid."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        status_code: int = 400,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.status_code = status_code
        self.details = dict(details or {})


class ListStructuredDocumentContextSelectionsQuery(BaseModel):
    query_ref: str | None = None
    assembler_ref: str | None = None
    section_ref: str | None = None
    document_ref: str | None = None
    revision_ref: str | None = None
    selected: bool | None = None
    limit: int = 100


def _fetch(conn: Any, query: str, *args: Any) -> list[dict[str, Any]]:
    if hasattr(conn, "fetch") and callable(conn.fetch):
        rows = conn.fetch(query, *args)
    else:
        rows = conn.execute(query, *args)
    return [dict(row) for row in rows or []]


def _text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise StructuredDocumentQueryError(
            "structured_documents.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _limit(value: object) -> int:
    try:
        limit = int(value or 100)
    except (TypeError, ValueError) as exc:
        raise StructuredDocumentQueryError(
            "structured_documents.invalid_submission",
            "limit must be an integer",
            details={"field": "limit", "value": value},
        ) from exc
    if limit < 1 or limit > 1000:
        raise StructuredDocumentQueryError(
            "structured_documents.invalid_submission",
            "limit must be between 1 and 1000",
            details={"field": "limit", "value": limit},
        )
    return limit


def list_context_selection_receipts(
    conn: Any,
    *,
    query_ref: str | None = None,
    assembler_ref: str | None = None,
    section_ref: str | None = None,
    document_ref: str | None = None,
    revision_ref: str | None = None,
    selected: bool | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return inspectable context-selection receipts."""

    clauses = ["SELECT * FROM structured_document_context_receipt_summary WHERE TRUE"]
    args: list[Any] = []
    filters = {
        "query_ref": _text(query_ref, field_name="query_ref"),
        "assembler_ref": _text(assembler_ref, field_name="assembler_ref"),
        "section_ref": _text(section_ref, field_name="section_ref"),
        "document_ref": _text(document_ref, field_name="document_ref"),
        "revision_ref": _text(revision_ref, field_name="revision_ref"),
    }
    for field_name, value in filters.items():
        if value is None:
            continue
        args.append(value)
        clauses.append(f"AND {field_name} = ${len(args)}")
    if selected is not None:
        args.append(bool(selected))
        clauses.append(f"AND selected = ${len(args)}")
    args.append(_limit(limit))
    clauses.append(f"ORDER BY created_at DESC LIMIT ${len(args)}")
    return _fetch(conn, "\n".join(clauses), *args)


def handle_list_context_selection_receipts(
    command: ListStructuredDocumentContextSelectionsQuery,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_context_selection_receipts(
        subsystems.get_pg_conn(),
        query_ref=command.query_ref,
        assembler_ref=command.assembler_ref,
        section_ref=command.section_ref,
        document_ref=command.document_ref,
        revision_ref=command.revision_ref,
        selected=command.selected,
        limit=command.limit,
    )
    return {
        "status": "listed",
        "context_selection_receipts": rows,
        "count": len(rows),
    }


__all__ = [
    "ListStructuredDocumentContextSelectionsQuery",
    "StructuredDocumentQueryError",
    "handle_list_context_selection_receipts",
    "list_context_selection_receipts",
]
