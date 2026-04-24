"""Structured document authority commands."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


_ALLOWED_REASON_CODES = {
    "structure_match",
    "lexical_match",
    "semantic_assertion_match",
    "synonym_expansion_match",
    "authority_weight",
    "operator_policy",
}


class StructuredDocumentAuthorityError(RuntimeError):
    """Raised when structured document authority rejects a command."""

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
    recorded_by: str = "authority.structured_documents"
    idempotency_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def _row(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StructuredDocumentAuthorityError(
            "structured_documents.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise StructuredDocumentAuthorityError(
                "structured_documents.invalid_submission",
                f"{field_name} must be a JSON object",
                details={"field": field_name},
            ) from exc
    if not isinstance(value, dict):
        raise StructuredDocumentAuthorityError(
            "structured_documents.invalid_submission",
            f"{field_name} must be a JSON object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return dict(value)


def _string_list(value: object, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [item.strip() for item in value.split(",") if item.strip()]
    if not isinstance(value, list):
        raise StructuredDocumentAuthorityError(
            "structured_documents.invalid_submission",
            f"{field_name} must be a list of strings",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise StructuredDocumentAuthorityError(
                "structured_documents.invalid_submission",
                f"{field_name} entries must be non-empty strings",
                details={"field": field_name},
            )
        normalized.append(item.strip())
    return normalized


def _score_total(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError) as exc:
        raise StructuredDocumentAuthorityError(
            "structured_documents.invalid_submission",
            "score_total must be numeric",
            details={"field": "score_total", "value": value},
        ) from exc


def _existing_context_selection(
    conn: Any,
    *,
    idempotency_key: str | None,
) -> dict[str, Any] | None:
    if not idempotency_key:
        return None
    row = conn.fetchrow(
        """
        SELECT selection_receipt_id,
               query_ref,
               assembler_ref,
               section_ref,
               selected,
               score_total,
               score_breakdown,
               deterministic_reason_codes,
               semantic_assertion_ids,
               source_receipt_id,
               authority_event_id,
               idempotency_key,
               metadata,
               created_at
          FROM structured_document_context_selection_receipts
         WHERE idempotency_key = $1
        """,
        idempotency_key,
    )
    return None if row is None else _row(row)


def record_context_selection(
    conn: Any,
    command: RecordStructuredDocumentContextSelectionCommand,
) -> dict[str, Any]:
    """Persist one inspectable context-selection receipt."""

    query_ref = _require_text(command.query_ref, field_name="query_ref")
    assembler_ref = _require_text(command.assembler_ref, field_name="assembler_ref")
    section_ref = _require_text(command.section_ref, field_name="section_ref")
    source_receipt_id = _optional_text(command.source_receipt_id, field_name="source_receipt_id")
    recorded_by = _require_text(command.recorded_by, field_name="recorded_by")
    idempotency_key = _optional_text(command.idempotency_key, field_name="idempotency_key")
    score_total = _score_total(command.score_total)
    score_breakdown = _json_object(command.score_breakdown, field_name="score_breakdown")
    metadata = _json_object(command.metadata, field_name="metadata")
    deterministic_reason_codes = _string_list(
        command.deterministic_reason_codes,
        field_name="deterministic_reason_codes",
    )
    semantic_assertion_ids = _string_list(
        command.semantic_assertion_ids,
        field_name="semantic_assertion_ids",
    )

    if command.selected and not deterministic_reason_codes:
        raise StructuredDocumentAuthorityError(
            "structured_documents.invalid_submission",
            "selected receipts must include at least one deterministic_reason_code",
            details={"field": "deterministic_reason_codes"},
        )
    invalid_reason_codes = sorted(
        code for code in deterministic_reason_codes if code not in _ALLOWED_REASON_CODES
    )
    if invalid_reason_codes:
        raise StructuredDocumentAuthorityError(
            "structured_documents.invalid_submission",
            "deterministic_reason_codes contain unsupported values",
            details={"field": "deterministic_reason_codes", "values": invalid_reason_codes},
        )

    existing = _existing_context_selection(conn, idempotency_key=idempotency_key)
    if existing is not None:
        event_id = existing.get("authority_event_id")
        return {
            "status": "replayed",
            "context_selection_receipt": existing,
            "authority_event_ids": [str(event_id)] if event_id else [],
        }

    selection_receipt_id = str(uuid4())
    inserted = conn.fetchrow(
        """
        INSERT INTO structured_document_context_selection_receipts (
            selection_receipt_id,
            query_ref,
            assembler_ref,
            section_ref,
            selected,
            score_total,
            score_breakdown,
            deterministic_reason_codes,
            semantic_assertion_ids,
            source_receipt_id,
            idempotency_key,
            metadata
        ) VALUES (
            $1::uuid,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7::jsonb,
            $8::jsonb,
            $9::jsonb,
            $10::uuid,
            $11,
            $12::jsonb
        )
        RETURNING selection_receipt_id,
                  query_ref,
                  assembler_ref,
                  section_ref,
                  selected,
                  score_total,
                  score_breakdown,
                  deterministic_reason_codes,
                  semantic_assertion_ids,
                  source_receipt_id,
                  authority_event_id,
                  idempotency_key,
                  metadata,
                  created_at
        """,
        selection_receipt_id,
        query_ref,
        assembler_ref,
        section_ref,
        command.selected,
        score_total,
        json.dumps(score_breakdown, sort_keys=True, default=str),
        json.dumps(deterministic_reason_codes, sort_keys=True, default=str),
        json.dumps(semantic_assertion_ids, sort_keys=True, default=str),
        source_receipt_id,
        idempotency_key,
        json.dumps(metadata, sort_keys=True, default=str),
    )

    authority_event_id = str(uuid4())
    event_payload = {
        "selection_receipt_id": selection_receipt_id,
        "query_ref": query_ref,
        "assembler_ref": assembler_ref,
        "section_ref": section_ref,
        "selected": command.selected,
        "score_total": score_total,
        "deterministic_reason_codes": deterministic_reason_codes,
        "semantic_assertion_ids": semantic_assertion_ids,
        "source_receipt_id": source_receipt_id,
    }
    conn.execute(
        """
        INSERT INTO authority_events (
            event_id,
            authority_domain_ref,
            aggregate_ref,
            event_type,
            event_payload,
            idempotency_key,
            operation_ref,
            emitted_by
        ) VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6, $7, $8)
        """,
        authority_event_id,
        "authority.structured_documents",
        f"{query_ref}:{section_ref}",
        "structured_document_context_selected",
        json.dumps(event_payload, sort_keys=True, default=str),
        idempotency_key,
        "structured_documents.record_context_selection",
        recorded_by,
    )
    row = conn.fetchrow(
        """
        UPDATE structured_document_context_selection_receipts
           SET authority_event_id = $2::uuid
         WHERE selection_receipt_id = $1::uuid
         RETURNING selection_receipt_id,
                   query_ref,
                   assembler_ref,
                   section_ref,
                   selected,
                   score_total,
                   score_breakdown,
                   deterministic_reason_codes,
                   semantic_assertion_ids,
                   source_receipt_id,
                   authority_event_id,
                   idempotency_key,
                   metadata,
                   created_at
        """,
        selection_receipt_id,
        authority_event_id,
    )
    return {
        "status": "recorded",
        "context_selection_receipt": _row(row) or _row(inserted),
        "authority_event_ids": [authority_event_id],
    }


def handle_record_context_selection(
    command: RecordStructuredDocumentContextSelectionCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return record_context_selection(subsystems.get_pg_conn(), command)


__all__ = [
    "RecordStructuredDocumentContextSelectionCommand",
    "StructuredDocumentAuthorityError",
    "handle_record_context_selection",
    "record_context_selection",
]
