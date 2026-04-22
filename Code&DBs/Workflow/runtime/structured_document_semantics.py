"""Structured-document semantic authority helpers.

This module keeps the V1 boundary intentionally narrow:

* structured document sections are addressable authority objects;
* typed semantic assertions carry meaning;
* embeddings are recall projections only;
* context selection receipts must explain selected context with deterministic
  reasons, score parts, assertion ids, and authority/event references.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any
from uuid import uuid4

from runtime.semantic_assertions import (
    RESERVED_QUALIFIER_KEYS,
    semantic_assertion_id,
)

STRUCTURED_DOCUMENT_AUTHORITY_DOMAIN = "authority.structured_documents"
STRUCTURED_DOCUMENT_SECTION_KIND = "structured_document_section"
STRUCTURED_DOCUMENT_CONTEXT_SELECTED_EVENT = "structured_document_context_selected"
STRUCTURED_DOCUMENT_CONTEXT_SELECTION_OPERATION = (
    "structured-documents-record-context-selection"
)
SECTION_EMBEDDING_INPUT_RECIPE_V1 = "structured_document.section_embedding_input.v1"

DETERMINISTIC_REASON_CODES = frozenset(
    {
        "structure_match",
        "lexical_match",
        "semantic_assertion_match",
        "synonym_expansion_match",
        "authority_weight",
        "operator_policy",
    }
)

SCORE_PART_KEYS = (
    "structure_match",
    "lexical_match",
    "semantic_assertion_match",
    "synonym_expansion_match",
    "vector_similarity",
    "usage_prior",
    "authority_weight",
    "deprecated_or_superseded_penalty",
    "context_bloat_penalty",
)


class StructuredDocumentSemanticError(RuntimeError):
    """Raised when structured-document semantic authority rejects a write."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class StructuredContextScore:
    """Inspectable score for one structured-document context candidate."""

    score_total: float
    score_breakdown: dict[str, float]
    deterministic_reason_codes: tuple[str, ...]
    semantic_assertion_ids: tuple[str, ...]
    eligible_for_authoritative_context: bool

    def to_json(self) -> dict[str, Any]:
        return {
            "score_total": self.score_total,
            "score_breakdown": dict(self.score_breakdown),
            "deterministic_reason_codes": list(self.deterministic_reason_codes),
            "semantic_assertion_ids": list(self.semantic_assertion_ids),
            "eligible_for_authoritative_context": self.eligible_for_authoritative_context,
        }


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StructuredDocumentSemanticError(
            "structured_document.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value.strip()


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _as_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise StructuredDocumentSemanticError(
                "structured_document.invalid_submission",
                f"{field_name} must be a JSON object",
                details={"field": field_name},
            ) from exc
    if not isinstance(value, Mapping):
        raise StructuredDocumentSemanticError(
            "structured_document.invalid_submission",
            f"{field_name} must be a mapping",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    return dict(_as_mapping(value, field_name=field_name))


def _text_tuple(value: object, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_require_text(value, field_name=field_name),)
    if not isinstance(value, Sequence):
        raise StructuredDocumentSemanticError(
            "structured_document.invalid_submission",
            f"{field_name} must be a sequence of strings",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        text = _require_text(item, field_name=f"{field_name}[{index}]")
        if text not in normalized:
            normalized.append(text)
    return tuple(normalized)


def _number(value: object, *, field_name: str) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError) as exc:
        raise StructuredDocumentSemanticError(
            "structured_document.invalid_submission",
            f"{field_name} must be numeric",
            details={"field": field_name, "value": value},
        ) from exc


def _ref_fragment(value: object, *, field_name: str) -> str:
    raw = _require_text(value, field_name=field_name)
    lowered = raw.lower()
    chars = [
        char if char.isascii() and (char.isalnum() or char in {".", "_", "-"}) else "-"
        for char in lowered
    ]
    collapsed = "".join(chars).strip("-")
    while "--" in collapsed:
        collapsed = collapsed.replace("--", "-")
    digest = hashlib.blake2s(raw.encode("utf-8"), digest_size=5).hexdigest()
    if not collapsed:
        collapsed = "ref"
    if len(collapsed) > 64:
        collapsed = collapsed[:64].rstrip("-")
    return f"{collapsed}-{digest}"


def structured_section_ref(
    *,
    document_ref: str,
    revision_ref: str,
    node_index: int,
) -> str:
    """Return a stable subject ref for one structured document section."""

    if node_index < 0:
        raise StructuredDocumentSemanticError(
            "structured_document.invalid_submission",
            "node_index must be non-negative",
            details={"field": "node_index", "value": node_index},
        )
    document = _ref_fragment(document_ref, field_name="document_ref")
    revision = _ref_fragment(revision_ref, field_name="revision_ref")
    return f"doc:{document}@{revision}#node-{node_index:06d}"


def section_embedding_input(
    *,
    document_title: str,
    breadcrumb: str,
    heading: str,
    semantic_predicates: Sequence[str] | None,
    local_text: str,
) -> str:
    """Return deterministic text used by the section embedding projection."""

    predicates = ", ".join(
        sorted(_text_tuple(semantic_predicates, field_name="semantic_predicates"))
    )
    parts = [
        f"title: {document_title.strip()}",
        f"breadcrumb: {breadcrumb.strip()}",
        f"heading: {heading.strip()}",
        f"semantic_predicates: {predicates}",
        "local_text:",
        local_text.strip(),
    ]
    return "\n".join(parts).strip() + "\n"


def _score_parts(**values: float) -> dict[str, float]:
    return {key: _number(values.get(key, 0), field_name=key) for key in SCORE_PART_KEYS}


def score_context_candidate(
    *,
    structure_match: float = 0,
    lexical_match: float = 0,
    semantic_assertion_match: float = 0,
    synonym_expansion_match: float = 0,
    vector_similarity: float = 0,
    usage_prior: float = 0,
    authority_weight: float = 0,
    deprecated_or_superseded_penalty: float = 0,
    context_bloat_penalty: float = 0,
    semantic_assertion_ids: Sequence[str] | None = None,
    reason_codes: Sequence[str] | None = None,
) -> StructuredContextScore:
    """Score a candidate and mark whether it can be selected as authority."""

    assertion_ids = _text_tuple(
        semantic_assertion_ids,
        field_name="semantic_assertion_ids",
    )
    parts = _score_parts(
        structure_match=structure_match,
        lexical_match=lexical_match,
        semantic_assertion_match=semantic_assertion_match,
        synonym_expansion_match=synonym_expansion_match,
        vector_similarity=vector_similarity,
        usage_prior=usage_prior,
        authority_weight=authority_weight,
        deprecated_or_superseded_penalty=deprecated_or_superseded_penalty,
        context_bloat_penalty=context_bloat_penalty,
    )

    reasons: list[str] = []
    for key in (
        "structure_match",
        "lexical_match",
        "semantic_assertion_match",
        "synonym_expansion_match",
        "authority_weight",
    ):
        if parts[key] > 0:
            reasons.append(key)
    if assertion_ids and "semantic_assertion_match" not in reasons:
        reasons.append("semantic_assertion_match")

    for code in _text_tuple(reason_codes, field_name="reason_codes"):
        if code not in reasons:
            reasons.append(code)

    eligible = bool(DETERMINISTIC_REASON_CODES.intersection(reasons))
    if parts["vector_similarity"] > 0 and not eligible:
        reasons.append("vector_recall_only")

    total = (
        parts["structure_match"]
        + parts["lexical_match"]
        + parts["semantic_assertion_match"]
        + parts["synonym_expansion_match"]
        + parts["vector_similarity"]
        + parts["usage_prior"]
        + parts["authority_weight"]
        - parts["deprecated_or_superseded_penalty"]
        - parts["context_bloat_penalty"]
    )
    return StructuredContextScore(
        score_total=round(total, 6),
        score_breakdown=parts,
        deterministic_reason_codes=tuple(dict.fromkeys(reasons)),
        semantic_assertion_ids=assertion_ids,
        eligible_for_authoritative_context=eligible,
    )


def ensure_selected_context_is_authoritative(
    *,
    selected: bool,
    deterministic_reason_codes: Sequence[str] | None,
) -> None:
    """Reject vector-only or reasonless authoritative selections."""

    if not selected:
        return
    reasons = set(_text_tuple(deterministic_reason_codes, field_name="deterministic_reason_codes"))
    if not reasons.intersection(DETERMINISTIC_REASON_CODES):
        raise StructuredDocumentSemanticError(
            "structured_document.vector_only_selection",
            "selected context requires a deterministic authority reason",
            details={
                "deterministic_reason_codes": sorted(reasons),
                "allowed_reason_codes": sorted(DETERMINISTIC_REASON_CODES),
            },
        )


def context_selection_receipt_payload(
    *,
    query_ref: str,
    assembler_ref: str,
    section_ref: str,
    selected: bool,
    score: StructuredContextScore,
    source_receipt_id: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the durable payload for a context selection receipt."""

    ensure_selected_context_is_authoritative(
        selected=selected,
        deterministic_reason_codes=score.deterministic_reason_codes,
    )
    return {
        "query_ref": _require_text(query_ref, field_name="query_ref"),
        "assembler_ref": _require_text(assembler_ref, field_name="assembler_ref"),
        "section_ref": _require_text(section_ref, field_name="section_ref"),
        "selected": bool(selected),
        "score_total": score.score_total,
        "score_breakdown": dict(score.score_breakdown),
        "deterministic_reason_codes": list(score.deterministic_reason_codes),
        "semantic_assertion_ids": list(score.semantic_assertion_ids),
        "source_receipt_id": _optional_text(source_receipt_id, field_name="source_receipt_id"),
        "metadata": _json_object(metadata, field_name="metadata"),
    }


def semantic_assertion_command_for_section(
    *,
    predicate_slug: str,
    section_ref: str,
    object_kind: str,
    object_ref: str,
    source_kind: str,
    source_ref: str,
    evidence_ref: str | None = None,
    qualifiers_json: Mapping[str, Any] | None = None,
    bound_decision_id: str | None = None,
) -> dict[str, Any]:
    """Build a semantic_assertions.record command for a section subject."""

    qualifiers = _json_object(qualifiers_json, field_name="qualifiers_json")
    hidden = sorted(
        key for key in qualifiers if str(key).strip().lower() in RESERVED_QUALIFIER_KEYS
    )
    if hidden:
        raise StructuredDocumentSemanticError(
            "structured_document.hidden_authority",
            "qualifiers_json cannot hide semantic assertion authority fields",
            details={"conflicting_keys": hidden},
        )
    subject_ref = _require_text(section_ref, field_name="section_ref")
    predicate = _require_text(predicate_slug, field_name="predicate_slug")
    normalized_object_kind = _require_text(object_kind, field_name="object_kind")
    normalized_object_ref = _require_text(object_ref, field_name="object_ref")
    normalized_source_kind = _require_text(source_kind, field_name="source_kind")
    normalized_source_ref = _require_text(source_ref, field_name="source_ref")
    return {
        "semantic_assertion_id": semantic_assertion_id(
            predicate_slug=predicate,
            subject_kind=STRUCTURED_DOCUMENT_SECTION_KIND,
            subject_ref=subject_ref,
            object_kind=normalized_object_kind,
            object_ref=normalized_object_ref,
            source_kind=normalized_source_kind,
            source_ref=normalized_source_ref,
        ),
        "predicate_slug": predicate,
        "subject_kind": STRUCTURED_DOCUMENT_SECTION_KIND,
        "subject_ref": subject_ref,
        "object_kind": normalized_object_kind,
        "object_ref": normalized_object_ref,
        "qualifiers_json": qualifiers,
        "source_kind": normalized_source_kind,
        "source_ref": normalized_source_ref,
        "evidence_ref": _optional_text(evidence_ref, field_name="evidence_ref"),
        "bound_decision_id": _optional_text(bound_decision_id, field_name="bound_decision_id"),
        "assertion_status": "active",
    }


def _command_value(command: object, name: str, default: Any = None) -> Any:
    if isinstance(command, Mapping):
        return command.get(name, default)
    return getattr(command, name, default)


def _row(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _fetch(conn: Any, query: str, *args: Any) -> list[dict[str, Any]]:
    if hasattr(conn, "fetch") and callable(conn.fetch):
        rows = conn.fetch(query, *args)
    else:
        rows = conn.execute(query, *args)
    return [dict(row) for row in rows or []]


def _existing_context_selection(conn: Any, idempotency_key: str | None) -> dict[str, Any] | None:
    if idempotency_key is None:
        return None
    row = conn.fetchrow(
        """
        SELECT *
          FROM structured_document_context_selection_receipts
         WHERE idempotency_key = $1
        """,
        idempotency_key,
    )
    return None if row is None else _row(row)


def record_context_selection(conn: Any, command: object) -> dict[str, Any]:
    """Record a structured-document context selection and emit an event."""

    query_ref = _require_text(_command_value(command, "query_ref"), field_name="query_ref")
    assembler_ref = _require_text(_command_value(command, "assembler_ref"), field_name="assembler_ref")
    section_ref = _require_text(_command_value(command, "section_ref"), field_name="section_ref")
    selected = bool(_command_value(command, "selected", False))
    score_total = _number(_command_value(command, "score_total", 0), field_name="score_total")
    score_breakdown = _json_object(_command_value(command, "score_breakdown", {}), field_name="score_breakdown")
    deterministic_reason_codes = _text_tuple(
        _command_value(command, "deterministic_reason_codes", ()),
        field_name="deterministic_reason_codes",
    )
    semantic_assertion_ids = _text_tuple(
        _command_value(command, "semantic_assertion_ids", ()),
        field_name="semantic_assertion_ids",
    )
    source_receipt_id = _optional_text(
        _command_value(command, "source_receipt_id"),
        field_name="source_receipt_id",
    )
    idempotency_key = _optional_text(
        _command_value(command, "idempotency_key"),
        field_name="idempotency_key",
    )
    metadata = _json_object(_command_value(command, "metadata", {}), field_name="metadata")

    ensure_selected_context_is_authoritative(
        selected=selected,
        deterministic_reason_codes=deterministic_reason_codes,
    )

    existing = _existing_context_selection(conn, idempotency_key)
    if existing is not None:
        event_id = existing.get("authority_event_id")
        return {
            "status": "replayed",
            "context_selection": existing,
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
            $1::uuid, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb,
            $10::uuid, $11, $12::jsonb
        )
        RETURNING *
        """,
        selection_receipt_id,
        query_ref,
        assembler_ref,
        section_ref,
        selected,
        score_total,
        json.dumps(score_breakdown, sort_keys=True, default=str),
        json.dumps(list(deterministic_reason_codes), sort_keys=True, default=str),
        json.dumps(list(semantic_assertion_ids), sort_keys=True, default=str),
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
        "selected": selected,
        "score_total": score_total,
        "score_breakdown": score_breakdown,
        "deterministic_reason_codes": list(deterministic_reason_codes),
        "semantic_assertion_ids": list(semantic_assertion_ids),
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
        STRUCTURED_DOCUMENT_AUTHORITY_DOMAIN,
        section_ref,
        STRUCTURED_DOCUMENT_CONTEXT_SELECTED_EVENT,
        json.dumps(event_payload, sort_keys=True, default=str),
        idempotency_key,
        STRUCTURED_DOCUMENT_CONTEXT_SELECTION_OPERATION,
        "authority.structured_documents",
    )
    row = conn.fetchrow(
        """
        UPDATE structured_document_context_selection_receipts
           SET authority_event_id = $2::uuid
         WHERE selection_receipt_id = $1::uuid
         RETURNING *
        """,
        selection_receipt_id,
        authority_event_id,
    )
    return {
        "status": "recorded",
        "context_selection": _row(row) or _row(inserted),
        "authority_event_ids": [authority_event_id],
    }


def list_context_selection_receipts(
    conn: Any,
    *,
    query_ref: str | None = None,
    section_ref: str | None = None,
    selected: bool | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Read structured-document context selection receipt summaries."""

    clauses = ["SELECT * FROM structured_document_context_receipt_summary WHERE TRUE"]
    args: list[Any] = []
    normalized_query = _optional_text(query_ref, field_name="query_ref")
    normalized_section = _optional_text(section_ref, field_name="section_ref")
    if normalized_query is not None:
        args.append(normalized_query)
        clauses.append(f"AND query_ref = ${len(args)}")
    if normalized_section is not None:
        args.append(normalized_section)
        clauses.append(f"AND section_ref = ${len(args)}")
    if selected is not None:
        args.append(bool(selected))
        clauses.append(f"AND selected = ${len(args)}")
    try:
        normalized_limit = int(limit)
    except (TypeError, ValueError) as exc:
        raise StructuredDocumentSemanticError(
            "structured_document.invalid_submission",
            "limit must be an integer",
            details={"field": "limit", "value": limit},
        ) from exc
    if normalized_limit < 1 or normalized_limit > 1000:
        raise StructuredDocumentSemanticError(
            "structured_document.invalid_submission",
            "limit must be between 1 and 1000",
            details={"field": "limit", "value": normalized_limit},
        )
    args.append(normalized_limit)
    clauses.append(f"ORDER BY created_at DESC LIMIT ${len(args)}")
    return _fetch(conn, "\n".join(clauses), *args)


def projection_freshness_payload(*, projection_ref: str, status: str = "unknown") -> dict[str, Any]:
    """Return a standard projection freshness shape for operation results."""

    return {
        projection_ref: {
            "freshness_status": status,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
    }


__all__ = [
    "DETERMINISTIC_REASON_CODES",
    "SECTION_EMBEDDING_INPUT_RECIPE_V1",
    "SCORE_PART_KEYS",
    "STRUCTURED_DOCUMENT_AUTHORITY_DOMAIN",
    "STRUCTURED_DOCUMENT_CONTEXT_SELECTED_EVENT",
    "STRUCTURED_DOCUMENT_CONTEXT_SELECTION_OPERATION",
    "STRUCTURED_DOCUMENT_SECTION_KIND",
    "StructuredContextScore",
    "StructuredDocumentSemanticError",
    "context_selection_receipt_payload",
    "ensure_selected_context_is_authoritative",
    "list_context_selection_receipts",
    "projection_freshness_payload",
    "record_context_selection",
    "score_context_candidate",
    "section_embedding_input",
    "semantic_assertion_command_for_section",
    "structured_section_ref",
]
