"""Structured document authority queries."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import re
from typing import Any

from pydantic import BaseModel, Field


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_.:-]*")


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


class AssembleStructuredDocumentContextQuery(BaseModel):
    query_ref: str
    query_text: str | None = None
    document_ref: str | None = None
    revision_ref: str | None = None
    max_sections: int = 8
    include_rejected: bool = False
    assembler_ref: str = "authority.structured_documents.context_assembler.v1"
    metadata: dict[str, Any] = Field(default_factory=dict)


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


def _max_sections(value: object) -> int:
    try:
        max_sections = int(value or 8)
    except (TypeError, ValueError) as exc:
        raise StructuredDocumentQueryError(
            "structured_documents.invalid_submission",
            "max_sections must be an integer",
            details={"field": "max_sections", "value": value},
        ) from exc
    if max_sections < 1 or max_sections > 200:
        raise StructuredDocumentQueryError(
            "structured_documents.invalid_submission",
            "max_sections must be between 1 and 200",
            details={"field": "max_sections", "value": max_sections},
        )
    return max_sections


def _json_list(value: object) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return [value]
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _json_object(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _canonical_json_hash(value: object, *, purpose: str) -> str:
    payload = {
        "purpose": purpose,
        "value": value,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return f"sha256:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"


def _normalize_text(value: object) -> str:
    return " ".join(str(value or "").split())


def canonical_text_hash(value: object) -> str:
    """Return the stable text hash used by structured document projections."""

    normalized = _normalize_text(value)
    return f"sha256:{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def _tokens(value: object) -> set[str]:
    return {match.group(0) for match in _TOKEN_RE.finditer(_normalize_text(value).lower())}


def _group(rows: list[dict[str, Any]], field_name: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get(field_name) or "").strip()
        if not key:
            continue
        grouped.setdefault(key, []).append(row)
    return grouped


def _fetch_candidate_sections(
    conn: Any,
    *,
    document_ref: str | None,
    revision_ref: str | None,
    candidate_limit: int,
) -> list[dict[str, Any]]:
    return _fetch(
        conn,
        """
        SELECT sections.section_ref,
               sections.document_ref,
               sections.revision_ref,
               sections.parent_section_ref,
               sections.node_index,
               sections.heading_level,
               sections.heading,
               sections.heading_path,
               sections.breadcrumb,
               sections.content_text,
               sections.content_hash,
               sections.token_estimate,
               sections.metadata AS section_metadata,
               revisions.title AS document_title,
               revisions.source_kind,
               revisions.source_ref,
               revisions.content_hash AS revision_content_hash,
               revisions.source_receipt_id AS revision_source_receipt_id,
               revisions.updated_at AS revision_updated_at
          FROM structured_document_sections sections
          JOIN structured_document_revisions revisions
            ON revisions.document_ref = sections.document_ref
           AND revisions.revision_ref = sections.revision_ref
         WHERE revisions.document_status = 'active'
           AND ($1::text IS NULL OR sections.document_ref = $1::text)
           AND ($2::text IS NULL OR sections.revision_ref = $2::text)
         ORDER BY sections.document_ref, sections.revision_ref, sections.node_index
         LIMIT $3
        """,
        document_ref,
        revision_ref,
        candidate_limit,
    )


def _fetch_section_tags(conn: Any, section_refs: list[str]) -> list[dict[str, Any]]:
    if not section_refs:
        return []
    return _fetch(
        conn,
        """
        WITH tagged_sections AS (
            SELECT assertions.subject_ref AS section_ref,
                   assertions.object_ref AS tag_ref,
                   assertions.semantic_assertion_id,
                   assertions.source_kind AS effective_source,
                   assertions.source_ref,
                   assertions.evidence_ref
              FROM semantic_current_assertions assertions
             WHERE assertions.subject_kind = 'structured_document_section'
               AND assertions.predicate_slug = 'tagged_as'
               AND assertions.assertion_status = 'active'
               AND assertions.subject_ref = ANY($1::text[])
        )
        SELECT tagged_sections.section_ref,
               tags.tag_ref,
               tags.tag_key,
               tags.tag_value,
               tags.summary,
               tagged_sections.semantic_assertion_id,
               effective_source AS source,
               tagged_sections.source_ref,
               tagged_sections.evidence_ref
          FROM tagged_sections
          JOIN knowledge_tag_catalog tags
            ON tags.tag_ref = tagged_sections.tag_ref
         WHERE tags.tag_status = 'active'
         ORDER BY tagged_sections.section_ref, tags.tag_key, tags.tag_value
        """,
        section_refs,
    )


def _fetch_section_semantics(conn: Any, section_refs: list[str]) -> list[dict[str, Any]]:
    if not section_refs:
        return []
    return _fetch(
        conn,
        """
        SELECT section_ref,
               semantic_assertion_id,
               predicate_slug,
               object_kind,
               object_ref,
               qualifiers_json,
               source_kind,
               source_ref,
               evidence_ref,
               bound_decision_id,
               valid_from,
               valid_to
          FROM structured_document_section_semantics
         WHERE section_ref = ANY($1::text[])
           AND (valid_to IS NULL OR valid_to > now())
         ORDER BY section_ref, predicate_slug, object_kind, object_ref
        """,
        section_refs,
    )


def _fetch_section_digests(conn: Any, section_refs: list[str]) -> list[dict[str, Any]]:
    if not section_refs:
        return []
    return _fetch(
        conn,
        """
        SELECT digest_ref,
               revision_ref,
               digest_type,
               subject_kind,
               subject_ref,
               summary_text,
               input_refs,
               input_hash,
               output_hash,
               recipe_ref,
               recipe_version,
               source_receipt_id,
               created_at
          FROM knowledge_digest_revisions
         WHERE digest_status = 'active'
           AND subject_kind = 'structured_document_section'
           AND subject_ref = ANY($1::text[])
         ORDER BY subject_ref, created_at DESC, digest_ref
        """,
        section_refs,
    )


def _fetch_section_classifications(conn: Any, section_refs: list[str]) -> list[dict[str, Any]]:
    if not section_refs:
        return []
    return _fetch(
        conn,
        """
        SELECT object_kind,
               field_path,
               tag_key,
               tag_value,
               source,
               confidence
          FROM data_dictionary_classifications_effective
         WHERE object_kind = ANY($1::text[])
         ORDER BY object_kind, field_path, tag_key, tag_value
        """,
        section_refs,
    )


def _score_candidate(
    section: dict[str, Any],
    *,
    query_terms: set[str],
    tags: list[dict[str, Any]],
    semantics: list[dict[str, Any]],
    digests: list[dict[str, Any]],
    classifications: list[dict[str, Any]],
) -> tuple[float, dict[str, Any], list[str]]:
    searchable_text = " ".join(
        str(value or "")
        for value in (
            section.get("document_title"),
            section.get("breadcrumb"),
            section.get("heading"),
            section.get("content_text"),
            " ".join(str(item or "") for item in _json_list(section.get("heading_path"))),
            " ".join(str(tag.get("tag_value") or "") for tag in tags),
            " ".join(str(tag.get("summary") or "") for tag in tags),
            " ".join(str(digest.get("summary_text") or "") for digest in digests),
        )
    )
    candidate_terms = _tokens(searchable_text)
    lexical_matches = sorted(query_terms.intersection(candidate_terms))
    heading_terms = _tokens(section.get("heading")) | _tokens(section.get("breadcrumb"))
    structure_matches = sorted(query_terms.intersection(heading_terms))

    score_breakdown: dict[str, Any] = {
        "lexical_match_count": len(lexical_matches),
        "structure_match_count": len(structure_matches),
        "semantic_assertion_count": len(semantics),
        "tag_count": len(tags),
        "digest_count": len(digests),
        "classification_count": len(classifications),
    }
    score = 0.0
    reason_codes: list[str] = []
    if lexical_matches:
        score += len(lexical_matches) * 10
        reason_codes.append("lexical_match")
    if structure_matches:
        score += len(structure_matches) * 5
        reason_codes.append("structure_match")
    if semantics:
        score += len(semantics) * 20
        reason_codes.append("semantic_assertion_match")
    if tags:
        score += len(tags) * 4
    if digests:
        score += len(digests) * 3

    classification_weight = 0.0
    for classification in classifications:
        if str(classification.get("tag_value") or "").lower() in {"high", "critical"}:
            classification_weight += 8
        else:
            classification_weight += 2
    authority_semantics = [
        semantic
        for semantic in semantics
        if str(semantic.get("object_kind") or "").startswith("authority")
        or str(semantic.get("object_ref") or "").startswith("authority.")
    ]
    policy_tags = [
        tag
        for tag in tags
        if str(tag.get("tag_key") or "").lower() in {"policy", "operator_policy"}
    ]
    if authority_semantics or policy_tags or classification_weight:
        score += 6 * len(authority_semantics) + 4 * len(policy_tags) + classification_weight
        reason_codes.append("authority_weight")
    if any(str(tag.get("source") or tag.get("source_kind") or "") == "operator" for tag in tags):
        score += 2
        reason_codes.append("operator_policy")

    score_breakdown.update(
        {
            "lexical_matches": lexical_matches,
            "structure_matches": structure_matches,
            "authority_semantic_count": len(authority_semantics),
            "policy_tag_count": len(policy_tags),
            "classification_weight": classification_weight,
        }
    )
    return score, score_breakdown, list(dict.fromkeys(reason_codes))


def _candidate_packet(
    section: dict[str, Any],
    *,
    rank: int,
    selected: bool,
    score_total: float,
    score_breakdown: dict[str, Any],
    deterministic_reason_codes: list[str],
    tags: list[dict[str, Any]],
    semantics: list[dict[str, Any]],
    digests: list[dict[str, Any]],
    classifications: list[dict[str, Any]],
) -> dict[str, Any]:
    semantic_assertion_ids = [
        str(row.get("semantic_assertion_id"))
        for row in [*tags, *semantics]
        if row.get("semantic_assertion_id")
    ]
    digest_refs = [str(row.get("digest_ref")) for row in digests if row.get("digest_ref")]
    return {
        "rank": rank,
        "selected": selected,
        "section_ref": section.get("section_ref"),
        "document_ref": section.get("document_ref"),
        "revision_ref": section.get("revision_ref"),
        "parent_section_ref": section.get("parent_section_ref"),
        "node_index": section.get("node_index"),
        "heading_level": section.get("heading_level"),
        "heading": section.get("heading") or "",
        "heading_path": _json_list(section.get("heading_path")),
        "breadcrumb": section.get("breadcrumb") or "",
        "content_text": section.get("content_text") or "",
        "content_hash": section.get("content_hash") or canonical_text_hash(section.get("content_text")),
        "token_estimate": int(section.get("token_estimate") or 0),
        "section_metadata": _json_object(section.get("section_metadata")),
        "document_title": section.get("document_title") or "",
        "source_kind": section.get("source_kind") or "",
        "source_ref": section.get("source_ref") or "",
        "revision_content_hash": section.get("revision_content_hash"),
        "revision_source_receipt_id": section.get("revision_source_receipt_id"),
        "revision_updated_at": section.get("revision_updated_at"),
        "score_total": score_total,
        "score_breakdown": score_breakdown,
        "deterministic_reason_codes": deterministic_reason_codes,
        "semantic_assertion_ids": list(dict.fromkeys(semantic_assertion_ids)),
        "tag_refs": [str(row.get("tag_ref")) for row in tags if row.get("tag_ref")],
        "tags": tags,
        "semantic_assertions": semantics,
        "digest_refs": digest_refs,
        "digests": digests,
        "classifications": classifications,
    }


def assemble_context(
    conn: Any,
    *,
    query_ref: str,
    query_text: str | None = None,
    document_ref: str | None = None,
    revision_ref: str | None = None,
    max_sections: int = 8,
    include_rejected: bool = False,
    assembler_ref: str = "authority.structured_documents.context_assembler.v1",
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble ranked structured-document context plus receipt-write inputs."""

    query_ref = _text(query_ref, field_name="query_ref") or ""
    assembler_ref = _text(assembler_ref, field_name="assembler_ref") or ""
    query_text = _text(query_text, field_name="query_text") if query_text is not None else None
    document_ref = _text(document_ref, field_name="document_ref")
    revision_ref = _text(revision_ref, field_name="revision_ref")
    max_sections = _max_sections(max_sections)
    candidate_limit = max(25, max_sections * 5)

    sections = _fetch_candidate_sections(
        conn,
        document_ref=document_ref,
        revision_ref=revision_ref,
        candidate_limit=candidate_limit,
    )
    section_refs = [str(row.get("section_ref")) for row in sections if row.get("section_ref")]
    tags_by_section = _group(_fetch_section_tags(conn, section_refs), "section_ref")
    semantics_by_section = _group(_fetch_section_semantics(conn, section_refs), "section_ref")
    digests_by_section = _group(_fetch_section_digests(conn, section_refs), "subject_ref")
    classifications_by_section = _group(
        _fetch_section_classifications(conn, section_refs),
        "object_kind",
    )

    query_terms = _tokens(query_text or query_ref)
    ranked: list[dict[str, Any]] = []
    for section in sections:
        section_ref = str(section.get("section_ref") or "")
        tags = tags_by_section.get(section_ref, [])
        semantics = semantics_by_section.get(section_ref, [])
        digests = digests_by_section.get(section_ref, [])
        classifications = classifications_by_section.get(section_ref, [])
        score_total, score_breakdown, reason_codes = _score_candidate(
            section,
            query_terms=query_terms,
            tags=tags,
            semantics=semantics,
            digests=digests,
            classifications=classifications,
        )
        ranked.append(
            {
                "section": section,
                "section_ref": section_ref,
                "score_total": score_total,
                "score_breakdown": score_breakdown,
                "deterministic_reason_codes": reason_codes,
                "tags": tags,
                "semantics": semantics,
                "digests": digests,
                "classifications": classifications,
            }
        )
    ranked.sort(
        key=lambda row: (
            -float(row["score_total"]),
            int(row["section"].get("node_index") or 0),
            str(row["section_ref"]),
        )
    )

    candidates: list[dict[str, Any]] = []
    for index, row in enumerate(ranked, start=1):
        candidates.append(
            _candidate_packet(
                row["section"],
                rank=index,
                selected=index <= max_sections,
                score_total=float(row["score_total"]),
                score_breakdown=row["score_breakdown"],
                deterministic_reason_codes=row["deterministic_reason_codes"],
                tags=row["tags"],
                semantics=row["semantics"],
                digests=row["digests"],
                classifications=row["classifications"],
            )
        )

    selected_sections = [candidate for candidate in candidates if candidate["selected"]]
    visible_candidates = candidates if include_rejected else selected_sections
    packet = {
        "query_ref": query_ref,
        "query_text": query_text or "",
        "query_text_hash": canonical_text_hash(query_text or query_ref),
        "assembler_ref": assembler_ref,
        "document_ref": document_ref,
        "revision_ref": revision_ref,
        "selection_count": len(selected_sections),
        "candidate_count": len(candidates),
        "omitted_candidate_count": max(0, len(candidates) - len(visible_candidates)),
        "selected_sections": selected_sections,
        "candidate_sections": visible_candidates,
        "metadata": dict(metadata or {}),
    }
    packet_hash = _canonical_json_hash(packet, purpose="structured_documents.context_packet.v1")

    receipt_inputs = []
    for candidate in candidates:
        receipt_payload = {
            "query_ref": query_ref,
            "assembler_ref": assembler_ref,
            "section_ref": candidate["section_ref"],
            "selected": bool(candidate["selected"]),
            "packet_hash": packet_hash,
        }
        receipt_inputs.append(
            {
                "query_ref": query_ref,
                "assembler_ref": assembler_ref,
                "section_ref": candidate["section_ref"],
                "selected": bool(candidate["selected"]),
                "score_total": candidate["score_total"],
                "score_breakdown": candidate["score_breakdown"],
                "deterministic_reason_codes": candidate["deterministic_reason_codes"],
                "semantic_assertion_ids": candidate["semantic_assertion_ids"],
                "idempotency_key": _canonical_json_hash(
                    receipt_payload,
                    purpose="structured_documents.context_selection_receipt.v1",
                ),
                "metadata": {
                    "packet_hash": packet_hash,
                    "rank": candidate["rank"],
                    "query_text_hash": packet["query_text_hash"],
                    "document_ref": candidate["document_ref"],
                    "revision_ref": candidate["revision_ref"],
                },
            }
        )

    return {
        "status": "assembled",
        "query_ref": query_ref,
        "assembler_ref": assembler_ref,
        "context_packet": packet,
        "packet_hash": packet_hash,
        "selection_receipt_operation": "structured_documents.record_context_selection",
        "selection_receipt_inputs": receipt_inputs,
    }


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


def handle_assemble_context(
    command: AssembleStructuredDocumentContextQuery,
    subsystems: Any,
) -> dict[str, Any]:
    return assemble_context(
        subsystems.get_pg_conn(),
        query_ref=command.query_ref,
        query_text=command.query_text,
        document_ref=command.document_ref,
        revision_ref=command.revision_ref,
        max_sections=command.max_sections,
        include_rejected=command.include_rejected,
        assembler_ref=command.assembler_ref,
        metadata=command.metadata,
    )


__all__ = [
    "AssembleStructuredDocumentContextQuery",
    "ListStructuredDocumentContextSelectionsQuery",
    "StructuredDocumentQueryError",
    "assemble_context",
    "canonical_text_hash",
    "handle_assemble_context",
    "handle_list_context_selection_receipts",
    "list_context_selection_receipts",
]
