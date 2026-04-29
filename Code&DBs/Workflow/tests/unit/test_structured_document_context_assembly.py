from __future__ import annotations

from runtime.operations.queries.structured_documents import (
    AssembleStructuredDocumentContextQuery,
    assemble_context,
    canonical_text_hash,
    handle_assemble_context,
)


class _Conn:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[object, ...]]] = []

    def fetch(self, query: str, *args: object) -> list[dict]:
        self.queries.append((query, args))
        if "FROM structured_document_sections" in query:
            return [
                {
                    "section_ref": "section.retention",
                    "document_ref": "doc.ops",
                    "revision_ref": "rev.1",
                    "parent_section_ref": None,
                    "node_index": 1,
                    "heading_level": 2,
                    "heading": "Retention",
                    "heading_path": ["Ops", "Retention"],
                    "breadcrumb": "Ops > Retention",
                    "content_text": "Workflow retention policy must keep selection receipts.",
                    "content_hash": canonical_text_hash(
                        "Workflow retention policy must keep selection receipts."
                    ),
                    "token_estimate": 8,
                    "section_metadata": {},
                    "document_title": "Ops Manual",
                    "source_kind": "operator_document",
                    "source_ref": "doc.ops",
                    "revision_content_hash": canonical_text_hash("Ops Manual"),
                    "revision_source_receipt_id": None,
                    "revision_updated_at": "2026-04-29T00:00:00Z",
                },
                {
                    "section_ref": "section.ui",
                    "document_ref": "doc.ops",
                    "revision_ref": "rev.1",
                    "parent_section_ref": None,
                    "node_index": 2,
                    "heading_level": 2,
                    "heading": "UI",
                    "heading_path": ["Ops", "UI"],
                    "breadcrumb": "Ops > UI",
                    "content_text": "Dashboard color notes.",
                    "content_hash": canonical_text_hash("Dashboard color notes."),
                    "token_estimate": 3,
                    "section_metadata": {},
                    "document_title": "Ops Manual",
                    "source_kind": "operator_document",
                    "source_ref": "doc.ops",
                    "revision_content_hash": canonical_text_hash("Ops Manual"),
                    "revision_source_receipt_id": None,
                    "revision_updated_at": "2026-04-29T00:00:00Z",
                },
            ]
        if "JOIN knowledge_tag_catalog" in query:
            return [
                {
                    "section_ref": "section.retention",
                    "tag_ref": "knowledge_tag.policy.retention",
                    "tag_key": "policy",
                    "tag_value": "retention",
                    "summary": "Retention policy",
                    "semantic_assertion_id": "semantic_assertion.tagged_as.abc",
                    "source_kind": "operator",
                    "source_ref": "nate",
                    "evidence_ref": "section.retention",
                }
            ]
        if "FROM structured_document_section_semantics" in query:
            return [
                {
                    "section_ref": "section.retention",
                    "semantic_assertion_id": "semantic_assertion.constrains.abc",
                    "predicate_slug": "constrains",
                    "object_kind": "authority_domain",
                    "object_ref": "authority.workflow_runs",
                    "qualifiers_json": {"topic": "retention"},
                    "source_kind": "operator_document",
                    "source_ref": "doc.ops",
                    "evidence_ref": "section.retention",
                    "bound_decision_id": None,
                    "valid_from": "2026-04-29T00:00:00Z",
                    "valid_to": None,
                }
            ]
        if "FROM knowledge_digest_revisions" in query:
            return [
                {
                    "digest_ref": "knowledge_digest.retention",
                    "revision_ref": "digest.rev.1",
                    "digest_type": "section_summary",
                    "subject_kind": "structured_document_section",
                    "subject_ref": "section.retention",
                    "summary_text": "Retention receipts are durable evidence.",
                    "input_refs": [{"kind": "structured_document_section", "ref": "section.retention"}],
                    "input_hash": canonical_text_hash("Workflow retention policy"),
                    "output_hash": canonical_text_hash("Retention receipts are durable evidence."),
                    "recipe_ref": "recipe.section_summary",
                    "recipe_version": "v1",
                    "source_receipt_id": None,
                    "created_at": "2026-04-29T00:00:00Z",
                }
            ]
        if "FROM data_dictionary_classifications_effective" in query:
            return [
                {
                    "object_kind": "section.retention",
                    "field_path": "",
                    "tag_key": "operator_priority",
                    "tag_value": "high",
                    "source": "operator",
                    "confidence": 1.0,
                }
            ]
        raise AssertionError(query)


class _Subsystems:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _Conn:
        return self._conn


def test_canonical_text_hash_normalizes_whitespace() -> None:
    assert canonical_text_hash("Workflow   receipts\nmatter") == canonical_text_hash(
        "Workflow receipts matter"
    )
    assert canonical_text_hash("Workflow receipts matter").startswith("sha256:")


def test_assemble_context_returns_ranked_packet_and_receipt_inputs() -> None:
    conn = _Conn()
    result = assemble_context(
        conn,
        query_ref="query.retention",
        query_text="workflow retention",
        max_sections=1,
        include_rejected=True,
    )

    packet = result["context_packet"]
    selected = packet["selected_sections"]

    assert result["status"] == "assembled"
    assert result["packet_hash"].startswith("sha256:")
    assert packet["selection_count"] == 1
    assert selected[0]["section_ref"] == "section.retention"
    assert "semantic_assertion_match" in selected[0]["deterministic_reason_codes"]
    assert "authority_weight" in selected[0]["deterministic_reason_codes"]
    assert selected[0]["digest_refs"] == ["knowledge_digest.retention"]
    assert result["selection_receipt_operation"] == "structured_documents.record_context_selection"
    assert result["selection_receipt_inputs"][0]["selected"] is True
    assert result["selection_receipt_inputs"][0]["idempotency_key"].startswith("sha256:")
    assert any("effective_source AS source" in query for query, _args in conn.queries)
    assert any("valid_to IS NULL OR valid_to > now()" in query for query, _args in conn.queries)


def test_assemble_context_receipt_inputs_include_hidden_rejected_candidates() -> None:
    result = assemble_context(
        _Conn(),
        query_ref="query.browse",
        max_sections=1,
        include_rejected=False,
    )

    packet = result["context_packet"]

    assert packet["selection_count"] == 1
    assert len(packet["candidate_sections"]) == 1
    assert packet["omitted_candidate_count"] == 1
    assert len(result["selection_receipt_inputs"]) == 2
    assert [row["selected"] for row in result["selection_receipt_inputs"]] == [True, False]


def test_handle_assemble_context_uses_subsystem_connection() -> None:
    conn = _Conn()
    result = handle_assemble_context(
        AssembleStructuredDocumentContextQuery(
            query_ref="query.retention",
            query_text="workflow retention",
            max_sections=1,
        ),
        _Subsystems(conn),
    )

    assert result["query_ref"] == "query.retention"
    assert any("FROM structured_document_sections" in query for query, _args in conn.queries)
