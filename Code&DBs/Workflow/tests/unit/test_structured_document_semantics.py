from __future__ import annotations

import json
from typing import Any

import pytest

from runtime.structured_document_semantics import (
    STRUCTURED_DOCUMENT_CONTEXT_SELECTED_EVENT,
    STRUCTURED_DOCUMENT_SECTION_KIND,
    StructuredDocumentSemanticError,
    context_selection_receipt_payload,
    record_context_selection,
    score_context_candidate,
    section_embedding_input,
    semantic_assertion_command_for_section,
    structured_section_ref,
)


def test_structured_section_ref_is_stable_and_addressable() -> None:
    first = structured_section_ref(
        document_ref="Runtime Policy",
        revision_ref="rev 4",
        node_index=18,
    )
    second = structured_section_ref(
        document_ref="Runtime Policy",
        revision_ref="rev 4",
        node_index=18,
    )

    assert first == second
    assert first.startswith("doc:runtime-policy-")
    assert "@rev-4-" in first
    assert first.endswith("#node-000018")


def test_section_embedding_input_is_deterministic_and_includes_meaning_cues() -> None:
    text = section_embedding_input(
        document_title="Runtime Policy",
        breadcrumb="Governance > Runtime",
        heading="Fallback Policy",
        semantic_predicates=("constrains", "applies_to"),
        local_text="No local database guessing.",
    )

    assert text == section_embedding_input(
        document_title="Runtime Policy",
        breadcrumb="Governance > Runtime",
        heading="Fallback Policy",
        semantic_predicates=("applies_to", "constrains"),
        local_text="No local database guessing.",
    )
    assert "title: Runtime Policy" in text
    assert "breadcrumb: Governance > Runtime" in text
    assert "semantic_predicates: applies_to, constrains" in text
    assert "No local database guessing." in text


def test_vector_only_candidate_is_recall_not_authority() -> None:
    score = score_context_candidate(vector_similarity=0.73)

    assert score.score_total == 0.73
    assert score.eligible_for_authoritative_context is False
    assert score.deterministic_reason_codes == ("vector_recall_only",)
    with pytest.raises(StructuredDocumentSemanticError, match="deterministic"):
        context_selection_receipt_payload(
            query_ref="query.runtime-policy",
            assembler_ref="context.assembler",
            section_ref="doc:runtime@rev#node-000001",
            selected=True,
            score=score,
        )


def test_semantic_assertion_match_can_justify_selected_context() -> None:
    score = score_context_candidate(
        semantic_assertion_match=2,
        vector_similarity=0.5,
        semantic_assertion_ids=("semantic_assertion.constrains.abc",),
    )
    payload = context_selection_receipt_payload(
        query_ref="query.runtime-policy",
        assembler_ref="context.assembler",
        section_ref="doc:runtime@rev#node-000001",
        selected=True,
        score=score,
        source_receipt_id="00000000-0000-0000-0000-000000000001",
    )

    assert score.eligible_for_authoritative_context is True
    assert "semantic_assertion_match" in payload["deterministic_reason_codes"]
    assert payload["semantic_assertion_ids"] == ["semantic_assertion.constrains.abc"]
    assert payload["score_breakdown"]["vector_similarity"] == 0.5


def test_semantic_assertion_command_for_section_uses_explicit_authority_columns() -> None:
    command = semantic_assertion_command_for_section(
        predicate_slug="constrains",
        section_ref="doc:runtime@rev#node-000001",
        object_kind="authority_domain",
        object_ref="authority.workflow_runtime",
        source_kind="operator_decision",
        source_ref="operator_decision.runtime-policy",
        evidence_ref="receipt.context.1",
        qualifiers_json={"confidence": "operator-approved"},
    )

    assert command["subject_kind"] == STRUCTURED_DOCUMENT_SECTION_KIND
    assert command["subject_ref"] == "doc:runtime@rev#node-000001"
    assert command["predicate_slug"] == "constrains"
    assert command["object_kind"] == "authority_domain"
    assert command["object_ref"] == "authority.workflow_runtime"
    assert command["source_kind"] == "operator_decision"
    assert command["source_ref"] == "operator_decision.runtime-policy"
    assert command["semantic_assertion_id"].startswith("semantic_assertion.constrains.")
    assert "subject_ref" not in command["qualifiers_json"]


def test_semantic_assertion_command_rejects_hidden_authority_qualifiers() -> None:
    with pytest.raises(StructuredDocumentSemanticError, match="hide"):
        semantic_assertion_command_for_section(
            predicate_slug="defines",
            section_ref="doc:runtime@rev#node-000001",
            object_kind="term",
            object_ref="standing_orders",
            source_kind="operator_decision",
            source_ref="operator_decision.runtime-policy",
            qualifiers_json={"subject_ref": "shadow"},
        )


class _FakeConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, args))
        if "WHERE idempotency_key = $1" in query:
            return None
        if "INSERT INTO structured_document_context_selection_receipts" in query:
            return {
                "selection_receipt_id": args[0],
                "query_ref": args[1],
                "assembler_ref": args[2],
                "section_ref": args[3],
                "selected": args[4],
                "score_total": args[5],
                "score_breakdown": json.loads(args[6]),
                "deterministic_reason_codes": json.loads(args[7]),
                "semantic_assertion_ids": json.loads(args[8]),
                "authority_event_id": None,
            }
        if "UPDATE structured_document_context_selection_receipts" in query:
            return {
                "selection_receipt_id": args[0],
                "authority_event_id": args[1],
            }
        raise AssertionError(f"unexpected fetchrow: {query}")

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.execute_calls.append((query, args))
        return []


def test_record_context_selection_emits_authority_event() -> None:
    conn = _FakeConn()
    score = score_context_candidate(
        structure_match=1,
        semantic_assertion_ids=("semantic_assertion.applies_to.abc",),
    )
    result = record_context_selection(
        conn,
        {
            "query_ref": "query.runtime",
            "assembler_ref": "context.assembler",
            "section_ref": "doc:runtime@rev#node-000001",
            "selected": True,
            "score_total": score.score_total,
            "score_breakdown": score.score_breakdown,
            "deterministic_reason_codes": score.deterministic_reason_codes,
            "semantic_assertion_ids": score.semantic_assertion_ids,
            "idempotency_key": "ctx:runtime:1",
        },
    )

    assert result["status"] == "recorded"
    assert result["authority_event_ids"]
    assert conn.execute_calls[0][1][3] == STRUCTURED_DOCUMENT_CONTEXT_SELECTED_EVENT
