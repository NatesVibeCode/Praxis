from __future__ import annotations

from typing import Any
from uuid import uuid4

from runtime.operations.commands.structured_documents import (
    RecordStructuredDocumentContextSelectionCommand,
    handle_record_context_selection,
)
from runtime.operations.queries.structured_documents import (
    ListStructuredDocumentContextSelectionsQuery,
    handle_list_context_selection_receipts,
)


class _FakeConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.selection_receipt_id = str(uuid4())
        self.authority_event_id = str(uuid4())

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM structured_document_context_selection_receipts" in normalized and "WHERE idempotency_key" in normalized:
            return None
        if "INSERT INTO structured_document_context_selection_receipts" in normalized:
            self.selection_receipt_id = args[0]
            return {
                "selection_receipt_id": args[0],
                "query_ref": args[1],
                "assembler_ref": args[2],
                "section_ref": args[3],
                "selected": args[4],
                "score_total": args[5],
                "score_breakdown": {"lexical": 1.0},
                "deterministic_reason_codes": ["lexical_match"],
                "semantic_assertion_ids": ["assertion-1"],
                "source_receipt_id": args[9],
                "authority_event_id": None,
                "idempotency_key": args[10],
                "metadata": {"lane": "unit"},
                "created_at": "2026-04-23T00:00:00+00:00",
            }
        if normalized.startswith("UPDATE structured_document_context_selection_receipts"):
            self.authority_event_id = args[1]
            return {
                "selection_receipt_id": args[0],
                "query_ref": "query-1",
                "assembler_ref": "assembler.test",
                "section_ref": "section-1",
                "selected": True,
                "score_total": 1.25,
                "score_breakdown": {"lexical": 1.0},
                "deterministic_reason_codes": ["lexical_match"],
                "semantic_assertion_ids": ["assertion-1"],
                "source_receipt_id": None,
                "authority_event_id": args[1],
                "idempotency_key": "selection:test",
                "metadata": {"lane": "unit"},
                "created_at": "2026-04-23T00:00:00+00:00",
            }
        raise AssertionError(f"unexpected fetchrow query: {query}")

    def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        if "structured_document_context_receipt_summary" in query:
            return [{
                "query_ref": "query-1",
                "assembler_ref": "assembler.test",
                "section_ref": "section-1",
                "document_ref": "document-1",
                "revision_ref": "revision-1",
                "breadcrumb": "Doc > Intro",
                "selected": True,
                "score_total": 1.25,
                "score_breakdown": {"lexical": 1.0},
                "deterministic_reason_codes": ["lexical_match"],
                "semantic_assertion_ids": ["assertion-1"],
                "source_receipt_id": None,
                "authority_event_id": self.authority_event_id,
                "created_at": "2026-04-23T00:00:00+00:00",
            }]
        return []

    def execute(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.execute_calls.append((query, args))
        return []

    def executed_sql(self) -> str:
        return "\n".join(
            [query for query, _ in self.fetchrow_calls]
            + [query for query, _ in self.fetch_calls]
            + [query for query, _ in self.execute_calls]
        )


class _Subsystems:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def get_pg_conn(self) -> _FakeConn:
        return self._conn


def test_record_context_selection_persists_receipt_and_event() -> None:
    conn = _FakeConn()

    result = handle_record_context_selection(
        RecordStructuredDocumentContextSelectionCommand(
            query_ref="query-1",
            assembler_ref="assembler.test",
            section_ref="section-1",
            selected=True,
            score_total=1.25,
            score_breakdown={"lexical": 1.0},
            deterministic_reason_codes=["lexical_match"],
            semantic_assertion_ids=["assertion-1"],
            idempotency_key="selection:test",
            metadata={"lane": "unit"},
        ),
        _Subsystems(conn),
    )

    assert result["status"] == "recorded"
    assert result["context_selection_receipt"]["section_ref"] == "section-1"
    assert result["authority_event_ids"] == [conn.authority_event_id]
    sql = conn.executed_sql()
    assert "INSERT INTO structured_document_context_selection_receipts" in sql
    assert "INSERT INTO authority_events" in sql
    assert "UPDATE structured_document_context_selection_receipts" in sql


def test_list_context_selection_receipts_reads_projection_summary() -> None:
    conn = _FakeConn()

    result = handle_list_context_selection_receipts(
        ListStructuredDocumentContextSelectionsQuery(
            query_ref="query-1",
            selected=True,
        ),
        _Subsystems(conn),
    )

    assert result["status"] == "listed"
    assert result["context_selection_receipts"][0]["document_ref"] == "document-1"
    assert result["context_selection_receipts"][0]["selected"] is True
    assert "structured_document_context_receipt_summary" in conn.fetch_calls[0][0]
