from __future__ import annotations

from datetime import datetime, timezone

from runtime import receipt_store


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, *params):
        self.calls.append((sql, params))
        return [
            {
                "id": 7,
                "label": "deploy-check",
                "agent": "openai/gpt-5.4",
                "status": "succeeded",
                "failure_code": "",
                "timestamp": datetime(2026, 4, 8, 18, 45, tzinfo=timezone.utc),
                "raw_json": {
                    "run_id": "workflow_run_123",
                    "outputs": {"note": "receipt evidence"},
                },
            }
        ]


def test_search_receipts_uses_raw_receipt_evidence(monkeypatch) -> None:
    conn = _FakeConn()
    monkeypatch.setattr(receipt_store, "_conn", lambda: conn)

    results = receipt_store.search_receipts("receipt evidence", limit=3)

    assert len(results) == 1
    sql, params = conn.calls[0]
    assert "to_tsvector('english', COALESCE(rs.raw_json::text, ''))" in sql
    assert "spec_name" not in sql
    assert params == ("receipt evidence", 3)
    assert results[0].run_id == "workflow_run_123"
