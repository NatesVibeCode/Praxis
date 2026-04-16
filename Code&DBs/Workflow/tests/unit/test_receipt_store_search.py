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
                "receipt_id": "receipt-7",
                "workflow_id": "workflow-test",
                "run_id": "workflow_run_123",
                "request_id": "request-1",
                "node_id": "deploy-check",
                "attempt_no": 1,
                "started_at": datetime(2026, 4, 8, 18, 40, tzinfo=timezone.utc),
                "finished_at": datetime(2026, 4, 8, 18, 45, tzinfo=timezone.utc),
                "executor_type": "openai/gpt-5.4",
                "status": "succeeded",
                "failure_code": "",
                "inputs": {},
                "outputs": {"note": "receipt evidence"},
                "artifacts": {},
                "decision_refs": [],
            }
        ]


def test_search_receipts_uses_raw_receipt_evidence(monkeypatch) -> None:
    conn = _FakeConn()
    monkeypatch.setattr(receipt_store, "_conn", lambda: conn)

    results = receipt_store.search_receipts("receipt evidence", limit=3)

    assert len(results) == 1
    sql, params = conn.calls[0]
    assert "COALESCE(outputs::text, '') ILIKE" in sql
    assert "raw_json" not in sql
    assert params == ("receipt evidence", 3)
    assert results[0].run_id == "workflow_run_123"


def test_conn_uses_runtime_database_authority_resolution(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        receipt_store,
        "resolve_runtime_database_url",
        lambda: "postgresql://repo.test/workflow",
    )
    monkeypatch.setattr(
        receipt_store,
        "ensure_postgres_available",
        lambda *, env: captured.update({"env": env}) or object(),
    )

    result = receipt_store._conn()

    assert result is not None
    assert captured["env"] == {"WORKFLOW_DATABASE_URL": "postgresql://repo.test/workflow"}
