from __future__ import annotations

import json
from types import SimpleNamespace

from runtime import failure_category_backfill as backfill_mod


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.receipt_update_payloads: list[dict[str, object]] = []

    def execute(self, query: str, *args: object):
        normalized = " ".join(query.split())
        self.calls.append(normalized)

        match len(self.calls) - 1:
            case 0:
                return [
                    {
                        "receipt_id": "receipt-1",
                        "failure_code": "rate_limit",
                        "outputs": {},
                    }
                ]
            case 1:
                self.receipt_update_payloads.append(json.loads(str(args[1])))
                return [{"receipt_id": args[0]}]
            case 2:
                return [{"id": "job-1"}]
            case 3:
                return [{"cnt": 4}]
            case 4:
                return [{"id": "meta-1"}]
            case 5:
                return [{"cnt": 2}]
            case 6:
                return [{"failure_category": "rate_limit", "cnt": 1}]
            case 7:
                return [{"failure_zone": "external", "cnt": 1}]
            case 8:
                return [{"cnt": 0}]
            case _:
                raise AssertionError(f"unexpected query: {normalized}")


class _FakeClassification:
    category = SimpleNamespace(value="rate_limit")
    is_transient = True

    def to_dict(self) -> dict[str, object]:
        return {"category": "rate_limit", "confidence": 0.99}


def test_backfill_failure_categories_updates_projection_fields(monkeypatch) -> None:
    monkeypatch.setattr(backfill_mod, "classify_failure", lambda *_args, **_kwargs: _FakeClassification())
    conn = _FakeConn()

    payload = backfill_mod.backfill_failure_categories(conn)

    assert payload["receipts_scanned"] == 1
    assert payload["receipts_updated"] == 1
    assert payload["workflow_jobs_updated"] == 1
    assert payload["workflow_jobs_remaining"] == 4
    assert payload["receipt_meta_updated"] == 1
    assert payload["receipt_meta_remaining"] == 2
    assert payload["remaining_unclassified"] == 0
    assert payload["final_breakdown"] == [{"failure_category": "rate_limit", "count": 1}]
    assert payload["zone_breakdown"] == [{"failure_zone": "external", "count": 1}]
    assert conn.receipt_update_payloads[0]["failure_zone"] == "external"
    assert conn.receipt_update_payloads[0]["failure_classification"]["category"] == "rate_limit"
