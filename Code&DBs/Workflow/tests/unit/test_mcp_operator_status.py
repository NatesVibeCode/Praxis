from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://localhost:5432/praxis")

from surfaces.mcp.tools import operator


class _FakeConn:
    def __init__(self, *, zone_rows=None, fail_zone_lookup: bool = False) -> None:
        self._zone_rows = zone_rows or []
        self._fail_zone_lookup = fail_zone_lookup

    def execute(self, sql: str, *args):
        if "FROM failure_category_zones" in sql:
            if self._fail_zone_lookup:
                raise RuntimeError("zone authority unavailable")
            return self._zone_rows
        if "FROM workflow_runs" in sql:
            return []
        raise AssertionError(f"Unexpected SQL: {sql}")


def _receipt_record(*, status: str, failure_code: str = "", failure_category: str = ""):
    payload = {
        "status": status,
        "failure_code": failure_code,
        "failure_category": failure_category,
    }
    return SimpleNamespace(
        status=status,
        failure_code=failure_code,
        to_dict=lambda payload=payload: dict(payload),
    )


def test_praxis_status_uses_zone_authority_for_adjusted_pass_rate(monkeypatch) -> None:
    monkeypatch.setattr(
        operator._subs,
        "get_pg_conn",
        lambda: _FakeConn(zone_rows=[{"category": "provider_timeout", "zone": "external"}]),
    )
    monkeypatch.setattr(
        "runtime.receipt_store.list_receipts",
        lambda **_kwargs: [
            _receipt_record(status="succeeded"),
            _receipt_record(
                status="failed",
                failure_code="provider_timeout",
                failure_category="provider_timeout",
            ),
        ],
    )
    monkeypatch.setattr(
        "runtime.receipt_store.receipt_stats",
        lambda **_kwargs: {"totals": {"receipts": 2}},
    )

    result = operator.tool_praxis_status({"since_hours": 24})

    assert result["observability_state"] == "ready"
    assert result["zone_authority_ready"] is True
    assert result["failure_breakdown"]["by_zone"] == {"external": 1}
    assert result["adjusted_pass_rate"] == 1.0


def test_praxis_status_reports_degraded_when_zone_lookup_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        operator._subs,
        "get_pg_conn",
        lambda: _FakeConn(fail_zone_lookup=True),
    )
    monkeypatch.setattr(
        "runtime.receipt_store.list_receipts",
        lambda **_kwargs: [
            _receipt_record(
                status="failed",
                failure_code="provider_timeout",
                failure_category="provider_timeout",
            ),
        ],
    )
    monkeypatch.setattr(
        "runtime.receipt_store.receipt_stats",
        lambda **_kwargs: {"totals": {"receipts": 1}},
    )

    result = operator.tool_praxis_status({"since_hours": 24})

    assert result["observability_state"] == "degraded"
    assert result["zone_authority_ready"] is False
    assert result["adjusted_pass_rate"] is None
    assert result["errors"][0]["code"] == "failure_category_zones_lookup_failed"


def test_praxis_status_scans_the_full_receipt_window(monkeypatch) -> None:
    captured: dict[str, int] = {}

    monkeypatch.setattr(
        operator._subs,
        "get_pg_conn",
        lambda: _FakeConn(zone_rows=[{"category": "provider_timeout", "zone": "external"}]),
    )
    monkeypatch.setattr(
        "runtime.receipt_store.receipt_stats",
        lambda **_kwargs: {"totals": {"receipts": 6001}},
    )

    def _fake_list_receipts(*, limit: int, since_hours: int):
        captured["limit"] = limit
        captured["since_hours"] = since_hours
        return []

    monkeypatch.setattr("runtime.receipt_store.list_receipts", _fake_list_receipts)

    result = operator.tool_praxis_status({"since_hours": 24})

    assert captured == {"limit": 6001, "since_hours": 24}
    assert result["total_workflows"] == 0
