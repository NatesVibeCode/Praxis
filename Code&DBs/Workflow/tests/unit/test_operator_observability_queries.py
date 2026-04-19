from __future__ import annotations

import asyncio
from types import SimpleNamespace

from runtime.operations.queries import operator_observability


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


def test_operator_status_snapshot_uses_zone_authority_for_adjusted_pass_rate(monkeypatch) -> None:
    subsystems = SimpleNamespace(
        get_pg_conn=lambda: _FakeConn(
            zone_rows=[{"category": "provider_timeout", "zone": "external"}]
        )
    )
    monkeypatch.setattr(
        "runtime.operations.queries.operator_observability.list_receipts",
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
        "runtime.operations.queries.operator_observability.receipt_stats",
        lambda **_kwargs: {"totals": {"receipts": 2}},
    )

    result = operator_observability.handle_query_operator_status_snapshot(
        operator_observability.QueryOperatorStatusSnapshot(since_hours=24),
        subsystems,
    )

    assert result["observability_state"] == "ready"
    assert result["zone_authority_ready"] is True
    assert result["failure_breakdown"]["by_zone"] == {"external": 1}
    assert result["adjusted_pass_rate"] == 1.0


def test_operator_status_snapshot_reports_degraded_when_zone_lookup_fails(monkeypatch) -> None:
    subsystems = SimpleNamespace(
        get_pg_conn=lambda: _FakeConn(fail_zone_lookup=True)
    )
    monkeypatch.setattr(
        "runtime.operations.queries.operator_observability.list_receipts",
        lambda **_kwargs: [
            _receipt_record(
                status="failed",
                failure_code="provider_timeout",
                failure_category="provider_timeout",
            ),
        ],
    )
    monkeypatch.setattr(
        "runtime.operations.queries.operator_observability.receipt_stats",
        lambda **_kwargs: {"totals": {"receipts": 1}},
    )

    result = operator_observability.handle_query_operator_status_snapshot(
        operator_observability.QueryOperatorStatusSnapshot(since_hours=24),
        subsystems,
    )

    assert result["observability_state"] == "degraded"
    assert result["zone_authority_ready"] is False
    assert result["adjusted_pass_rate"] is None
    assert result["errors"][0]["code"] == "failure_category_zones_lookup_failed"


def test_operator_status_snapshot_scans_the_full_receipt_window(monkeypatch) -> None:
    captured: dict[str, int] = {}
    subsystems = SimpleNamespace(
        get_pg_conn=lambda: _FakeConn(
            zone_rows=[{"category": "provider_timeout", "zone": "external"}]
        )
    )
    monkeypatch.setattr(
        "runtime.operations.queries.operator_observability.receipt_stats",
        lambda **_kwargs: {"totals": {"receipts": 6001}},
    )

    def _fake_list_receipts(*, limit: int, since_hours: int):
        captured["limit"] = limit
        captured["since_hours"] = since_hours
        return []

    monkeypatch.setattr(
        "runtime.operations.queries.operator_observability.list_receipts",
        _fake_list_receipts,
    )

    result = operator_observability.handle_query_operator_status_snapshot(
        operator_observability.QueryOperatorStatusSnapshot(since_hours=24),
        subsystems,
    )

    assert captured == {"limit": 6001, "since_hours": 24}
    assert result["total_workflows"] == 0


def test_run_status_view_uses_async_evidence_reader(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeEvidenceReader:
        def __init__(self, *, env=None, **_kwargs) -> None:
            captured["env"] = env

        def evidence_timeline(self, _run_id: str):
            raise AssertionError("sync evidence_timeline must not be used inside async handler")

        async def load_evidence_timeline(self, *, run_id: str):
            captured["run_id"] = run_id
            return ("evidence-row",)

    async def _fake_support(*, run_id: str, env=None, **_kwargs):
        captured["support_run_id"] = run_id
        captured["support_env"] = env
        return SimpleNamespace(outbox_depth=0)

    def _fake_inspect_run(**kwargs):
        captured["inspection_evidence"] = kwargs["canonical_evidence"]
        return SimpleNamespace(operator_frame_source="missing", operator_frames=())

    def _fake_operator_status_run(**kwargs):
        captured["status_evidence"] = kwargs["canonical_evidence"]
        return {"run_id": kwargs["run_id"], "ok": True}

    monkeypatch.setattr("storage.postgres.PostgresEvidenceReader", _FakeEvidenceReader)
    monkeypatch.setattr("observability.load_native_operator_support", _fake_support)
    monkeypatch.setattr("observability.inspect_run", _fake_inspect_run)
    monkeypatch.setattr("observability.operator_status_run", _fake_operator_status_run)
    monkeypatch.setattr("observability.render_operator_status", lambda _view: "rendered")

    env = {"WORKFLOW_DATABASE_URL": "postgresql://example/praxis"}
    subsystems = SimpleNamespace(_postgres_env=lambda: env)
    result = asyncio.run(
        operator_observability.handle_query_run_status_view(
            operator_observability.QueryRunScopedOperatorView(run_id="run-async"),
            subsystems,
        )
    )

    assert result["view"] == "status"
    assert result["payload"] == {"run_id": "run-async", "ok": True}
    assert result["rendered"] == "rendered"
    assert captured["env"] == env
    assert captured["support_env"] == env
    assert captured["run_id"] == "run-async"
    assert captured["inspection_evidence"] == ("evidence-row",)
    assert captured["status_evidence"] == ("evidence-row",)


def test_replay_ready_bugs_rejects_refresh_backfill() -> None:
    subsystems = SimpleNamespace()

    try:
        operator_observability.handle_query_replay_ready_bugs(
            operator_observability.QueryReplayReadyBugs(
                limit=10,
                refresh_backfill=True,
            ),
            subsystems,
        )
    except ValueError as exc:
        assert "read-only" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected refresh_backfill to fail closed")
