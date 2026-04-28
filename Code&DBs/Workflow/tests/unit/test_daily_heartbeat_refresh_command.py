from __future__ import annotations

from datetime import datetime, timezone

from runtime.daily_heartbeat import HeartbeatRunResult, ProbeSnapshot
from runtime.operations.commands import daily_heartbeat_refresh as refresh


def _heartbeat_result(*, scope: str, triggered_by: str) -> HeartbeatRunResult:
    now = datetime(2026, 4, 28, 18, 30, tzinfo=timezone.utc)
    return HeartbeatRunResult(
        heartbeat_run_id="heartbeat_run.credentials.20260428T183000Z.test",
        scope=scope,
        triggered_by=triggered_by,
        started_at=now,
        completed_at=now,
        status="partial",
        probes_total=2,
        probes_ok=1,
        probes_failed=1,
        summary=f"scope={scope} total=2 ok=1 failed=1",
        snapshots=[
            ProbeSnapshot(
                probe_kind="credential_expiry",
                subject_id="openai",
                subject_sub="api",
                status="ok",
                summary="openai credential healthy",
                days_until_expiry=20,
            ),
            ProbeSnapshot(
                probe_kind="credential_expiry",
                subject_id="anthropic",
                subject_sub="api",
                status="failed",
                summary="anthropic credential missing",
            ),
        ],
    )


def test_daily_heartbeat_refresh_wraps_runtime_result(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run(command: refresh.DailyHeartbeatRefreshCommand) -> HeartbeatRunResult:
        captured["command"] = command
        return _heartbeat_result(
            scope=command.scope,
            triggered_by=command.triggered_by,
        )

    monkeypatch.setattr(refresh, "_run_heartbeat", _fake_run)

    result = refresh.handle_daily_heartbeat_refresh(
        refresh.DailyHeartbeatRefreshCommand(
            scope="credentials",
            triggered_by="http",
        ),
        object(),
    )

    assert result["ok"] is True
    assert result["status"] == "partial"
    assert result["heartbeat_run_id"] == "heartbeat_run.credentials.20260428T183000Z.test"
    assert result["authority"] == {
        "operation_name": "operator.daily_heartbeat_refresh",
        "event_type": "daily.heartbeat.refreshed",
        "writes": ["heartbeat_runs", "heartbeat_probe_snapshots"],
    }
    assert result["event_payload"] == {
        "heartbeat_run_id": "heartbeat_run.credentials.20260428T183000Z.test",
        "scope": "credentials",
        "triggered_by": "http",
        "status": "partial",
        "probes_total": 2,
        "probes_ok": 1,
        "probes_failed": 1,
        "summary": "scope=credentials total=2 ok=1 failed=1",
        "source_refs": [
            "table.heartbeat_runs",
            "table.heartbeat_probe_snapshots",
        ],
    }
    assert captured["command"].scope == "credentials"
    assert captured["command"].triggered_by == "http"
