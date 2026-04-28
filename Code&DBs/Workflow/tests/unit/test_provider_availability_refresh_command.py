from __future__ import annotations

from datetime import datetime, timezone

from runtime.daily_heartbeat import HeartbeatRunResult, ProbeSnapshot
from runtime.operations.commands import provider_availability_refresh as refresh


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, *args: object):
        self.executed.append((sql, args))
        return []


class _FakeSubsystems:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def get_pg_conn(self) -> _FakeConn:
        return self.conn


def _heartbeat_result(*, status: str = "partial") -> HeartbeatRunResult:
    now = datetime(2026, 4, 28, 15, 0, tzinfo=timezone.utc)
    return HeartbeatRunResult(
        heartbeat_run_id="heartbeat_run.providers.20260428T150000Z.test",
        scope="providers",
        triggered_by="mcp",
        started_at=now,
        completed_at=now,
        status=status,
        probes_total=2,
        probes_ok=1,
        probes_failed=1,
        summary="scope=providers total=2 ok=1 failed=1",
        snapshots=[
            ProbeSnapshot(
                probe_kind="provider_usage",
                subject_id="openai",
                subject_sub="cli_llm",
                status="ok",
                summary="openai/cli_llm: ok",
                latency_ms=123,
                input_tokens=4,
                output_tokens=1,
                details={
                    "model_slug": "gpt-5.4-mini",
                    "transport_kind": "cli",
                    "returncode": 0,
                },
            ),
            ProbeSnapshot(
                probe_kind="provider_usage",
                subject_id="google",
                subject_sub="cli_llm",
                status="failed",
                summary="google/cli_llm: failed",
                latency_ms=1000,
                details={
                    "model_slug": None,
                    "transport_kind": "cli",
                    "returncode": 1,
                },
            ),
        ],
    )


def test_provider_availability_refresh_summarizes_probe_and_refreshes_projection(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run(command: refresh.ProviderAvailabilityRefreshCommand):
        captured["command"] = command
        return _heartbeat_result()

    monkeypatch.setattr(refresh, "_run_provider_heartbeat", _fake_run)
    subsystems = _FakeSubsystems()

    result = refresh.handle_provider_availability_refresh(
        refresh.ProviderAvailabilityRefreshCommand(
            provider_slugs=("openai", "google"),
            adapter_types=("cli_llm",),
            max_concurrency=2,
            timeout_s=30,
            runtime_profile_ref="praxis",
        ),
        subsystems,
    )

    assert result["ok"] is True
    assert result["status"] == "partial"
    assert result["provider_health"] == "degraded"
    assert result["heartbeat_run_id"] == "heartbeat_run.providers.20260428T150000Z.test"
    assert result["status_counts"] == {"ok": 1, "failed": 1}
    assert result["snapshots"][0]["provider_slug"] == "openai"
    assert result["snapshots"][0]["source_ref"] == "table.heartbeat_probe_snapshots"
    assert result["control_plane_refresh"] == {
        "ok": True,
        "projection_ref": "projection.private_provider_control_plane_snapshot",
        "runtime_profile_ref": "praxis",
    }
    assert captured["command"].max_concurrency == 2
    assert subsystems.conn.executed == [
        ("SELECT refresh_private_provider_control_plane_snapshot($1)", ("praxis",))
    ]
    assert result["event_payload"]["heartbeat_run_id"] == result["heartbeat_run_id"]


def test_provider_availability_refresh_can_skip_snapshot_payload(monkeypatch) -> None:
    monkeypatch.setattr(refresh, "_run_provider_heartbeat", lambda _command: _heartbeat_result(status="succeeded"))

    result = refresh.handle_provider_availability_refresh(
        refresh.ProviderAvailabilityRefreshCommand(
            include_snapshots=False,
            refresh_control_plane=False,
        ),
        _FakeSubsystems(),
    )

    assert result["status"] == "succeeded"
    assert "snapshots" not in result
    assert result["control_plane_refresh"] == {"ok": False, "skipped": True}
