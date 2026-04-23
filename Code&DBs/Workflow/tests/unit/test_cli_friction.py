from __future__ import annotations

import json
from io import StringIO

from runtime.friction_ledger import FrictionType
from surfaces.cli import friction as cli_friction


def test_tracking_stdout_tees_and_bounds_output() -> None:
    target = StringIO()
    tracked = cli_friction.TrackingStdout(target, max_chars=5)

    tracked.write("abcdef")
    tracked.write("gh")

    assert target.getvalue() == "abcdefgh"
    assert tracked.captured_output() == "abcde"
    assert tracked.truncated is True


def test_record_cli_command_failure_writes_to_friction_ledger(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeLedger:
        def __init__(self, conn: object) -> None:
            captured["conn"] = conn

        def record(
            self,
            *,
            friction_type: FrictionType,
            source: str,
            job_label: str,
            message: str,
        ) -> None:
            captured["friction_type"] = friction_type
            captured["source"] = source
            captured["job_label"] = job_label
            captured["message"] = json.loads(message)

    monkeypatch.setattr(cli_friction, "FrictionLedger", _FakeLedger)
    monkeypatch.setattr(cli_friction, "get_workflow_pool", lambda *, env: "pool")
    monkeypatch.setattr(cli_friction, "SyncPostgresConnection", lambda pool: f"conn:{pool}")

    recorded = cli_friction.record_cli_command_failure(
        args=["status", "--since-hours", "24000"],
        exit_code=2,
        output_text=(
            "workflow status does not support arguments: --since-hours 24000\n"
            "time-window filtering is not implemented\n"
        ),
        env={"WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis"},
    )

    assert recorded is True
    assert captured["conn"] == "conn:pool"
    assert captured["friction_type"] == FrictionType.HARD_FAILURE
    assert captured["source"] == "cli.workflow"
    assert captured["job_label"] == "workflow status"
    message = captured["message"]
    assert message["event"] == "cli_command_failure"
    assert message["exit_code"] == 2
    assert message["command"] == "status --since-hours 24000"
    assert message["reason_code"] == "cli.unsupported_arguments"
    assert message["fingerprint"] == cli_friction.cli_failure_fingerprint(
        ["status", "--since-hours", "24000"],
        reason_code="cli.unsupported_arguments",
    )


def test_record_cli_command_failure_skips_db_authority_failures(monkeypatch) -> None:
    def _unexpected_pool(*, env):
        raise AssertionError("DB-unavailable failures should not re-enter DB recording")

    monkeypatch.setattr(cli_friction, "get_workflow_pool", _unexpected_pool)

    recorded = cli_friction.record_cli_command_failure(
        args=["records", "list", "--never-run"],
        exit_code=1,
        output_text=json.dumps(
            {
                "status": "error",
                "reason_code": "workflow_records.db_authority_unavailable",
                "message": "db down",
            }
        ),
        env={"WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis"},
    )

    assert recorded is False
