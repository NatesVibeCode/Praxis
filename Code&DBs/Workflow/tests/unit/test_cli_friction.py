from __future__ import annotations

import json
from io import StringIO
from types import SimpleNamespace

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

    monkeypatch.setattr(
        cli_friction,
        "_recording_dependencies",
        lambda: (
            _FakeLedger,
            FrictionType,
            lambda pool: f"conn:{pool}",
            lambda *, env: "pool",
            lambda repo_root, *, env: {"WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis"},
        ),
    )

    recorded = cli_friction.record_cli_command_failure(
        args=["status", "--since-hours", "24000"],
        exit_code=2,
        output_text=(
            "workflow status does not support arguments: --since-hours 24000\n"
            "time-window filtering is not implemented\n"
        ),
        env={
            "WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis",
            "PRAXIS_CLI_FRICTION_RECORDING": "1",
        },
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

    monkeypatch.setattr(
        cli_friction,
        "_recording_dependencies",
        lambda: (
            object,
            FrictionType,
            object,
            _unexpected_pool,
            lambda repo_root, *, env: {"WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis"},
        ),
    )

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
        env={
            "WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis",
            "PRAXIS_CLI_FRICTION_RECORDING": "1",
        },
    )

    assert recorded is False


def test_record_cli_command_failure_is_disabled_under_pytest_by_default(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "test")

    def _unexpected_dependencies():
        raise AssertionError("pytest default should not reach DB dependencies")

    monkeypatch.setattr(
        cli_friction,
        "_recording_dependencies",
        _unexpected_dependencies,
    )

    recorded = cli_friction.record_cli_command_failure(
        args=["status", "--since-hours", "24000"],
        exit_code=2,
        output_text="workflow status does not support arguments",
        env={"WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis"},
    )

    assert recorded is False


def test_record_shell_command_failure_uses_same_friction_ledger_path(monkeypatch) -> None:
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

    monkeypatch.setattr(
        cli_friction,
        "_recording_dependencies",
        lambda: (
            _FakeLedger,
            FrictionType,
            lambda pool: f"conn:{pool}",
            lambda *, env: "pool",
            lambda repo_root, *, env: {"WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis"},
        ),
    )

    recorded = cli_friction.record_shell_command_failure(
        args=["run", "spec.queue.json"],
        exit_code=1,
        output_text="workflow database authority resolution returned no payload.\n",
        env={
            "WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis",
            "PRAXIS_CLI_FRICTION_RECORDING": "1",
        },
        source="cli.praxis",
        command_label_prefix="workflow",
    )

    assert recorded is True
    assert captured["conn"] == "conn:pool"
    assert captured["friction_type"] == FrictionType.HARD_FAILURE
    assert captured["source"] == "cli.praxis"
    assert captured["job_label"] == "workflow run"
    message = captured["message"]
    assert message["event"] == "cli_command_failure"
    assert message["exit_code"] == 1
    assert message["command"] == "run spec.queue.json"
    assert message["reason_code"] == "cli.command_failed"


def test_repeated_cli_failure_promotes_to_bug(monkeypatch) -> None:
    captured: dict[str, object] = {}
    args = ["status", "--since-hours", "24000"]
    fingerprint = cli_friction.cli_failure_fingerprint(
        args,
        reason_code="cli.unsupported_arguments",
    )

    class _FakeLedger:
        def __init__(self, conn: object) -> None:
            captured["conn"] = conn

        def record(self, **kwargs: object) -> None:
            captured["record"] = kwargs

        def patterns(self, **kwargs: object):
            captured["patterns_kwargs"] = kwargs
            return [
                SimpleNamespace(
                    fingerprint=fingerprint,
                    count=3,
                    command="status --since-hours 24000",
                    sample="workflow status does not support arguments",
                    event_ids=("evt1", "evt2", "evt3"),
                    promotion_candidate=True,
                )
            ]

    class _FakeBugTracker:
        def __init__(self, conn: object) -> None:
            captured["bug_conn"] = conn

        def list_bugs(self, **kwargs: object):
            captured["list_bugs_kwargs"] = kwargs
            return []

        def file_bug(self, **kwargs: object):
            captured["file_bug_kwargs"] = kwargs
            return SimpleNamespace(bug_id="BUG-TEST"), []

    monkeypatch.setattr(
        cli_friction,
        "_recording_dependencies",
        lambda: (
            _FakeLedger,
            FrictionType,
            lambda pool: f"conn:{pool}",
            lambda *, env: "pool",
            lambda repo_root, *, env: {"WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis"},
        ),
    )
    monkeypatch.setattr(
        cli_friction,
        "_bug_tracker_dependencies",
        lambda: (
            _FakeBugTracker,
            SimpleNamespace(P2="P2"),
            SimpleNamespace(RUNTIME="RUNTIME"),
        ),
    )

    recorded = cli_friction.record_cli_command_failure(
        args=args,
        exit_code=2,
        output_text="workflow status does not support arguments\n",
        env={
            "WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis",
            "PRAXIS_CLI_FRICTION_RECORDING": "1",
            "PRAXIS_CLI_FRICTION_AUTO_BUG_THRESHOLD": "2",
        },
    )

    assert recorded is True
    assert captured["patterns_kwargs"]["source"] == "cli.workflow"
    assert captured["patterns_kwargs"]["promotion_threshold"] == 2
    assert captured["list_bugs_kwargs"]["source_issue_id"] == f"cli.workflow-friction:{fingerprint}"
    filed = captured["file_bug_kwargs"]
    assert filed["source_kind"] == "friction_ledger"
    assert filed["source_issue_id"] == f"cli.workflow-friction:{fingerprint}"
    assert filed["filed_by"] == "cli_friction_auto_promoter"
    assert filed["tags"] == ("auto_cli_friction", "cli.workflow")
    assert filed["resume_context"]["event_ids"] == ["evt1", "evt2", "evt3"]


def test_record_shell_command_failure_defaults_to_praxis_labeling(monkeypatch) -> None:
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

    monkeypatch.setattr(
        cli_friction,
        "_recording_dependencies",
        lambda: (
            _FakeLedger,
            FrictionType,
            lambda pool: f"conn:{pool}",
            lambda *, env: "pool",
            lambda repo_root, *, env: {"WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis"},
        ),
    )

    recorded = cli_friction.record_shell_command_failure(
        args=["install"],
        exit_code=1,
        output_text="praxis install is no longer supported.\n",
        env={
            "WORKFLOW_DATABASE_URL": "postgresql://example.invalid/praxis",
            "PRAXIS_CLI_FRICTION_RECORDING": "1",
        },
    )

    assert recorded is True
    assert captured["source"] == "cli.praxis"
    assert captured["job_label"] == "praxis install"
    message = captured["message"]
    assert message["command"] == "install"
