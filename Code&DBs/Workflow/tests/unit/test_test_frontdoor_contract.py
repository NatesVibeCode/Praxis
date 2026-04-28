from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def _load_test_frontdoor(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://localhost/praxis_test")
    module_path = Path(__file__).resolve().parents[4] / "scripts" / "test_frontdoor.py"
    spec = importlib.util.spec_from_file_location("test_frontdoor_contract_subject", module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_validate_payload_fails_when_spec_failed_banner_despite_zero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frontdoor = _load_test_frontdoor(monkeypatch)

    monkeypatch.setattr(
        frontdoor,
        "_run_command",
        lambda _command: {
            "duration_s": 0.01,
            "returncode": 0,
            "stdout": "=== Spec Validation: FAILED ===\nName: smoke\nWorkflow ID: cli_validate_smoke\n",
            "stderr": "",
        },
    )

    payload = frontdoor._validate_payload(["config/cascade/specs/demo.queue.json"])

    assert payload["ok"] is False
    assert any("spec validation reported FAILURE" in err for err in payload["errors"])
    assert payload["warnings"] == []


def test_validate_payload_treats_structured_authority_error_as_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frontdoor = _load_test_frontdoor(monkeypatch)

    monkeypatch.setattr(
        frontdoor,
        "_run_command",
        lambda _command: {
            "duration_s": 0.01,
            "returncode": 1,
            "stdout": (
                "=== Spec Validation: FAILED ===\n"
                "Agent Resolution:\n"
                "  job: gpt-5.4-mini -> AUTHORITY ERROR "
                "(PostgresConfigurationError: WORKFLOW_DATABASE_URL authority unavailable)\n"
                "agent authority unavailable: PostgresConfigurationError: "
                "WORKFLOW_DATABASE_URL authority unavailable\n"
            ),
            "stderr": "",
        },
    )

    payload = frontdoor._validate_payload(["config/cascade/specs/demo.queue.json"])

    assert payload["ok"] is False
    assert payload["errors"] == [
        "workflow validation failed: agent resolution was blocked by the sandbox permission surface"
    ]
    assert payload["warnings"] == [
        "workflow spec parsed, but agent resolution was blocked by the sandbox permission surface"
    ]


def test_help_is_available_without_database_env(monkeypatch: pytest.MonkeyPatch) -> None:
    module_path = Path(__file__).resolve().parents[4] / "scripts" / "test_frontdoor.py"
    env = os.environ.copy()
    env.pop("WORKFLOW_DATABASE_URL", None)

    run = subprocess.run(
        [sys.executable, str(module_path), "--help"],
        cwd=str(module_path.parents[1]),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert run.returncode == 0
    payload = json.loads(run.stdout)
    assert payload["ok"] is True
    assert payload["errors"] == []
    assert payload["results"]["usage"].startswith("usage: ./scripts/test.sh")


def test_help_lists_python_dependency_audit_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frontdoor = _load_test_frontdoor(monkeypatch)

    payload = frontdoor._help_payload()

    assert "python-dependency-audit" in payload["results"]["commands"]


def test_pytest_commands_bootstrap_database_authority_without_leaking_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frontdoor = _load_test_frontdoor(monkeypatch)

    command = frontdoor._suite_command_text("unit")

    assert command.startswith(
        ". ./scripts/_workflow_env.sh && workflow_load_repo_env && PYTHONPATH="
    )
    assert "postgresql://localhost/praxis_test" not in command
    assert " -m pytest --noconftest -q " in command


def test_check_affected_rejects_queue_with_no_known_suite_matches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    frontdoor = _load_test_frontdoor(monkeypatch)
    queue_file = tmp_path / "unmapped.queue.json"
    queue_file.write_text(
        json.dumps(
            {
                "name": "unmapped queue",
                "workflow_id": "workflow.unmapped",
                "read_scope": ["docs/operator-notes/not-covered-by-suite.md"],
            }
        ),
        encoding="utf-8",
    )

    payload = frontdoor._check_affected_payload([str(queue_file)])

    assert payload["ok"] is False
    assert payload["errors"] == ["no known test suites matched the queue file paths"]
    assert payload["warnings"] == []
    assert payload["results"]["affected_suites"] == []
    assert payload["results"]["unclassified_paths"] == [
        "docs/operator-notes/not-covered-by-suite.md"
    ]


def test_python_dependency_audit_reports_unsupported_when_tool_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frontdoor = _load_test_frontdoor(monkeypatch)

    monkeypatch.setattr(frontdoor.importlib.util, "find_spec", lambda _name: None)
    monkeypatch.setattr(frontdoor.shutil, "which", lambda _name: None)

    payload = frontdoor._python_dependency_audit_payload([])

    assert payload["ok"] is False
    assert payload["results"]["status"] == "unsupported"
    assert payload["results"]["requirements_file"].endswith("Code&DBs/Workflow/requirements.runtime.txt")
    assert "pip-audit" in payload["errors"][0]
