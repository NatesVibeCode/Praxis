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


def test_validate_payload_downgrades_structured_authority_error_to_warning(
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

    assert payload["ok"] is True
    assert payload["errors"] == []
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
