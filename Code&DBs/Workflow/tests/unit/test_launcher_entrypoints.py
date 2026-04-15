from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from surfaces.api.handlers import workflow_launcher


REPO_ROOT = Path(__file__).resolve().parents[4]


def _run_launcher_help(script_name: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(REPO_ROOT / "scripts" / script_name), "help"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
        text=True,
    )


def test_workflow_launcher_prefers_praxis_entrypoint() -> None:
    resolved = workflow_launcher._resolve_launcher_path()

    assert resolved == str(REPO_ROOT / "scripts" / "praxis")


def test_praxis_help_uses_canonical_command_name() -> None:
    completed = _run_launcher_help("praxis")

    assert "Usage: praxis <command> [service]" in completed.stdout
    assert "workflow ...            Canonical execution, query, and operator authority" in completed.stdout
    assert "db ...                  Schema authority plus SQL scaffolds" in completed.stdout
    assert "start [service...]" in completed.stdout
    assert "scheduler" in completed.stdout
    assert "Native launchd control is disabled." in completed.stdout
    assert "Scheduler is not containerized yet." not in completed.stdout
    assert "praxis-ctl start" not in completed.stdout


def test_praxis_workflow_passthrough_uses_workflow_frontdoor() -> None:
    completed = subprocess.run(
        [str(REPO_ROOT / "scripts" / "praxis"), "workflow", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == 0
    assert "Most used:" in completed.stdout
    assert "workflow tools list" in completed.stdout


def test_praxis_db_passthrough_uses_praxis_root_frontdoor() -> None:
    completed = subprocess.run(
        [str(REPO_ROOT / "scripts" / "praxis"), "db", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == 0
    assert "usage: praxis db <status|plan|apply|describe|primitive|table|view> [args]" in completed.stdout
    assert "praxis db primitive scaffold" in completed.stdout


def test_praxis_workflow_alias_script_delegates_to_canonical_frontdoor() -> None:
    completed = subprocess.run(
        [str(REPO_ROOT / "scripts" / "praxis-workflow"), "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == 0
    assert "Most used:" in completed.stdout
    assert "workflow inspect <run_id>" in completed.stdout


def test_praxis_ctl_help_preserves_alias_command_name() -> None:
    completed = _run_launcher_help("praxis-ctl")

    assert "Usage: praxis-ctl <command> [service]" in completed.stdout
    assert "praxis-ctl start [--foreground|postgres|api|workflow-api|worker|scheduler]" in completed.stdout
    assert "praxis start [postgres|api|workflow-api|worker|scheduler]" not in completed.stdout


def test_launcher_status_payload_uses_fast_frontdoor_profile(monkeypatch) -> None:
    calls: list[tuple[tuple[str, ...], dict[str, str] | None, float | None]] = []

    def _fake_run_launcher_json(*args: str, allow_failure: bool = False, extra_env=None, timeout_s=None):
        assert allow_failure is False
        calls.append((args, extra_env, timeout_s))
        return {
            "brand": "Praxis Engine",
            "service_manager": "scripts/praxis",
            "compatibility_alias": "scripts/praxis-ctl",
            "preferred_command": "praxis",
            "services": [],
            "doctor": {
                "services_ready": True,
                "database_reachable": True,
                "schema_bootstrapped": True,
                "api_server_ready": True,
                "workflow_api_ready": True,
                "mcp_bridge_ready": True,
                "ui_ready": True,
                "launch_url": "http://127.0.0.1:8420/app",
                "dashboard_url": "http://127.0.0.1:8420/app",
                "api_docs_url": "http://127.0.0.1:8420/docs",
                "dependency_truth": {"ok": True},
            },
            "dependency_truth": {"ok": True},
            "launch_url": "http://127.0.0.1:8420/app",
            "dashboard_url": "http://127.0.0.1:8420/app",
            "api_docs_url": "http://127.0.0.1:8420/docs",
        }

    monkeypatch.setattr(workflow_launcher, "_run_launcher_json", _fake_run_launcher_json)

    payload = workflow_launcher.launcher_status_payload()

    assert payload["ready"] is True
    assert [call[0] for call in calls] == [("status", "--json")]
    assert calls[0][1] is not None
    assert calls[0][1]["PRAXIS_ALPHA_TIMEOUT_API_HEALTH_S"] == "0.5"
    assert calls[0][2] == workflow_launcher._STATUS_TIMEOUT_S


def test_run_launcher_json_raises_structured_timeout(monkeypatch) -> None:
    def _fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=kwargs.get("args") or args[0], timeout=kwargs.get("timeout", 0))

    monkeypatch.setattr(subprocess, "run", _fake_run)

    with pytest.raises(workflow_launcher.LauncherAuthorityError, match="timed out"):
        workflow_launcher._run_launcher_json("status", "--json", timeout_s=1.5)
