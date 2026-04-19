from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from surfaces.api.handlers import workflow_launcher


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_legacy_launch_ui_shell_wrapper_is_absent() -> None:
    assert not (REPO_ROOT / "Code&DBs" / "Workflow" / "scripts" / "launch-ui.sh").exists()


def test_legacy_service_manager_shell_wrapper_is_absent() -> None:
    assert not (REPO_ROOT / "scripts" / "praxis-service-manager").exists()


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
    assert "launch                  Start Docker services, probe launcher readiness, and optionally open /app" in completed.stdout
    assert "doctor --json           Emit semantic launcher readiness as JSON" in completed.stdout
    assert "start [service...]" in completed.stdout
    assert "scheduler" in completed.stdout
    assert "Native launchd install/setup control has been removed." in completed.stdout
    assert "Scheduler is not containerized yet." not in completed.stdout
    assert "praxis-service-manager" not in completed.stdout


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


def test_praxis_workflow_run_no_longer_routes_through_workflow_sh(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "spec.queue.json"
    spec_path.write_text('{"name":"probe","workflow_id":"probe","phase":"test","jobs":[]}\n', encoding="utf-8")
    capture_path = tmp_path / "python-args.txt"
    fake_python = tmp_path / "fake_python"
    fake_python.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -eu",
                "printf '%s\\n' \"$@\" > \"$CAPTURE_PATH\"",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    completed = subprocess.run(
        [str(REPO_ROOT / "scripts" / "praxis"), "workflow", "run", str(spec_path), "--dry-run"],
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHON_BIN": str(fake_python),
            "CAPTURE_PATH": str(capture_path),
        },
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == 0
    captured = capture_path.read_text(encoding="utf-8")
    assert "surfaces.cli.main" in captured
    assert "workflow\nrun\n" in captured
    assert "workflow.sh" not in captured


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
    assert "launch                  Start Docker services, probe launcher readiness, and optionally open /app" in completed.stdout
    assert "doctor --json           Emit semantic launcher readiness as JSON" in completed.stdout
    assert "scripts/praxis-ctl remains a compatibility alias." in completed.stdout


def test_praxis_install_reports_removed_native_service_manager() -> None:
    completed = subprocess.run(
        [str(REPO_ROOT / "scripts" / "praxis"), "install"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == 1
    assert "praxis install is no longer supported." in completed.stderr
    assert "Native install/setup launchd control has been removed." in completed.stderr
    assert "Native Praxis service-manager is disabled." not in completed.stderr


def test_praxis_status_json_is_served_from_canonical_frontdoor(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -eu",
                "if [ \"$1\" != \"compose\" ]; then",
                "  echo \"unexpected command: $*\" >&2",
                "  exit 1",
                "fi",
                "shift",
                "if [ \"$1\" = \"ps\" ] && [ \"$2\" = \"--format\" ] && [ \"$3\" = \"json\" ]; then",
                "  cat <<'EOF'",
                "[",
                "  {\"Service\":\"postgres\",\"State\":\"running\",\"Publishers\":[{\"PublishedPort\":5432,\"TargetPort\":5432,\"Protocol\":\"tcp\",\"URL\":\"tcp://127.0.0.1\"}]},",
                "  {\"Service\":\"api-server\",\"State\":\"running\",\"Publishers\":[{\"PublishedPort\":8420,\"TargetPort\":8420,\"Protocol\":\"tcp\",\"URL\":\"tcp://127.0.0.1\"}]},",
                "  {\"Service\":\"workflow-worker\",\"State\":\"running\",\"Publishers\":[]},",
                "  {\"Service\":\"scheduler\",\"State\":\"running\",\"Publishers\":[]}",
                "]",
                "EOF",
                "  exit 0",
                "fi",
                "echo \"unexpected docker invocation: $*\" >&2",
                "exit 1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)

    completed = subprocess.run(
        [str(REPO_ROOT / "scripts" / "praxis"), "status", "--json"],
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
            "PYTHON_BIN": sys.executable,
            "WORKFLOW_DATABASE_URL": "postgresql://localhost:5432/praxis",
            "PRAXIS_LAUNCHER_STATE_DIR": str(tmp_path / "state"),
        },
        capture_output=True,
        check=False,
        text=True,
    )

    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    assert payload["service_manager"] == "scripts/praxis"
    assert payload["compatibility_alias"] == "scripts/praxis-ctl"
    assert payload["preferred_command"] == "praxis"
    assert [service["name"] for service in payload["services"]] == [
        "postgres",
        "api-server",
        "workflow-worker",
        "scheduler",
    ]


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
                "workflow_operational": True,
                "api_server_ready": True,
                "workflow_api_ready": True,
                "mcp_bridge_ready": True,
                "ui_ready": True,
                "launch_url": "https://praxis.example/app",
                "dashboard_url": "https://praxis.example/app",
                "api_docs_url": "https://praxis.example/docs",
                "dependency_truth": {"ok": True},
            },
            "dependency_truth": {"ok": True},
            "launch_url": "https://praxis.example/app",
            "dashboard_url": "https://praxis.example/app",
            "api_docs_url": "https://praxis.example/docs",
        }

    monkeypatch.setattr(workflow_launcher, "_run_launcher_json", _fake_run_launcher_json)
    monkeypatch.setattr(
        workflow_launcher,
        "workflow_database_status_payload",
        lambda: {"database_reachable": True, "schema_bootstrapped": False, "workflow_operational": True},
    )

    payload = workflow_launcher.launcher_status_payload()

    assert payload["ready"] is True
    assert payload["launch_url"] == "https://praxis.example/app"
    assert payload["dashboard_url"] == "https://praxis.example/app"
    assert payload["api_docs_url"] == "https://praxis.example/docs"
    assert [call[0] for call in calls] == [("status", "--json")]
    assert calls[0][1] is not None
    assert calls[0][1]["PRAXIS_ALPHA_TIMEOUT_API_HEALTH_S"] == "0.5"
    assert calls[0][2] == workflow_launcher._STATUS_TIMEOUT_S


def test_launcher_status_payload_does_not_invent_local_urls(monkeypatch) -> None:
    monkeypatch.setattr(
        workflow_launcher,
        "_run_launcher_json",
        lambda *args, **kwargs: {"doctor": {}, "services": []},
    )
    monkeypatch.setattr(
        workflow_launcher,
        "workflow_database_status_payload",
        lambda: {"database_reachable": True, "schema_bootstrapped": True},
    )

    payload = workflow_launcher.launcher_status_payload()

    assert payload["launch_url"] is None
    assert payload["dashboard_url"] is None
    assert payload["api_docs_url"] is None


def test_run_launcher_json_raises_structured_timeout(monkeypatch) -> None:
    def _fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=kwargs.get("args") or args[0], timeout=kwargs.get("timeout", 0))

    monkeypatch.setattr(subprocess, "run", _fake_run)

    with pytest.raises(workflow_launcher.LauncherAuthorityError, match="timed out"):
        workflow_launcher._run_launcher_json("status", "--json", timeout_s=1.5)
