from __future__ import annotations

import io
import subprocess
from pathlib import Path
from types import SimpleNamespace

from surfaces.cli.commands import operate


REPO_ROOT = Path(__file__).resolve().parents[4]


def test_legacy_supervisor_shell_wrapper_is_absent() -> None:
    assert not (REPO_ROOT / "Code&DBs" / "Workflow" / "scripts" / "supervisor.sh").exists()


def test_supervisor_command_help_marks_compatibility_wrapper() -> None:
    stdout = io.StringIO()

    exit_code = operate._supervisor_command(["--help"], stdout=stdout)

    assert exit_code == 2
    assert "Legacy compatibility wrapper around ./scripts/praxis" in stdout.getvalue()


def test_supervisor_command_delegates_to_praxis(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run(command, *, capture_output, text, check):
        captured["command"] = command
        captured["capture_output"] = capture_output
        captured["text"] = text
        captured["check"] = check
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", _fake_run)
    stdout = io.StringIO()

    exit_code = operate._supervisor_command(["status"], stdout=stdout)

    assert exit_code == 0
    assert captured == {
        "command": [str(REPO_ROOT / "scripts" / "praxis"), "status"],
        "capture_output": False,
        "text": True,
        "check": False,
    }
