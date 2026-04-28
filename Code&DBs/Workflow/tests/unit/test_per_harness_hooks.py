"""Unit tests for the per-harness PreToolUse / BeforeTool hook entry scripts.

Three hooks live in different dirs (one per harness) but share a contract:
  - read JSON `{tool_name, tool_input}` from stdin
  - call `surfaces.policy.check(tool_name, tool_input)` (with harness-name
    aliasing baked into the matcher)
  - emit JSON response with `hookSpecificOutput.additionalContext` when an
    explicit non-advisory standing order matched
  - otherwise stay silent and fail open so routine tool calls do not create
    transcript noise
  - fail open on any error

We drive the entry shell scripts as subprocesses with a fixture trigger
registry, asserting on the JSON output. This catches:
  - bash quoting / heredoc bugs
  - PYTHONPATH wiring
  - the response shape Claude/Gemini/Codex actually consume

Each hook gets the same handful of tests:
  - explicit matched standing order → response carries additionalContext + correct hookEventName
  - no match → silent success
  - bogus stdin → silent fail-open success
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
TRIGGER_FIXTURE = {
    "$schema_version": 1,
    "triggers": [
        {
            "decision_key": "test::hook-bash-fixture",
            "title": "hook test fixture: docker restart",
            "decision_provenance": "explicit",
            "match": [{"tool": "Bash", "regex": r"^\s*docker\s+restart"}],
        }
    ],
}


@pytest.fixture
def fixture_registry(tmp_path: Path) -> Path:
    path = tmp_path / "operator-decision-triggers.json"
    path.write_text(json.dumps(TRIGGER_FIXTURE), encoding="utf-8")
    return path


def _run_hook(
    script: Path,
    project_dir_var: str,
    payload: dict | str,
    fixture_registry: Path,
) -> tuple[int, str, str]:
    """Run a hook script with stdin = payload, returning (rc, stdout, stderr).

    `project_dir_var` is the env var the harness uses for project root
    (`CLAUDE_PROJECT_DIR` / `GEMINI_PROJECT_DIR` / `CODEX_PROJECT_DIR`).
    """
    if isinstance(payload, dict):
        stdin_text = json.dumps(payload)
    else:
        stdin_text = payload
    env = {
        **os.environ,
        project_dir_var: str(REPO_ROOT),
        "PRAXIS_TRIGGER_REGISTRY": str(fixture_registry),
    }
    proc = subprocess.run(
        [str(script)],
        input=stdin_text,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return proc.returncode, proc.stdout, proc.stderr


# =============================================================================
# Claude Code hook (.claude/hooks/preact-orient-friction.sh)
# =============================================================================

CLAUDE_HOOK = REPO_ROOT / ".claude" / "hooks" / "preact-orient-friction.sh"


def test_claude_hook_match_emits_additional_context(fixture_registry: Path) -> None:
    if not CLAUDE_HOOK.exists():
        pytest.skip(f"hook missing: {CLAUDE_HOOK}")
    rc, stdout, _ = _run_hook(
        CLAUDE_HOOK,
        "CLAUDE_PROJECT_DIR",
        {"tool_name": "Bash", "tool_input": {"command": "docker restart praxis-x"}},
        fixture_registry,
    )
    assert rc == 0
    body = json.loads(stdout)
    assert body.get("continue") is True
    hso = body.get("hookSpecificOutput", {})
    assert hso.get("hookEventName") == "PreToolUse"
    assert "STANDING ORDER MATCH" in hso.get("additionalContext", "")
    assert "test::hook-bash-fixture" in hso["additionalContext"]


def test_claude_hook_no_match_stays_silent(fixture_registry: Path) -> None:
    if not CLAUDE_HOOK.exists():
        pytest.skip(f"hook missing: {CLAUDE_HOOK}")
    rc, stdout, _ = _run_hook(
        CLAUDE_HOOK,
        "CLAUDE_PROJECT_DIR",
        {"tool_name": "Bash", "tool_input": {"command": "ls -la"}},
        fixture_registry,
    )
    assert rc == 0
    assert stdout == ""


def test_claude_hook_bogus_stdin_fails_open(fixture_registry: Path) -> None:
    if not CLAUDE_HOOK.exists():
        pytest.skip(f"hook missing: {CLAUDE_HOOK}")
    rc, stdout, _ = _run_hook(
        CLAUDE_HOOK,
        "CLAUDE_PROJECT_DIR",
        "{not valid json",
        fixture_registry,
    )
    assert rc == 0
    assert stdout == ""


# =============================================================================
# Gemini CLI hook (.gemini/hooks/preact-orient-friction.sh)
# =============================================================================

GEMINI_HOOK = REPO_ROOT / ".gemini" / "hooks" / "preact-orient-friction.sh"


def test_gemini_hook_match_via_run_shell_command_alias(fixture_registry: Path) -> None:
    """Gemini's native `run_shell_command` should fire the Bash trigger."""
    if not GEMINI_HOOK.exists():
        pytest.skip(f"hook missing: {GEMINI_HOOK}")
    rc, stdout, _ = _run_hook(
        GEMINI_HOOK,
        "GEMINI_PROJECT_DIR",
        {
            "tool_name": "run_shell_command",
            "tool_input": {"command": "docker restart praxis-x"},
        },
        fixture_registry,
    )
    assert rc == 0
    body = json.loads(stdout)
    hso = body.get("hookSpecificOutput", {})
    # Gemini uses BeforeTool as its event name, not PreToolUse.
    assert hso.get("hookEventName") == "BeforeTool"
    assert "STANDING ORDER MATCH" in hso.get("additionalContext", "")
    assert "test::hook-bash-fixture" in hso["additionalContext"]


def test_gemini_hook_no_match_stays_silent(fixture_registry: Path) -> None:
    if not GEMINI_HOOK.exists():
        pytest.skip(f"hook missing: {GEMINI_HOOK}")
    rc, stdout, _ = _run_hook(
        GEMINI_HOOK,
        "GEMINI_PROJECT_DIR",
        {"tool_name": "run_shell_command", "tool_input": {"command": "ls"}},
        fixture_registry,
    )
    assert rc == 0
    assert stdout == ""


def test_gemini_hook_bogus_stdin_fails_open(fixture_registry: Path) -> None:
    if not GEMINI_HOOK.exists():
        pytest.skip(f"hook missing: {GEMINI_HOOK}")
    rc, stdout, _ = _run_hook(
        GEMINI_HOOK,
        "GEMINI_PROJECT_DIR",
        "garbage payload",
        fixture_registry,
    )
    assert rc == 0
    assert stdout == ""


# =============================================================================
# Codex CLI hook (.codex/hooks/preact-orient-friction.sh)
# =============================================================================

CODEX_HOOK = REPO_ROOT / ".codex" / "hooks" / "preact-orient-friction.sh"


def test_codex_hook_match_via_local_shell_alias(fixture_registry: Path) -> None:
    """Codex's native `local_shell` should fire the Bash trigger."""
    if not CODEX_HOOK.exists():
        pytest.skip(f"hook missing: {CODEX_HOOK}")
    rc, stdout, _ = _run_hook(
        CODEX_HOOK,
        "CODEX_PROJECT_DIR",
        {
            "tool_name": "local_shell",
            "tool_input": {"command": ["docker", "restart", "praxis-x"]},
        },
        fixture_registry,
    )
    assert rc == 0
    body = json.loads(stdout)
    hso = body.get("hookSpecificOutput", {})
    # Codex uses PreToolUse as its event name.
    assert hso.get("hookEventName") == "PreToolUse"
    assert "STANDING ORDER MATCH" in hso.get("additionalContext", "")


def test_codex_hook_local_shell_argv_list_collapsed(fixture_registry: Path) -> None:
    """Codex's local_shell often passes command as argv list; the hook must
    join it into a single string before feeding the regex matcher."""
    if not CODEX_HOOK.exists():
        pytest.skip(f"hook missing: {CODEX_HOOK}")
    rc, stdout, _ = _run_hook(
        CODEX_HOOK,
        "CODEX_PROJECT_DIR",
        {
            "tool_name": "local_shell",
            # argv shape — without collapse, regex sees "['docker', ...]"
            # not "docker restart ..."
            "tool_input": {"command": ["docker", "restart", "praxis-x"]},
        },
        fixture_registry,
    )
    body = json.loads(stdout)
    # If argv collapse is broken, the regex won't match and the hook will
    # stay silent with no additionalContext.
    hso = body.get("hookSpecificOutput")
    assert hso is not None, "argv-list command was not collapsed into a string"
    assert "STANDING ORDER MATCH" in hso.get("additionalContext", "")


def test_codex_hook_no_match_stays_silent(fixture_registry: Path) -> None:
    if not CODEX_HOOK.exists():
        pytest.skip(f"hook missing: {CODEX_HOOK}")
    rc, stdout, _ = _run_hook(
        CODEX_HOOK,
        "CODEX_PROJECT_DIR",
        {"tool_name": "local_shell", "tool_input": {"command": ["ls"]}},
        fixture_registry,
    )
    assert rc == 0
    assert stdout == ""


def test_codex_hook_bogus_stdin_fails_open(fixture_registry: Path) -> None:
    if not CODEX_HOOK.exists():
        pytest.skip(f"hook missing: {CODEX_HOOK}")
    rc, stdout, _ = _run_hook(
        CODEX_HOOK,
        "CODEX_PROJECT_DIR",
        "not even json",
        fixture_registry,
    )
    assert rc == 0
    assert stdout == ""


# =============================================================================
# Universal CLI shim (bin/praxis-policy-check)
# =============================================================================

UNIVERSAL_SHIM = REPO_ROOT / "bin" / "praxis-policy-check"


def test_universal_shim_match_emits_surface(fixture_registry: Path) -> None:
    """The shim is the substrate Cursor + plain-shell consumers call."""
    if not UNIVERSAL_SHIM.exists():
        pytest.skip(f"shim missing: {UNIVERSAL_SHIM}")
    env = {
        **os.environ,
        "PRAXIS_TRIGGER_REGISTRY": str(fixture_registry),
    }
    proc = subprocess.run(
        [str(UNIVERSAL_SHIM), "Bash", '{"command":"docker restart praxis-x"}'],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode == 0
    assert "STANDING ORDER MATCH" in proc.stdout
    assert "test::hook-bash-fixture" in proc.stdout


def test_universal_shim_no_match_silent(fixture_registry: Path) -> None:
    if not UNIVERSAL_SHIM.exists():
        pytest.skip(f"shim missing: {UNIVERSAL_SHIM}")
    env = {
        **os.environ,
        "PRAXIS_TRIGGER_REGISTRY": str(fixture_registry),
    }
    proc = subprocess.run(
        [str(UNIVERSAL_SHIM), "Bash", '{"command":"ls"}'],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode == 0
    assert proc.stdout.strip() == ""


def test_universal_shim_gemini_alias(fixture_registry: Path) -> None:
    """Shim accepts Gemini-native tool name and applies the same matcher."""
    if not UNIVERSAL_SHIM.exists():
        pytest.skip(f"shim missing: {UNIVERSAL_SHIM}")
    env = {
        **os.environ,
        "PRAXIS_TRIGGER_REGISTRY": str(fixture_registry),
    }
    proc = subprocess.run(
        [str(UNIVERSAL_SHIM), "run_shell_command", '{"command":"docker restart praxis-x"}'],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode == 0
    assert "STANDING ORDER MATCH" in proc.stdout
