#!/usr/bin/env python3
"""preact_orient_friction (Codex CLI PreToolUse hook).

Codex supports hook prompts, but flooding every raw tool call with context is
too expensive. This hook records friction evidence for every match and injects
context only for explicit, non-advisory operator decisions.

Tool-name normalization happens inside `surfaces.policy._normalize_tool_name`
so the trigger registry stays harness-neutral.

Fails open. If the policy module can't load or friction emission fails,
the agent's tool call proceeds.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any


def _emit(response: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(response))
    sys.stdout.flush()
    sys.exit(0)


def _continue() -> None:
    if os.environ.get("PRAXIS_HOOK_VERBOSE") == "1":
        _emit({"continue": True})
    sys.exit(0)


def _should_inject_context(matches: list[Any]) -> bool:
    return any(
        getattr(match, "provenance", "") == "explicit"
        and not bool(getattr(match, "advisory_only", True))
        for match in matches
    )


def _repo_root() -> str:
    return os.environ.get(
        "CODEX_PROJECT_DIR",
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    )


def _import_trigger_check():
    repo = _repo_root()
    workflow_root = os.path.join(repo, "Code&DBs", "Workflow")
    if workflow_root not in sys.path:
        sys.path.insert(0, workflow_root)
    try:
        from surfaces.policy import check, render_additional_context  # type: ignore  # noqa: E402

        return check, render_additional_context
    except Exception:
        return None, None


def _emit_friction_event(
    decision_keys: list[str],
    tool_name: str,
    tool_input: dict[str, Any],
) -> bool:
    repo = _repo_root()
    praxis_agent = os.path.join(repo, "bin", "praxis-agent")
    if not os.access(praxis_agent, os.X_OK):
        return False

    if tool_name in ("Bash", "local_shell", "shell", "run_shell_command"):
        # Codex's local_shell input shape can be either {command:[...]} or
        # {command:"..."}; coerce to a string for the friction subject.
        cmd = tool_input.get("command")
        if isinstance(cmd, list):
            cmd = " ".join(str(c) for c in cmd)
        subject = str(cmd or "")[:300]
    elif tool_name in ("Edit", "MultiEdit", "Write", "apply_patch", "replace", "write_file", "Read", "read_file"):
        subject = str(tool_input.get("file_path") or tool_input.get("path") or "")[:300]
    else:
        subject = tool_name

    payload = {
        "action": "record",
        "event_type": "WARN_ONLY",
        "source": "preact_orient_hook",
        "subject_kind": "agent_action",
        "subject_ref": tool_name,
        "decision_keys": list(decision_keys),
        "metadata": {
            "subject": subject,
            "matched_decisions": list(decision_keys),
            "harness": "codex_cli",
        },
    }
    try:
        result = subprocess.run(
            [praxis_agent, "praxis_friction", "--input-json", json.dumps(payload)],
            capture_output=True,
            text=True,
            timeout=4,
        )
        return result.returncode == 0
    except Exception:
        return False


def main() -> None:
    try:
        payload = json.loads(sys.stdin.read())
    except Exception:
        _continue()

    tool_name = str(payload.get("tool_name") or "").strip()
    if not tool_name:
        _continue()

    tool_input = payload.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        _continue()

    # Codex `local_shell` may pass `command` as a list of argv tokens;
    # collapse to a single string so the registry's regex sees the full
    # command line.
    if tool_name in ("local_shell", "shell"):
        cmd = tool_input.get("command")
        if isinstance(cmd, list):
            tool_input = {**tool_input, "command": " ".join(str(c) for c in cmd)}

    check, render = _import_trigger_check()
    if check is None or render is None:
        _continue()

    matches = check(tool_name, tool_input)
    if not matches:
        _continue()

    _emit_friction_event(
        [m.decision_key for m in matches],
        tool_name,
        tool_input,
    )

    if _should_inject_context(matches) or os.environ.get("PRAXIS_FORCE_HOOK_CONTEXT") == "1":
        additional_context = render(matches, tool_name)
        _emit({
            "continue": True,
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "additionalContext": additional_context,
            },
        })
    _continue()


if __name__ == "__main__":
    main()
