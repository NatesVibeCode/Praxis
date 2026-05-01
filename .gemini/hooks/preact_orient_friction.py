#!/usr/bin/env python3
"""preact_orient_friction (Gemini CLI BeforeTool hook).

Gemini CLI accepts the same input idea as Claude (`{tool_name, tool_input}`).
Flooding every raw tool call with context is too expensive, so this hook records
friction evidence for every match and injects context only for explicit,
non-advisory operator decisions.

Tool-name normalization happens inside `surfaces.policy._normalize_tool_name`
so the trigger registry doesn't need parallel entries per harness.

Fails open. If the policy module can't load or the friction emission
fails, the agent's tool call proceeds.
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
        "GEMINI_PROJECT_DIR",
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
    """Best-effort FrictionEvent emission via bin/praxis-agent. Same audit-
    trail surface as Claude — we just tag `harness=gemini_cli` so per-harness
    compliance can be analyzed without forking the ledger."""
    repo = _repo_root()
    praxis_agent = os.path.join(repo, "bin", "praxis-agent")
    if not os.access(praxis_agent, os.X_OK):
        return False

    if tool_name in ("Bash", "run_shell_command", "ShellTool"):
        subject = str(tool_input.get("command") or "")[:300]
    elif tool_name in ("Edit", "replace", "MultiEdit", "Write", "write_file", "Read", "read_file"):
        subject = str(tool_input.get("file_path") or "")[:300]
    else:
        subject = tool_name

    metadata: dict[str, Any] = {
        "subject": subject,
        "matched_decisions": list(decision_keys),
        "harness": "gemini_cli",
    }
    task_mode = (os.environ.get("PRAXIS_TASK_MODE") or "").strip().lower()
    if task_mode:
        metadata["task_mode"] = task_mode
    payload = {
        "action": "record",
        "event_type": "WARN_ONLY",
        "source": "preact_orient_hook",
        "subject_kind": "agent_action",
        "subject_ref": tool_name,
        "decision_keys": list(decision_keys),
        "task_mode": task_mode or None,
        "metadata": metadata,
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


def _session_ref() -> str | None:
    for key in ("GEMINI_SESSION_ID", "AGENT_SESSION_ID", "SESSION_ID"):
        value = str(os.environ.get(key) or "").strip()
        if value:
            return value[:200]
    return None


def _record_action_fingerprint(
    tool_name: str,
    tool_input: dict[str, Any],
) -> bool:
    repo = _repo_root()
    praxis_agent = os.path.join(repo, "bin", "praxis-agent")
    if not os.access(praxis_agent, os.X_OK):
        return False

    payload = {
        "action": "record",
        "tool_name": tool_name,
        "tool_input": tool_input,
        "source_surface": "gemini:host",
        "session_ref": _session_ref(),
        "payload_meta": {
            "harness": "gemini_cli",
            "task_mode": (os.environ.get("PRAXIS_TASK_MODE") or "").strip().lower() or None,
        },
    }
    try:
        result = subprocess.run(
            [praxis_agent, "praxis_action_fingerprints", "--input-json", json.dumps(payload)],
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

    _record_action_fingerprint(tool_name, tool_input)

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
                "hookEventName": "BeforeTool",
                "additionalContext": additional_context,
            },
        })
    _continue()


if __name__ == "__main__":
    main()
