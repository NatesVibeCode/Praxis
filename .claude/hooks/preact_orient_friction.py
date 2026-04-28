#!/usr/bin/env python3
"""preact_orient_friction — Claude Code PreToolUse hook.

Hook payload (stdin) → trigger registry match → friction event + injected
additionalContext. Per-harness layer above the universal gateway-side check
in `surfaces.mcp.invocation.invoke_tool`. The gateway covers any agent that
calls Praxis MCP/CLI/HTTP. This hook covers Claude Code's *raw* tool calls
(Bash, Edit, Write, MultiEdit, Read) that don't go through the Praxis
gateway — agents shelling out to docker, editing source files, etc.

Codex and Gemini have their own equivalents; see
`policy/HARNESS_INTEGRATION.md` for the per-harness recipe pointing at the
same `surfaces.policy.trigger_check` module and the same
`policy/operator-decision-triggers.json` registry.

Design contract (per /praxis-debate fork, round 3):
- Surfaces, doesn't enforce.
- Reuses existing surfaces (operator_decisions, friction_ledger, MCP
  invocation pipeline).
- No new ledger.
- Fails open. Registry missing or import failed → tool call proceeds.
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


def _repo_root() -> str:
    return os.environ.get(
        "CLAUDE_PROJECT_DIR",
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    )


def _import_trigger_check():
    """Import the universal trigger-check module from
    `Code&DBs/Workflow/surfaces/policy/`. The module is the single matcher
    used by both the gateway-side check (in invoke_tool) and per-harness
    hooks like this one. Returns None on any import failure (degrade
    gracefully)."""
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
    """Best-effort FrictionEvent emission via bin/praxis-agent. The agent
    surface routes into the api-server container's gateway, which records
    the FrictionEvent through the standard authority chain. If the
    container is down or praxis-agent missing, we degrade to additionalContext-
    only — the surface still fires, just without the audit-trail row."""
    repo = _repo_root()
    praxis_agent = os.path.join(repo, "bin", "praxis-agent")
    if not os.access(praxis_agent, os.X_OK):
        return False

    # Compact the tool_input so the friction event metadata stays bounded.
    if tool_name == "Bash":
        subject = str(tool_input.get("command") or "")[:300]
    elif tool_name in ("Edit", "MultiEdit", "Write", "Read"):
        subject = str(tool_input.get("file_path") or "")[:300]
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
            "harness": "claude_code",
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

    check, render = _import_trigger_check()
    if check is None or render is None:
        # Trigger module unavailable — fail open. The agent loses surfacing
        # for this turn, but the tool call proceeds. The gateway-side check
        # in invoke_tool still fires for any Praxis MCP call the agent makes.
        _continue()

    matches = check(tool_name, tool_input)
    if not matches:
        _continue()

    # Best-effort friction emission. Independent of the surfacing return.
    _emit_friction_event(
        [m.decision_key for m in matches],
        tool_name,
        tool_input,
    )

    additional_context = render(matches, tool_name)
    _emit({
        "continue": True,
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": additional_context,
        },
    })


if __name__ == "__main__":
    main()
