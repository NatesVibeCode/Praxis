"""Runtime-target setup CLI command."""

from __future__ import annotations

import json
from typing import TextIO

from runtime.setup_wizard import setup_payload_for_cli
from runtime.workspace_paths import repo_root as workspace_repo_root


def _setup_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"help", "--help", "-h"}:
        stdout.write(
            "\n".join(
                [
                    "usage: workflow setup <doctor|plan|apply|graph> [--json] [--yes]",
                    "",
                    "Runtime-target setup client. API/MCP own setup authority;",
                    "the doctor payload now includes the native_instance contract so",
                    "operators can compare runtime target and repo-local instance.",
                    "",
                    "'graph' evaluates the onboarding gate-probe graph and returns",
                    "each gate's status, observed state, and remediation hint.",
                    "",
                    "SSH is build/deploy transport only.",
                ]
            )
            + "\n"
        )
        return 0
    mode = args[0]
    if mode not in {"doctor", "plan", "apply", "graph"}:
        stdout.write("usage: workflow setup <doctor|plan|apply|graph> [--json] [--yes]\n")
        return 2
    approved = "--yes" in args
    payload = setup_payload_for_cli(mode, repo_root=workspace_repo_root(), apply=approved)
    stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return 0 if payload.get("ok", True) else 1
