"""Runtime-target setup CLI command."""

from __future__ import annotations

import json
from typing import TextIO

from runtime.setup_wizard import setup_apply_gate_payload, setup_payload_for_cli
from runtime.workspace_paths import repo_root as workspace_repo_root


def _extract_flag_value(args: list[str], flag: str) -> str | None:
    prefix = f"{flag}="
    for arg in args:
        if arg == flag:
            index = args.index(arg)
            if index + 1 < len(args):
                return args[index + 1]
        if arg.startswith(prefix):
            return arg[len(prefix):]
    return None


def _setup_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"help", "--help", "-h"}:
        stdout.write(
            "\n".join(
                [
                    "usage: workflow setup <doctor|plan|apply|graph> [--json] [--yes]",
                    "                              [--gate <gate_ref>] [--apply-ref <apply_ref>]",
                    "",
                    "Runtime-target setup client. API/MCP own setup authority.",
                    "",
                    "  doctor / plan : runtime-target contract (package, sandbox, DB, API).",
                    "  graph         : onboarding gate-probe graph (per-gate status, hints).",
                    "  apply         : apply one gate's registered handler. Pass --gate or",
                    "                  --apply-ref to pick the target. Handlers that mutate",
                    "                  disk or secrets also require --yes.",
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
    gate_ref = _extract_flag_value(args, "--gate")
    apply_ref = _extract_flag_value(args, "--apply-ref")

    if mode == "apply":
        from runtime.setup_wizard import setup_apply_payload

        payload = setup_apply_payload(
            approved=approved,
            gate_ref=gate_ref,
            apply_ref=apply_ref,
            repo_root=workspace_repo_root(),
            authority_surface="cli",
        )
    elif mode == "apply" and (gate_ref or apply_ref):
        payload = setup_apply_gate_payload(
            gate_ref=gate_ref,
            apply_ref=apply_ref,
            repo_root=workspace_repo_root(),
            approved=approved,
            applied_by="cli_setup_apply",
            authority_surface="local_bootstrap_diagnostic",
        )
    else:
        payload = setup_payload_for_cli(mode, repo_root=workspace_repo_root(), apply=approved)
    stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return 0 if payload.get("ok", True) else 1
