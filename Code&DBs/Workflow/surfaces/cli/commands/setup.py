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


def _extract_flag_values(args: list[str], flag: str) -> list[str]:
    values: list[str] = []
    prefix = f"{flag}="
    for index, arg in enumerate(args):
        if arg == flag and index + 1 < len(args):
            values.append(args[index + 1])
        elif arg.startswith(prefix):
            values.append(arg[len(prefix):])
    return [value for value in values if str(value).strip()]


def _setup_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"help", "--help", "-h"}:
        stdout.write(
            "\n".join(
                [
                    "usage: workflow setup <doctor|plan|apply|graph> [--json] [--yes]",
                    "                              [--gate <gate_ref>] [--apply-ref <apply_ref>]",
                    "                              [--repo-rule <text>] [--sop <text>]",
                    "                              [--anti-pattern <text>] [--forbidden-action <text>]",
                    "                              [--sensitive-system <text-or-json>]",
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
    repo_rules = _extract_flag_values(args, "--repo-rule")
    sops = _extract_flag_values(args, "--sop")
    anti_patterns = _extract_flag_values(args, "--anti-pattern")
    forbidden_actions = _extract_flag_values(args, "--forbidden-action")
    sensitive_system_inputs = _extract_flag_values(args, "--sensitive-system")
    sensitive_systems = []
    for raw in sensitive_system_inputs:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = raw
        sensitive_systems.append(parsed)
    submitted_by = _extract_flag_value(args, "--submitted-by")
    change_reason = _extract_flag_value(args, "--change-reason")
    disclosure_repeat_limit = _extract_flag_value(args, "--disclosure-repeat-limit")

    if mode == "apply":
        payload = setup_apply_gate_payload(
            gate_ref=gate_ref,
            apply_ref=apply_ref,
            repo_root=workspace_repo_root(),
            approved=approved,
            applied_by="cli_setup_apply",
            authority_surface="cli",
            apply_kwargs={
                "repo_rules": repo_rules or None,
                "sops": sops or None,
                "anti_patterns": anti_patterns or None,
                "forbidden_actions": forbidden_actions or None,
                "sensitive_systems": sensitive_systems or None,
                "submitted_by": submitted_by,
                "change_reason": change_reason,
                "disclosure_repeat_limit": disclosure_repeat_limit,
            },
        )
    else:
        payload = setup_payload_for_cli(mode, repo_root=workspace_repo_root(), apply=approved)
    stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return 0 if payload.get("ok", True) else 1
