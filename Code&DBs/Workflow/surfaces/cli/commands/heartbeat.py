"""Heartbeat CLI command: `praxis workflow heartbeat`.

Triggers one daily-heartbeat cycle on demand. Scope can be restricted to a
single probe kind; default is ``all``. Output is JSON by default, pretty by
``--pretty``.
"""
from __future__ import annotations

import argparse
from typing import TextIO

from surfaces.cli.mcp_tools import print_json, run_cli_tool


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workflow heartbeat",
        description=(
            "Run one heartbeat probe cycle "
            "(providers, connectors, credentials, MCP, model_retirement)."
        ),
    )
    parser.add_argument(
        "--scope",
        choices=["all", "providers", "connectors", "credentials", "mcp", "model_retirement"],
        default="all",
    )
    parser.add_argument("--pretty", action="store_true", help="Human-readable summary instead of JSON.")
    return parser


def _parse(args: list[str]) -> argparse.Namespace | int:
    parser = _parser()
    try:
        return parser.parse_args(args)
    except SystemExit as exc:
        return int(exc.code or 2)


def _pretty(result_json: dict, stdout: TextIO) -> None:
    stdout.write(
        f"heartbeat {result_json['heartbeat_run_id']}\n"
        f"  scope: {result_json['scope']} (triggered_by={result_json['triggered_by']})\n"
        f"  status: {result_json['status']}\n"
        f"  probes: {result_json['probes_ok']} ok / {result_json['probes_failed']} failed / {result_json['probes_total']} total\n"
    )
    for snap in result_json.get("snapshots", []):
        line = f"    [{snap['probe_kind']:18s}] {snap['status']:8s} {snap['subject_id']}"
        extras: list[str] = []
        if snap.get("latency_ms") is not None:
            extras.append(f"{snap['latency_ms']}ms")
        if snap.get("input_tokens") or snap.get("output_tokens"):
            extras.append(f"tokens={snap.get('input_tokens') or 0}+{snap.get('output_tokens') or 0}")
        if snap.get("days_until_expiry") is not None:
            extras.append(f"{snap['days_until_expiry']}d")
        if extras:
            line += "  (" + ", ".join(extras) + ")"
        stdout.write(line + "\n")


def _heartbeat_command(args: list[str], *, stdout: TextIO) -> int:
    if args and args[0] in {"-h", "--help"}:
        _parser().print_help(stdout)
        return 0
    parsed = _parse(args)
    if isinstance(parsed, int):
        return parsed

    exit_code, payload = run_cli_tool(
        "praxis_daily_heartbeat",
        {"scope": parsed.scope, "triggered_by": "cli"},
    )

    if parsed.pretty and isinstance(payload, dict) and payload.get("heartbeat_run_id"):
        _pretty(payload, stdout)
    else:
        print_json(stdout, payload)

    if exit_code != 0:
        return exit_code

    status = str(payload.get("status") or "")
    if status in {"succeeded", "partial"}:
        return 0
    return 1
