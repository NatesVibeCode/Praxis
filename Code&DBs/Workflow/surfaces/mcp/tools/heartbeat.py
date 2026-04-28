"""Tools: praxis_daily_heartbeat.

On-command trigger for one daily-heartbeat probe cycle. Mirrors what the
launchd daemon runs at 09:30 and what the CLI front door (`praxis workflow
heartbeat`) offers, so agents can refresh observability without shelling out.

The sibling ``praxis_heartbeat`` tool (in session.py) runs the knowledge-graph
maintenance cycle — a completely different subsystem. They share the noun
but not the authority.
"""
from __future__ import annotations

from typing import Any

from .operator import _execute_catalog_tool


_VALID_SCOPES = (
    "all",
    "providers",
    "connectors",
    "credentials",
    "mcp",
    "model_retirement",
)
_VALID_TRIGGERED_BY = ("launchd", "cli", "mcp", "http", "test")


def tool_praxis_daily_heartbeat(params: dict, _progress_emitter=None) -> dict:
    """Run one heartbeat probe cycle and return the persisted result."""
    scope = str(params.get("scope") or "all").strip() or "all"
    if scope not in _VALID_SCOPES:
        return {
            "ok": False,
            "error": (
                "scope must be one of "
                "all|providers|connectors|credentials|mcp|model_retirement "
                f"(got {scope!r})"
            ),
            "error_code": "operator.daily_heartbeat_refresh.invalid_input",
            "operation_name": "operator.daily_heartbeat_refresh",
        }

    triggered_by = str(params.get("triggered_by") or "mcp").strip().lower() or "mcp"
    if triggered_by not in _VALID_TRIGGERED_BY:
        return {
            "ok": False,
            "error": (
                "triggered_by must be one of launchd|cli|mcp|http|test "
                f"(got {triggered_by!r})"
            ),
            "error_code": "operator.daily_heartbeat_refresh.invalid_input",
            "operation_name": "operator.daily_heartbeat_refresh",
        }

    if _progress_emitter:
        _progress_emitter.log(
            f"Running heartbeat scope={scope} triggered_by={triggered_by}"
        )

    return _execute_catalog_tool(
        operation_name="operator.daily_heartbeat_refresh",
        payload={
            "scope": scope,
            "triggered_by": triggered_by,
        },
    )


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_daily_heartbeat": (
        tool_praxis_daily_heartbeat,
        {
            "description": (
                "Run one daily-heartbeat probe cycle on demand and persist the results to "
                "heartbeat_runs + heartbeat_probe_snapshots through CQRS authority. Probes "
                "cover provider CLI usage (claude/codex/gemini latency + token counts), "
                "connector liveness (catalog health), credential expiry (keychain/env API "
                "keys + OAuth tokens), and MCP server liveness (stdio initialize handshake).\n\n"
                "USE WHEN: you want a fresh snapshot of external-integration health without "
                "waiting for the 09:30 launchd run — e.g. after rotating a credential, adding "
                "a provider, or investigating a suspected outage.\n\n"
                "EXAMPLES:\n"
                "  All scopes:           praxis_daily_heartbeat()\n"
                "  Credentials:          praxis_daily_heartbeat(scope='credentials')\n"
                "  Provider probes:      praxis_daily_heartbeat(scope='providers')\n"
                "  Retirement detector:  praxis_daily_heartbeat(scope='model_retirement')\n\n"
                "DO NOT USE: for fast platform-readiness checks — that's praxis_health. For the "
                "knowledge-graph maintenance cycle, use praxis_heartbeat (different subsystem). "
                "This tool spawns real subprocess CLI calls and can take tens of seconds."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "string",
                        "description": "Which probe kind to run.",
                        "enum": [
                            "all",
                            "providers",
                            "connectors",
                            "credentials",
                            "mcp",
                            "model_retirement",
                        ],
                        "default": "all",
                    },
                    "triggered_by": {
                        "type": "string",
                        "description": (
                            "Recorded caller lane for the persisted heartbeat row. "
                            "Defaults to mcp for direct tool calls."
                        ),
                        "enum": ["launchd", "cli", "mcp", "http", "test"],
                        "default": "mcp",
                    },
                },
            },
        },
    ),
}
