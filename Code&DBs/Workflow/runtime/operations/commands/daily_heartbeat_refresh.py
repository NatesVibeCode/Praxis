"""CQRS command wrapper for the daily external-health heartbeat.

This wraps ``runtime.daily_heartbeat.run_daily_heartbeat`` in one registered
operation so scripts, CLI aliases, and MCP tools share a single receipt-backed
authority path instead of importing the runtime writer directly.
"""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
from typing import Literal

from pydantic import BaseModel

from runtime.daily_heartbeat import run_daily_heartbeat


HeartbeatScope = Literal[
    "all",
    "providers",
    "connectors",
    "credentials",
    "mcp",
    "model_retirement",
]

HeartbeatTrigger = Literal["launchd", "cli", "mcp", "http", "test"]


class DailyHeartbeatRefreshCommand(BaseModel):
    """Input contract for ``operator.daily_heartbeat_refresh``."""

    scope: HeartbeatScope = "all"
    triggered_by: HeartbeatTrigger = "mcp"


def _run_heartbeat(command: DailyHeartbeatRefreshCommand):
    return run_sync_safe(
        run_daily_heartbeat(
            scope=command.scope,
            triggered_by=command.triggered_by,
        )
    )


def handle_daily_heartbeat_refresh(
    command: DailyHeartbeatRefreshCommand,
    _subsystems,
) -> dict[str, object]:
    heartbeat = _run_heartbeat(command)
    payload = heartbeat.to_json()
    payload["ok"] = True
    payload["authority"] = {
        "operation_name": "operator.daily_heartbeat_refresh",
        "event_type": "daily.heartbeat.refreshed",
        "writes": ["heartbeat_runs", "heartbeat_probe_snapshots"],
    }
    payload["event_payload"] = {
        "heartbeat_run_id": heartbeat.heartbeat_run_id,
        "scope": heartbeat.scope,
        "triggered_by": heartbeat.triggered_by,
        "status": heartbeat.status,
        "probes_total": heartbeat.probes_total,
        "probes_ok": heartbeat.probes_ok,
        "probes_failed": heartbeat.probes_failed,
        "summary": heartbeat.summary,
        "source_refs": [
            "table.heartbeat_runs",
            "table.heartbeat_probe_snapshots",
        ],
    }
    return payload


__all__ = [
    "DailyHeartbeatRefreshCommand",
    "handle_daily_heartbeat_refresh",
]
