"""CQRS command wrapper for provider availability refresh.

The provider probe itself is still the daily-heartbeat provider scope: it is
the existing writer for ``heartbeat_runs`` and ``heartbeat_probe_snapshots``.
This command gives that write one operation-catalog entry, one receipt, one
conceptual event, and an optional control-plane projection refresh.
"""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
from collections import Counter
from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.daily_heartbeat import HeartbeatRunResult, run_daily_heartbeat


_CONTROL_PLANE_PROJECTION_REF = "projection.private_provider_control_plane_snapshot"
_PROVIDER_USAGE_SOURCE_REF = "table.heartbeat_probe_snapshots"


class ProviderAvailabilityRefreshCommand(BaseModel):
    """Input contract for ``operator.provider_availability_refresh``."""

    provider_slugs: tuple[str, ...] = ()
    adapter_types: tuple[str, ...] = ()
    timeout_s: int = Field(default=60, ge=1, le=600)
    max_concurrency: int = Field(default=4, ge=1, le=16)
    refresh_control_plane: bool = True
    runtime_profile_ref: str | None = None
    include_snapshots: bool = True

    @field_validator("provider_slugs", "adapter_types", mode="before")
    @classmethod
    def _coerce_text_tuple(cls, value: Any) -> tuple[str, ...]:
        if value is None or value == "":
            return ()
        if isinstance(value, str):
            raw_values = value.split(",")
        else:
            raw_values = list(value)
        return tuple(
            text
            for item in raw_values
            if (text := str(item or "").strip().lower())
        )


def _run_provider_heartbeat(command: ProviderAvailabilityRefreshCommand) -> HeartbeatRunResult:
    return run_sync_safe(
        run_daily_heartbeat(
            scope="providers",
            triggered_by="mcp",
            timeouts_s={"providers": command.timeout_s},
            provider_slugs=command.provider_slugs or None,
            adapter_types=command.adapter_types or None,
            provider_concurrency=command.max_concurrency,
        )
    )


def _snapshot_row(snapshot: dict[str, Any], *, heartbeat_run_id: str) -> dict[str, Any]:
    details = snapshot.get("details")
    detail_map = details if isinstance(details, dict) else {}
    return {
        "provider_slug": snapshot.get("subject_id"),
        "adapter_type": snapshot.get("subject_sub"),
        "status": snapshot.get("status"),
        "summary": snapshot.get("summary"),
        "latency_ms": snapshot.get("latency_ms"),
        "input_tokens": snapshot.get("input_tokens"),
        "output_tokens": snapshot.get("output_tokens"),
        "estimated_cost_usd": snapshot.get("estimated_cost_usd"),
        "model_slug": detail_map.get("model_slug"),
        "transport_kind": detail_map.get("transport_kind"),
        "rate_limited": bool(detail_map.get("rate_limited")),
        "returncode": detail_map.get("returncode"),
        "heartbeat_run_id": heartbeat_run_id,
        "source_ref": _PROVIDER_USAGE_SOURCE_REF,
    }


def _availability_status(counts: Counter[str]) -> str:
    if counts.get("failed", 0) and not counts.get("ok", 0):
        return "failed"
    if counts.get("failed", 0) or counts.get("degraded", 0) or counts.get("warning", 0):
        return "degraded"
    if counts.get("ok", 0):
        return "healthy"
    return "unknown"


def _refresh_control_plane(
    conn: Any,
    *,
    runtime_profile_ref: str | None,
) -> dict[str, Any]:
    try:
        if runtime_profile_ref:
            conn.execute(
                "SELECT refresh_private_provider_control_plane_snapshot($1)",
                runtime_profile_ref,
            )
        else:
            conn.execute("SELECT refresh_private_provider_control_plane_snapshot(NULL)")
    except Exception as exc:  # noqa: BLE001 - command result should explain projection failure
        return {
            "ok": False,
            "projection_ref": _CONTROL_PLANE_PROJECTION_REF,
            "error_code": "provider_availability.control_plane_refresh_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {
        "ok": True,
        "projection_ref": _CONTROL_PLANE_PROJECTION_REF,
        "runtime_profile_ref": runtime_profile_ref,
    }


def handle_provider_availability_refresh(
    command: ProviderAvailabilityRefreshCommand,
    subsystems: Any,
) -> dict[str, Any]:
    heartbeat = _run_provider_heartbeat(command)
    heartbeat_payload = heartbeat.to_json()
    snapshots = [
        _snapshot_row(snapshot, heartbeat_run_id=heartbeat.heartbeat_run_id)
        for snapshot in heartbeat_payload.get("snapshots", [])
        if isinstance(snapshot, dict)
    ]
    counts = Counter(str(row.get("status") or "unknown") for row in snapshots)
    provider_health = _availability_status(counts)

    control_plane_refresh = {"ok": False, "skipped": True}
    if command.refresh_control_plane:
        control_plane_refresh = _refresh_control_plane(
            subsystems.get_pg_conn(),
            runtime_profile_ref=command.runtime_profile_ref,
        )

    status = heartbeat.status
    if command.refresh_control_plane and not control_plane_refresh.get("ok"):
        status = "partial" if heartbeat.status == "succeeded" else heartbeat.status

    event_payload = {
        "heartbeat_run_id": heartbeat.heartbeat_run_id,
        "provider_health": provider_health,
        "heartbeat_status": heartbeat.status,
        "status_counts": dict(counts),
        "probes_total": heartbeat.probes_total,
        "probes_ok": heartbeat.probes_ok,
        "probes_failed": heartbeat.probes_failed,
        "provider_slugs": list(command.provider_slugs),
        "adapter_types": list(command.adapter_types),
        "max_concurrency": command.max_concurrency,
        "timeout_s": command.timeout_s,
        "control_plane_refresh": control_plane_refresh,
        "source_refs": [
            "table.heartbeat_runs",
            _PROVIDER_USAGE_SOURCE_REF,
            _CONTROL_PLANE_PROJECTION_REF,
        ],
    }

    payload: dict[str, Any] = {
        "ok": True,
        "status": status,
        "provider_health": provider_health,
        "heartbeat_run_id": heartbeat.heartbeat_run_id,
        "heartbeat_status": heartbeat.status,
        "summary": heartbeat.summary,
        "status_counts": dict(counts),
        "probes_total": heartbeat.probes_total,
        "probes_ok": heartbeat.probes_ok,
        "probes_failed": heartbeat.probes_failed,
        "control_plane_refresh": control_plane_refresh,
        "authority": {
            "operation_name": "operator.provider_availability_refresh",
            "event_type": "provider.availability.refreshed",
            "writes": ["heartbeat_runs", "heartbeat_probe_snapshots"],
            "projection_ref": _CONTROL_PLANE_PROJECTION_REF,
        },
        "event_payload": event_payload,
    }
    if command.include_snapshots:
        payload["snapshots"] = snapshots
    return payload


__all__ = [
    "ProviderAvailabilityRefreshCommand",
    "handle_provider_availability_refresh",
]
