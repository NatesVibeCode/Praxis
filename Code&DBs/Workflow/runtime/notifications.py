"""Workflow completion notifications and webhooks.

Supports fire-and-forget notifications for workflow completion events via multiple
channels: file appends (JSONL), stderr logging, and HTTP webhooks.

Integrates with the workflow result recording pipeline to notify operators about
workflow outcomes without blocking the main execution flow.
"""

from __future__ import annotations

import json
import os
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .workflow import WorkflowResult

__all__ = [
    "NotificationChannel",
    "NotificationConfig",
    "notify_workflow_complete",
    "notify_batch_complete",
    "load_config",
]


# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CONFIG_PATH = _PROJECT_ROOT / "config" / "notifications.json"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NotificationChannel:
    """Single notification destination.

    Attributes:
        kind: "webhook", "file", or "stderr"
        target: URL for webhook, file path for file, or "stderr" for stderr
    """

    kind: str
    target: str

    def __post_init__(self) -> None:
        if self.kind not in ("webhook", "file", "stderr"):
            raise ValueError(f"Invalid notification kind: {self.kind!r}")


@dataclass(frozen=True, slots=True)
class NotificationConfig:
    """Notification configuration.

    Attributes:
        channels: List of NotificationChannel destinations
        notify_on: List of statuses to notify for ("succeeded", "failed", etc.)
        quiet_success: If True, suppress succeeded notifications
    """

    channels: list[NotificationChannel]
    notify_on: list[str]
    quiet_success: bool = False

    def should_notify(self, status: str) -> bool:
        """Return True if we should notify for this status."""
        if self.quiet_success and status == "succeeded":
            return False
        return status in self.notify_on


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def load_config(path: str | Path | None = None) -> NotificationConfig | None:
    """Load notification config from JSON file.

    Returns None if the file doesn't exist (notifications disabled).
    Raises ValueError if the file is malformed.
    """
    if path is None:
        path = _DEFAULT_CONFIG_PATH

    path = Path(path)
    if not path.exists():
        return None

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        raise ValueError(f"Failed to load notification config from {path}: {exc}")

    channels_data = data.get("channels", [])
    channels = [NotificationChannel(**ch) for ch in channels_data]

    return NotificationConfig(
        channels=channels,
        notify_on=data.get("notify_on", ["failed"]),
        quiet_success=data.get("quiet_success", False),
    )


# ---------------------------------------------------------------------------
# Notification implementations
# ---------------------------------------------------------------------------


def _notify_file(channel: NotificationChannel, payload: dict[str, Any]) -> None:
    """Append a JSON line to a file."""
    path = Path(channel.target)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(payload) + "\n")


def _notify_stderr(channel: NotificationChannel, payload: dict[str, Any]) -> None:
    """Print a one-line summary to stderr."""
    status = payload.get("status", "unknown")
    run_id = payload.get("run_id", "?")
    reason = payload.get("reason_code", "?")
    latency = payload.get("latency_ms", 0)
    print(
        f"[workflow] {status}: {run_id} ({reason}, {latency}ms)",
        file=sys.stderr,
    )


def _notify_webhook(channel: NotificationChannel, payload: dict[str, Any]) -> None:
    """POST the result JSON to the webhook URL."""
    import urllib.request

    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        channel.target,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def notify_workflow_complete(
    result: WorkflowResult,
    *,
    config: NotificationConfig | None = None,
) -> None:
    """Send notifications when a workflow run completes.

    Notifies via all configured channels that match the result status.

    Args:
        result: The WorkflowResult from run_workflow()
        config: NotificationConfig. If None, loads from default path.
    """
    if config is None:
        config = load_config()

    if config is None:
        # Notifications disabled (no config file)
        return

    if not config.should_notify(result.status):
        return

    payload = result.to_json()

    for channel in config.channels:
        if channel.kind == "file":
            _notify_file(channel, payload)
        elif channel.kind == "stderr":
            _notify_stderr(channel, payload)
        elif channel.kind == "webhook":
            _notify_webhook(channel, payload)


def notify_batch_complete(
    results: list[WorkflowResult],
    *,
    config: NotificationConfig | None = None,
) -> None:
    """Send a summary notification for a batch of dispatches.

    Sends a single summary notification with total, succeeded, failed counts
    and wall-clock time span.

    Args:
        results: List of WorkflowResult objects
        config: NotificationConfig. If None, loads from default path.
    """
    if not results:
        return

    if config is None:
        config = load_config()

    if config is None:
        return

    # Don't use status filtering for batch summaries — always send
    succeeded = sum(1 for r in results if r.status == "succeeded")
    failed = sum(1 for r in results if r.status == "failed")

    # Wall clock: earliest start to latest finish
    starts = [r.started_at for r in results]
    finishes = [r.finished_at for r in results]
    earliest_start = min(starts) if starts else datetime.now(timezone.utc)
    latest_finish = max(finishes) if finishes else datetime.now(timezone.utc)
    wall_clock_ms = int((latest_finish - earliest_start).total_seconds() * 1000)

    payload = {
        "kind": "workflow_batch_summary",
        "total": len(results),
        "succeeded": succeeded,
        "failed": failed,
        "wall_clock_ms": wall_clock_ms,
        "earliest_start": earliest_start.isoformat(),
        "latest_finish": latest_finish.isoformat(),
    }

    for channel in config.channels:
        if channel.kind == "file":
            _notify_file(channel, payload)
        elif channel.kind == "stderr":
            # For batch, just print the summary line
            print(
                f"[dispatch] batch summary: {len(results)} total, {succeeded} succeeded, {failed} failed ({wall_clock_ms}ms)",
                file=sys.stderr,
            )
        elif channel.kind == "webhook":
            _notify_webhook(channel, payload)
