#!/usr/bin/env python3
"""Poll Praxis Engine subscriber events, append a CSV, and notify macOS."""

from __future__ import annotations

import argparse
import csv
import json
import os
import plistlib
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_DB = "praxisengine-subscribers"
DEFAULT_CSV = Path.home() / "Documents" / "PraxisEngine" / "subscribers.csv"
DEFAULT_STATE = (
    Path.home()
    / "Library"
    / "Application Support"
    / "PraxisEngine"
    / "subscriber_notifier_state.json"
)
LAUNCH_AGENT_LABEL = "com.praxisengine.subscriber-notifier"
LAUNCH_AGENT_PATH = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCH_AGENT_LABEL}.plist"


@dataclass(frozen=True)
class SubscriberEvent:
    event_id: int
    email: str
    source: str
    created_at: str


def run_wrangler_query(db: str, sql: str, wrangler: str, site_root: Path) -> list[dict[str, Any]]:
    command = [
        *shlex.split(wrangler),
        "d1",
        "execute",
        db,
        "--remote",
        "--json",
        "--command",
        sql,
    ]
    result = subprocess.run(
        command,
        cwd=site_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())

    payload = json.loads(result.stdout)
    if isinstance(payload, list):
        records: list[dict[str, Any]] = []
        for item in payload:
            records.extend(item.get("results") or item.get("result") or [])
        return records
    if isinstance(payload, dict):
        return payload.get("results") or payload.get("result") or []
    raise RuntimeError("Unexpected Wrangler JSON response")


def fetch_events(db: str, after_id: int, limit: int, wrangler: str, site_root: Path) -> list[SubscriberEvent]:
    sql = (
        "SELECT id, email, source, created_at "
        "FROM subscriber_events "
        f"WHERE id > {int(after_id)} "
        "ORDER BY id ASC "
        f"LIMIT {int(limit)}"
    )
    rows = run_wrangler_query(db, sql, wrangler, site_root)
    return [
        SubscriberEvent(
            event_id=int(row["id"]),
            email=str(row["email"]),
            source=str(row.get("source") or "landing"),
            created_at=str(row["created_at"]),
        )
        for row in rows
    ]


def fetch_max_event_id(db: str, wrangler: str, site_root: Path) -> int:
    rows = run_wrangler_query(
        db,
        "SELECT COALESCE(MAX(id), 0) AS max_id FROM subscriber_events",
        wrangler,
        site_root,
    )
    if not rows:
        return 0
    return int(rows[0].get("max_id") or 0)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def save_state(path: Path, last_event_id: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_event_id": last_event_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def max_event_id_from_csv(path: Path) -> int:
    if not path.exists():
        return 0
    max_id = 0
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                max_id = max(max_id, int(row.get("event_id") or 0))
            except ValueError:
                continue
    return max_id


def append_csv(path: Path, events: list[SubscriberEvent]) -> None:
    if not events:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["event_id", "email", "source", "created_at", "seen_at"],
        )
        if not exists:
            writer.writeheader()
        seen_at = datetime.now(timezone.utc).isoformat()
        for event in events:
            writer.writerow(
                {
                    "event_id": event.event_id,
                    "email": event.email,
                    "source": event.source,
                    "created_at": event.created_at,
                    "seen_at": seen_at,
                }
            )


def notify(event: SubscriberEvent) -> None:
    script = (
        'display notification "'
        + escape_applescript(event.email)
        + '" with title "New Praxis Engine signup" subtitle "'
        + escape_applescript(event.source)
        + '"'
    )
    subprocess.run(["osascript", "-e", script], check=False)


def escape_applescript(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def site_root() -> Path:
    return Path(__file__).resolve().parents[1]


def command_run(args: argparse.Namespace) -> int:
    state = load_state(args.state)
    last_event_id = int(state.get("last_event_id") or 0)
    if last_event_id == 0:
        last_event_id = max_event_id_from_csv(args.csv)
    if args.initialize:
        current_max = fetch_max_event_id(args.db, args.wrangler, site_root())
        save_state(args.state, current_max)
        print(f"Initialized notifier checkpoint at event {current_max}.")
        return 0

    while True:
        events = fetch_events(args.db, last_event_id, args.limit, args.wrangler, site_root())
        append_csv(args.csv, events)
        for event in events:
            if not args.no_notify:
                notify(event)
            print(f"{event.event_id}: {event.email} ({event.source})")
            last_event_id = max(last_event_id, event.event_id)
        if events:
            save_state(args.state, last_event_id)
        if args.once:
            return 0
        time.sleep(args.interval)


def command_install_launch_agent(args: argparse.Namespace) -> int:
    LAUNCH_AGENT_PATH.parent.mkdir(parents=True, exist_ok=True)
    program_arguments = [
        sys.executable,
        str(Path(__file__).resolve()),
        "run",
        "--db",
        args.db,
        "--csv",
        str(args.csv),
        "--state",
        str(args.state),
        "--interval",
        str(args.interval),
        "--wrangler",
        args.wrangler,
    ]
    plist = {
        "Label": LAUNCH_AGENT_LABEL,
        "ProgramArguments": program_arguments,
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(args.state.parent / "subscriber_notifier.out.log"),
        "StandardErrorPath": str(args.state.parent / "subscriber_notifier.err.log"),
        "EnvironmentVariables": {
            "PATH": os.environ.get("PATH", "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin")
        },
    }
    with LAUNCH_AGENT_PATH.open("wb") as handle:
        plistlib.dump(plist, handle)
    subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}", str(LAUNCH_AGENT_PATH)], check=False)
    subprocess.run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(LAUNCH_AGENT_PATH)], check=True)
    subprocess.run(["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{LAUNCH_AGENT_LABEL}"], check=False)
    print(f"Installed and started {LAUNCH_AGENT_PATH}.")
    return 0


def command_uninstall_launch_agent(_: argparse.Namespace) -> int:
    subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}", str(LAUNCH_AGENT_PATH)], check=False)
    if LAUNCH_AGENT_PATH.exists():
        LAUNCH_AGENT_PATH.unlink()
    print(f"Removed {LAUNCH_AGENT_PATH}.")
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    subcommands = root.add_subparsers(dest="command", required=True)

    run = subcommands.add_parser("run", help="Poll D1, append CSV, and notify.")
    add_common(run)
    run.add_argument("--once", action="store_true", help="Run one polling pass and exit.")
    run.add_argument("--no-notify", action="store_true", help="Append CSV without desktop notifications.")
    run.add_argument("--initialize", action="store_true", help="Set checkpoint to current max event and exit.")
    run.set_defaults(func=command_run)

    install = subcommands.add_parser("install-launch-agent", help="Install and start the macOS LaunchAgent.")
    add_common(install)
    install.set_defaults(func=command_install_launch_agent)

    uninstall = subcommands.add_parser("uninstall-launch-agent", help="Stop and remove the macOS LaunchAgent.")
    uninstall.set_defaults(func=command_uninstall_launch_agent)
    return root


def add_common(command: argparse.ArgumentParser) -> None:
    command.add_argument("--db", default=DEFAULT_DB, help=f"D1 database name. Default: {DEFAULT_DB}")
    command.add_argument("--csv", type=Path, default=DEFAULT_CSV, help=f"CSV path. Default: {DEFAULT_CSV}")
    command.add_argument("--state", type=Path, default=DEFAULT_STATE, help=f"State path. Default: {DEFAULT_STATE}")
    command.add_argument("--interval", type=int, default=1800, help="Polling interval in seconds.")
    command.add_argument("--limit", type=int, default=100, help="Max events per polling pass.")
    command.add_argument(
        "--wrangler",
        default="npx --yes wrangler@latest",
        help="Wrangler command prefix.",
    )


def main() -> int:
    args = parser().parse_args()
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
