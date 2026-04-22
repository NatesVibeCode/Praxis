#!/usr/bin/env python3
"""Read-only health endpoint for a Windows + WSL Praxis Postgres target."""

from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


PORT = int(os.environ.get("PRAXIS_STATUS_PORT", "8480"))
DATABASE_NAME = os.environ.get("PRAXIS_DB_NAME", "praxis")
DATABASE_USER = os.environ.get("PRAXIS_DB_USER", "postgres")
STARTED_AT = time.time()


def _run(
    argv: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int = 5,
) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            argv,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return 124, "timeout"
    except OSError as exc:
        return 1, str(exc)
    return proc.returncode, (proc.stdout or proc.stderr).strip()


def _psql(sql: str) -> tuple[int, str]:
    env = os.environ.copy()
    env.setdefault("PGDATABASE", DATABASE_NAME)
    env.setdefault("PGUSER", DATABASE_USER)
    return _run(["psql", "-tAc", sql], env=env, timeout=5)


def postgres_status() -> dict[str, Any]:
    env = os.environ.copy()
    env.setdefault("PGDATABASE", DATABASE_NAME)
    env.setdefault("PGUSER", DATABASE_USER)
    rc, _ = _run(["pg_isready", "-q", "-d", DATABASE_NAME], env=env, timeout=5)
    if rc != 0:
        return {"up": False, "version": None, "connections": None}

    _, version = _psql("SHOW server_version;")
    _, connections = _psql(
        "SELECT count(*) FROM pg_stat_activity WHERE state IS NOT NULL;"
    )
    try:
        connection_count = int(connections)
    except ValueError:
        connection_count = None
    return {
        "up": True,
        "version": version or None,
        "connections": connection_count,
    }


def pgvector_status() -> dict[str, Any]:
    _, version = _psql("SELECT extversion FROM pg_extension WHERE extname='vector';")
    version = version.strip()
    return {"installed": bool(version), "version": version or None}


def disk_status() -> dict[str, Any]:
    try:
        stat = os.statvfs("/var/lib/postgresql")
        free_gb = round(stat.f_bavail * stat.f_frsize / (1024**3), 1)
        total_gb = round(stat.f_blocks * stat.f_frsize / (1024**3), 1)
        used_pct = round(100.0 * (1 - stat.f_bavail / stat.f_blocks), 1)
    except OSError as exc:
        return {"error": str(exc)}
    return {"free_gb": free_gb, "total_gb": total_gb, "used_pct": used_pct}


def build_health() -> dict[str, Any]:
    postgres = postgres_status()
    pgvector = pgvector_status()
    disk = disk_status()

    if not postgres["up"]:
        status = "down"
    elif not pgvector["installed"] or disk.get("used_pct", 0) > 90:
        status = "degraded"
    else:
        status = "ok"

    return {
        "status": status,
        "postgres": postgres,
        "pgvector": pgvector,
        "disk": disk,
        "uptime_sec": int(time.time() - STARTED_AT),
        "checked_at": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps(build_health(), indent=2).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        print(f"[{now}] {self.address_string()} {fmt % args}")


def main() -> int:
    print(f"Praxis status server listening on 0.0.0.0:{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
