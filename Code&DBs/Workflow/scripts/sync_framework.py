"""Shared sync framework for model registry scripts.

Provides: asyncpg connection management, HTTP fetch, JSON serialisation,
decision-ref generation, CLI argument parsing, and a run-and-print entry point.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

import asyncpg

__all__ = [
    "utc_now",
    "decision_ref",
    "http_json",
    "jsonb",
    "db_connect",
    "add_database_url_arg",
    "add_dry_run_arg",
    "require_database_url",
    "run_and_print",
]


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def decision_ref(kind: str) -> str:
    """Return a stable decision ref stamped to the current UTC second."""
    return f"decision.{kind}.{utc_now().strftime('%Y%m%dT%H%M%SZ')}"


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def http_json(url: str, *, headers: dict[str, str]) -> dict[str, Any]:
    """GET *url* with *headers*, parse JSON, raise RuntimeError on failure."""
    request = Request(url, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {body[:300]}") from exc
    except URLError as exc:
        raise RuntimeError(f"request failed for {url}: {exc}") from exc


# ---------------------------------------------------------------------------
# JSON / asyncpg helpers
# ---------------------------------------------------------------------------

def jsonb(value: Any) -> str:
    """Compact, sorted JSON string suitable for asyncpg ``$n::jsonb`` params."""
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

async def db_connect(database_url: str) -> asyncpg.Connection:
    """Open and return an asyncpg connection."""
    return await asyncpg.connect(database_url)


def _normalize_database_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        raise SystemExit("--database-url is required (or set WORKFLOW_DATABASE_URL)")
    if not text.startswith(("postgresql://", "postgres://")):
        raise SystemExit("WORKFLOW_DATABASE_URL must use a postgres:// or postgresql:// DSN")

    parsed = urlsplit(text)
    if parsed.username or parsed.scheme not in {"postgresql", "postgres"}:
        return text

    hostname = parsed.hostname or "localhost"
    netloc = "postgres"
    if parsed.password:
        netloc += f":{parsed.password}"
    netloc += f"@{hostname}"
    if parsed.port is not None:
        netloc += f":{parsed.port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------

def add_database_url_arg(parser: argparse.ArgumentParser) -> None:
    """Add ``--database-url`` (defaults to ``WORKFLOW_DATABASE_URL``)."""
    parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres DSN. Defaults to WORKFLOW_DATABASE_URL.",
    )


def add_dry_run_arg(parser: argparse.ArgumentParser) -> None:
    """Add ``--dry-run`` flag."""
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and plan without writing to Postgres.",
    )


def require_database_url(args: argparse.Namespace) -> str:
    """Return the database URL or exit with a clear message."""
    url = getattr(args, "database_url", None)
    if not isinstance(url, str) or not url.strip():
        url = os.environ.get("WORKFLOW_DATABASE_URL")
    if not isinstance(url, str) or not url.strip():
        raise SystemExit("--database-url is required (or set WORKFLOW_DATABASE_URL)")
    return _normalize_database_url(url)


# ---------------------------------------------------------------------------
# Entry-point helper
# ---------------------------------------------------------------------------

def run_and_print(coro: Any) -> int:
    """Run *coro* with asyncio, pretty-print the result dict, return 0."""
    result = asyncio.run(coro)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0
