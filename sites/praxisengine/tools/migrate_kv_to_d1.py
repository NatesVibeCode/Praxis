#!/usr/bin/env python3
"""Migrate Praxis Engine subscriber records from Cloudflare KV into D1."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_DB = "praxisengine-subscribers"
DEFAULT_KV_NAMESPACE_ID = "98bed6e8ef244433a19abe00bffbb323"


@dataclass(frozen=True)
class KvSubscriber:
    email: str
    source: str
    submitted_at: str


def run(command: list[str], cwd: Path) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def list_email_keys(namespace_id: str, wrangler: str, cwd: Path) -> list[str]:
    command = [
        *shlex.split(wrangler),
        "kv",
        "key",
        "list",
        "--namespace-id",
        namespace_id,
        "--remote",
        "--prefix",
        "email:",
    ]
    rows = json.loads(run(command, cwd))
    return [str(row["name"]) for row in rows]


def get_kv_json(namespace_id: str, key: str, wrangler: str, cwd: Path) -> dict[str, Any]:
    command = [
        *shlex.split(wrangler),
        "kv",
        "key",
        "get",
        key,
        "--namespace-id",
        namespace_id,
        "--remote",
    ]
    return json.loads(run(command, cwd))


def normalize_record(payload: dict[str, Any], fallback_email: str) -> KvSubscriber:
    email = str(payload.get("email") or fallback_email).strip().lower()
    source = str(payload.get("source") or "landing")[:64]
    raw_ts = payload.get("ts") or payload.get("submitted_at")
    submitted_at = normalize_timestamp(raw_ts)
    return KvSubscriber(email=email, source=source, submitted_at=submitted_at)


def normalize_timestamp(raw: Any) -> str:
    if isinstance(raw, str) and "T" in raw:
        return raw
    if isinstance(raw, (int, float)):
        import datetime as dt

        seconds = raw / 1000 if raw > 10_000_000_000 else raw
        return dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc).isoformat()
    import datetime as dt

    return dt.datetime.now(dt.timezone.utc).isoformat()


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def upsert_sql(records: list[KvSubscriber]) -> str:
    statements: list[str] = []
    for record in records:
        email = sql_quote(record.email)
        source = sql_quote(record.source)
        submitted_at = sql_quote(record.submitted_at)
        statements.append(
            """
INSERT INTO subscribers (
  email,
  first_source,
  last_source,
  created_at,
  updated_at,
  submit_count
)
VALUES ({email}, {source}, {source}, {submitted_at}, {submitted_at}, 1)
ON CONFLICT(email) DO UPDATE SET
  last_source = excluded.last_source,
  updated_at = excluded.updated_at;
INSERT INTO subscriber_events (email, source, created_at)
SELECT {email}, {source}, {submitted_at}
WHERE NOT EXISTS (
  SELECT 1 FROM subscriber_events
  WHERE email = {email}
    AND source = {source}
    AND created_at = {submitted_at}
);
""".format(
                email=email,
                source=source,
                submitted_at=submitted_at,
            ).strip()
        )
    return "\n".join(statements)


def execute_sql(db: str, sql: str, wrangler: str, cwd: Path) -> None:
    if not sql.strip():
        return
    command = [
        *shlex.split(wrangler),
        "d1",
        "execute",
        db,
        "--remote",
        "--command",
        sql,
        "--yes",
    ]
    run(command, cwd)


def main() -> int:
    args = parser().parse_args()
    cwd = Path(__file__).resolve().parents[1]
    keys = list_email_keys(args.kv_namespace_id, args.wrangler, cwd)
    records: list[KvSubscriber] = []
    for key in keys:
        payload = get_kv_json(args.kv_namespace_id, key, args.wrangler, cwd)
        fallback_email = key.removeprefix("email:")
        records.append(normalize_record(payload, fallback_email))

    if args.dry_run:
        print(json.dumps([record.__dict__ for record in records], indent=2))
        return 0

    for start in range(0, len(records), args.batch_size):
        execute_sql(args.db, upsert_sql(records[start : start + args.batch_size]), args.wrangler, cwd)
    print(f"Migrated {len(records)} subscriber record(s) into {args.db}.")
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--db", default=DEFAULT_DB, help=f"D1 database name. Default: {DEFAULT_DB}")
    root.add_argument(
        "--kv-namespace-id",
        default=DEFAULT_KV_NAMESPACE_ID,
        help="Cloudflare KV namespace id containing email:* keys.",
    )
    root.add_argument("--batch-size", type=int, default=50)
    root.add_argument("--dry-run", action="store_true")
    root.add_argument(
        "--wrangler",
        default="npx --yes wrangler@latest",
        help="Wrangler command prefix.",
    )
    return root


if __name__ == "__main__":
    raise SystemExit(main())
