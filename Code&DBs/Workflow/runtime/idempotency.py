from __future__ import annotations

import hashlib
import json
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


@dataclass(frozen=True)
class IdempotencyResult:
    is_replay: bool
    is_conflict: bool
    existing_run_id: str | None
    response_snapshot: dict | None
    created_at: datetime | None


def canonical_hash(body: dict) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def check_idempotency(
    conn: "SyncPostgresConnection",
    surface: str,
    key: str,
    payload_hash: str,
    *,
    replayable_run_states: Collection[str] | None = None,
) -> IdempotencyResult:
    rows = conn.execute(
        """SELECT payload_hash, run_id, response_snapshot, created_at
           FROM idempotency_ledger
           WHERE surface = $1 AND idempotency_key = $2
           LIMIT 1""",
        surface, key,
    )
    if not rows:
        return IdempotencyResult(False, False, None, None, None)
    row = rows[0]
    if row.get("payload_hash") == payload_hash:
        existing_run_id = row.get("run_id")
        if replayable_run_states is not None and existing_run_id:
            run_rows = conn.execute(
                """SELECT current_state
                   FROM workflow_runs
                   WHERE run_id = $1
                   LIMIT 1""",
                existing_run_id,
            )
            if run_rows:
                current_state = str(run_rows[0].get("current_state") or "").strip()
                if current_state not in replayable_run_states:
                    return IdempotencyResult(
                        False,
                        False,
                        existing_run_id,
                        row.get("response_snapshot"),
                        row.get("created_at"),
                    )
            else:
                return IdempotencyResult(
                    False,
                    False,
                    existing_run_id,
                    row.get("response_snapshot"),
                    row.get("created_at"),
                )
        return IdempotencyResult(
            True,
            False,
            existing_run_id,
            row.get("response_snapshot"),
            row.get("created_at"),
        )
    return IdempotencyResult(
        False,
        True,
        row.get("run_id"),
        row.get("response_snapshot"),
        row.get("created_at"),
    )


def record_idempotency(
    conn: "SyncPostgresConnection",
    surface: str,
    key: str,
    payload_hash: str,
    run_id: str | None = None,
    response_snapshot: dict | None = None,
) -> None:
    conn.execute(
        """INSERT INTO idempotency_ledger
           (surface, idempotency_key, payload_hash, run_id, response_snapshot)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (surface, idempotency_key) DO UPDATE
           SET payload_hash = EXCLUDED.payload_hash,
               run_id = COALESCE(EXCLUDED.run_id, idempotency_ledger.run_id),
               response_snapshot = COALESCE(EXCLUDED.response_snapshot, idempotency_ledger.response_snapshot)""",
        surface,
        key,
        payload_hash,
        run_id,
        json.dumps(response_snapshot) if response_snapshot is not None else None,
    )
