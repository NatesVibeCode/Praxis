"""Canonical post-workflow graph sync hook and durable sync-status authority."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from storage.postgres import SyncPostgresConnection, ensure_postgres_available

_SYNC_STATUSES = ("pending", "succeeded", "degraded", "skipped")
_SYNC_STATUS_DDL = """
CREATE TABLE IF NOT EXISTS workflow_run_sync_status (
    run_id TEXT PRIMARY KEY
        REFERENCES workflow_runs (run_id)
        ON DELETE CASCADE,
    sync_status TEXT NOT NULL
        CHECK (sync_status IN ('pending', 'succeeded', 'degraded', 'skipped')),
    sync_cycle_id TEXT,
    sync_error_count INTEGER NOT NULL DEFAULT 0
        CHECK (sync_error_count >= 0),
    total_findings INTEGER NOT NULL DEFAULT 0
        CHECK (total_findings >= 0),
    total_actions INTEGER NOT NULL DEFAULT 0
        CHECK (total_actions >= 0),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS workflow_run_sync_status_updated_at_idx
    ON workflow_run_sync_status (updated_at DESC);
"""


@dataclass(frozen=True, slots=True)
class WorkflowRunSyncStatus:
    """Durable sync-status snapshot for one persisted run."""

    run_id: str
    sync_status: str = "skipped"
    sync_cycle_id: str | None = None
    sync_error_count: int = 0
    total_findings: int = 0
    total_actions: int = 0
    last_error: str = ""
    updated_at: datetime | None = None

    def to_json(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "sync_status": self.sync_status,
            "sync_cycle_id": self.sync_cycle_id,
            "sync_error_count": self.sync_error_count,
            "total_findings": self.total_findings,
            "total_actions": self.total_actions,
            "last_error": self.last_error,
            "updated_at": None if self.updated_at is None else self.updated_at.isoformat(),
        }


def _connection(conn: SyncPostgresConnection | None = None) -> SyncPostgresConnection:
    return conn if conn is not None else ensure_postgres_available()


def _repo_root(repo_root: str | Path | None = None) -> Path:
    if repo_root is not None:
        return Path(repo_root).resolve()
    return Path(__file__).resolve().parents[3]


def _json_mapping(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def _request_roadmap_item_ids(request_envelope: object) -> tuple[str, ...]:
    envelope = _json_mapping(request_envelope)
    spec_snapshot = _json_mapping(envelope.get("spec_snapshot"))
    roadmap_item_ids: list[str] = []
    for candidate in (envelope, spec_snapshot):
        raw_items = candidate.get("roadmap_items")
        if not isinstance(raw_items, list):
            continue
        for item in raw_items:
            if isinstance(item, str):
                normalized = item.strip()
                if normalized and normalized not in roadmap_item_ids:
                    roadmap_item_ids.append(normalized)
    return tuple(roadmap_item_ids)


def closeout_workflow_run_roadmap_items(
    run_id: str,
    *,
    conn: SyncPostgresConnection | None = None,
) -> dict[str, object]:
    """Mark packet-backed roadmap items done when their run succeeded."""

    db = _connection(conn)
    run_row = db.fetchrow(
        """
        SELECT current_state, started_at, finished_at, request_envelope
        FROM workflow_runs
        WHERE run_id = $1
        """,
        run_id,
    )
    if run_row is None:
        return {
            "run_id": run_id,
            "current_state": None,
            "requested_items": [],
            "updated_items": [],
            "already_completed_items": [],
            "missing_items": [],
            "reason_codes": ["run_missing"],
        }

    current_state = str(run_row["current_state"])
    started_at = run_row.get("started_at")
    finished_at = run_row.get("finished_at")
    roadmap_item_ids = _request_roadmap_item_ids(run_row.get("request_envelope"))
    if current_state != "succeeded":
        return {
            "run_id": run_id,
            "current_state": current_state,
            "requested_items": list(roadmap_item_ids),
            "updated_items": [],
            "already_completed_items": [],
            "missing_items": [],
            "reason_codes": ["run_not_succeeded"],
        }
    if started_at is None or finished_at is None:
        return {
            "run_id": run_id,
            "current_state": current_state,
            "requested_items": list(roadmap_item_ids),
            "updated_items": [],
            "already_completed_items": [],
            "missing_items": [],
            "reason_codes": ["run_lifecycle_incomplete"],
        }
    if finished_at < started_at:
        return {
            "run_id": run_id,
            "current_state": current_state,
            "requested_items": list(roadmap_item_ids),
            "updated_items": [],
            "already_completed_items": [],
            "missing_items": [],
            "reason_codes": ["run_lifecycle_invalid"],
        }
    if not roadmap_item_ids:
        return {
            "run_id": run_id,
            "current_state": current_state,
            "requested_items": [],
            "updated_items": [],
            "already_completed_items": [],
            "missing_items": [],
            "reason_codes": ["missing_roadmap_items"],
        }

    existing_rows = db.execute(
        """
        SELECT roadmap_item_id, status, lifecycle, completed_at
        FROM roadmap_items
        WHERE roadmap_item_id = ANY($1::text[])
        """,
        list(roadmap_item_ids),
    )
    existing_by_id = {
        str(row["roadmap_item_id"]): row
        for row in existing_rows
    }
    missing_items = [
        roadmap_item_id
        for roadmap_item_id in roadmap_item_ids
        if roadmap_item_id not in existing_by_id
    ]
    pending_ids = [
        roadmap_item_id
        for roadmap_item_id, row in existing_by_id.items()
        if row.get("completed_at") is None
    ]
    already_completed_items = [
        roadmap_item_id
        for roadmap_item_id, row in existing_by_id.items()
        if row.get("completed_at") is not None
    ]
    updated_rows = []
    if pending_ids:
        updated_rows = db.execute(
            """
            UPDATE public.roadmap_items
            SET status = 'done',
                lifecycle = 'completed',
                completed_at = COALESCE(completed_at, now()),
                updated_at = now()
            WHERE roadmap_item_id = ANY($1::text[])
            RETURNING roadmap_item_id
            """,
            pending_ids,
        )
    updated_items = [str(row["roadmap_item_id"]) for row in updated_rows]
    reason_codes: list[str] = []
    if missing_items:
        reason_codes.append("missing_roadmap_rows")
    if already_completed_items:
        reason_codes.append("already_completed")
    return {
        "run_id": run_id,
        "current_state": current_state,
        "requested_items": list(roadmap_item_ids),
        "updated_items": updated_items,
        "already_completed_items": already_completed_items,
        "missing_items": missing_items,
        "reason_codes": reason_codes,
    }


def ensure_workflow_run_sync_status_schema(
    conn: SyncPostgresConnection | None = None,
) -> None:
    """Create the durable sync-status table if it is missing."""

    _connection(conn).execute_script(_SYNC_STATUS_DDL)


def record_workflow_run_sync_status(
    run_id: str,
    *,
    sync_status: str,
    sync_cycle_id: str | None = None,
    sync_error_count: int = 0,
    total_findings: int = 0,
    total_actions: int = 0,
    last_error: str = "",
    conn: SyncPostgresConnection | None = None,
) -> WorkflowRunSyncStatus:
    """Upsert one canonical sync-status row."""

    if sync_status not in _SYNC_STATUSES:
        raise ValueError(f"invalid sync_status: {sync_status}")

    db = _connection(conn)
    ensure_workflow_run_sync_status_schema(db)
    row = db.fetchrow(
        """
        INSERT INTO workflow_run_sync_status (
            run_id,
            sync_status,
            sync_cycle_id,
            sync_error_count,
            total_findings,
            total_actions,
            last_error
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (run_id) DO UPDATE
        SET sync_status = EXCLUDED.sync_status,
            sync_cycle_id = EXCLUDED.sync_cycle_id,
            sync_error_count = EXCLUDED.sync_error_count,
            total_findings = EXCLUDED.total_findings,
            total_actions = EXCLUDED.total_actions,
            last_error = EXCLUDED.last_error,
            updated_at = now()
        RETURNING run_id,
                  sync_status,
                  sync_cycle_id,
                  sync_error_count,
                  total_findings,
                  total_actions,
                  last_error,
                  updated_at
        """,
        run_id,
        sync_status,
        sync_cycle_id,
        max(sync_error_count, 0),
        max(total_findings, 0),
        max(total_actions, 0),
        last_error,
    )
    return _row_to_status(row)


def get_workflow_run_sync_status(
    run_id: str,
    *,
    conn: SyncPostgresConnection | None = None,
) -> WorkflowRunSyncStatus:
    """Load sync status for a run, defaulting to `skipped` when absent."""

    db = _connection(conn)
    ensure_workflow_run_sync_status_schema(db)
    row = db.fetchrow(
        """
        SELECT run_id,
               sync_status,
               sync_cycle_id,
               sync_error_count,
               total_findings,
               total_actions,
               last_error,
               updated_at
        FROM workflow_run_sync_status
        WHERE run_id = $1
        """,
        run_id,
    )
    if row is None:
        return WorkflowRunSyncStatus(run_id=run_id)
    return _row_to_status(row)


def latest_workflow_run_sync_status(
    *,
    conn: SyncPostgresConnection | None = None,
) -> WorkflowRunSyncStatus | None:
    """Return the most recently updated sync-status row, if any."""

    db = _connection(conn)
    ensure_workflow_run_sync_status_schema(db)
    row = db.fetchrow(
        """
        SELECT run_id,
               sync_status,
               sync_cycle_id,
               sync_error_count,
               total_findings,
               total_actions,
               last_error,
               updated_at
        FROM workflow_run_sync_status
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """
    )
    if row is None:
        return None
    return _row_to_status(row)


def repair_workflow_run_sync(
    run_id: str | None = None,
    *,
    conn: SyncPostgresConnection | None = None,
    repo_root: str | Path | None = None,
) -> WorkflowRunSyncStatus:
    """Rerun the canonical post-workflow sync hook for one run."""

    db = _connection(conn)
    target_run_id = run_id
    if target_run_id is None:
        row = db.fetchrow(
            """
            SELECT run_id
            FROM workflow_run_sync_status
            WHERE sync_status = 'degraded'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
        if row is None:
            row = db.fetchrow(
                """
                SELECT run_id
                FROM workflow_runs
                ORDER BY requested_at DESC
                LIMIT 1
                """
            )
        if row is None:
            raise RuntimeError("no persisted workflow run available for sync repair")
        target_run_id = str(row["run_id"])

    return run_post_workflow_sync(
        target_run_id,
        conn=db,
        repo_root=repo_root,
    )


def backfill_workflow_proof(
    *,
    run_id: str | None = None,
    limit: int | None = None,
    conn: SyncPostgresConnection | None = None,
    repo_root: str | Path | None = None,
) -> dict[str, object]:
    """Rebuild receipt provenance and memory proof edges for historical runs."""

    db = _connection(conn)
    from memory.engine import MemoryEngine
    from memory.sync import MemorySync
    from runtime.receipt_store import backfill_receipt_provenance, proof_metrics

    resolved_repo_root = str(_repo_root(repo_root))
    receipt_backfill = backfill_receipt_provenance(
        run_id=run_id,
        limit=limit,
        repo_root=resolved_repo_root,
        conn=db,
    )
    memory_sync = MemorySync(db, MemoryEngine(db))
    memory_backfill = memory_sync.backfill_receipts(
        run_id=run_id,
        limit=limit,
    )
    roadmap_closeout = (
        closeout_workflow_run_roadmap_items(run_id, conn=db)
        if run_id is not None
        else {
            "run_id": None,
            "current_state": None,
            "requested_items": [],
            "updated_items": [],
            "already_completed_items": [],
            "missing_items": [],
            "reason_codes": ["run_id_required"],
        }
    )
    metrics = proof_metrics(conn=db)
    return {
        "run_id": run_id,
        "requested_limit": limit,
        "repo_root": resolved_repo_root,
        "receipt_backfill": receipt_backfill,
        "memory_backfill": memory_backfill,
        "roadmap_closeout": roadmap_closeout,
        "proof_metrics": metrics,
    }


def run_post_workflow_sync(
    run_id: str,
    *,
    conn: SyncPostgresConnection | None = None,
    repo_root: str | Path | None = None,
) -> WorkflowRunSyncStatus:
    """Run the canonical graph-sync flow and persist the outcome."""

    if not run_id or run_id.startswith(("cached:", "error:")):
        return WorkflowRunSyncStatus(run_id=run_id or "unknown")

    db = _connection(conn)
    ensure_workflow_run_sync_status_schema(db)
    record_workflow_run_sync_status(
        run_id,
        sync_status="pending",
        conn=db,
    )

    from runtime.heartbeat_runner import HeartbeatRunner

    from runtime.embedding_service import EmbeddingService

    embedder = EmbeddingService()
    runner = HeartbeatRunner(
        conn=db,
        embedder=embedder,
        include_probers=False,
    )
    result = runner.run_once()
    proof_backfill = backfill_workflow_proof(
        run_id=run_id,
        conn=db,
        repo_root=repo_root,
    )
    proof_actions = int(
        (
            (proof_backfill.get("receipt_backfill") or {}).get("updated_receipts")
            or 0
        )
    ) + int(
        (
            (proof_backfill.get("memory_backfill") or {}).get("actions")
            or 0
        )
    ) + int(
        len((proof_backfill.get("roadmap_closeout") or {}).get("updated_items") or [])
    )
    return record_workflow_run_sync_status(
        run_id,
        sync_status="failed" if result.errors else "succeeded",
        sync_cycle_id=result.cycle_id,
        sync_error_count=result.errors,
        total_findings=0,
        total_actions=proof_actions,
        last_error=_summarize_cycle_errors(result),
        conn=db,
    )


def _summarize_cycle_errors(result) -> str:
    errors: list[str] = []
    for mr in getattr(result, "module_results", ()):
        if not mr.ok and mr.error:
            errors.append(f"{mr.module_name}: {mr.error}")
    return "; ".join(errors)[:1000]


def _row_to_status(row) -> WorkflowRunSyncStatus:
    return WorkflowRunSyncStatus(
        run_id=str(row["run_id"]),
        sync_status=str(row["sync_status"]),
        sync_cycle_id=row["sync_cycle_id"],
        sync_error_count=int(row["sync_error_count"] or 0),
        total_findings=int(row["total_findings"] or 0),
        total_actions=int(row["total_actions"] or 0),
        last_error=str(row["last_error"] or ""),
        updated_at=row["updated_at"],
    )
