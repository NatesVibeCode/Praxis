from __future__ import annotations

from datetime import datetime, timezone

import memory.engine as memory_engine_module
import memory.sync as memory_sync_module
import runtime.receipt_store as receipt_store_module
import runtime.post_workflow_sync as sync_module


class _FakeConn:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}
        self.workflow_run_id: str | None = "run-fallback"
        self.workflow_runs: dict[str, dict[str, object]] = {}
        self.roadmap_rows: dict[str, dict[str, object]] = {}

    def execute_script(self, sql: str) -> None:
        self.last_schema_sql = sql

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if "SELECT roadmap_item_id, status, completed_at FROM roadmap_items" in normalized:
            ids = [str(item) for item in args[0]]
            return [
                self.roadmap_rows[roadmap_item_id]
                for roadmap_item_id in ids
                if roadmap_item_id in self.roadmap_rows
            ]
        if "UPDATE public.roadmap_items SET status = 'done'" in normalized:
            ids = [str(item) for item in args[0]]
            updated = []
            for roadmap_item_id in ids:
                row = self.roadmap_rows.get(roadmap_item_id)
                if row is None:
                    continue
                row["status"] = "done"
                row["completed_at"] = row.get("completed_at") or datetime(2026, 4, 8, 12, 5, tzinfo=timezone.utc)
                updated.append({"roadmap_item_id": roadmap_item_id})
            return updated
        raise AssertionError(f"unexpected execute query: {normalized}")

    def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if "INSERT INTO workflow_run_sync_status" in normalized:
            run_id = str(args[0])
            row = {
                "run_id": run_id,
                "sync_status": str(args[1]),
                "sync_cycle_id": args[2],
                "sync_error_count": int(args[3]),
                "total_findings": int(args[4]),
                "total_actions": int(args[5]),
                "last_error": str(args[6]),
                "updated_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
            }
            self.rows[run_id] = row
            return row
        if "FROM workflow_run_sync_status WHERE run_id = $1" in normalized:
            return self.rows.get(str(args[0]))
        if "FROM workflow_run_sync_status ORDER BY updated_at DESC, created_at DESC LIMIT 1" in normalized:
            if not self.rows:
                return None
            return next(reversed(list(self.rows.values())))
        if "FROM workflow_run_sync_status WHERE sync_status = 'degraded'" in normalized:
            degraded = [row for row in self.rows.values() if row["sync_status"] == "degraded"]
            if not degraded:
                return None
            return {"run_id": degraded[-1]["run_id"]}
        if "SELECT current_state, started_at, finished_at, request_envelope FROM workflow_runs WHERE run_id = $1" in normalized:
            return self.workflow_runs.get(str(args[0]))
        if "FROM workflow_runs ORDER BY requested_at DESC LIMIT 1" in normalized:
            if self.workflow_run_id is None:
                return None
            return {"run_id": self.workflow_run_id}
        raise AssertionError(f"unexpected query: {normalized}")


def test_record_and_get_workflow_run_sync_status_round_trip() -> None:
    conn = _FakeConn()

    written = sync_module.record_workflow_run_sync_status(
        "run-1",
        sync_status="degraded",
        sync_cycle_id="cycle-1",
        sync_error_count=2,
        total_findings=3,
        total_actions=4,
        last_error="boom",
        conn=conn,
    )
    loaded = sync_module.get_workflow_run_sync_status("run-1", conn=conn)
    latest = sync_module.latest_workflow_run_sync_status(conn=conn)

    assert written.run_id == "run-1"
    assert loaded.sync_status == "degraded"
    assert loaded.sync_cycle_id == "cycle-1"
    assert loaded.sync_error_count == 2
    assert loaded.total_findings == 3
    assert loaded.total_actions == 4
    assert loaded.last_error == "boom"
    assert latest is not None
    assert latest.run_id == "run-1"


def test_repair_workflow_run_sync_prefers_latest_degraded_run(monkeypatch) -> None:
    conn = _FakeConn()
    sync_module.record_workflow_run_sync_status(
        "run-degraded",
        sync_status="degraded",
        sync_error_count=1,
        conn=conn,
    )
    seen: list[str] = []

    def _fake_run_post_workflow_sync(run_id: str, *, conn=None, repo_root=None):
        seen.append(run_id)
        return sync_module.WorkflowRunSyncStatus(
            run_id=run_id,
            sync_status="succeeded",
            sync_cycle_id="cycle-repair",
            sync_error_count=0,
        )

    monkeypatch.setattr(sync_module, "run_post_workflow_sync", _fake_run_post_workflow_sync)

    repaired = sync_module.repair_workflow_run_sync(conn=conn)

    assert seen == ["run-degraded"]
    assert repaired.run_id == "run-degraded"
    assert repaired.sync_status == "succeeded"


def test_backfill_workflow_proof_reconciles_receipts_and_memory(monkeypatch) -> None:
    conn = _FakeConn()
    conn.workflow_runs["run-1"] = {
        "current_state": "succeeded",
        "started_at": datetime(2026, 4, 8, 11, 55, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc),
        "request_envelope": {
            "spec_snapshot": {
                "roadmap_items": ["roadmap_item.alpha", "roadmap_item.beta"],
            }
        },
    }
    conn.roadmap_rows["roadmap_item.alpha"] = {
        "roadmap_item_id": "roadmap_item.alpha",
        "status": "active",
        "completed_at": None,
    }
    conn.roadmap_rows["roadmap_item.beta"] = {
        "roadmap_item_id": "roadmap_item.beta",
        "status": "done",
        "completed_at": datetime(2026, 4, 8, 11, 0, tzinfo=timezone.utc),
    }
    seen: dict[str, object] = {}

    class _FakeMemoryEngine:
        def __init__(self, conn_arg) -> None:
            seen["engine_conn"] = conn_arg

    class _FakeMemorySync:
        def __init__(self, conn_arg, engine_arg) -> None:
            seen["sync_conn"] = conn_arg
            seen["sync_engine"] = engine_arg

        def backfill_receipts(self, *, run_id=None, limit=None):
            seen["memory_run_id"] = run_id
            seen["memory_limit"] = limit
            return {"run_id": run_id, "requested_limit": limit, "synced_receipts": 2, "actions": 5}

    def _fake_backfill_receipt_provenance(*, run_id=None, limit=None, repo_root=None, conn=None):
        seen["receipt_run_id"] = run_id
        seen["receipt_limit"] = limit
        seen["repo_root"] = repo_root
        seen["receipt_conn"] = conn
        return {"run_id": run_id, "requested_limit": limit, "updated_receipts": 2}

    def _fake_proof_metrics(*, since_hours=0, conn=None):
        seen["metrics_conn"] = conn
        return {"receipts": {"total": 10}}

    monkeypatch.setattr(memory_engine_module, "MemoryEngine", _FakeMemoryEngine)
    monkeypatch.setattr(memory_sync_module, "MemorySync", _FakeMemorySync)
    monkeypatch.setattr(receipt_store_module, "backfill_receipt_provenance", _fake_backfill_receipt_provenance)
    monkeypatch.setattr(receipt_store_module, "proof_metrics", _fake_proof_metrics)

    payload = sync_module.backfill_workflow_proof(
        run_id="run-1",
        limit=5,
        conn=conn,
        repo_root="/repo",
    )

    assert payload["receipt_backfill"]["updated_receipts"] == 2
    assert payload["memory_backfill"]["actions"] == 5
    assert payload["roadmap_closeout"]["updated_items"] == ["roadmap_item.alpha"]
    assert payload["roadmap_closeout"]["already_completed_items"] == ["roadmap_item.beta"]
    assert payload["proof_metrics"]["receipts"]["total"] == 10
    assert seen["receipt_run_id"] == "run-1"
    assert seen["memory_run_id"] == "run-1"
    assert seen["repo_root"] == "/repo"


def test_closeout_workflow_run_roadmap_items_marks_packet_items_done() -> None:
    conn = _FakeConn()
    conn.workflow_runs["run-closeout"] = {
        "current_state": "succeeded",
        "started_at": datetime(2026, 4, 8, 11, 50, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 4, 8, 12, 5, tzinfo=timezone.utc),
        "request_envelope": {
            "roadmap_items": ["roadmap_item.alpha"],
            "spec_snapshot": {
                "roadmap_items": ["roadmap_item.alpha", "roadmap_item.beta", "roadmap_item.alpha"],
            },
        },
    }
    conn.roadmap_rows["roadmap_item.alpha"] = {
        "roadmap_item_id": "roadmap_item.alpha",
        "status": "active",
        "completed_at": None,
    }
    conn.roadmap_rows["roadmap_item.beta"] = {
        "roadmap_item_id": "roadmap_item.beta",
        "status": "done",
        "completed_at": datetime(2026, 4, 8, 11, 0, tzinfo=timezone.utc),
    }

    payload = sync_module.closeout_workflow_run_roadmap_items("run-closeout", conn=conn)

    assert payload == {
        "run_id": "run-closeout",
        "current_state": "succeeded",
        "requested_items": ["roadmap_item.alpha", "roadmap_item.beta"],
        "updated_items": ["roadmap_item.alpha"],
        "already_completed_items": ["roadmap_item.beta"],
        "missing_items": [],
        "reason_codes": ["already_completed"],
    }


def test_closeout_workflow_run_roadmap_items_refuses_incomplete_lifecycle() -> None:
    conn = _FakeConn()
    conn.workflow_runs["run-incomplete"] = {
        "current_state": "succeeded",
        "started_at": datetime(2026, 4, 8, 11, 50, tzinfo=timezone.utc),
        "finished_at": None,
        "request_envelope": {
            "roadmap_items": ["roadmap_item.alpha"],
        },
    }
    conn.roadmap_rows["roadmap_item.alpha"] = {
        "roadmap_item_id": "roadmap_item.alpha",
        "status": "active",
        "completed_at": None,
    }

    payload = sync_module.closeout_workflow_run_roadmap_items("run-incomplete", conn=conn)

    assert payload == {
        "run_id": "run-incomplete",
        "current_state": "succeeded",
        "requested_items": ["roadmap_item.alpha"],
        "updated_items": [],
        "already_completed_items": [],
        "missing_items": [],
        "reason_codes": ["run_lifecycle_incomplete"],
    }
