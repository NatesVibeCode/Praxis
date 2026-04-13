from __future__ import annotations

import json
from datetime import datetime, timezone

from memory.schema_projector import SchemaProjector
from memory.sync import MemorySync
from memory.types import EntityType, RelationType


class _RecordingEngine:
    def __init__(self) -> None:
        self.inserts = []
        self.edges = []

    def insert(self, entity):
        self.inserts.append(entity)
        return entity.id

    def add_edge(self, edge):
        self.edges.append(edge)
        return True


class _ReceiptSyncConn:
    def __init__(self, rows):
        self._rows = rows
        self.watermark_updates = []

    def fetchrow(self, query: str, *args):
        if "memory_sync_watermarks" in query:
            return {
                "last_synced_id": 0,
                "last_synced_at": datetime(1970, 1, 1, tzinfo=timezone.utc),
            }
        return None

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith(
            "SELECT evidence_seq, receipt_id, run_id, node_id, status, started_at, finished_at, failure_code, inputs, outputs FROM receipts"
        ):
            return list(self._rows)
        if normalized.startswith("UPDATE memory_sync_watermarks"):
            self.watermark_updates.append(args)
            return []
        return []


class _SchemaConn:
    def __init__(self, entity_type: str):
        self.entity_type = entity_type
        self.updates = []

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT entity_type FROM memory_entities WHERE id = $1 LIMIT 1"):
            return [{"entity_type": self.entity_type}]
        if normalized.startswith("UPDATE memory_entities SET entity_type = $2, metadata = COALESCE(metadata, '{}'::jsonb) || $3::jsonb, updated_at = NOW() WHERE id = $1"):
            self.updates.append(args)
            return []
        return []


def test_memory_sync_projects_verification_receipt_proof():
    ts = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    rows = [
        {
            "evidence_seq": 101,
            "receipt_id": "receipt:workflow_demo:17:1",
            "run_id": "workflow_demo",
            "node_id": "build",
            "status": "failed",
            "started_at": ts,
            "finished_at": ts,
            "failure_code": "verification.failed",
            "inputs": {
                "job_label": "build",
                "agent_slug": "openai/gpt-5.4",
                "write_scope": ["Code&DBs/Workflow/runtime/compiler.py"],
                "workspace_ref": "workspace://praxis",
                "runtime_profile_ref": "runtime://praxis",
            },
            "outputs": {
                "status": "failed",
                "token_input": 10,
                "token_output": 5,
                "cost_usd": 0.1,
                "verification_status": "failed",
                "verification": {
                    "total": 2,
                    "passed": 1,
                    "failed": 1,
                    "all_passed": False,
                    "results": [{"label": "pytest", "passed": False}],
                },
                "verification_bindings": [
                    {
                        "verification_ref": "verification.python.pytest_file",
                        "inputs": {"path": "Code&DBs/Workflow/runtime/compiler.py"},
                    }
                ],
                "mutation_provenance": {
                    "write_paths": ["Code&DBs/Workflow/runtime/compiler.py"],
                },
            },
        }
    ]
    conn = _ReceiptSyncConn(rows)
    engine = _RecordingEngine()

    actions = MemorySync(conn, engine)._sync_receipts()

    assert actions == 6
    assert len(engine.inserts) == 6
    assert engine.inserts[0].entity_type == EntityType.task
    assert engine.inserts[0].metadata["verification_status"] == "failed"
    assert engine.inserts[0].metadata["mutation_paths"] == ["Code&DBs/Workflow/runtime/compiler.py"]
    assert engine.inserts[2].entity_type == EntityType.fact
    assert engine.inserts[2].metadata["entity_subtype"] == "verification_result"
    assert engine.inserts[4].entity_type == EntityType.fact
    assert engine.inserts[4].metadata["entity_subtype"] == "failure_result"
    assert engine.inserts[5].entity_type == EntityType.code_unit
    assert engine.inserts[5].metadata["path"] == "Code&DBs/Workflow/runtime/compiler.py"
    assert [edge.relation_type for edge in engine.edges] == [
        RelationType.produced,
        RelationType.recorded_in,
        RelationType.verified_by,
        RelationType.recorded_in,
        RelationType.related_to,
    ]
    assert conn.watermark_updates[0][3] == 1


def test_schema_projector_promotes_legacy_table_entities_out_of_module():
    conn = _SchemaConn("module")
    projector = SchemaProjector(conn, engine=_RecordingEngine())

    entity_type = projector._ensure_table_entity_type("table:workflow_runs")

    assert entity_type == EntityType.table
    assert len(conn.updates) == 1
    assert conn.updates[0][1] == "table"
    assert json.loads(conn.updates[0][2])["entity_subtype"] == "schema_table"


def test_memory_sync_does_not_emit_verified_by_edges_for_skipped_verification():
    ts = datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc)
    rows = [
        {
            "evidence_seq": 102,
            "receipt_id": "receipt:workflow_demo:18:1",
            "run_id": "workflow_demo",
            "node_id": "build",
            "status": "succeeded",
            "started_at": ts,
            "finished_at": ts,
            "failure_code": "",
            "inputs": {
                "job_label": "build",
                "agent_slug": "openai/gpt-5.4",
                "write_scope": ["Code&DBs/Workflow/runtime/compiler.py"],
            },
            "outputs": {
                "status": "succeeded",
                "verification_status": "skipped",
                "verified_paths": ["Code&DBs/Workflow/runtime/compiler.py"],
            },
        }
    ]
    conn = _ReceiptSyncConn(rows)
    engine = _RecordingEngine()

    actions = MemorySync(conn, engine)._sync_receipts()

    assert actions == 3
    assert len(engine.inserts) == 3
    assert [edge.relation_type for edge in engine.edges] == [
        RelationType.produced,
        RelationType.recorded_in,
    ]
