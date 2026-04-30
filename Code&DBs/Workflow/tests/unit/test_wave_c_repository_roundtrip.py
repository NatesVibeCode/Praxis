from __future__ import annotations

import json
from datetime import datetime, timezone

from runtime.execution_packet_authority import rebuild_workflow_run_packet_inspection
from runtime.workflow.job_runtime_context import (
    load_workflow_job_runtime_context,
    persist_workflow_job_runtime_contexts,
)
from storage.postgres.compile_artifact_repository import PostgresCompileArtifactRepository


class _CompileArtifactConn:
    def __init__(self) -> None:
        self.compile_artifacts: list[dict[str, object]] = []
        self.execution_packets: list[dict[str, object]] = []

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if "INSERT INTO compile_artifacts" in normalized:
            row = {
                "compile_artifact_id": args[0],
                "artifact_kind": args[1],
                "artifact_ref": args[2],
                "revision_ref": args[3],
                "parent_artifact_ref": args[4],
                "input_fingerprint": args[5],
                "content_hash": args[6],
                "authority_refs": json.loads(args[7]),
                "payload": json.loads(args[8]),
                "decision_ref": args[9],
            }
            existing = next(
                (
                    index
                    for index, candidate in enumerate(self.compile_artifacts)
                    if candidate["artifact_kind"] == row["artifact_kind"]
                    and candidate["revision_ref"] == row["revision_ref"]
                ),
                None,
            )
            if existing is None:
                self.compile_artifacts.append(row)
            else:
                self.compile_artifacts[existing] = row
            return []
        if "FROM compile_artifacts" in normalized:
            artifact_kind = str(args[0])
            input_fingerprint = str(args[1])
            return [
                dict(row)
                for row in self.compile_artifacts
                if row["artifact_kind"] == artifact_kind
                and row["input_fingerprint"] == input_fingerprint
            ]
        if "INSERT INTO execution_packets" in normalized:
            row = {
                "execution_packet_id": args[0],
                "definition_revision": args[1],
                "plan_revision": args[2],
                "packet_revision": args[3],
                "parent_artifact_ref": args[4],
                "packet_version": args[5],
                "packet_hash": args[6],
                "workflow_id": args[7],
                "run_id": args[8],
                "spec_name": args[9],
                "source_kind": args[10],
                "authority_refs": json.loads(args[11]),
                "model_messages": json.loads(args[12]),
                "reference_bindings": json.loads(args[13]),
                "capability_bindings": json.loads(args[14]),
                "verify_refs": json.loads(args[15]),
                "authority_inputs": json.loads(args[16]),
                "file_inputs": json.loads(args[17]),
                "payload": json.loads(args[18]),
                "decision_ref": args[19],
            }
            existing = next(
                (
                    index
                    for index, candidate in enumerate(self.execution_packets)
                    if candidate["definition_revision"] == row["definition_revision"]
                    and candidate["plan_revision"] == row["plan_revision"]
                    and candidate["packet_revision"] == row["packet_revision"]
                ),
                None,
            )
            if existing is None:
                self.execution_packets.append(row)
            else:
                self.execution_packets[existing] = row
            return []
        if "FROM execution_packets" in normalized and "WHERE run_id = $1" in normalized:
            run_id = str(args[0])
            return [
                dict(row)
                for row in self.execution_packets
                if row["run_id"] == run_id
            ]
        if "FROM execution_packets" in normalized and "WHERE packet_revision = $1" in normalized:
            packet_revision = str(args[0])
            return [
                dict(row)
                for row in self.execution_packets
                if row["packet_revision"] == packet_revision
            ]
        raise AssertionError(query)


class _WorkflowJobRuntimeContextConn:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], dict[str, object]] = {}

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if "INSERT INTO workflow_job_runtime_context" not in normalized:
            raise AssertionError(query)
        self.rows[(str(args[0]), str(args[1]))] = {
            "run_id": args[0],
            "job_label": args[1],
            "workflow_id": args[2],
            "execution_context_shard": args[3],
            "execution_bundle": args[4],
            "created_at": datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
        }
        return []

    def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM workflow_jobs" in normalized:
            # load_workflow_job_authority_binding queries this table to merge
            # the per-job authority binding into the runtime context. The
            # fake has no binding to return; the loader treats None as
            # unbound and the round-trip remains stable.
            return {"authority_binding": None}
        if "FROM workflow_job_runtime_context" not in normalized:
            raise AssertionError(query)
        row = self.rows.get((str(args[0]), str(args[1])))
        return None if row is None else dict(row)


class _ExecutionPacketAuthorityConn:
    def __init__(self) -> None:
        self.run_row = {
            "run_id": "run.wave_c",
            "workflow_id": "workflow.wave_c",
            "request_id": "request.wave_c",
            "workflow_definition_id": "definition.wave_c",
            "current_state": "running",
            "request_envelope": {
                "name": "Wave C Workflow",
                "spec_snapshot": {
                    "definition_revision": "definition.wave_c",
                    "plan_revision": "plan.wave_c",
                    "verify_refs": ["verify.wave_c"],
                    "packet_provenance": {
                        "source_kind": "workflow_submit",
                        "note": "round-trip",
                    },
                },
            },
            "requested_at": datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
            "admitted_at": datetime(2026, 4, 14, 12, 1, tzinfo=timezone.utc),
            "started_at": datetime(2026, 4, 14, 12, 2, tzinfo=timezone.utc),
            "finished_at": None,
            "last_event_id": "evt.wave_c",
            "packet_inspection": None,
        }
        self.execution_packets = [
            {
                "payload": {
                    "run_id": "run.wave_c",
                    "workflow_id": "workflow.wave_c",
                    "spec_name": "Wave C Workflow",
                    "source_kind": "workflow_submit",
                    "definition_revision": "definition.wave_c",
                    "plan_revision": "plan.wave_c",
                    "packet_revision": "packet.wave_c",
                    "packet_hash": "abc123",
                    "authority_refs": ["definition.wave_c", "plan.wave_c"],
                    "verify_refs": ["verify.wave_c"],
                    "authority_inputs": {
                        "packet_provenance": {
                            "source_kind": "workflow_submit",
                            "note": "round-trip",
                        }
                    },
                    "file_inputs": {"paths": ["runtime/compile_artifacts.py"]},
                }
            }
        ]
        self.updated_packet_inspection: dict[str, object] | None = None

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM workflow_runs" in normalized:
            return [dict(self.run_row)] if str(args[0]) == "run.wave_c" else []
        if "FROM execution_packets" in normalized:
            return list(self.execution_packets) if str(args[0]) == "run.wave_c" else []
        if normalized.startswith("UPDATE workflow_runs SET packet_inspection = $2::jsonb"):
            self.updated_packet_inspection = json.loads(args[1]) if args[1] else None
            return []
        raise AssertionError(query)


def test_compile_artifact_repository_round_trip_persists_compile_and_packet_rows() -> None:
    conn = _CompileArtifactConn()
    repository = PostgresCompileArtifactRepository(conn)

    compile_artifact_id = repository.upsert_compile_artifact(
        compile_artifact_id="compile_artifact.wave_c",
        artifact_kind="definition",
        artifact_ref="definition.wave_c",
        revision_ref="definition.wave_c",
        parent_artifact_ref=None,
        input_fingerprint="fingerprint.wave_c",
        content_hash="a" * 64,
        authority_refs=("authority.wave_c",),
        payload={"definition_revision": "definition.wave_c"},
        decision_ref="decision.wave_c",
    )
    execution_packet_id = repository.upsert_execution_packet(
        execution_packet_id="execution_packet.wave_c",
        definition_revision="definition.wave_c",
        plan_revision="plan.wave_c",
        packet_revision="packet.wave_c",
        parent_artifact_ref="plan.wave_c",
        packet_version=1,
        packet_hash="b" * 64,
        workflow_id="workflow.wave_c",
        run_id="run.wave_c",
        spec_name="Wave C Workflow",
        source_kind="workflow_submit",
        authority_refs=("definition.wave_c", "plan.wave_c"),
        model_messages=({"role": "user", "content": "ship wave c"},),
        reference_bindings=({"binding": "reference.wave_c"},),
        capability_bindings=({"binding": "capability.wave_c"},),
        verify_refs=("verify.wave_c",),
        authority_inputs={"packet_provenance": {"source_kind": "workflow_submit"}},
        file_inputs={"paths": ["runtime/compile_artifacts.py"]},
        payload={"packet_revision": "packet.wave_c", "packet_version": 1},
        decision_ref="decision.packet.wave_c",
    )

    compile_rows = repository.load_compile_artifacts_for_input(
        artifact_kind="definition",
        input_fingerprint="fingerprint.wave_c",
    )
    run_rows = repository.load_execution_packets_for_run(run_id="run.wave_c")
    revision_rows = repository.load_execution_packets_for_revision(
        packet_revision="packet.wave_c"
    )

    assert compile_artifact_id == "compile_artifact.wave_c"
    assert execution_packet_id == "execution_packet.wave_c"
    assert compile_rows == [
        {
            "compile_artifact_id": "compile_artifact.wave_c",
            "artifact_kind": "definition",
            "artifact_ref": "definition.wave_c",
            "revision_ref": "definition.wave_c",
            "parent_artifact_ref": None,
            "input_fingerprint": "fingerprint.wave_c",
            "content_hash": "a" * 64,
            "authority_refs": ["authority.wave_c"],
            "payload": {"definition_revision": "definition.wave_c"},
            "decision_ref": "decision.wave_c",
        }
    ]
    assert len(run_rows) == 1
    assert run_rows[0]["run_id"] == "run.wave_c"
    assert run_rows[0]["verify_refs"] == ["verify.wave_c"]
    assert revision_rows == run_rows


def test_job_runtime_context_authority_round_trip_persists_and_loads_context() -> None:
    conn = _WorkflowJobRuntimeContextConn()

    persist_workflow_job_runtime_contexts(
        conn,
        run_id="run.wave_c",
        workflow_id="workflow.wave_c",
        execution_context_shards={
            "wave_c_tests": {
                "write_scope": ["runtime/compile_artifacts.py"],
                "verify_refs": ["verify.wave_c"],
            }
        },
        execution_bundles={
            "wave_c_tests": {
                "tool_bucket": "general",
                "job_label": "wave_c_tests",
            }
        },
    )

    row = load_workflow_job_runtime_context(
        conn,
        run_id="run.wave_c",
        job_label="wave_c_tests",
    )

    assert row is not None
    assert row["run_id"] == "run.wave_c"
    assert row["job_label"] == "wave_c_tests"
    assert row["workflow_id"] == "workflow.wave_c"
    assert row["execution_context_shard"] == {
        "write_scope": ["runtime/compile_artifacts.py"],
        "verify_refs": ["verify.wave_c"],
    }
    assert row["execution_bundle"] == {
        "tool_bucket": "general",
        "job_label": "wave_c_tests",
    }


def test_execution_packet_authority_round_trip_rebuilds_packet_inspection() -> None:
    conn = _ExecutionPacketAuthorityConn()

    inspection = rebuild_workflow_run_packet_inspection(conn, run_id="run.wave_c")

    assert inspection is not None
    assert inspection["packet_count"] == 1
    assert inspection["packet_revision"] == "packet.wave_c"
    assert inspection["drift"]["status"] == "aligned"
    assert inspection["execution"]["run_id"] == "run.wave_c"
    assert conn.updated_packet_inspection == inspection
