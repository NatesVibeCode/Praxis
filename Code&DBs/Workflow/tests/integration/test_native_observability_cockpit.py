from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

import asyncpg
import pytest

from memory.retrieval_telemetry import RetrievalMetric, TelemetryStore
from policy.workflow_lanes import bootstrap_workflow_lane_catalog_schema
from runtime.debate_metrics import DebateMetricsCollector
from runtime.instance import (
    PRAXIS_RECEIPTS_DIR_ENV,
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
    PRAXIS_TOPOLOGY_DIR_ENV,
)
from runtime.work_item_workflow_bindings import record_work_item_workflow_binding
from storage.migrations import (
    workflow_bootstrap_migration_statements,
    workflow_migration_statements,
)
from storage.postgres import (
    PostgresConfigurationError,
    PostgresEvidenceReader,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    ensure_postgres_available,
    resolve_workflow_database_url,
)
from surfaces.api import native_operator_surface, operator_read
from surfaces.api.frontdoor import status as frontdoor_status
from surfaces.cli.main import main as workflow_cli_main

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001
REPO_ROOT = Path(__file__).resolve().parents[4]


async def _fake_load_persona_activation(self, *, env, run_id, as_of):
    del self, env
    return {
        "kind": "native_operator_persona_activation",
        "authority": "test.stub",
        "selector_authority": "test.stub",
        "selector": {
            "run_id": run_id,
            "binding_scope": "native_runtime",
            "workspace_ref": "workspace.test",
            "runtime_profile_ref": "runtime_profile.test",
            "as_of": as_of.isoformat(),
        },
        "persona_profile": None,
        "persona_context_bindings": [],
    }


def test_native_operator_cockpit_command_scopes_binding_query_to_the_requested_run(
    monkeypatch,
) -> None:
    env = _operator_env()
    as_of = datetime(2026, 4, 2, 23, 15, tzinfo=timezone.utc)
    suffix = uuid.uuid4().hex[:10]
    run_id = f"run.{suffix}.cockpit"
    other_run_id = f"run.{suffix}.other"
    binding_ids = asyncio.run(
        _seed_cockpit_bindings(
            env=env,
            as_of=as_of,
            suffix=suffix,
            run_id=run_id,
            other_run_id=other_run_id,
        )
    )
    captured_query: dict[str, object] = {}

    class _FakeCockpit:
        def to_json(self) -> dict[str, object]:
            return {"kind": "operator_cockpit", "status_state": "ready"}

    async def _fake_load_surface(
        self,
        *,
        env,
        instance,
        run_id,
        as_of,
        persona_payload,
        query_payload,
        authoritative_work_bindings,
    ):
        captured_query["query_payload"] = query_payload
        captured_query["authoritative_work_bindings"] = authoritative_work_bindings
        return native_operator_surface.NativeOperatorSurfaceReadModel(
            provenance=native_operator_surface.NativeOperatorSurfaceProvenance(as_of=as_of),
            native_instance=instance,
            run_id=run_id,
            as_of=as_of,
            instruction_authority={
                "kind": "native_operator_instruction_authority",
                "authority": "test.native_operator_surface",
            },
            persona=persona_payload,
            status={
                "kind": "native_operator_status_truth",
                "authority": "test.native_operator_surface",
                "run": {"run_id": run_id},
                "inspection": None,
            },
            receipts={
                "kind": "native_operator_receipt_truth",
                "authority": "test.native_operator_surface",
                "run_id": run_id,
                "evidence_row_count": 0,
                "receipt_count": 0,
                "latest_evidence_seq": None,
                "latest_receipt_id": None,
                "latest_receipt_type": None,
                "terminal_status": None,
                "status_counts": {},
                "receipts": [],
            },
            query=query_payload,
            cockpit=_FakeCockpit(),
        )

    def _fake_query_operator_surface(
        *,
        env=None,
        as_of=None,
        bug_ids=None,
        roadmap_item_ids=None,
        cutover_gate_ids=None,
        work_item_workflow_binding_ids=None,
        workflow_run_ids=None,
    ) -> dict[str, object]:
        return {
            "kind": "operator_query",
            "as_of": as_of.isoformat() if as_of else None,
            "query": {
                "bug_ids": list(bug_ids) if bug_ids else None,
                "roadmap_item_ids": list(roadmap_item_ids) if roadmap_item_ids else None,
                "cutover_gate_ids": list(cutover_gate_ids) if cutover_gate_ids else None,
                "work_item_workflow_binding_ids": list(work_item_workflow_binding_ids) if work_item_workflow_binding_ids else None,
                "workflow_run_ids": list(workflow_run_ids) if workflow_run_ids else None,
            },
            "counts": {
                "bugs": 0,
                "roadmap_items": 0,
                "cutover_gates": 0,
                "work_item_workflow_bindings": 1,
            },
            "bugs": [],
            "roadmap_items": [],
            "cutover_gates": [],
            "work_item_workflow_bindings": [
                {
                    "work_item_workflow_binding_id": binding_ids["governing_binding_id"],
                    "binding_kind": "governed_by",
                    "binding_status": "active",
                    "source": {
                        "kind": "bug",
                        "id": binding_ids["governing_bug_id"],
                        "bug_id": binding_ids["governing_bug_id"],
                    },
                    "targets": {
                        "workflow_class_id": binding_ids["governing_workflow_class_id"],
                        "workflow_run_id": run_id,
                    },
                    "bound_by_decision_id": binding_ids["governing_decision_id"],
                    "created_at": as_of.isoformat(),
                    "updated_at": as_of.isoformat(),
                }
            ],
        }

    monkeypatch.setattr(native_operator_surface, "_now", lambda: as_of)
    monkeypatch.setattr(native_operator_surface, "query_operator_surface", _fake_query_operator_surface)
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_surface",
        _fake_load_surface,
    )
    monkeypatch.setattr(
        native_operator_surface.NativeOperatorSurfaceFrontdoor,
        "_load_persona_activation",
        _fake_load_persona_activation,
    )

    stdout = StringIO()
    assert workflow_cli_main(["native-operator", "cockpit", run_id], env=env, stdout=stdout) == 0

    payload = json.loads(stdout.getvalue())
    assert payload["kind"] == "native_operator_surface"
    assert payload["run_id"] == run_id
    assert payload["cockpit"] == {"kind": "operator_cockpit", "status_state": "ready"}
    assert payload["query"]["query"] == {
        "bug_ids": None,
        "roadmap_item_ids": None,
        "cutover_gate_ids": None,
        "work_item_workflow_binding_ids": [binding_ids["governing_binding_id"]],
        "workflow_run_ids": [run_id],
    }
    assert payload["query"]["counts"]["work_item_workflow_bindings"] == 1
    assert payload["query"]["work_item_workflow_bindings"] == [
        {
            "work_item_workflow_binding_id": binding_ids["governing_binding_id"],
            "binding_kind": "governed_by",
            "binding_status": "active",
            "source": {
                "kind": "bug",
                "id": binding_ids["governing_bug_id"],
                "bug_id": binding_ids["governing_bug_id"],
            },
            "targets": {
                "workflow_class_id": binding_ids["governing_workflow_class_id"],
                "workflow_run_id": run_id,
            },
            "bound_by_decision_id": binding_ids["governing_decision_id"],
            "created_at": as_of.isoformat(),
            "updated_at": as_of.isoformat(),
        }
    ]
    assert binding_ids["other_binding_id"] not in json.dumps(payload["query"])
    assert captured_query["query_payload"] == payload["query"]
    authoritative_work_bindings = captured_query["authoritative_work_bindings"]
    assert len(authoritative_work_bindings) == 1
    assert authoritative_work_bindings[0].work_item_workflow_binding_id == binding_ids["governing_binding_id"]
    assert authoritative_work_bindings[0].workflow_run_id == run_id


def test_native_observability_feedback_round_trips_into_activity_truth_without_definition_mutation() -> None:
    env = _operator_env()
    as_of = datetime(2026, 4, 3, 3, 0, tzinfo=timezone.utc)
    suffix = uuid.uuid4().hex[:10]
    baseline = asyncio.run(
        _seed_activity_truth_loop_baseline(
            env=env,
            as_of=as_of,
            suffix=suffix,
        )
    )

    sync_conn = ensure_postgres_available(env=env)
    debate_id = f"debate.activity-truth.{suffix}"
    debate_metrics = DebateMetricsCollector(conn=sync_conn)
    debate_metrics.record_round(
        debate_id,
        "skeptic",
        (
            'This run proves drift only if "workflow_events" and receipts agree on the same '
            "admitted definition and evidence sequence."
        ),
        1.25,
        round_number=1,
        persona_position=0,
    )
    debate_metrics.record_round(
        debate_id,
        "verifier",
        (
            "Therefore we must bind run, receipt, and durable telemetry through one bug-backed "
            "activity truth loop instead of mutating the definition in place."
        ),
        1.4,
        round_number=1,
        persona_position=1,
    )
    debate_metrics.record_synthesis(
        debate_id,
        consensus_points=[
            "Bind durable observability evidence through canonical bug evidence links.",
            "Keep admitted workflow definition rows immutable until an explicit decision lands.",
        ],
        disagreements=[
            "Whether retrieval telemetry should gain a first-class workflow_run_id column.",
        ],
        synthesis_text=(
            "Evidence-backed feedback should attach to the run via bugs, roadmap items, and "
            "bindings, while the admitted definition remains unchanged."
        ),
    )
    debate_run_id = debate_metrics._debate_run_id

    retrieval_metric = RetrievalMetric(
        query_fingerprint=suffix[:8],
        pattern_name="activity_truth_loop",
        result_count=2,
        score_min=0.71,
        score_max=0.93,
        score_mean=0.82,
        score_stddev=0.11,
        tie_break_count=0,
        latency_ms=18.5,
        timestamp=as_of,
    )
    TelemetryStore(sync_conn).record(retrieval_metric)

    proof = asyncio.run(
        _complete_activity_truth_loop_binding(
            env=env,
            as_of=as_of,
            suffix=suffix,
            baseline=baseline,
            debate_run_id=debate_run_id,
            retrieval_metric=retrieval_metric,
        )
    )

    status_payload = frontdoor_status(run_id=proof["run_id"], env=env)
    timeline = PostgresEvidenceReader(env=env).evidence_timeline(proof["run_id"])
    payload = operator_read.query_operator_surface(
        env=env,
        as_of=as_of,
        bug_ids=[proof["bug_id"]],
        roadmap_item_ids=[proof["roadmap_item_id"]],
        work_item_workflow_binding_ids=[proof["binding_id"]],
        workflow_run_ids=[proof["run_id"]],
    )

    assert proof["definition_snapshot_before"] == proof["definition_snapshot_after"]
    assert status_payload["run"]["run_id"] == proof["run_id"]
    assert status_payload["run"]["workflow_definition_id"] == proof["workflow_definition_id"]
    assert status_payload["run"]["current_state"] == "succeeded"
    assert status_payload["run"]["last_event_id"] == proof["event_id"]
    assert status_payload["inspection"]["watermark"]["evidence_seq"] == 2
    assert [row.kind for row in timeline] == ["workflow_event", "receipt"]
    assert [row.evidence_seq for row in timeline] == [1, 2]
    assert timeline[0].record.event_id == proof["event_id"]
    assert timeline[0].record.payload["workflow_definition_id"] == proof["workflow_definition_id"]
    assert timeline[1].record.receipt_id == proof["receipt_id"]
    assert timeline[1].record.inputs["workflow_definition_id"] == proof["workflow_definition_id"]
    assert timeline[1].record.outputs == {
        "result": "activity_truth_captured",
        "definition_hash": proof["definition_hash"],
    }

    assert payload["query"] == {
        "bug_ids": [proof["bug_id"]],
        "roadmap_item_ids": [proof["roadmap_item_id"]],
        "cutover_gate_ids": None,
        "work_item_workflow_binding_ids": [proof["binding_id"]],
        "workflow_run_ids": [proof["run_id"]],
    }
    assert payload["counts"]["bugs"] == 1
    assert payload["counts"]["roadmap_items"] == 1
    assert payload["counts"]["work_item_workflow_bindings"] == 1
    instruction_authority = payload["instruction_authority"]
    assert instruction_authority["kind"] == "operator_query_instruction_authority"
    assert (
        instruction_authority["authority"]
        == "surfaces.api.operator_read.query_operator_surface"
    )
    assert "roadmap_truth" in instruction_authority["packet_read_order"]
    assert "queue_refs" in instruction_authority["packet_read_order"]
    assert "bugs" in instruction_authority["packet_read_order"]
    assert "work_item_workflow_bindings" in instruction_authority["packet_read_order"]
    assert instruction_authority["roadmap_truth"]["authority"] == "roadmap_items"
    assert instruction_authority["roadmap_truth"]["roadmap_item_ids"] == [
        proof["roadmap_item_id"]
    ]
    assert instruction_authority["roadmap_truth"]["items"][0]["roadmap_item_id"] == proof[
        "roadmap_item_id"
    ]
    assert instruction_authority["roadmap_truth"]["items"][0]["roadmap_key"].startswith(
        f"roadmap.{suffix}.activity-truth"
    )
    assert instruction_authority["roadmap_truth"]["items"][0]["title"] == (
        "Review activity truth evidence drift"
    )
    assert instruction_authority["roadmap_truth"]["items"][0]["decision_ref"] == proof[
        "decision_id"
    ]
    assert instruction_authority["queue_refs"]["workflow_run_ids"] == [proof["run_id"]]
    assert instruction_authority["queue_refs"]["work_item_workflow_binding_ids"] == [
        proof["binding_id"]
    ]
    assert instruction_authority["directive"].startswith("Read roadmap-backed rows")

    bug = payload["bugs"][0]
    assert bug["bug_id"] == proof["bug_id"]
    assert bug["source_kind"] == "activity_truth_loop"
    assert bug["discovered_in_run_id"] == proof["run_id"]
    assert bug["discovered_in_receipt_id"] == proof["receipt_id"]
    assert bug["decision_ref"] == proof["decision_id"]

    roadmap_item = payload["roadmap_items"][0]
    assert roadmap_item["roadmap_item_id"] == proof["roadmap_item_id"]
    assert roadmap_item["source_bug_id"] == proof["bug_id"]
    assert roadmap_item["acceptance_criteria"] == {
        "must_have": [
            "Bind observability evidence back to the run without mutating canonical definitions."
        ],
    }

    binding = payload["work_item_workflow_bindings"][0]
    assert binding["work_item_workflow_binding_id"] == proof["binding_id"]
    assert binding["source"] == {
        "kind": "roadmap_item",
        "id": proof["roadmap_item_id"],
        "roadmap_item_id": proof["roadmap_item_id"],
    }
    assert binding["targets"] == {"workflow_run_id": proof["run_id"]}
    assert binding["bound_by_decision_id"] == proof["decision_id"]

    assert proof["bug_evidence_links"] == [
        {
            "evidence_kind": "debate_consensus",
            "evidence_ref": debate_run_id,
            "evidence_role": "observed_in",
        },
        {
            "evidence_kind": "retrieval_metric",
            "evidence_ref": f"{retrieval_metric.pattern_name}:{retrieval_metric.query_fingerprint}",
            "evidence_role": "observed_in",
        },
        {
            "evidence_kind": "workflow_event",
            "evidence_ref": proof["event_id"],
            "evidence_role": "observed_in",
        },
        {
            "evidence_kind": "workflow_receipt",
            "evidence_ref": proof["receipt_id"],
            "evidence_role": "observed_in",
        },
        {
            "evidence_kind": "workflow_run",
            "evidence_ref": proof["run_id"],
            "evidence_role": "observed_in",
        },
    ]
    assert proof["debate_round_rows"] == [
        {
            "round_number": 1,
            "persona_position": 0,
            "persona": "skeptic",
            "debate_id": debate_id,
        },
        {
            "round_number": 1,
            "persona_position": 1,
            "persona": "verifier",
            "debate_id": debate_id,
        },
    ]
    debate_consensus_row = dict(proof["debate_consensus_row"])
    consensus_points = debate_consensus_row["consensus_points"]
    disagreements = debate_consensus_row["disagreements"]
    if isinstance(consensus_points, str):
        consensus_points = json.loads(consensus_points)
    if isinstance(disagreements, str):
        disagreements = json.loads(disagreements)
    assert debate_consensus_row["debate_run_id"] == debate_run_id
    assert debate_consensus_row["debate_id"] == debate_id
    assert debate_consensus_row["total_rounds"] == 2
    assert consensus_points == [
        "Bind durable observability evidence through canonical bug evidence links.",
        "Keep admitted workflow definition rows immutable until an explicit decision lands.",
    ]
    assert disagreements == [
        "Whether retrieval telemetry should gain a first-class workflow_run_id column.",
    ]
    assert proof["retrieval_metric_row"] == {
        "query_fingerprint": retrieval_metric.query_fingerprint,
        "pattern_name": retrieval_metric.pattern_name,
        "result_count": retrieval_metric.result_count,
        "latency_ms": retrieval_metric.latency_ms,
    }


async def _seed_cockpit_bindings(
    *,
    env: dict[str, str],
    as_of: datetime,
    suffix: str,
    run_id: str,
    other_run_id: str,
) -> dict[str, str]:
    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_control_plane_schema(conn)
        await bootstrap_workflow_lane_catalog_schema(conn)
        await _bootstrap_workflow_migration(conn, "008_workflow_class_and_schedule_schema.sql")
        await _bootstrap_workflow_migration(conn, "009_bug_and_roadmap_authority.sql")
        await _bootstrap_workflow_migration(conn, "082_event_log.sql")
        await _bootstrap_workflow_migration(conn, "010_operator_control_authority.sql")
        await _bootstrap_workflow_migration(conn, "132_issue_backlog_authority.sql")
        await _bootstrap_workflow_migration(conn, "146_semantic_assertion_substrate.sql")

        governing_decision_id = await _seed_operator_decision(conn, as_of=as_of, suffix=f"{suffix}.governing")
        other_decision_id = await _seed_operator_decision(conn, as_of=as_of, suffix=f"{suffix}.other")
        governing_bug_id = await _seed_bug(conn, as_of=as_of, suffix=f"{suffix}.governing")
        other_bug_id = await _seed_bug(conn, as_of=as_of, suffix=f"{suffix}.other")
        governing_workflow_class_id = await _seed_workflow_class(
            conn,
            as_of=as_of,
            suffix=f"{suffix}.governing",
        )
        other_workflow_class_id = await _seed_workflow_class(
            conn,
            as_of=as_of,
            suffix=f"{suffix}.other",
        )
        await _seed_workflow_run(conn, run_id=run_id, suffix=f"{suffix}.governing", as_of=as_of)
        await _seed_workflow_run(conn, run_id=other_run_id, suffix=f"{suffix}.other", as_of=as_of)

        governing_binding = await record_work_item_workflow_binding(
            conn,
            binding_kind="governed_by",
            bug_id=governing_bug_id,
            workflow_class_id=governing_workflow_class_id,
            workflow_run_id=run_id,
            binding_status="active",
            bound_by_decision_id=governing_decision_id,
            created_at=as_of,
            updated_at=as_of,
        )
        other_binding = await record_work_item_workflow_binding(
            conn,
            binding_kind="governed_by",
            bug_id=other_bug_id,
            workflow_class_id=other_workflow_class_id,
            workflow_run_id=other_run_id,
            binding_status="active",
            bound_by_decision_id=other_decision_id,
            created_at=as_of,
            updated_at=as_of,
        )
        return {
            "governing_binding_id": governing_binding.work_item_workflow_binding_id,
            "governing_bug_id": governing_bug_id,
            "governing_workflow_class_id": governing_workflow_class_id,
            "governing_decision_id": governing_decision_id,
            "other_binding_id": other_binding.work_item_workflow_binding_id,
        }
    finally:
        await conn.close()


async def _seed_activity_truth_loop_baseline(
    *,
    env: dict[str, str],
    as_of: datetime,
    suffix: str,
) -> dict[str, object]:
    run_id = f"run.{suffix}.activity-truth"
    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_control_plane_schema(conn)
        await bootstrap_workflow_lane_catalog_schema(conn)
        await _bootstrap_workflow_migration(conn, "008_workflow_class_and_schedule_schema.sql")
        await _bootstrap_workflow_migration(conn, "009_bug_and_roadmap_authority.sql")
        await _bootstrap_workflow_migration(conn, "082_event_log.sql")
        await _bootstrap_workflow_migration(conn, "010_operator_control_authority.sql")
        await _bootstrap_workflow_migration(conn, "132_issue_backlog_authority.sql")
        await _bootstrap_workflow_migration(conn, "146_semantic_assertion_substrate.sql")
        await _bootstrap_workflow_migration(conn, "040_debate_metrics.sql")

        run_seed = await _seed_workflow_run(conn, run_id=run_id, suffix=suffix, as_of=as_of)
        await _seed_workflow_definition_contract_rows(
            conn,
            workflow_definition_id=run_seed["workflow_definition_id"],
            suffix=suffix,
            as_of=as_of,
        )
        event_id = await _seed_workflow_event(
            conn,
            workflow_id=run_seed["workflow_id"],
            request_id=run_seed["request_id"],
            run_id=run_id,
            workflow_definition_id=run_seed["workflow_definition_id"],
            definition_hash=run_seed["definition_hash"],
            suffix=suffix,
            as_of=as_of,
        )
        receipt_id = await _seed_workflow_receipt(
            conn,
            workflow_id=run_seed["workflow_id"],
            request_id=run_seed["request_id"],
            run_id=run_id,
            workflow_definition_id=run_seed["workflow_definition_id"],
            definition_hash=run_seed["definition_hash"],
            suffix=suffix,
            as_of=as_of,
            causation_id=event_id,
        )
        await conn.execute(
            """
            UPDATE workflow_runs
            SET current_state = 'succeeded',
                started_at = $2,
                finished_at = $3,
                last_event_id = $4
            WHERE run_id = $1
            """,
            run_id,
            as_of,
            as_of + timedelta(minutes=1),
            event_id,
        )
        return {
            "run_id": run_id,
            "workflow_id": run_seed["workflow_id"],
            "request_id": run_seed["request_id"],
            "workflow_definition_id": run_seed["workflow_definition_id"],
            "definition_hash": run_seed["definition_hash"],
            "event_id": event_id,
            "receipt_id": receipt_id,
            "definition_snapshot_before": await _snapshot_workflow_definition(
                conn,
                workflow_definition_id=run_seed["workflow_definition_id"],
            ),
        }
    finally:
        await conn.close()


async def _complete_activity_truth_loop_binding(
    *,
    env: dict[str, str],
    as_of: datetime,
    suffix: str,
    baseline: dict[str, object],
    debate_run_id: str,
    retrieval_metric: RetrievalMetric,
) -> dict[str, object]:
    conn = await connect_workflow_database(env=env)
    try:
        decision_id = await _seed_operator_decision(conn, as_of=as_of, suffix=f"{suffix}.activity-truth")
        bug_id = await _seed_bug(
            conn,
            as_of=as_of,
            suffix=f"{suffix}.activity-truth",
            title="Activity truth evidence drift requires explicit review",
            summary=(
                "Workflow run evidence and durable observability artifacts require a bound review "
                "path instead of in-place definition mutation."
            ),
            source_kind="activity_truth_loop",
            decision_ref=decision_id,
            discovered_in_run_id=str(baseline["run_id"]),
            discovered_in_receipt_id=str(baseline["receipt_id"]),
        )
        roadmap_item_id = await _seed_roadmap_item(
            conn,
            as_of=as_of,
            suffix=f"{suffix}.activity-truth",
            source_bug_id=bug_id,
            decision_ref=decision_id,
        )
        binding = await record_work_item_workflow_binding(
            conn,
            binding_kind="observed_feedback",
            roadmap_item_id=roadmap_item_id,
            workflow_run_id=str(baseline["run_id"]),
            binding_status="active",
            bound_by_decision_id=decision_id,
            created_at=as_of,
            updated_at=as_of,
        )
        await _seed_bug_evidence_links(
            conn,
            bug_id=bug_id,
            as_of=as_of,
            suffix=suffix,
            evidence_links=(
                ("workflow_run", str(baseline["run_id"]), "observed_in"),
                ("workflow_receipt", str(baseline["receipt_id"]), "observed_in"),
                ("workflow_event", str(baseline["event_id"]), "observed_in"),
                ("debate_consensus", debate_run_id, "observed_in"),
                (
                    "retrieval_metric",
                    f"{retrieval_metric.pattern_name}:{retrieval_metric.query_fingerprint}",
                    "observed_in",
                ),
            ),
        )

        debate_round_rows = await conn.fetch(
            """
            SELECT round_number, persona_position, persona, debate_id
            FROM debate_round_metrics
            WHERE debate_run_id = $1
            ORDER BY round_number, persona_position
            """,
            debate_run_id,
        )
        consensus_row = await conn.fetchrow(
            """
            SELECT debate_run_id, debate_id, total_rounds, consensus_points, disagreements
            FROM debate_consensus
            WHERE debate_run_id = $1
            """,
            debate_run_id,
        )
        retrieval_row = await conn.fetchrow(
            """
            SELECT query_fingerprint, pattern_name, result_count, latency_ms
            FROM retrieval_metrics
            WHERE query_fingerprint = $1
              AND pattern_name = $2
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            retrieval_metric.query_fingerprint,
            retrieval_metric.pattern_name,
        )
        bug_evidence_rows = await conn.fetch(
            """
            SELECT evidence_kind, evidence_ref, evidence_role
            FROM bug_evidence_links
            WHERE bug_id = $1
            ORDER BY evidence_kind, evidence_ref
            """,
            bug_id,
        )
        return {
            "run_id": baseline["run_id"],
            "workflow_definition_id": baseline["workflow_definition_id"],
            "definition_hash": baseline["definition_hash"],
            "event_id": baseline["event_id"],
            "receipt_id": baseline["receipt_id"],
            "bug_id": bug_id,
            "roadmap_item_id": roadmap_item_id,
            "binding_id": binding.work_item_workflow_binding_id,
            "decision_id": decision_id,
            "definition_snapshot_before": baseline["definition_snapshot_before"],
            "definition_snapshot_after": await _snapshot_workflow_definition(
                conn,
                workflow_definition_id=str(baseline["workflow_definition_id"]),
            ),
            "bug_evidence_links": [dict(row) for row in bug_evidence_rows],
            "debate_round_rows": [dict(row) for row in debate_round_rows],
            "debate_consensus_row": dict(consensus_row) if consensus_row is not None else None,
            "retrieval_metric_row": dict(retrieval_row) if retrieval_row is not None else None,
        }
    finally:
        await conn.close()


async def _bootstrap_workflow_migration(conn, filename: str) -> None:
    statements = (
        workflow_bootstrap_migration_statements(filename)
        if filename in {"082_event_log.sql", "040_debate_metrics.sql"}
        else workflow_migration_statements(filename)
    )
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in statements:
            if statement.strip().upper() in {"BEGIN", "COMMIT"}:
                continue
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if getattr(exc, "sqlstate", None) in {"42P07", "42710"}:
                    continue
                raise


async def _seed_operator_decision(conn, *, as_of: datetime, suffix: str) -> str:
    decision_id = f"operator_decision.{suffix}.cockpit"
    await conn.execute(
        """
        INSERT INTO operator_decisions (
            operator_decision_id,
            decision_key,
            decision_kind,
            decision_status,
            title,
            rationale,
            decided_by,
            decision_source,
            effective_from,
            effective_to,
            decided_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10, $11, $12
        )
        ON CONFLICT (operator_decision_id) DO UPDATE SET
            decision_key = EXCLUDED.decision_key,
            decision_kind = EXCLUDED.decision_kind,
            decision_status = EXCLUDED.decision_status,
            title = EXCLUDED.title,
            rationale = EXCLUDED.rationale,
            decided_by = EXCLUDED.decided_by,
            decision_source = EXCLUDED.decision_source,
            effective_from = EXCLUDED.effective_from,
            decided_at = EXCLUDED.decided_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        decision_id,
        f"decision:{suffix}:cockpit",
        "binding",
        "recorded",
        "Cockpit binding decision",
        "Authorizes one run-scoped cockpit binding",
        "operator",
        "manual",
        as_of,
        as_of,
        as_of,
        as_of,
    )
    return decision_id


async def _seed_bug(
    conn,
    *,
    as_of: datetime,
    suffix: str,
    title: str = "Cockpit truth binding bug",
    summary: str = "Ensures cockpit authority is run-scoped",
    source_kind: str = "manual",
    decision_ref: str | None = None,
    discovered_in_run_id: str | None = None,
    discovered_in_receipt_id: str | None = None,
) -> str:
    bug_id = f"bug.{suffix}.cockpit"
    await conn.execute(
        """
        INSERT INTO bugs (
            bug_id,
            bug_key,
            title,
            status,
            severity,
            priority,
            summary,
            source_kind,
            discovered_in_run_id,
            discovered_in_receipt_id,
            decision_ref,
            opened_at,
            resolved_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL, $13, $14
        )
        ON CONFLICT (bug_id) DO UPDATE SET
            bug_key = EXCLUDED.bug_key,
            title = EXCLUDED.title,
            status = EXCLUDED.status,
            severity = EXCLUDED.severity,
            priority = EXCLUDED.priority,
            summary = EXCLUDED.summary,
            source_kind = EXCLUDED.source_kind,
            discovered_in_run_id = EXCLUDED.discovered_in_run_id,
            discovered_in_receipt_id = EXCLUDED.discovered_in_receipt_id,
            decision_ref = EXCLUDED.decision_ref,
            opened_at = EXCLUDED.opened_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        bug_id,
        f"bug-key.{suffix}.cockpit",
        title,
        "open",
        "medium",
        "p2",
        summary,
        source_kind,
        discovered_in_run_id,
        discovered_in_receipt_id,
        f"decision:{suffix}:cockpit" if decision_ref is None else decision_ref,
        as_of,
        as_of,
        as_of,
    )
    return bug_id


async def _seed_roadmap_item(
    conn,
    *,
    as_of: datetime,
    suffix: str,
    source_bug_id: str,
    decision_ref: str,
) -> str:
    roadmap_item_id = f"roadmap_item.{suffix}.cockpit"
    await conn.execute(
        """
        INSERT INTO roadmap_items (
            roadmap_item_id,
            roadmap_key,
            title,
            item_kind,
            status,
            priority,
            parent_roadmap_item_id,
            source_bug_id,
            summary,
            acceptance_criteria,
            decision_ref,
            target_start_at,
            target_end_at,
            completed_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, NULL, $7, $8, $9::jsonb, $10, NULL, NULL, NULL, $11, $12
        )
        ON CONFLICT (roadmap_item_id) DO UPDATE SET
            roadmap_key = EXCLUDED.roadmap_key,
            title = EXCLUDED.title,
            item_kind = EXCLUDED.item_kind,
            status = EXCLUDED.status,
            priority = EXCLUDED.priority,
            source_bug_id = EXCLUDED.source_bug_id,
            summary = EXCLUDED.summary,
            acceptance_criteria = EXCLUDED.acceptance_criteria,
            decision_ref = EXCLUDED.decision_ref,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        roadmap_item_id,
        f"roadmap.{suffix}.activity-truth",
        "Review activity truth evidence drift",
        "capability",
        "proposed",
        "p1",
        source_bug_id,
        "Turn activity truth drift into an explicit review path with durable evidence.",
        json.dumps(
            {
                "must_have": [
                    "Bind observability evidence back to the run without mutating canonical definitions."
                ],
            }
        ),
        decision_ref,
        as_of,
        as_of,
    )
    return roadmap_item_id


async def _seed_workflow_class(conn, *, as_of: datetime, suffix: str) -> str:
    workflow_lane_id = f"workflow_lane.{suffix}.cockpit"
    workflow_class_id = f"workflow_class.{suffix}.cockpit"
    await conn.execute(
        """
        INSERT INTO workflow_lanes (
            workflow_lane_id,
            lane_name,
            lane_kind,
            status,
            concurrency_cap,
            default_route_kind,
            review_required,
            retry_policy,
            effective_from,
            effective_to,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, NULL, $10
        )
        ON CONFLICT (workflow_lane_id) DO UPDATE SET
            lane_name = EXCLUDED.lane_name,
            lane_kind = EXCLUDED.lane_kind,
            status = EXCLUDED.status,
            concurrency_cap = EXCLUDED.concurrency_cap,
            default_route_kind = EXCLUDED.default_route_kind,
            review_required = EXCLUDED.review_required,
            retry_policy = EXCLUDED.retry_policy,
            effective_from = EXCLUDED.effective_from,
            created_at = EXCLUDED.created_at
        """,
        workflow_lane_id,
        f"lane-{suffix}",
        "review",
        "active",
        1,
        "manual",
        False,
        '{"max_attempts": 1}',
        as_of,
        as_of,
    )
    await conn.execute(
        """
        INSERT INTO workflow_classes (
            workflow_class_id,
            class_name,
            class_kind,
            workflow_lane_id,
            status,
            queue_shape,
            throttle_policy,
            review_required,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, NULL, $10, $11
        )
        ON CONFLICT (workflow_class_id) DO UPDATE SET
            class_name = EXCLUDED.class_name,
            class_kind = EXCLUDED.class_kind,
            workflow_lane_id = EXCLUDED.workflow_lane_id,
            status = EXCLUDED.status,
            queue_shape = EXCLUDED.queue_shape,
            throttle_policy = EXCLUDED.throttle_policy,
            review_required = EXCLUDED.review_required,
            effective_from = EXCLUDED.effective_from,
            decision_ref = EXCLUDED.decision_ref,
            created_at = EXCLUDED.created_at
        """,
        workflow_class_id,
        f"class-{suffix}",
        "review",
        workflow_lane_id,
        "active",
        '{"shape":"single-run"}',
        '{"max_attempts":1}',
        False,
        as_of,
        f"decision:{suffix}:workflow-class",
        as_of,
    )
    return workflow_class_id


async def _seed_workflow_run(conn, *, run_id: str, suffix: str, as_of: datetime) -> dict[str, str]:
    """Seed the minimum FK chain: workflow_definition → admission_decision → workflow_run."""
    defn_id = f"workflow_definition.{suffix}.cockpit.v1"
    admission_id = f"admission.{suffix}.cockpit"
    workflow_id = f"workflow.{suffix}.cockpit"
    request_id = f"request.{suffix}.cockpit"
    definition_hash = f"sha256:defn:{suffix}"

    await conn.execute(
        """
        INSERT INTO workflow_definitions (
            workflow_definition_id, workflow_id, schema_version, definition_version,
            definition_hash, status, request_envelope, normalized_definition, created_at
        ) VALUES ($1, $2, 1, 1, $3, 'active', $4::jsonb, $5::jsonb, $6)
        ON CONFLICT (workflow_definition_id) DO NOTHING
        """,
        defn_id, workflow_id, definition_hash,
        '{"kind":"test","workspace_ref":"workspace.test","runtime_profile_ref":"runtime_profile.test"}',
        '{"nodes":[],"edges":[]}', as_of,
    )
    await conn.execute(
        """
        INSERT INTO admission_decisions (
            admission_decision_id, workflow_id, request_id, decision, reason_code,
            decided_at, decided_by, policy_snapshot_ref, validation_result_ref,
            authority_context_ref
        ) VALUES ($1, $2, $3, 'admit', 'test.cockpit_seed', $4, 'test', 'snap:test', 'val:test', 'auth:test')
        ON CONFLICT (admission_decision_id) DO NOTHING
        """,
        admission_id, workflow_id, request_id, as_of,
    )
    await conn.execute(
        """
        INSERT INTO workflow_runs (
            run_id, workflow_id, request_id, request_digest, authority_context_digest,
            workflow_definition_id, admitted_definition_hash, run_idempotency_key,
            schema_version, request_envelope, context_bundle_id, admission_decision_id,
            current_state, terminal_reason_code, requested_at, admitted_at, started_at,
            finished_at, last_event_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 1, $9::jsonb, $10, $11,
            'claim_accepted', NULL, $12, $13, NULL, NULL, NULL
        )
        ON CONFLICT (run_id) DO NOTHING
        """,
        run_id, workflow_id, request_id, f"sha256:req:{suffix}",
        f"sha256:auth:{suffix}", defn_id, definition_hash,
        request_id,
        '{"kind":"test","workspace_ref":"workspace.test","runtime_profile_ref":"runtime_profile.test"}',
        f"context_bundle.{suffix}",
        admission_id, as_of, as_of,
    )
    return {
        "workflow_definition_id": defn_id,
        "workflow_id": workflow_id,
        "request_id": request_id,
        "admission_decision_id": admission_id,
        "definition_hash": definition_hash,
    }


async def _seed_workflow_definition_contract_rows(
    conn,
    *,
    workflow_definition_id: str,
    suffix: str,
    as_of: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO workflow_definition_nodes (
            workflow_definition_node_id,
            workflow_definition_id,
            node_id,
            node_type,
            schema_version,
            adapter_type,
            display_name,
            inputs,
            expected_outputs,
            success_condition,
            failure_behavior,
            authority_requirements,
            execution_boundary,
            position_index
        ) VALUES
            ($1, $2, 'node.observe', 'observer', 1, 'native', 'Observe activity truth',
             $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, 0),
            ($9, $2, 'node.review', 'review', 1, 'native', 'Review evidence linkage',
             $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, 1)
        ON CONFLICT (workflow_definition_node_id) DO NOTHING
        """,
        f"workflow_definition_node.{suffix}.observe",
        workflow_definition_id,
        json.dumps({"requires": ["run", "receipts", "events"]}),
        json.dumps({"evidence": ["workflow_event"]}),
        json.dumps({"must_emit": ["evidence_seq"]}),
        json.dumps({"on_failure": "halt"}),
        json.dumps({"authority": "workflow_definition"}),
        json.dumps({"workspace": "runtime"}),
        f"workflow_definition_node.{suffix}.review",
        json.dumps({"requires": ["bug_evidence_links"]}),
        json.dumps({"evidence": ["roadmap_item"]}),
        json.dumps({"must_emit": ["review_path"]}),
        json.dumps({"on_failure": "escalate"}),
        json.dumps({"authority": "operator"}),
        json.dumps({"workspace": "control_plane"}),
    )
    await conn.execute(
        """
        INSERT INTO workflow_definition_edges (
            workflow_definition_edge_id,
            workflow_definition_id,
            edge_id,
            edge_type,
            schema_version,
            from_node_id,
            to_node_id,
            release_condition,
            payload_mapping,
            position_index
        ) VALUES (
            $1, $2, $3, 'transition', 1, 'node.observe', 'node.review', $4::jsonb, $5::jsonb, 0
        )
        ON CONFLICT (workflow_definition_edge_id) DO NOTHING
        """,
        f"workflow_definition_edge.{suffix}.observe.review",
        workflow_definition_id,
        f"edge.{suffix}.observe.review",
        json.dumps({"when": "evidence_captured"}),
        json.dumps({"run_id": "$.run_id"}),
    )
    await conn.execute(
        """
        UPDATE workflow_definitions
        SET normalized_definition = $2::jsonb,
            request_envelope = $3::jsonb,
            created_at = $4
        WHERE workflow_definition_id = $1
        """,
        workflow_definition_id,
        json.dumps(
            {
                "nodes": ["node.observe", "node.review"],
                "edges": ["edge.observe.review"],
            }
        ),
        json.dumps({"kind": "activity_truth_loop", "seeded_at": as_of.isoformat()}),
        as_of,
    )


async def _seed_workflow_event(
    conn,
    *,
    workflow_id: str,
    request_id: str,
    run_id: str,
    workflow_definition_id: str,
    definition_hash: str,
    suffix: str,
    as_of: datetime,
) -> str:
    event_id = f"event.{suffix}.activity-truth"
    transition_seq = 1
    await conn.execute(
        """
        INSERT INTO workflow_events (
            event_id,
            event_type,
            schema_version,
            workflow_id,
            run_id,
            request_id,
            causation_id,
            node_id,
            occurred_at,
            evidence_seq,
            actor_type,
            reason_code,
            payload
        ) VALUES (
            $1, $2, 1, $3, $4, $5, NULL, 'node.observe', $6, 1, 'runtime', $7, $8::jsonb
        )
        ON CONFLICT (event_id) DO NOTHING
        """,
        event_id,
        "activity_truth_observed",
        workflow_id,
        run_id,
        request_id,
        as_of - timedelta(seconds=30),
        "activity.truth.observed",
        json.dumps(
            {
                "route_identity": _route_identity(
                    workflow_id=workflow_id,
                    request_id=request_id,
                    run_id=run_id,
                    suffix=suffix,
                    transition_seq=transition_seq,
                ),
                "transition_seq": transition_seq,
                "workflow_definition_id": workflow_definition_id,
                "admitted_definition_hash": definition_hash,
                "observed_artifacts": ["debate_consensus", "retrieval_metric"],
            }
        ),
    )
    return event_id


async def _seed_workflow_receipt(
    conn,
    *,
    workflow_id: str,
    request_id: str,
    run_id: str,
    workflow_definition_id: str,
    definition_hash: str,
    suffix: str,
    as_of: datetime,
    causation_id: str,
) -> str:
    receipt_id = f"receipt.{suffix}.activity-truth"
    transition_seq = 2
    await conn.execute(
        """
        INSERT INTO receipts (
            receipt_id,
            receipt_type,
            schema_version,
            workflow_id,
            run_id,
            request_id,
            causation_id,
            node_id,
            attempt_no,
            supersedes_receipt_id,
            started_at,
            finished_at,
            evidence_seq,
            executor_type,
            status,
            inputs,
            outputs,
            artifacts,
            failure_code,
            decision_refs
        ) VALUES (
            $1, $2, 1, $3, $4, $5, $6, 'node.review', 1, NULL, $7, $8, 2,
            'native_operator', 'succeeded', $9::jsonb, $10::jsonb, '[]'::jsonb, NULL, '[]'::jsonb
        )
        ON CONFLICT (receipt_id) DO NOTHING
        """,
        receipt_id,
        "workflow_completion_receipt",
        workflow_id,
        run_id,
        request_id,
        causation_id,
        as_of - timedelta(seconds=20),
        as_of,
        json.dumps(
            {
                "route_identity": _route_identity(
                    workflow_id=workflow_id,
                    request_id=request_id,
                    run_id=run_id,
                    suffix=suffix,
                    transition_seq=transition_seq,
                ),
                "transition_seq": transition_seq,
                "workflow_definition_id": workflow_definition_id,
            }
        ),
        json.dumps(
            {
                "result": "activity_truth_captured",
                "definition_hash": definition_hash,
            }
        ),
    )
    return receipt_id


def _route_identity(
    *,
    workflow_id: str,
    request_id: str,
    run_id: str,
    suffix: str,
    transition_seq: int,
) -> dict[str, object]:
    return {
        "workflow_id": workflow_id,
        "run_id": run_id,
        "request_id": request_id,
        "authority_context_ref": f"authority_context.{suffix}",
        "authority_context_digest": f"sha256:authority:{suffix}",
        "claim_id": f"claim.{suffix}",
        "lease_id": None,
        "proposal_id": None,
        "promotion_decision_id": None,
        "attempt_no": 1,
        "transition_seq": transition_seq,
    }


async def _snapshot_workflow_definition(
    conn,
    *,
    workflow_definition_id: str,
) -> dict[str, object]:
    definition_row = await conn.fetchrow(
        """
        SELECT workflow_definition_id, workflow_id, definition_hash, normalized_definition
        FROM workflow_definitions
        WHERE workflow_definition_id = $1
        """,
        workflow_definition_id,
    )
    node_rows = await conn.fetch(
        """
        SELECT
            node_id,
            node_type,
            adapter_type,
            display_name,
            inputs,
            expected_outputs,
            success_condition,
            failure_behavior,
            authority_requirements,
            execution_boundary,
            position_index
        FROM workflow_definition_nodes
        WHERE workflow_definition_id = $1
        ORDER BY position_index, node_id
        """,
        workflow_definition_id,
    )
    edge_rows = await conn.fetch(
        """
        SELECT
            edge_id,
            edge_type,
            from_node_id,
            to_node_id,
            release_condition,
            payload_mapping,
            position_index
        FROM workflow_definition_edges
        WHERE workflow_definition_id = $1
        ORDER BY position_index, edge_id
        """,
        workflow_definition_id,
    )
    assert definition_row is not None
    return {
        "definition": dict(definition_row),
        "nodes": [dict(row) for row in node_rows],
        "edges": [dict(row) for row in edge_rows],
    }


async def _seed_bug_evidence_links(
    conn,
    *,
    bug_id: str,
    as_of: datetime,
    suffix: str,
    evidence_links: tuple[tuple[str, str, str], ...],
) -> None:
    for index, (evidence_kind, evidence_ref, evidence_role) in enumerate(evidence_links, start=1):
        await conn.execute(
            """
            INSERT INTO bug_evidence_links (
                bug_evidence_link_id,
                bug_id,
                evidence_kind,
                evidence_ref,
                evidence_role,
                created_at,
                created_by,
                notes
            ) VALUES (
                $1, $2, $3, $4, $5, $6, 'test.native_observability_cockpit', $7
            )
            ON CONFLICT (bug_evidence_link_id) DO NOTHING
            """,
            f"bug_evidence_link.{suffix}.{index}",
            bug_id,
            evidence_kind,
            evidence_ref,
            evidence_role,
            as_of,
            f"Bound {evidence_kind} evidence into the activity truth loop.",
        )


def _operator_env() -> dict[str, str]:
    try:
        database_url = resolve_workflow_database_url()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for the cockpit scoping integration test: "
            f"{exc.reason_code}"
        )
    env = {
        "WORKFLOW_DATABASE_URL": database_url,
        PRAXIS_RECEIPTS_DIR_ENV: str(REPO_ROOT / "artifacts" / "runtime_receipts"),
        PRAXIS_RUNTIME_PROFILE_ENV: "praxis",
        PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(REPO_ROOT / "config" / "runtime_profiles.json"),
        PRAXIS_TOPOLOGY_DIR_ENV: str(REPO_ROOT / "artifacts" / "runtime_topology"),
    }
    try:
        ensure_postgres_available(env=env).fetchval("SELECT 1")
    except Exception as exc:
        pytest.skip(
            "Workflow database must be reachable for the cockpit integration tests: "
            f"{type(exc).__name__}"
        )
    return env
