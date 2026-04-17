from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import pytest

from policy.workflow_lanes import bootstrap_workflow_lane_catalog_schema
from runtime.instance import (
    PRAXIS_RECEIPTS_DIR_ENV,
    PRAXIS_RUNTIME_PROFILE_ENV,
    PRAXIS_RUNTIME_PROFILES_CONFIG_ENV,
    PRAXIS_TOPOLOGY_DIR_ENV,
    resolve_native_instance,
)
from runtime.work_item_workflow_bindings import work_item_workflow_binding_id
from storage.migrations import workflow_migration_statements
from storage.postgres import (
    PostgresConfigurationError,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    resolve_workflow_database_url,
)
from surfaces.api import operator_read

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001
REPO_ROOT = Path(__file__).resolve().parents[4]


def test_native_operator_query_surface_reads_canonical_rows_without_mutation() -> None:
    seed_result = asyncio.run(_seed_native_operator_query_surface_test_data())
    registry_path = REPO_ROOT / str(seed_result["registry_path"])
    try:
        _assert_native_operator_query_surface_reads_canonical_rows_without_mutation(seed_result)
    finally:
        registry_path.unlink(missing_ok=True)


def test_workflow_run_packet_observability_rolls_up_sources_and_contract_drift() -> None:
    as_of = datetime(2026, 4, 10, 17, 0, tzinfo=timezone.utc)

    class _MissingColumnError(Exception):
        sqlstate = "42703"

        def __init__(self, column_name: str) -> None:
            super().__init__(f'column "{column_name}" does not exist')

    class _FakePacketConnection:
        async def fetch(self, query: str, *args: object):
            workflow_run_ids = args[0]
            assert workflow_run_ids == ["run.derived", "run.missing"]
            if "NULL::jsonb AS packet_inspection" not in query and "packet_inspection" in query:
                raise _MissingColumnError("packet_inspection")
            return [
                {
                    "run_id": "run.derived",
                    "workflow_id": "workflow.native-self-hosted-smoke.abc123",
                    "request_id": "request.native-self-hosted-smoke.abc123",
                    "request_digest": "digest.derived",
                    "workflow_definition_id": "workflow_definition.native_self_hosted_smoke.v1.abc123",
                    "admitted_definition_hash": "sha256:smoke.abc123",
                    "current_state": "claim_accepted",
                    "terminal_reason_code": None,
                    "run_idempotency_key": "request.native-self-hosted-smoke.abc123",
                    "packet_inspection": None,
                    "request_envelope": {
                        "name": "native smoke",
                        "definition_hash": "sha256:smoke.abc123",
                        "spec_snapshot": {
                            "definition_revision": "definition.rev.smoke",
                            "plan_revision": "plan.rev.smoke",
                            "verify_refs": ["verify.smoke"],
                            "packet_provenance": {
                                "source_kind": "shadow_execution",
                            },
                        },
                    },
                    "requested_at": as_of,
                    "admitted_at": as_of,
                    "started_at": as_of,
                    "finished_at": None,
                    "last_event_id": None,
                    "packets": [
                        {
                            "packet_revision": "packet.rev.smoke.1",
                            "packet_hash": "packet.hash.smoke.1",
                            "run_id": "run.derived",
                            "workflow_id": "workflow.native-self-hosted-smoke.abc123",
                            "spec_name": "native smoke",
                            "source_kind": "shadow_execution",
                            "definition_revision": "definition.rev.smoke",
                            "plan_revision": "plan.rev.smoke",
                            "authority_refs": [
                                "definition.rev.smoke",
                                "plan.rev.smoke",
                            ],
                            "verify_refs": ["verify.smoke"],
                        }
                    ],
                    "operator_frames": [
                        {
                            "operator_frame_id": "operator_frame.derived.0",
                            "node_id": "foreach",
                            "operator_kind": "foreach",
                            "frame_state": "running",
                            "item_index": 0,
                            "iteration_index": None,
                            "source_snapshot": {"item": "alpha"},
                            "aggregate_outputs": {},
                            "active_count": 1,
                            "stop_reason": None,
                            "started_at": as_of,
                            "finished_at": None,
                        }
                    ],
                },
                {
                    "run_id": "run.missing",
                    "workflow_id": "workflow.native-self-hosted-smoke.def456",
                    "request_id": "request.native-self-hosted-smoke.def456",
                    "request_digest": "digest.missing",
                    "workflow_definition_id": "workflow_definition.native_self_hosted_smoke.v1.def456",
                    "admitted_definition_hash": "sha256:smoke.def456",
                    "current_state": "claim_rejected",
                    "terminal_reason_code": "provider.timeout",
                    "run_idempotency_key": "request.native-self-hosted-smoke.def456",
                    "packet_inspection": None,
                    "request_envelope": {
                        "name": "native smoke",
                        "definition_hash": "sha256:smoke.def456",
                    },
                    "requested_at": as_of,
                    "admitted_at": as_of,
                    "started_at": None,
                    "finished_at": as_of,
                    "last_event_id": None,
                    "packets": [],
                    "operator_frames": [],
                },
            ]

    records, summary = asyncio.run(
        operator_read.NativeOperatorQueryFrontdoor()._fetch_workflow_run_packet_inspections(
            conn=_FakePacketConnection(),
            workflow_run_ids=("run.derived", "run.missing"),
        )
    )

    by_id = {record.workflow_run_id: record for record in records}
    assert by_id["run.derived"].packet_inspection_source == "derived"
    assert by_id["run.derived"].synthetic_run is True
    assert by_id["run.derived"].isolation_suffix == "abc123"
    assert by_id["run.derived"].operator_frame_source == "canonical_operator_frames"
    assert by_id["run.derived"].operator_frame_count == 1
    assert by_id["run.derived"].operator_frame_state_counts == (("running", 1),)
    assert by_id["run.derived"].to_json()["operator_frames"] == [
        {
            "operator_frame_id": "operator_frame.derived.0",
            "node_id": "foreach",
            "operator_kind": "foreach",
            "frame_state": "running",
            "item_index": 0,
            "iteration_index": None,
            "source_snapshot": {"item": "alpha"},
            "aggregate_outputs": {},
            "active_count": 1,
            "stop_reason": None,
            "started_at": as_of.isoformat(),
            "finished_at": None,
        }
    ]
    assert by_id["run.missing"].packet_inspection_source == "missing"
    assert by_id["run.missing"].failure_category == "provider_timeout"
    assert by_id["run.missing"].operator_frame_source == "canonical_operator_frames"
    assert by_id["run.missing"].operator_frame_count == 0
    assert summary is not None
    assert summary.to_json() == {
        "kind": "workflow_run_observability",
        "observability_digest": "2 runs | 50.0% packet coverage | 100.0% operator-frame coverage | dominant failure provider_timeout | 2 synthetic | 2 isolated | 1 active frame run(s) | 1 drift refs",
        "workflow_run_count": 2,
        "packet_inspection_source_counts": {
            "derived": 1,
            "missing": 1,
        },
        "packet_inspection_coverage_rate": 0.5,
        "operator_frame_source_counts": {
            "canonical_operator_frames": 2,
        },
        "operator_frame_coverage_rate": 1.0,
        "active_operator_frame_run_count": 1,
        "failure_category_counts": {
            "in_progress": 1,
            "provider_timeout": 1,
        },
        "dominant_failure_category": "provider_timeout",
        "synthetic_run_count": 2,
        "isolated_run_count": 2,
        "missing_workflow_run_ids": [],
        "contract_drift_refs": ["workflow_runs.packet_inspection_column_missing"],
    }


async def _seed_native_operator_query_surface_test_data() -> dict[str, object]:
    """Seed test data asynchronously and return IDs for sync assertion."""
    env = _operator_query_env()
    as_of = datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc)
    suffix = uuid.uuid4().hex[:10]
    registry_path = Path("artifacts") / f"operator_query_assessment_{suffix}.txt"
    (REPO_ROOT / registry_path).parent.mkdir(parents=True, exist_ok=True)
    (REPO_ROOT / registry_path).write_text("assessment signal\n", encoding="utf-8")

    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_control_plane_schema(conn)
        await bootstrap_workflow_lane_catalog_schema(conn)
        await _bootstrap_workflow_migration(conn, "008_workflow_class_and_schedule_schema.sql")
        await _bootstrap_workflow_migration(conn, "009_bug_and_roadmap_authority.sql")
        await _bootstrap_workflow_migration(conn, "010_operator_control_authority.sql")
        await _bootstrap_workflow_migration(conn, "132_issue_backlog_authority.sql")

        bug_id = await _seed_bug(conn, as_of=as_of, suffix=suffix)
        await _seed_bug_evidence_link(conn, as_of=as_of, suffix=suffix, bug_id=bug_id)
        roadmap_item_id = await _seed_roadmap_item(
            conn,
            as_of=as_of,
            suffix=suffix,
            registry_path=str(registry_path),
        )
        decision_id = await _seed_operator_decision(conn, as_of=as_of, suffix=suffix)
        gate_id = await _seed_cutover_gate(
            conn,
            as_of=as_of,
            suffix=suffix,
            roadmap_item_id=roadmap_item_id,
            decision_id=decision_id,
        )
        await _seed_workflow_lane(conn, as_of=as_of, suffix=suffix)
        await _seed_workflow_class(conn, as_of=as_of, suffix=suffix)
        binding_id = await _seed_binding(
            conn,
            as_of=as_of,
            bug_id=bug_id,
            decision_id=decision_id,
            suffix=suffix,
        )
        return {
            "env": env,
            "as_of": as_of,
            "suffix": suffix,
            "bug_id": bug_id,
            "roadmap_item_id": roadmap_item_id,
            "decision_id": decision_id,
            "gate_id": gate_id,
            "binding_id": binding_id,
            "registry_path": str(registry_path),
        }
    finally:
        await conn.close()


def _assert_native_operator_query_surface_reads_canonical_rows_without_mutation(
    seed: dict[str, object],
) -> None:
    """Call the sync query surface outside the event loop and assert correctness."""
    env = seed["env"]
    as_of = seed["as_of"]
    suffix = seed["suffix"]
    bug_id = seed["bug_id"]
    roadmap_item_id = seed["roadmap_item_id"]
    decision_id = seed["decision_id"]
    gate_id = seed["gate_id"]
    binding_id = seed["binding_id"]

    payload = operator_read.query_operator_surface(
        env=env,
        as_of=as_of,
        bug_ids=[bug_id],
        roadmap_item_ids=[roadmap_item_id],
        cutover_gate_ids=[gate_id],
        work_item_workflow_binding_ids=[binding_id],
    )

    assert payload["kind"] == "operator_query"
    assert payload["instruction_authority"] == {
        "kind": "operator_query_instruction_authority",
        "authority": "surfaces.api.operator_read.query_operator_surface",
        "packet_read_order": [
            "roadmap_truth",
            "queue_refs",
            "work_item_assessments",
            "work_item_closeout_recommendations",
            "bugs",
            "cutover_gates",
            "work_item_workflow_bindings",
        ],
        "roadmap_truth": {
            "authority": "roadmap_items",
            "roadmap_item_ids": [roadmap_item_id],
            "items": [
                {
                        "roadmap_item_id": roadmap_item_id,
                        "roadmap_key": f"roadmap.{suffix}.query",
                        "title": f"Query surface roadmap {suffix}",
                        "status": "active",
                        "priority": "p1",
                        "parent_roadmap_item_id": None,
                        "decision_ref": f"decision.{suffix}.query",
                    }
                ],
            },
        "queue_refs": {
            "workflow_run_ids": [],
            "work_item_workflow_binding_ids": [binding_id],
            "cutover_gate_ids": [gate_id],
        },
        "work_item_assessments": {
            "authority": "runtime.work_item_assessment.assess_work_items",
            "count": 2,
            "kinds": ["bug", "roadmap_item"],
        },
        "work_item_closeout_recommendations": {
            "authority": "runtime.work_item_assessment.assess_work_items",
            "count": 1,
            "actions": ["preview_work_item_closeout"],
        },
        "directive": (
            "Read roadmap-backed rows, queue refs, work-item assessments, and closeout recommendations here before using repo files or prior chat state."
        ),
    }
    assert payload["as_of"] == as_of.isoformat()
    assert payload["native_instance"] == resolve_native_instance(env=env).to_contract()
    assert payload["query"] == {
        "bug_ids": [bug_id],
        "roadmap_item_ids": [roadmap_item_id],
        "cutover_gate_ids": [gate_id],
        "work_item_workflow_binding_ids": [binding_id],
        "workflow_run_ids": None,
    }
    assert payload["counts"] == {
        "bugs": 1,
        "roadmap_items": 1,
        "cutover_gates": 1,
        "work_item_workflow_bindings": 1,
        "work_item_assessments": 2,
        "work_item_closeout_recommendations": 1,
    }

    bug = payload["bugs"][0]
    assert bug["bug_id"] == bug_id
    assert bug["bug_key"] == f"bug-key.{suffix}.query"
    assert bug["status"] == "open"
    assert bug["source_kind"] == "manual"
    assert bug["assessment"]["freshness_state"] == "needs_review"
    assert bug["assessment"]["closeout"]["state"] == "review_before_closeout"
    assert bug["assessment"]["closeout"]["action"] == "preview_work_item_closeout"
    assert bug["assessment"]["closeout"]["bug_ids"] == [bug_id]
    assert bug["assessment"]["closeout"]["roadmap_item_ids"] == [roadmap_item_id]
    assert "architecture_changed" in bug["assessment"]["reason_codes"]
    assert bug["assessment"]["associated_paths"] == [seed["registry_path"]]

    roadmap_item = payload["roadmap_items"][0]
    assert roadmap_item["roadmap_item_id"] == roadmap_item_id
    assert roadmap_item["roadmap_key"] == f"roadmap.{suffix}.query"
    assert roadmap_item["source_bug_id"] == bug_id
    assert roadmap_item["registry_paths"] == [seed["registry_path"]]
    assert roadmap_item["acceptance_criteria"] == {
        "must_have": ["operator-query", "canonical-rows"],
    }
    assert roadmap_item["assessment"]["freshness_state"] == "needs_review"
    assert roadmap_item["assessment"]["resolution_state"] == "candidate_completed"
    assert roadmap_item["assessment"]["closeout"]["state"] == "review_before_closeout"
    assert "architecture_changed" in roadmap_item["assessment"]["reason_codes"]
    assert "source_bug_fix_proof_present" in roadmap_item["assessment"]["reason_codes"]

    recommendation = payload["work_item_closeout_recommendations"][0]
    assert recommendation == {
        "anchor_kind": "bug",
        "anchor_id": bug_id,
        "closeout_state": "review_before_closeout",
        "closeout_action": "preview_work_item_closeout",
        "confidence": 0.92,
        "reason_codes": [
            "architecture_changed",
            "workflow_binding_present",
            "validating_fix_evidence_present",
        ],
        "bug_ids": [bug_id],
        "roadmap_item_ids": [roadmap_item_id],
    }

    gate = payload["cutover_gates"][0]
    assert gate["cutover_gate_id"] == gate_id
    assert gate["gate_key"] == f"gate.{suffix}.query"
    assert gate["target_kind"] == "roadmap_item"
    assert gate["target_ref"] == roadmap_item_id
    assert gate["gate_policy"] == {"mode": "manual_review", "owner": "operator"}

    binding = payload["work_item_workflow_bindings"][0]
    assert binding["work_item_workflow_binding_id"] == binding_id
    assert binding["source"] == {
        "kind": "bug",
        "id": bug_id,
        "bug_id": bug_id,
    }
    assert binding["targets"] == {
        "workflow_class_id": f"workflow_class.{suffix}.query",
    }
    assert binding["bound_by_decision_id"] == decision_id
    assert {
        (assessment["item_kind"], assessment["item_id"])
        for assessment in payload["work_item_assessments"]
    } == {
        ("bug", bug_id),
        ("roadmap_item", roadmap_item_id),
    }


async def _bootstrap_workflow_migration(conn, filename: str) -> None:
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in workflow_migration_statements(filename):
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if getattr(exc, "sqlstate", None) in {"42P07", "42710"}:
                    continue
                raise


async def _seed_bug(conn, *, as_of: datetime, suffix: str) -> str:
    bug_id = f"bug.{suffix}.query"
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
            decision_ref,
            opened_at,
            resolved_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, $12
        )
        ON CONFLICT (bug_id) DO UPDATE SET
            bug_key = EXCLUDED.bug_key,
            title = EXCLUDED.title,
            status = EXCLUDED.status,
            severity = EXCLUDED.severity,
            priority = EXCLUDED.priority,
            summary = EXCLUDED.summary,
            source_kind = EXCLUDED.source_kind,
            decision_ref = EXCLUDED.decision_ref,
            opened_at = EXCLUDED.opened_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        bug_id,
        f"bug-key.{suffix}.query",
        f"Query surface bug {suffix}",
        "open",
        "medium",
        "p2",
        "Bug visible through the native operator query surface",
        "manual",
        f"decision.{suffix}.query",
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
    registry_path: str,
) -> str:
    roadmap_item_id = f"roadmap_item.{suffix}.query"
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
            registry_paths,
            summary,
            acceptance_criteria,
            decision_ref,
            target_start_at,
            target_end_at,
            completed_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, NULL, $7, $8::jsonb, $9, $10::jsonb, $11, NULL, NULL, NULL, $12, $13
        )
        ON CONFLICT (roadmap_item_id) DO UPDATE SET
            roadmap_key = EXCLUDED.roadmap_key,
            title = EXCLUDED.title,
            item_kind = EXCLUDED.item_kind,
            status = EXCLUDED.status,
            priority = EXCLUDED.priority,
            source_bug_id = EXCLUDED.source_bug_id,
            registry_paths = EXCLUDED.registry_paths,
            summary = EXCLUDED.summary,
            acceptance_criteria = EXCLUDED.acceptance_criteria,
            decision_ref = EXCLUDED.decision_ref,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        roadmap_item_id,
        f"roadmap.{suffix}.query",
        f"Query surface roadmap {suffix}",
        "initiative",
        "active",
        "p1",
        f"bug.{suffix}.query",
        json.dumps([registry_path]),
        "Roadmap item visible through the native operator query surface",
        json.dumps(
            {"must_have": ["operator-query", "canonical-rows"]},
            sort_keys=True,
            separators=(",", ":"),
        ),
        f"decision.{suffix}.query",
        as_of,
        as_of,
    )
    return roadmap_item_id


async def _seed_bug_evidence_link(
    conn,
    *,
    as_of: datetime,
    suffix: str,
    bug_id: str,
) -> None:
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
            $1, $2, 'workflow_receipt', $3, 'validates_fix', $4, 'test.native_operator_query_surface', $5
        )
        ON CONFLICT (bug_evidence_link_id) DO NOTHING
        """,
        f"bug_evidence_link.{suffix}.query",
        bug_id,
        f"receipt.{suffix}.query",
        as_of,
        "Seed explicit fix proof for closeout recommendation coverage.",
    )


async def _seed_operator_decision(conn, *, as_of: datetime, suffix: str) -> str:
    decision_id = f"operator_decision.{suffix}.query"
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
        f"decision.{suffix}.query",
        "query",
        "decided",
        f"Query surface decision {suffix}",
        "Authorize the queryable operator surface rows",
        "operator.console",
        "manual",
        as_of,
        as_of,
        as_of,
        as_of,
    )
    return decision_id


async def _seed_cutover_gate(
    conn,
    *,
    as_of: datetime,
    suffix: str,
    roadmap_item_id: str,
    decision_id: str,
) -> str:
    gate_id = f"cutover_gate.{suffix}.query"
    await conn.execute(
        """
        INSERT INTO cutover_gates (
            cutover_gate_id,
            gate_key,
            gate_name,
            gate_kind,
            gate_status,
            roadmap_item_id,
            workflow_class_id,
            schedule_definition_id,
            gate_policy,
            required_evidence,
            opened_by_decision_id,
            closed_by_decision_id,
            opened_at,
            closed_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, NULL, NULL, $7::jsonb, $8::jsonb, $9, NULL, $10, NULL, $11, $12
        )
        ON CONFLICT (cutover_gate_id) DO UPDATE SET
            gate_key = EXCLUDED.gate_key,
            gate_name = EXCLUDED.gate_name,
            gate_kind = EXCLUDED.gate_kind,
            gate_status = EXCLUDED.gate_status,
            roadmap_item_id = EXCLUDED.roadmap_item_id,
            gate_policy = EXCLUDED.gate_policy,
            required_evidence = EXCLUDED.required_evidence,
            opened_by_decision_id = EXCLUDED.opened_by_decision_id,
            opened_at = EXCLUDED.opened_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        gate_id,
        f"gate.{suffix}.query",
        f"Query surface gate {suffix}",
        "cutover",
        "open",
        roadmap_item_id,
        json.dumps(
            {"mode": "manual_review", "owner": "operator"},
            sort_keys=True,
            separators=(",", ":"),
        ),
        json.dumps(
            {"must_have": ["operator-query", "canonical-rows"]},
            sort_keys=True,
            separators=(",", ":"),
        ),
        decision_id,
        as_of,
        as_of,
        as_of,
    )
    return gate_id


async def _seed_workflow_lane(conn, *, as_of: datetime, suffix: str) -> None:
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
        f"workflow_lane.{suffix}.query",
        f"query-surface-{suffix}",
        "review",
        "active",
        1,
        "manual",
        True,
        '{"max_attempts":1}',
        as_of,
        as_of,
    )


async def _seed_workflow_class(conn, *, as_of: datetime, suffix: str) -> None:
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
        f"workflow_class.{suffix}.query",
        f"query-{suffix}",
        "review",
        f"workflow_lane.{suffix}.query",
        "active",
        '{"shape":"single-run"}',
        '{"max_attempts":1}',
        False,
        as_of,
        f"decision.{suffix}.query",
        as_of,
    )


async def _seed_binding(
    conn,
    *,
    as_of: datetime,
    bug_id: str,
    decision_id: str,
    suffix: str,
) -> str:
    binding_id = work_item_workflow_binding_id(
        binding_kind="governed_by",
        bug_id=bug_id,
        workflow_class_id=f"workflow_class.{suffix}.query",
    )
    await conn.execute(
        """
        INSERT INTO work_item_workflow_bindings (
            work_item_workflow_binding_id,
            binding_kind,
            binding_status,
            roadmap_item_id,
            bug_id,
            cutover_gate_id,
            workflow_class_id,
            schedule_definition_id,
            workflow_run_id,
            bound_by_decision_id,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, NULL, $4, NULL, $5, NULL, NULL, $6, $7, $8
        )
        ON CONFLICT (work_item_workflow_binding_id) DO UPDATE SET
            binding_kind = EXCLUDED.binding_kind,
            binding_status = EXCLUDED.binding_status,
            bug_id = EXCLUDED.bug_id,
            workflow_class_id = EXCLUDED.workflow_class_id,
            bound_by_decision_id = EXCLUDED.bound_by_decision_id,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        binding_id,
        "governed_by",
        "active",
        bug_id,
        f"workflow_class.{suffix}.query",
        decision_id,
        as_of,
        as_of,
    )
    return binding_id


def _operator_query_env() -> dict[str, str]:
    try:
        database_url = resolve_workflow_database_url()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for the native operator query integration test: "
            f"{exc.reason_code}"
        )
    return {
        "WORKFLOW_DATABASE_URL": database_url,
        PRAXIS_RECEIPTS_DIR_ENV: str(REPO_ROOT / "artifacts" / "runtime_receipts"),
        PRAXIS_RUNTIME_PROFILE_ENV: "praxis",
        PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(REPO_ROOT / "config" / "runtime_profiles.json"),
        PRAXIS_TOPOLOGY_DIR_ENV: str(REPO_ROOT / "artifacts" / "runtime_topology"),
    }
