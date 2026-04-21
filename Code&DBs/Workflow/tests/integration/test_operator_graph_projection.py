from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone

import pytest

from _pg_test_conn import bootstrap_workflow_migration
from observability.operator_topology import load_operator_graph_projection
from policy.workflow_lanes import bootstrap_workflow_lane_catalog_schema
from runtime.semantic_assertions import semantic_assertion_id
from storage.postgres import (
    PostgresConfigurationError,
    bootstrap_control_plane_schema,
    connect_workflow_database,
)

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _fixed_clock() -> datetime:
    return datetime(2026, 4, 2, 21, 0, tzinfo=timezone.utc)


async def _bootstrap_migration(conn, filename: str) -> None:
    await bootstrap_workflow_migration(
        conn,
        filename,
        bootstrap_allowed=True,
        schema_bootstrap_lock_id=_SCHEMA_BOOTSTRAP_LOCK_ID,
    )


async def _seed_workflow_lane_and_class(conn, *, as_of: datetime, suffix: str) -> str:
    workflow_lane_id = f"workflow_lane.operator-graph.{suffix}"
    workflow_class_id = f"workflow_class.operator-graph.{suffix}"

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
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11
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
            effective_to = EXCLUDED.effective_to,
            created_at = EXCLUDED.created_at
        """,
        workflow_lane_id,
        f"operator-graph-{suffix}",
        "review",
        "active",
        1,
        "manual",
        True,
        '{"max_attempts":1}',
        as_of,
        None,
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
            $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12
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
            effective_to = EXCLUDED.effective_to,
            decision_ref = EXCLUDED.decision_ref,
            created_at = EXCLUDED.created_at
        """,
        workflow_class_id,
        f"operator-graph-{suffix}",
        "review",
        workflow_lane_id,
        "active",
        '{"shape":"single-run"}',
        '{"max_attempts":1}',
        False,
        as_of,
        None,
        f"decision.operator-graph.{suffix}.workflow-class",
        as_of,
    )
    return workflow_class_id


async def _seed_operator_graph_rows(
    conn,
    *,
    suffix: str,
    as_of: datetime,
) -> tuple[str, str, str, str, str, str, str, str]:
    bug_id = f"bug.operator-graph.{suffix}"
    roadmap_item_id = f"roadmap_item.operator-graph.{suffix}"
    decision_id = f"operator_decision.operator-graph.{suffix}"
    decision_key = f"decision.operator-graph.{suffix}.primary"
    gate_id = f"cutover_gate.operator-graph.{suffix}"
    functional_area_id = f"functional_area.operator-graph-{suffix}"
    document_id = f"document.operator-graph.{suffix}"
    repo_path = f"Code&DBs/Workflow/runtime/operator_graph_{suffix}.py"

    workflow_class_id = await _seed_workflow_lane_and_class(
        conn,
        as_of=as_of,
        suffix=suffix,
    )

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
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
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
            effective_to = EXCLUDED.effective_to,
            decided_at = EXCLUDED.decided_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        decision_id,
        decision_key,
        "operator_graph",
        "decided",
        f"Operator graph decision {suffix}",
        "Graph projection uses this decision as the explicit control anchor.",
        "operator.console",
        "operator.review",
        as_of,
        None,
        as_of,
        as_of,
        as_of,
    )

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
            owner_ref,
            decision_ref,
            resolution_summary,
            opened_at,
            resolved_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
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
            owner_ref = EXCLUDED.owner_ref,
            decision_ref = EXCLUDED.decision_ref,
            resolution_summary = EXCLUDED.resolution_summary,
            opened_at = EXCLUDED.opened_at,
            resolved_at = EXCLUDED.resolved_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        bug_id,
        f"bug.operator-graph.{suffix}",
        f"Operator graph bug {suffix}",
        "open",
        "high",
        "p1",
        "Graph projection uses this bug as a canonical node.",
        "operator_graph",
        None,
        None,
        None,
        decision_key,
        None,
        as_of,
        None,
        as_of,
        as_of,
    )

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
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13, $14, $15, $16
        )
        ON CONFLICT (roadmap_item_id) DO UPDATE SET
            roadmap_key = EXCLUDED.roadmap_key,
            title = EXCLUDED.title,
            item_kind = EXCLUDED.item_kind,
            status = EXCLUDED.status,
            priority = EXCLUDED.priority,
            parent_roadmap_item_id = EXCLUDED.parent_roadmap_item_id,
            source_bug_id = EXCLUDED.source_bug_id,
            summary = EXCLUDED.summary,
            acceptance_criteria = EXCLUDED.acceptance_criteria,
            decision_ref = EXCLUDED.decision_ref,
            target_start_at = EXCLUDED.target_start_at,
            target_end_at = EXCLUDED.target_end_at,
            completed_at = EXCLUDED.completed_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        roadmap_item_id,
        f"roadmap.operator-graph.{suffix}",
        f"Operator graph roadmap {suffix}",
        "initiative",
        "active",
        "p1",
        None,
        bug_id,
        "Roadmap row projected into the operator graph.",
        json.dumps(
            {"required_state": "ready", "evidence": ["graph-projection"]},
            sort_keys=True,
            separators=(",", ":"),
        ),
        decision_key,
        None,
        None,
        None,
        as_of,
        as_of,
    )

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
            $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13, $14, $15, $16
        )
        ON CONFLICT (cutover_gate_id) DO UPDATE SET
            gate_key = EXCLUDED.gate_key,
            gate_name = EXCLUDED.gate_name,
            gate_kind = EXCLUDED.gate_kind,
            gate_status = EXCLUDED.gate_status,
            roadmap_item_id = EXCLUDED.roadmap_item_id,
            workflow_class_id = EXCLUDED.workflow_class_id,
            schedule_definition_id = EXCLUDED.schedule_definition_id,
            gate_policy = EXCLUDED.gate_policy,
            required_evidence = EXCLUDED.required_evidence,
            opened_by_decision_id = EXCLUDED.opened_by_decision_id,
            closed_by_decision_id = EXCLUDED.closed_by_decision_id,
            opened_at = EXCLUDED.opened_at,
            closed_at = EXCLUDED.closed_at,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        gate_id,
        f"gate.operator-graph.{suffix}",
        f"Operator graph gate {suffix}",
        "cutover",
        "open",
        roadmap_item_id,
        None,
        None,
        json.dumps(
            {"mode": "manual_review", "owner": "operator"},
            sort_keys=True,
            separators=(",", ":"),
        ),
        json.dumps(
            {"must_have": ["graph.projection", "graph.freshness"]},
            sort_keys=True,
            separators=(",", ":"),
        ),
        decision_id,
        None,
        as_of,
        None,
        as_of,
        as_of,
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
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
        ON CONFLICT (work_item_workflow_binding_id) DO UPDATE SET
            binding_kind = EXCLUDED.binding_kind,
            binding_status = EXCLUDED.binding_status,
            roadmap_item_id = EXCLUDED.roadmap_item_id,
            bug_id = EXCLUDED.bug_id,
            cutover_gate_id = EXCLUDED.cutover_gate_id,
            workflow_class_id = EXCLUDED.workflow_class_id,
            schedule_definition_id = EXCLUDED.schedule_definition_id,
            workflow_run_id = EXCLUDED.workflow_run_id,
            bound_by_decision_id = EXCLUDED.bound_by_decision_id,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        f"work_item_workflow_binding.operator-graph.{suffix}",
        "governed_by",
        "active",
        None,
        bug_id,
        None,
        workflow_class_id,
        None,
        None,
        decision_id,
        as_of,
        as_of,
    )

    await conn.execute(
        """
        INSERT INTO functional_areas (
            functional_area_id,
            area_slug,
            title,
            area_status,
            summary,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        )
        ON CONFLICT (functional_area_id) DO UPDATE SET
            area_slug = EXCLUDED.area_slug,
            title = EXCLUDED.title,
            area_status = EXCLUDED.area_status,
            summary = EXCLUDED.summary,
            updated_at = EXCLUDED.updated_at
        """,
        functional_area_id,
        f"operator-graph-{suffix}",
        f"Operator graph area {suffix}",
        "active",
        "Functional area row projected into the operator graph.",
        as_of,
        as_of,
    )

    await conn.execute(
        """
        INSERT INTO memory_entities (
            id,
            entity_type,
            name,
            content,
            metadata,
            source,
            confidence,
            archived,
            created_at,
            updated_at
        ) VALUES (
            $1, 'document', $2, $3, '{}'::jsonb, 'tests', 1.0, false, $4, $5
        )
        ON CONFLICT (id) DO UPDATE SET
            updated_at = EXCLUDED.updated_at
        """,
        document_id,
        f"Operator graph document {suffix}",
        "Workflow document projected into the operator graph.",
        as_of,
        as_of,
    )

    await conn.execute(
        """
        INSERT INTO operator_object_relations (
            operator_object_relation_id,
            relation_kind,
            relation_status,
            source_kind,
            source_ref,
            target_kind,
            target_ref,
            relation_metadata,
            bound_by_decision_id,
            created_at,
            updated_at
        ) VALUES
            ($1, 'grouped_in', 'active', 'roadmap_item', $2, 'functional_area', $3, '{"origin":"projection-test"}'::jsonb, $4, $5, $6),
            ($7, 'described_by', 'active', 'repo_path', $8, 'document', $9, '{"origin":"projection-test"}'::jsonb, NULL, $10, $11),
            ($12, 'implements', 'active', 'repo_path', $13, 'roadmap_item', $14, '{"origin":"projection-test"}'::jsonb, $15, $16, $17)
        ON CONFLICT (operator_object_relation_id) DO UPDATE SET
            relation_kind = EXCLUDED.relation_kind,
            relation_status = EXCLUDED.relation_status,
            source_kind = EXCLUDED.source_kind,
            source_ref = EXCLUDED.source_ref,
            target_kind = EXCLUDED.target_kind,
            target_ref = EXCLUDED.target_ref,
            relation_metadata = EXCLUDED.relation_metadata,
            bound_by_decision_id = EXCLUDED.bound_by_decision_id,
            updated_at = EXCLUDED.updated_at
        """,
        f"operator_object_relation:grouped-in:roadmap_item:{roadmap_item_id}:functional_area:{functional_area_id}",
        roadmap_item_id,
        functional_area_id,
        decision_id,
        as_of,
        as_of,
        f"operator_object_relation:described-by:repo_path:{repo_path}:document:{document_id}",
        repo_path,
        document_id,
        as_of,
        as_of,
        f"operator_object_relation:implements:repo_path:{repo_path}:roadmap_item:{roadmap_item_id}",
        repo_path,
        roadmap_item_id,
        decision_id,
        as_of,
        as_of,
    )

    return (
        bug_id,
        roadmap_item_id,
        decision_id,
        gate_id,
        workflow_class_id,
        functional_area_id,
        document_id,
        repo_path,
    )


async def _seed_semantic_assertion_rows(
    conn,
    *,
    suffix: str,
    as_of: datetime,
    roadmap_item_id: str,
    functional_area_id: str,
    decision_id: str,
) -> tuple[str, str]:
    authority_domain_ref = f"authority_domain.operator-graph.{suffix}"
    grouped_in_relation_id = (
        "operator_object_relation:grouped-in:roadmap_item:"
        f"{roadmap_item_id}:functional_area:{functional_area_id}"
    )
    grouped_in_assertion_id = semantic_assertion_id(
        predicate_slug="grouped_in",
        subject_kind="roadmap_item",
        subject_ref=roadmap_item_id,
        object_kind="functional_area",
        object_ref=functional_area_id,
        source_kind="operator_object_relation",
        source_ref=grouped_in_relation_id,
    )
    governed_by_policy_assertion_id = semantic_assertion_id(
        predicate_slug="governed_by_policy",
        subject_kind="authority_domain",
        subject_ref=authority_domain_ref,
        object_kind="operator_decision",
        object_ref=decision_id,
        source_kind="operator_decision",
        source_ref=decision_id,
    )

    await conn.execute(
        """
        INSERT INTO semantic_predicates (
            predicate_slug,
            predicate_status,
            subject_kind_allowlist,
            object_kind_allowlist,
            cardinality_mode,
            description,
            created_at,
            updated_at
        ) VALUES
            (
                'grouped_in',
                'active',
                '["roadmap_item"]'::jsonb,
                '["functional_area"]'::jsonb,
                'single_active_per_edge',
                'Compatibility overlay for roadmap_item grouped_in functional_area.',
                $1,
                $2
            ),
            (
                'governed_by_policy',
                'active',
                '["authority_domain"]'::jsonb,
                '["operator_decision"]'::jsonb,
                'many',
                'Authority-domain policy edge.',
                $3,
                $4
            )
        ON CONFLICT (predicate_slug) DO UPDATE SET
            predicate_status = EXCLUDED.predicate_status,
            subject_kind_allowlist = EXCLUDED.subject_kind_allowlist,
            object_kind_allowlist = EXCLUDED.object_kind_allowlist,
            cardinality_mode = EXCLUDED.cardinality_mode,
            description = EXCLUDED.description,
            updated_at = EXCLUDED.updated_at
        """,
        as_of,
        as_of,
        as_of,
        as_of,
    )

    await conn.execute(
        """
        INSERT INTO semantic_assertions (
            semantic_assertion_id,
            predicate_slug,
            assertion_status,
            subject_kind,
            subject_ref,
            object_kind,
            object_ref,
            qualifiers_json,
            source_kind,
            source_ref,
            evidence_ref,
            bound_decision_id,
            valid_from,
            valid_to,
            created_at,
            updated_at
        ) VALUES
            (
                $1,
                'grouped_in',
                'active',
                'roadmap_item',
                $2,
                'functional_area',
                $3,
                '{"origin":"semantic-overlay"}'::jsonb,
                'operator_object_relation',
                $4,
                NULL,
                $5,
                $6,
                NULL,
                $7,
                $8
            ),
            (
                $9,
                'governed_by_policy',
                'active',
                'authority_domain',
                $10,
                'operator_decision',
                $11,
                '{"origin":"semantic-test"}'::jsonb,
                'operator_decision',
                $12,
                NULL,
                $13,
                $14,
                NULL,
                $15,
                $16
            )
        ON CONFLICT (semantic_assertion_id) DO UPDATE SET
            predicate_slug = EXCLUDED.predicate_slug,
            assertion_status = EXCLUDED.assertion_status,
            subject_kind = EXCLUDED.subject_kind,
            subject_ref = EXCLUDED.subject_ref,
            object_kind = EXCLUDED.object_kind,
            object_ref = EXCLUDED.object_ref,
            qualifiers_json = EXCLUDED.qualifiers_json,
            source_kind = EXCLUDED.source_kind,
            source_ref = EXCLUDED.source_ref,
            evidence_ref = EXCLUDED.evidence_ref,
            bound_decision_id = EXCLUDED.bound_decision_id,
            valid_from = EXCLUDED.valid_from,
            valid_to = EXCLUDED.valid_to,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """,
        grouped_in_assertion_id,
        roadmap_item_id,
        functional_area_id,
        grouped_in_relation_id,
        decision_id,
        as_of,
        as_of,
        as_of,
        governed_by_policy_assertion_id,
        authority_domain_ref,
        decision_id,
        decision_id,
        decision_id,
        as_of,
        as_of,
        as_of,
    )
    return grouped_in_assertion_id, governed_by_policy_assertion_id


def test_operator_graph_projection_is_graph_ready_and_explicit() -> None:
    asyncio.run(_exercise_operator_graph_projection_is_graph_ready_and_explicit())


async def _exercise_operator_graph_projection_is_graph_ready_and_explicit() -> None:
    database_url = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://127.0.0.1/postgres")
    try:
        conn = await connect_workflow_database(
            env={"WORKFLOW_DATABASE_URL": database_url},
        )
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for operator graph projection integration test: "
            f"{exc.reason_code}"
        )

    transaction = conn.transaction()
    await transaction.start()
    try:
        await bootstrap_control_plane_schema(conn)
        await bootstrap_workflow_lane_catalog_schema(conn)
        for filename in (
            "008_workflow_class_and_schedule_schema.sql",
            "009_bug_and_roadmap_authority.sql",
            "136_operation_catalog_authority.sql",
            "195_operator_ideas_authority.sql",
            "010_operator_control_authority.sql",
            "015_memory_graph.sql",
            "132_issue_backlog_authority.sql",
            "134_operator_object_relations.sql",
            "146_semantic_assertion_substrate.sql",
        ):
            await _bootstrap_migration(conn, filename)

        suffix = _unique_suffix()
        as_of = _fixed_clock()
        (
            bug_id,
            roadmap_item_id,
            decision_id,
            gate_id,
            workflow_class_id,
            functional_area_id,
            document_id,
            repo_path,
        ) = await _seed_operator_graph_rows(conn, suffix=suffix, as_of=as_of)
        grouped_in_assertion_id, governed_by_policy_assertion_id = await _seed_semantic_assertion_rows(
            conn,
            suffix=suffix,
            as_of=as_of,
            roadmap_item_id=roadmap_item_id,
            functional_area_id=functional_area_id,
            decision_id=decision_id,
        )

        projection = await load_operator_graph_projection(
            conn,
            as_of=as_of,
        )

        assert projection.as_of == as_of
        assert projection.semantic_authority_state == "ready"
        assert projection.semantic_authority_reason_code == "semantic_assertions.active_window"
        # Completeness may be incomplete if the DB has stale test data from
        # previous runs with unresolvable decision_refs. Verify our own
        # seeded rows are present and correctly projected rather than
        # asserting global completeness.
        own_missing = tuple(
            ref for ref in projection.completeness.missing_evidence_refs
            if f"operator-graph.{suffix}" in ref
        )
        assert own_missing == (), f"own seeded data has missing refs: {own_missing}"
        assert projection.freshness.as_of == as_of
        assert projection.freshness.source_row_count >= 5

        # Find our seeded rows by ID (projection may contain stale data from other runs).
        own_bug_ids = {b.bug_id for b in projection.bugs}
        own_roadmap_ids = {r.roadmap_item_id for r in projection.roadmap_items}
        own_functional_area_ids = {f.functional_area_id for f in projection.functional_areas}
        own_decision_ids = {d.operator_decision_id for d in projection.operator_decisions}
        own_gate_ids = {g.cutover_gate_id for g in projection.cutover_gates}
        own_binding_ids = {b.work_item_workflow_binding_id for b in projection.work_item_workflow_bindings}
        own_relation_ids = {r.operator_object_relation_id for r in projection.object_relations}
        own_semantic_assertion_ids = {r.semantic_assertion_id for r in projection.semantic_assertions}
        assert bug_id in own_bug_ids
        assert roadmap_item_id in own_roadmap_ids
        assert functional_area_id in own_functional_area_ids
        assert decision_id in own_decision_ids
        assert gate_id in own_gate_ids
        assert f"work_item_workflow_binding.operator-graph.{suffix}" in own_binding_ids
        assert (
            f"operator_object_relation:grouped-in:roadmap_item:{roadmap_item_id}:functional_area:{functional_area_id}"
            in own_relation_ids
        )
        assert grouped_in_assertion_id in own_semantic_assertion_ids
        assert governed_by_policy_assertion_id in own_semantic_assertion_ids

        node_ids = {node.node_id for node in projection.nodes}
        assert f"bug:{bug_id}" in node_ids
        assert f"roadmap_item:{roadmap_item_id}" in node_ids
        assert f"functional_area:{functional_area_id}" in node_ids
        assert f"operator_decision:{decision_id}" in node_ids
        assert f"cutover_gate:{gate_id}" in node_ids
        assert f"workflow_class:{workflow_class_id}" in node_ids
        assert f"document:{document_id}" in node_ids
        assert f"repo_path:{repo_path}" in node_ids
        assert f"authority_domain:authority_domain.operator-graph.{suffix}" in node_ids

        edge_summaries = {
            (
                edge.edge_kind,
                edge.source_kind,
                edge.target_kind,
                edge.target_ref,
                edge.target_node_id,
                edge.authority_source,
            )
            for edge in projection.edges
        }
        expected_edges = {
            ("decision_ref", "bug", "operator_decision", "decision.operator-graph." + suffix + ".primary", f"operator_decision:{decision_id}", "bugs"),
            ("source_bug", "roadmap_item", "bug", bug_id, f"bug:{bug_id}", "roadmap_items"),
            ("decision_ref", "roadmap_item", "operator_decision", "decision.operator-graph." + suffix + ".primary", f"operator_decision:{decision_id}", "roadmap_items"),
            ("target_roadmap_item", "cutover_gate", "roadmap_item", roadmap_item_id, f"roadmap_item:{roadmap_item_id}", "cutover_gates"),
            ("opened_by_decision", "cutover_gate", "operator_decision", decision_id, f"operator_decision:{decision_id}", "cutover_gates"),
            ("bound_by_decision", "bug", "operator_decision", decision_id, f"operator_decision:{decision_id}", "work_item_workflow_bindings"),
            ("targets_workflow_class", "bug", "workflow_class", workflow_class_id, f"workflow_class:{workflow_class_id}", "work_item_workflow_bindings"),
            ("grouped_in", "roadmap_item", "functional_area", functional_area_id, f"functional_area:{functional_area_id}", "semantic_assertions"),
            ("bound_by_decision", "roadmap_item", "operator_decision", decision_id, f"operator_decision:{decision_id}", "semantic_assertions"),
            ("governed_by_policy", "authority_domain", "operator_decision", decision_id, f"operator_decision:{decision_id}", "semantic_assertions"),
            ("bound_by_decision", "authority_domain", "operator_decision", decision_id, f"operator_decision:{decision_id}", "semantic_assertions"),
            ("described_by", "repo_path", "document", document_id, f"document:{document_id}", "operator_object_relations_compatibility"),
            ("implements", "repo_path", "roadmap_item", roadmap_item_id, f"roadmap_item:{roadmap_item_id}", "operator_object_relations_compatibility"),
            ("relation_bound_by_decision", "repo_path", "operator_decision", decision_id, f"operator_decision:{decision_id}", "operator_object_relations_compatibility"),
        }
        assert expected_edges.issubset(edge_summaries), (
            f"missing expected edges: {expected_edges - edge_summaries}"
        )
        assert projection.freshness.semantic_source_row_count >= 2
        assert projection.freshness.compatibility_source_row_count >= 3
        assert projection.freshness.source_row_count >= 7
        grouped_in_edges = [
            edge
            for edge in projection.edges
            if edge.edge_kind == "grouped_in"
            and edge.source_kind == "roadmap_item"
            and edge.target_ref == functional_area_id
        ]
        assert len(grouped_in_edges) == 1
        assert grouped_in_edges[0].authority_source == "semantic_assertions"
    finally:
        await conn.close()
