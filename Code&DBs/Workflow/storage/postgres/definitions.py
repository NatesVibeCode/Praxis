"""Workflow definition persistence for the Postgres control plane."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

import asyncpg

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_child_row_payloads,
    _require_nonnegative_int,
    _require_positive_int,
    _require_text,
)


async def _insert_or_assert_workflow_definition(
    conn: asyncpg.Connection,
    *,
    workflow_definition_id: str,
    workflow_id: str,
    schema_version: int,
    definition_version: int,
    definition_hash: str,
    definition_status: str,
    request_envelope_json: str,
    created_at: datetime,
    supersedes_workflow_definition_id: str | None,
) -> None:
    inserted_definition_id = await conn.fetchval(
        """
        INSERT INTO workflow_definitions (
            workflow_definition_id,
            workflow_id,
            schema_version,
            definition_version,
            definition_hash,
            status,
            request_envelope,
            normalized_definition,
            created_at,
            supersedes_workflow_definition_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10)
        ON CONFLICT (workflow_definition_id) DO NOTHING
        RETURNING workflow_definition_id
        """,
        workflow_definition_id,
        workflow_id,
        schema_version,
        definition_version,
        definition_hash,
        definition_status,
        request_envelope_json,
        request_envelope_json,
        created_at,
        supersedes_workflow_definition_id,
    )
    if inserted_definition_id is not None:
        return

    existing_definition_matches = await conn.fetchval(
        """
        SELECT 1
        FROM workflow_definitions
        WHERE workflow_definition_id = $1
          AND workflow_id = $2
          AND schema_version = $3
          AND definition_version = $4
          AND definition_hash = $5
          AND status = $6
          AND request_envelope = $7::jsonb
          AND normalized_definition = $8::jsonb
          AND supersedes_workflow_definition_id IS NOT DISTINCT FROM $9
        """,
        workflow_definition_id,
        workflow_id,
        schema_version,
        definition_version,
        definition_hash,
        definition_status,
        request_envelope_json,
        request_envelope_json,
        supersedes_workflow_definition_id,
    )
    if existing_definition_matches == 1:
        return

    raise PostgresWriteError(
        "postgres.definition_conflict",
        "workflow_definition_id already exists with different canonical content",
        details={"workflow_definition_id": workflow_definition_id},
    )


async def _insert_or_assert_definition_node(
    conn: asyncpg.Connection,
    *,
    workflow_definition_node_id: str,
    workflow_definition_id: str,
    node_id: str,
    node_type: str,
    schema_version: int,
    adapter_type: str,
    display_name: str,
    inputs_json: str,
    expected_outputs_json: str,
    success_condition_json: str,
    failure_behavior_json: str,
    authority_requirements_json: str,
    execution_boundary_json: str,
    position_index: int,
) -> None:
    existing_node_row_id = await conn.fetchval(
        """
        SELECT workflow_definition_node_id
        FROM workflow_definition_nodes
        WHERE workflow_definition_id = $1
          AND node_id = $2
        """,
        workflow_definition_id,
        node_id,
    )
    if existing_node_row_id is not None and existing_node_row_id != workflow_definition_node_id:
        raise PostgresWriteError(
            "postgres.definition_conflict",
            "workflow definition node already exists with a different canonical identity",
            details={
                "workflow_definition_id": workflow_definition_id,
                "node_id": node_id,
                "existing_workflow_definition_node_id": existing_node_row_id,
                "incoming_workflow_definition_node_id": workflow_definition_node_id,
            },
        )

    inserted_node_id = await conn.fetchval(
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
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14)
        ON CONFLICT (workflow_definition_node_id) DO NOTHING
        RETURNING workflow_definition_node_id
        """,
        workflow_definition_node_id,
        workflow_definition_id,
        node_id,
        node_type,
        schema_version,
        adapter_type,
        display_name,
        inputs_json,
        expected_outputs_json,
        success_condition_json,
        failure_behavior_json,
        authority_requirements_json,
        execution_boundary_json,
        position_index,
    )
    if inserted_node_id is not None:
        return

    existing_node_matches = await conn.fetchval(
        """
        SELECT 1
        FROM workflow_definition_nodes
        WHERE workflow_definition_node_id = $1
          AND workflow_definition_id = $2
          AND node_id = $3
          AND node_type = $4
          AND schema_version = $5
          AND adapter_type = $6
          AND display_name = $7
          AND inputs = $8::jsonb
          AND expected_outputs = $9::jsonb
          AND success_condition = $10::jsonb
          AND failure_behavior = $11::jsonb
          AND authority_requirements = $12::jsonb
          AND execution_boundary = $13::jsonb
          AND position_index = $14
        """,
        workflow_definition_node_id,
        workflow_definition_id,
        node_id,
        node_type,
        schema_version,
        adapter_type,
        display_name,
        inputs_json,
        expected_outputs_json,
        success_condition_json,
        failure_behavior_json,
        authority_requirements_json,
        execution_boundary_json,
        position_index,
    )
    if existing_node_matches == 1:
        return

    raise PostgresWriteError(
        "postgres.definition_conflict",
        "workflow_definition_node_id already exists with different canonical content",
        details={
            "workflow_definition_id": workflow_definition_id,
            "workflow_definition_node_id": workflow_definition_node_id,
        },
    )


async def _insert_or_assert_definition_edge(
    conn: asyncpg.Connection,
    *,
    workflow_definition_edge_id: str,
    workflow_definition_id: str,
    edge_id: str,
    edge_type: str,
    schema_version: int,
    from_node_id: str,
    to_node_id: str,
    release_condition_json: str,
    payload_mapping_json: str,
    position_index: int,
) -> None:
    existing_edge_row_id = await conn.fetchval(
        """
        SELECT workflow_definition_edge_id
        FROM workflow_definition_edges
        WHERE workflow_definition_id = $1
          AND edge_id = $2
        """,
        workflow_definition_id,
        edge_id,
    )
    if existing_edge_row_id is not None and existing_edge_row_id != workflow_definition_edge_id:
        raise PostgresWriteError(
            "postgres.definition_conflict",
            "workflow definition edge already exists with a different canonical identity",
            details={
                "workflow_definition_id": workflow_definition_id,
                "edge_id": edge_id,
                "existing_workflow_definition_edge_id": existing_edge_row_id,
                "incoming_workflow_definition_edge_id": workflow_definition_edge_id,
            },
        )

    inserted_edge_id = await conn.fetchval(
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
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10)
        ON CONFLICT (workflow_definition_edge_id) DO NOTHING
        RETURNING workflow_definition_edge_id
        """,
        workflow_definition_edge_id,
        workflow_definition_id,
        edge_id,
        edge_type,
        schema_version,
        from_node_id,
        to_node_id,
        release_condition_json,
        payload_mapping_json,
        position_index,
    )
    if inserted_edge_id is not None:
        return

    existing_edge_matches = await conn.fetchval(
        """
        SELECT 1
        FROM workflow_definition_edges
        WHERE workflow_definition_edge_id = $1
          AND workflow_definition_id = $2
          AND edge_id = $3
          AND edge_type = $4
          AND schema_version = $5
          AND from_node_id = $6
          AND to_node_id = $7
          AND release_condition = $8::jsonb
          AND payload_mapping = $9::jsonb
          AND position_index = $10
        """,
        workflow_definition_edge_id,
        workflow_definition_id,
        edge_id,
        edge_type,
        schema_version,
        from_node_id,
        to_node_id,
        release_condition_json,
        payload_mapping_json,
        position_index,
    )
    if existing_edge_matches == 1:
        return

    raise PostgresWriteError(
        "postgres.definition_conflict",
        "workflow_definition_edge_id already exists with different canonical content",
        details={
            "workflow_definition_id": workflow_definition_id,
            "workflow_definition_edge_id": workflow_definition_edge_id,
        },
    )


async def _persist_workflow_definition(
    conn: asyncpg.Connection,
    *,
    submission: Any,  # WorkflowAdmissionSubmission
    request_envelope: Mapping[str, Any],
) -> None:
    """Persist the canonical admitted workflow definition row and child rows."""

    decision = submission.decision
    run = submission.run
    workflow_definition_id = _require_text(
        request_envelope.get("workflow_definition_id"),
        field_name="run.request_envelope.workflow_definition_id",
    )
    if workflow_definition_id != run.workflow_definition_id:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "run.request_envelope.workflow_definition_id must match run.workflow_definition_id",
            details={
                "run.request_envelope.workflow_definition_id": workflow_definition_id,
                "run.workflow_definition_id": run.workflow_definition_id,
            },
        )

    envelope_workflow_id = _require_text(
        request_envelope.get("workflow_id"),
        field_name="run.request_envelope.workflow_id",
    )
    if envelope_workflow_id != run.workflow_id:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "run.request_envelope.workflow_id must match run.workflow_id",
            details={
                "run.request_envelope.workflow_id": envelope_workflow_id,
                "run.workflow_id": run.workflow_id,
            },
        )

    envelope_request_id = _require_text(
        request_envelope.get("request_id"),
        field_name="run.request_envelope.request_id",
    )
    if envelope_request_id != run.request_id:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "run.request_envelope.request_id must match run.request_id",
            details={
                "run.request_envelope.request_id": envelope_request_id,
                "run.request_id": run.request_id,
            },
        )

    envelope_definition_hash = _require_text(
        request_envelope.get("definition_hash"),
        field_name="run.request_envelope.definition_hash",
    )
    if envelope_definition_hash != run.admitted_definition_hash:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "run.request_envelope.definition_hash must match run.admitted_definition_hash",
            details={
                "run.request_envelope.definition_hash": envelope_definition_hash,
                "run.admitted_definition_hash": run.admitted_definition_hash,
            },
        )

    envelope_schema_version = request_envelope.get("schema_version")
    if envelope_schema_version is not None and _require_positive_int(
        envelope_schema_version,
        field_name="run.request_envelope.schema_version",
    ) != run.schema_version:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "run.request_envelope.schema_version must match run.schema_version",
            details={
                "run.request_envelope.schema_version": envelope_schema_version,
                "run.schema_version": run.schema_version,
            },
        )

    definition_version = request_envelope.get("definition_version", 1)
    if isinstance(definition_version, bool) or not isinstance(definition_version, int):
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "run.request_envelope.definition_version must be an integer",
            details={"field": "run.request_envelope.definition_version"},
        )
    if definition_version <= 0:
        raise PostgresWriteError(
            "postgres.invalid_submission",
            "run.request_envelope.definition_version must be a positive integer",
            details={"field": "run.request_envelope.definition_version"},
        )

    supersedes_workflow_definition_id = _optional_text(
        request_envelope.get("supersedes_workflow_definition_id"),
        field_name="run.request_envelope.supersedes_workflow_definition_id",
    )
    definition_status = "admitted" if decision.decision == "admit" else "rejected"
    request_envelope_json = _encode_jsonb(
        request_envelope,
        field_name="run.request_envelope",
    )
    await _insert_or_assert_workflow_definition(
        conn,
        workflow_definition_id=workflow_definition_id,
        workflow_id=run.workflow_id,
        schema_version=run.schema_version,
        definition_version=definition_version,
        definition_hash=run.admitted_definition_hash,
        definition_status=definition_status,
        request_envelope_json=request_envelope_json,
        created_at=run.admitted_at,
        supersedes_workflow_definition_id=supersedes_workflow_definition_id,
    )

    required_node_keys = (
        "workflow_definition_node_id",
        "node_id",
        "node_type",
        "schema_version",
        "adapter_type",
        "display_name",
        "inputs",
        "expected_outputs",
        "success_condition",
        "failure_behavior",
        "authority_requirements",
        "execution_boundary",
        "position_index",
    )
    required_edge_keys = (
        "workflow_definition_edge_id",
        "edge_id",
        "edge_type",
        "schema_version",
        "from_node_id",
        "to_node_id",
        "release_condition",
        "payload_mapping",
        "position_index",
    )
    missing = object()
    nodes_value = request_envelope.get("nodes", missing)
    if nodes_value is not missing:
        nodes = _require_child_row_payloads(
            nodes_value,
            field_name="run.request_envelope.nodes",
            required_keys=required_node_keys,
        )
        for node in nodes:
            node_workflow_definition_id = node.get("workflow_definition_id")
            if node_workflow_definition_id is None:
                node_workflow_definition_id = workflow_definition_id
            else:
                node_workflow_definition_id = _require_text(
                    node_workflow_definition_id,
                    field_name="run.request_envelope.nodes.workflow_definition_id",
                )
            if node_workflow_definition_id != workflow_definition_id:
                raise PostgresWriteError(
                    "postgres.invalid_submission",
                    "definition node workflow_definition_id must match the parent definition",
                    details={
                        "node.workflow_definition_id": node_workflow_definition_id,
                        "workflow_definition_id": workflow_definition_id,
                    },
                )

            await _insert_or_assert_definition_node(
                conn,
                workflow_definition_node_id=_require_text(
                    node["workflow_definition_node_id"],
                    field_name="run.request_envelope.nodes.workflow_definition_node_id",
                ),
                workflow_definition_id=workflow_definition_id,
                node_id=_require_text(
                    node["node_id"],
                    field_name="run.request_envelope.nodes.node_id",
                ),
                node_type=_require_text(
                    node["node_type"],
                    field_name="run.request_envelope.nodes.node_type",
                ),
                schema_version=_require_positive_int(
                    node["schema_version"],
                    field_name="run.request_envelope.nodes.schema_version",
                ),
                adapter_type=_require_text(
                    node["adapter_type"],
                    field_name="run.request_envelope.nodes.adapter_type",
                ),
                display_name=_require_text(
                    node["display_name"],
                    field_name="run.request_envelope.nodes.display_name",
                ),
                inputs_json=_encode_jsonb(
                    node["inputs"],
                    field_name="run.request_envelope.nodes.inputs",
                ),
                expected_outputs_json=_encode_jsonb(
                    node["expected_outputs"],
                    field_name="run.request_envelope.nodes.expected_outputs",
                ),
                success_condition_json=_encode_jsonb(
                    node["success_condition"],
                    field_name="run.request_envelope.nodes.success_condition",
                ),
                failure_behavior_json=_encode_jsonb(
                    node["failure_behavior"],
                    field_name="run.request_envelope.nodes.failure_behavior",
                ),
                authority_requirements_json=_encode_jsonb(
                    node["authority_requirements"],
                    field_name="run.request_envelope.nodes.authority_requirements",
                ),
                execution_boundary_json=_encode_jsonb(
                    node["execution_boundary"],
                    field_name="run.request_envelope.nodes.execution_boundary",
                ),
                position_index=_require_nonnegative_int(
                    node["position_index"],
                    field_name="run.request_envelope.nodes.position_index",
                ),
            )

    edges_value = request_envelope.get("edges", missing)
    if edges_value is not missing:
        edges = _require_child_row_payloads(
            edges_value,
            field_name="run.request_envelope.edges",
            required_keys=required_edge_keys,
        )
        for edge in edges:
            edge_workflow_definition_id = edge.get("workflow_definition_id")
            if edge_workflow_definition_id is None:
                edge_workflow_definition_id = workflow_definition_id
            else:
                edge_workflow_definition_id = _require_text(
                    edge_workflow_definition_id,
                    field_name="run.request_envelope.edges.workflow_definition_id",
                )
            if edge_workflow_definition_id != workflow_definition_id:
                raise PostgresWriteError(
                    "postgres.invalid_submission",
                    "definition edge workflow_definition_id must match the parent definition",
                    details={
                        "edge.workflow_definition_id": edge_workflow_definition_id,
                        "workflow_definition_id": workflow_definition_id,
                    },
                )

            await _insert_or_assert_definition_edge(
                conn,
                workflow_definition_edge_id=_require_text(
                    edge["workflow_definition_edge_id"],
                    field_name="run.request_envelope.edges.workflow_definition_edge_id",
                ),
                workflow_definition_id=workflow_definition_id,
                edge_id=_require_text(
                    edge["edge_id"],
                    field_name="run.request_envelope.edges.edge_id",
                ),
                edge_type=_require_text(
                    edge["edge_type"],
                    field_name="run.request_envelope.edges.edge_type",
                ),
                schema_version=_require_positive_int(
                    edge["schema_version"],
                    field_name="run.request_envelope.edges.schema_version",
                ),
                from_node_id=_require_text(
                    edge["from_node_id"],
                    field_name="run.request_envelope.edges.from_node_id",
                ),
                to_node_id=_require_text(
                    edge["to_node_id"],
                    field_name="run.request_envelope.edges.to_node_id",
                ),
                release_condition_json=_encode_jsonb(
                    edge["release_condition"],
                    field_name="run.request_envelope.edges.release_condition",
                ),
                payload_mapping_json=_encode_jsonb(
                    edge["payload_mapping"],
                    field_name="run.request_envelope.edges.payload_mapping",
                ),
                position_index=_require_nonnegative_int(
                    edge["position_index"],
                    field_name="run.request_envelope.edges.position_index",
                ),
            )
