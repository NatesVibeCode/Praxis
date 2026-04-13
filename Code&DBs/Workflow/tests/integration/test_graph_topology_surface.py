from __future__ import annotations

from dataclasses import replace

from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from observability import (
    GraphTopologyEdge,
    GraphTopologyNode,
    graph_topology_run,
    replay_run,
)
from receipts import AppendOnlyWorkflowEvidenceWriter, EvidenceRow, WorkflowEventV1
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RuntimeOrchestrator, WorkflowIntakePlanner


def _request() -> WorkflowRequest:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:1111222233334444",
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        nodes=(
            WorkflowNodeContract(
                node_id="node_0",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="prepare",
                inputs={
                    "task_name": "prepare",
                    "input_payload": {"step": 0},
                },
                expected_outputs={"result": "prepared"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": workspace_ref},
                position_index=0,
            ),
            WorkflowNodeContract(
                node_id="node_1",
                node_type=MINIMAL_WORKFLOW_NODE_TYPE,
                adapter_type=MINIMAL_WORKFLOW_NODE_TYPE,
                display_name="admit",
                inputs={
                    "task_name": "admit",
                    "input_payload": {"step": 1},
                },
                expected_outputs={"result": "admitted"},
                success_condition={"status": "success"},
                failure_behavior={"status": "fail_closed"},
                authority_requirements={
                    "workspace_ref": workspace_ref,
                    "runtime_profile_ref": runtime_profile_ref,
                },
                execution_boundary={"workspace_ref": workspace_ref},
                position_index=1,
            ),
        ),
        edges=(
            WorkflowEdgeContract(
                edge_id="edge_0",
                edge_type=MINIMAL_WORKFLOW_EDGE_TYPE,
                from_node_id="node_0",
                to_node_id="node_1",
                release_condition={"upstream_result": "success"},
                payload_mapping={"prepared_result": "result"},
                position_index=0,
            ),
        ),
    )


def _resolver() -> RegistryResolver:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return RegistryResolver(
        workspace_records={
            workspace_ref: (
                WorkspaceAuthorityRecord(
                    workspace_ref=workspace_ref,
                    repo_root="/tmp/workspace.alpha",
                    workdir="/tmp/workspace.alpha/workdir",
                ),
            ),
        },
        runtime_profile_records={
            runtime_profile_ref: (
                RuntimeProfileAuthorityRecord(
                    runtime_profile_ref=runtime_profile_ref,
                    model_profile_id="model.alpha",
                    provider_policy_id="provider_policy.alpha",
                ),
            ),
        },
    )


def _execute_successful_run() -> tuple[AppendOnlyWorkflowEvidenceWriter, str]:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()
    RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )
    return writer, outcome.run_id


def _canonical_evidence() -> tuple[str, tuple[object, ...]]:
    writer, run_id = _execute_successful_run()
    return run_id, tuple(writer.evidence_timeline(run_id))


def _claim_received_row(canonical_evidence: tuple[EvidenceRow, ...]) -> EvidenceRow:
    for row in canonical_evidence:
        if row.kind == "workflow_event" and isinstance(row.record, WorkflowEventV1):
            if row.record.event_type == "claim_received":
                return row
    raise AssertionError("claim_received row not found")


def _claim_nodes(canonical_evidence: tuple[EvidenceRow, ...]) -> tuple[dict[str, object], ...]:
    claim_row = _claim_received_row(canonical_evidence)
    nodes = claim_row.record.payload["claim_envelope"]["nodes"]
    return tuple(dict(node) for node in nodes)


def _claim_edges(canonical_evidence: tuple[EvidenceRow, ...]) -> tuple[dict[str, object], ...]:
    claim_row = _claim_received_row(canonical_evidence)
    edges = claim_row.record.payload["claim_envelope"]["edges"]
    return tuple(dict(edge) for edge in edges)


def _with_claim_envelope(
    canonical_evidence: tuple[EvidenceRow, ...],
    *,
    nodes: tuple[dict[str, object], ...] | None = None,
    edges: tuple[dict[str, object], ...] | None = None,
) -> tuple[EvidenceRow, ...]:
    claim_row = _claim_received_row(canonical_evidence)
    payload = dict(claim_row.record.payload)
    claim_envelope = dict(payload["claim_envelope"])
    if nodes is not None:
        claim_envelope["nodes"] = nodes
    if edges is not None:
        claim_envelope["edges"] = edges
    payload["claim_envelope"] = claim_envelope
    mutated_event = replace(claim_row.record, payload=payload)
    return tuple(
        replace(row, record=mutated_event) if row.row_id == claim_row.row_id else row
        for row in canonical_evidence
    )


def _without_claim_received_event(
    canonical_evidence: tuple[object, ...],
) -> tuple[object, ...]:
    return tuple(
        row
        for row in canonical_evidence
        if not (
            getattr(row, "kind", None) == "workflow_event"
            and getattr(getattr(row, "record", None), "event_type", None) == "claim_received"
        )
    )


def _with_malformed_claim_envelope(
    canonical_evidence: tuple[object, ...],
) -> tuple[object, ...]:
    mutated = list(canonical_evidence)
    for index, row in enumerate(mutated):
        if getattr(row, "kind", None) != "workflow_event":
            continue
        if getattr(getattr(row, "record", None), "event_type", None) != "claim_received":
            continue
        mutated[index] = replace(
            row,
            record=replace(
                row.record,
                payload={
                    **row.record.payload,
                    "claim_envelope": "broken-envelope",
                },
            ),
        )
        break
    return tuple(mutated)


def _with_runtime_order_mismatch(
    canonical_evidence: tuple[EvidenceRow, ...],
) -> tuple[EvidenceRow, ...]:
    claim_row = _claim_received_row(canonical_evidence)
    claim_envelope = claim_row.record.payload["claim_envelope"]
    return _with_claim_envelope(
        canonical_evidence,
        nodes=(dict(claim_envelope["nodes"][0]),),
        edges=(),
    )


def test_graph_topology_view_is_explainable_from_runtime_truth() -> None:
    writer, run_id = _execute_successful_run()
    canonical_evidence = tuple(writer.evidence_timeline(run_id))

    topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )
    replay = replay_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )

    assert topology.request_id == "request.alpha"
    assert topology.completeness.is_complete is True
    assert topology.watermark.evidence_seq == writer.last_evidence_seq(run_id)
    assert topology.evidence_refs == tuple(row.row_id for row in canonical_evidence)
    assert topology.admitted_definition_ref == "workflow_definition.alpha.v1"
    assert topology.nodes == (
        GraphTopologyNode(
            node_id="node_0",
            node_type=MINIMAL_WORKFLOW_NODE_TYPE,
            display_name="prepare",
            position_index=0,
        ),
        GraphTopologyNode(
            node_id="node_1",
            node_type=MINIMAL_WORKFLOW_NODE_TYPE,
            display_name="admit",
            position_index=1,
        ),
    )
    assert topology.edges == (
        GraphTopologyEdge(
            edge_id="edge_0",
            edge_type=MINIMAL_WORKFLOW_EDGE_TYPE,
            from_node_id="node_0",
            to_node_id="node_1",
            position_index=0,
        ),
    )
    assert topology.runtime_node_order == replay.dependency_order == ("node_0", "node_1")


def test_graph_topology_view_marks_missing_claim_received_as_incomplete() -> None:
    run_id, canonical_evidence = _canonical_evidence()
    topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=_without_claim_received_event(canonical_evidence),
    )

    assert topology.completeness.is_complete is False
    assert "graph:claim_received_event" in topology.completeness.missing_evidence_refs
    assert topology.watermark.evidence_seq is not None


def test_graph_topology_view_marks_malformed_claim_envelope_as_incomplete() -> None:
    run_id, canonical_evidence = _canonical_evidence()
    topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=_with_malformed_claim_envelope(canonical_evidence),
    )

    assert topology.completeness.is_complete is False
    assert "graph:claim_envelope" in topology.completeness.missing_evidence_refs
    assert topology.nodes == ()
    assert topology.edges == ()


def test_graph_topology_view_marks_runtime_order_mismatch_as_incomplete() -> None:
    run_id, canonical_evidence = _canonical_evidence()
    topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=_with_runtime_order_mismatch(canonical_evidence),
    )

    assert topology.completeness.is_complete is False
    assert "graph:runtime_node_order" in topology.completeness.missing_evidence_refs
    assert topology.nodes == (
        GraphTopologyNode(
            node_id="node_0",
            node_type=MINIMAL_WORKFLOW_NODE_TYPE,
            display_name="prepare",
            position_index=0,
        ),
    )
    assert topology.runtime_node_order == ("node_0", "node_1")


def test_graph_topology_view_fails_closed_on_duplicate_node_ids() -> None:
    run_id, canonical_evidence = _canonical_evidence()
    nodes = list(_claim_nodes(canonical_evidence))
    nodes[1]["node_id"] = nodes[0]["node_id"]

    topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=_with_claim_envelope(
            canonical_evidence,
            nodes=tuple(nodes),
        ),
    )

    assert topology.completeness.is_complete is False
    assert "graph:duplicate_node_id:node_0" in topology.completeness.missing_evidence_refs


def test_graph_topology_view_fails_closed_on_duplicate_position_indexes() -> None:
    run_id, canonical_evidence = _canonical_evidence()
    nodes = list(_claim_nodes(canonical_evidence))
    nodes[1]["node_id"] = "node_1_duplicate"
    nodes[1]["position_index"] = nodes[0]["position_index"]

    topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=_with_claim_envelope(
            canonical_evidence,
            nodes=tuple(nodes),
        ),
    )

    assert topology.completeness.is_complete is False
    assert "graph:duplicate_node_position_index:0" in topology.completeness.missing_evidence_refs


def test_graph_topology_view_fails_closed_on_dangling_edges() -> None:
    run_id, canonical_evidence = _canonical_evidence()
    edges = list(_claim_edges(canonical_evidence))
    edges[0]["to_node_id"] = "node_missing"

    topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=_with_claim_envelope(
            canonical_evidence,
            edges=tuple(edges),
        ),
    )

    assert topology.completeness.is_complete is False
    assert "graph:dangling_edge_to:edge_0:node_missing" in topology.completeness.missing_evidence_refs
