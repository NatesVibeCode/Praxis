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
    OperatorFrameReadModel,
    graph_lineage_run,
    graph_topology_run,
)
from receipts import AppendOnlyWorkflowEvidenceWriter, EvidenceRow, ReceiptV1, WorkflowEventV1
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


def _execute_successful_run() -> tuple[object, object, tuple[EvidenceRow, ...], int]:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()
    result = RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )
    return (
        outcome,
        result,
        tuple(writer.evidence_timeline(result.run_id)),
        writer.last_evidence_seq(result.run_id),
    )


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


def _with_runtime_node_order_entry(
    canonical_evidence: tuple[EvidenceRow, ...],
    *,
    source_node_id: str,
    runtime_node_id: str,
) -> tuple[EvidenceRow, ...]:
    mutated_rows: list[EvidenceRow] = []
    for row in canonical_evidence:
        if row.kind == "workflow_event" and isinstance(row.record, WorkflowEventV1):
            if row.record.event_type == "node_started" and row.record.node_id == source_node_id:
                payload = dict(row.record.payload)
                payload["dependency_receipts"] = ()
                mutated_rows.append(
                    replace(
                        row,
                        record=replace(
                            row.record,
                            node_id=runtime_node_id,
                            payload=payload,
                        ),
                    )
                )
                continue
            if row.record.event_type == "node_succeeded" and row.record.node_id == source_node_id:
                mutated_rows.append(
                    replace(
                        row,
                        record=replace(row.record, node_id=runtime_node_id),
                    )
                )
                continue
        if row.kind == "receipt" and isinstance(row.record, ReceiptV1):
            if row.record.receipt_type in {"node_start_receipt", "node_execution_receipt"} and row.record.node_id == source_node_id:
                mutated_rows.append(
                    replace(
                        row,
                        record=replace(row.record, node_id=runtime_node_id),
                    )
                )
                continue
        mutated_rows.append(row)
    return tuple(mutated_rows)


def _assert_lineage_inherits_topology_completeness(
    *,
    run_id: str,
    canonical_evidence: tuple[EvidenceRow, ...],
    expected_missing_ref: str,
) -> None:
    topology = graph_topology_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )
    view = graph_lineage_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )

    assert topology.completeness.is_complete is False
    assert expected_missing_ref in topology.completeness.missing_evidence_refs
    assert view.completeness == topology.completeness


def test_graph_lineage_surface_is_explainable_from_runtime_truth() -> None:
    outcome, result, canonical_evidence, last_evidence_seq = _execute_successful_run()
    view = graph_lineage_run(
        run_id=result.run_id,
        canonical_evidence=canonical_evidence,
    )
    claim_row = _claim_received_row(canonical_evidence)

    assert view.request_id == "request.alpha"
    assert view.completeness.is_complete is True
    assert view.watermark.evidence_seq == last_evidence_seq
    assert view.evidence_refs == tuple(row.row_id for row in canonical_evidence)
    assert view.claim_received_ref == claim_row.row_id
    assert view.admitted_definition_ref == outcome.admitted_definition_ref
    assert view.admitted_definition_hash == outcome.admitted_definition_hash
    assert view.nodes == (
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
    assert view.edges == (
        GraphTopologyEdge(
            edge_id="edge_0",
            edge_type=MINIMAL_WORKFLOW_EDGE_TYPE,
            from_node_id="node_0",
            to_node_id="node_1",
            position_index=0,
        ),
    )
    assert view.runtime_node_order == result.node_order
    assert view.current_state == "succeeded"
    assert view.terminal_reason == "runtime.workflow_succeeded"


def test_graph_lineage_can_carry_operator_frame_summaries_without_receipt_reconstruction() -> None:
    _, result, canonical_evidence, _last_evidence_seq = _execute_successful_run()
    operator_frames = (
        OperatorFrameReadModel(
            operator_frame_id="operator_frame.alpha",
            node_id="foreach",
            operator_kind="foreach",
            frame_state="succeeded",
            item_index=0,
            source_snapshot={"item": "alpha"},
            aggregate_outputs={"item": "alpha"},
            active_count=0,
        ),
    )

    view = graph_lineage_run(
        run_id=result.run_id,
        canonical_evidence=canonical_evidence,
        operator_frame_source="canonical_operator_frames",
        operator_frames=operator_frames,
    )

    assert view.operator_frame_source == "canonical_operator_frames"
    assert view.operator_frames == operator_frames


def test_graph_lineage_inherits_topology_completeness_on_duplicate_node_ids() -> None:
    _, result, canonical_evidence, _ = _execute_successful_run()
    nodes = list(_claim_nodes(canonical_evidence))
    nodes[1]["node_id"] = nodes[0]["node_id"]

    _assert_lineage_inherits_topology_completeness(
        run_id=result.run_id,
        canonical_evidence=_with_claim_envelope(
            canonical_evidence,
            nodes=tuple(nodes),
        ),
        expected_missing_ref="graph:duplicate_node_id:node_0",
    )

def test_graph_lineage_inherits_topology_completeness_on_dangling_edges() -> None:
    _, result, canonical_evidence, _ = _execute_successful_run()
    edges = list(_claim_edges(canonical_evidence))
    edges[0]["to_node_id"] = "node_missing"

    _assert_lineage_inherits_topology_completeness(
        run_id=result.run_id,
        canonical_evidence=_with_claim_envelope(
            canonical_evidence,
            edges=tuple(edges),
        ),
        expected_missing_ref="graph:dangling_edge_to:edge_0:node_missing",
    )

def test_graph_lineage_inherits_topology_completeness_when_runtime_order_leaves_topology() -> None:
    _, result, canonical_evidence, _ = _execute_successful_run()

    _assert_lineage_inherits_topology_completeness(
        run_id=result.run_id,
        canonical_evidence=_with_runtime_node_order_entry(
            canonical_evidence,
            source_node_id="node_1",
            runtime_node_id="node_2",
        ),
        expected_missing_ref="graph:runtime_node_order_unknown:node_2",
    )
