from __future__ import annotations

import importlib
from dataclasses import dataclass, field, replace
from io import StringIO

from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from observability import graph_lineage_run, graph_topology_run
from observability.read_models import ProjectionCompleteness
from receipts import AppendOnlyWorkflowEvidenceWriter, EvidenceRow
from registry.domain import (
    RegistryResolver,
    RuntimeProfileAuthorityRecord,
    WorkspaceAuthorityRecord,
)
from runtime import RuntimeOrchestrator, WorkflowIntakePlanner
from surfaces.cli.main import main


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
                    "input_payload": {"step": 0, "allow_passthrough_echo": True},
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
                    "input_payload": {"step": 1, "allow_passthrough_echo": True},
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
                    sandbox_profile_ref=runtime_profile_ref,
                ),
            ),
        },
    )


def _execute_successful_run() -> tuple[str, tuple[EvidenceRow, ...]]:
    planner = WorkflowIntakePlanner(registry=_resolver())
    outcome = planner.plan(request=_request())
    writer = AppendOnlyWorkflowEvidenceWriter()
    RuntimeOrchestrator().execute_deterministic_path(
        intake_outcome=outcome,
        evidence_writer=writer,
    )
    return outcome.run_id, tuple(writer.evidence_timeline(outcome.run_id))


@dataclass
class _GraphSurfaceService:
    canonical_evidence: tuple[EvidenceRow, ...]
    topology_calls: list[str] = field(default_factory=list)
    lineage_calls: list[str] = field(default_factory=list)
    incomplete_topology_calls: set[str] = field(default_factory=set)

    def graph_topology_run(self, *, run_id: str):
        self.topology_calls.append(run_id)
        view = graph_topology_run(
            run_id=run_id,
            canonical_evidence=self.canonical_evidence,
        )
        if run_id in self.incomplete_topology_calls:
            return replace(
                view,
                completeness=ProjectionCompleteness(
                    is_complete=False,
                    missing_evidence_refs=(
                        "graph:node_shape",
                        "graph:runtime_node_order",
                    ),
                ),
            )
        return view

    def graph_lineage_run(self, *, run_id: str):
        self.lineage_calls.append(run_id)
        return graph_lineage_run(
            run_id=run_id,
            canonical_evidence=self.canonical_evidence,
        )


def _assert_lines(rendered: str, expected: list[str]) -> None:
    assert rendered.splitlines() == expected


def _topology_expected_lines(view) -> list[str]:
    lines = [
        "kind: graph_topology",
        f"run_id: {view.run_id}",
        f"request_id: {view.request_id}",
        f"completeness.is_complete: {'true' if view.completeness.is_complete else 'false'}",
        f"completeness.missing_evidence_refs_count: {len(view.completeness.missing_evidence_refs)}",
    ]
    lines.extend(
        f"completeness.missing_evidence_refs[{index}]: {ref}"
        for index, ref in enumerate(view.completeness.missing_evidence_refs)
    )
    lines.extend(
        [
            f"watermark.evidence_seq: {view.watermark.evidence_seq}",
            f"watermark.source: {view.watermark.source}",
            f"evidence_refs_count: {len(view.evidence_refs)}",
        ]
    )
    lines.extend(
        f"evidence_refs[{index}]: {ref}" for index, ref in enumerate(view.evidence_refs)
    )
    lines.extend(
        [
            f"admitted_definition_ref: {view.admitted_definition_ref}",
            f"nodes_count: {len(view.nodes)}",
        ]
    )
    for index, node in enumerate(view.nodes):
        lines.extend(
            [
                f"nodes[{index}].node_id: {node.node_id}",
                f"nodes[{index}].node_type: {node.node_type}",
                f"nodes[{index}].display_name: {node.display_name}",
                f"nodes[{index}].position_index: {node.position_index}",
            ]
        )
    lines.append(f"edges_count: {len(view.edges)}")
    for index, edge in enumerate(view.edges):
        lines.extend(
            [
                f"edges[{index}].edge_id: {edge.edge_id}",
                f"edges[{index}].edge_type: {edge.edge_type}",
                f"edges[{index}].from_node_id: {edge.from_node_id}",
                f"edges[{index}].to_node_id: {edge.to_node_id}",
                f"edges[{index}].position_index: {edge.position_index}",
            ]
        )
    lines.extend(
        [
            f"runtime_node_order_count: {len(view.runtime_node_order)}",
            *(
                f"runtime_node_order[{index}]: {node_id}"
                for index, node_id in enumerate(view.runtime_node_order)
            ),
        ]
    )
    return lines


def _lineage_expected_lines(view) -> list[str]:
    lines = [
        "kind: graph_lineage",
        f"run_id: {view.run_id}",
        f"request_id: {view.request_id}",
        f"completeness.is_complete: {'true' if view.completeness.is_complete else 'false'}",
        f"completeness.missing_evidence_refs_count: {len(view.completeness.missing_evidence_refs)}",
    ]
    lines.extend(
        f"completeness.missing_evidence_refs[{index}]: {ref}"
        for index, ref in enumerate(view.completeness.missing_evidence_refs)
    )
    lines.extend(
        [
            f"watermark.evidence_seq: {view.watermark.evidence_seq}",
            f"watermark.source: {view.watermark.source}",
            f"evidence_refs_count: {len(view.evidence_refs)}",
        ]
    )
    lines.extend(
        f"evidence_refs[{index}]: {ref}" for index, ref in enumerate(view.evidence_refs)
    )
    lines.extend(
        [
            f"claim_received_ref: {view.claim_received_ref}",
            f"admitted_definition_ref: {view.admitted_definition_ref}",
            f"admitted_definition_hash: {view.admitted_definition_hash}",
            f"nodes_count: {len(view.nodes)}",
        ]
    )
    for index, node in enumerate(view.nodes):
        lines.extend(
            [
                f"nodes[{index}].node_id: {node.node_id}",
                f"nodes[{index}].node_type: {node.node_type}",
                f"nodes[{index}].display_name: {node.display_name}",
                f"nodes[{index}].position_index: {node.position_index}",
            ]
        )
    lines.append(f"edges_count: {len(view.edges)}")
    for index, edge in enumerate(view.edges):
        lines.extend(
            [
                f"edges[{index}].edge_id: {edge.edge_id}",
                f"edges[{index}].edge_type: {edge.edge_type}",
                f"edges[{index}].from_node_id: {edge.from_node_id}",
                f"edges[{index}].to_node_id: {edge.to_node_id}",
                f"edges[{index}].position_index: {edge.position_index}",
            ]
        )
    lines.extend(
        [
            f"runtime_node_order_count: {len(view.runtime_node_order)}",
            *(
                f"runtime_node_order[{index}]: {node_id}"
                for index, node_id in enumerate(view.runtime_node_order)
            ),
            f"current_state: {view.current_state}",
            f"terminal_reason: {view.terminal_reason}",
            f"operator_frame_source: {view.operator_frame_source}",
            f"operator_frames_count: {len(view.operator_frames)}",
        ]
    )
    return lines


def test_cli_renders_graph_topology_and_lineage_surfaces() -> None:
    run_id, canonical_evidence = _execute_successful_run()
    service = _GraphSurfaceService(canonical_evidence=canonical_evidence)
    topology_view = graph_topology_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )
    lineage_view = graph_lineage_run(
        run_id=run_id,
        canonical_evidence=canonical_evidence,
    )

    topology_stdout = StringIO()
    topology_exit = main(
        ["graph-topology", run_id],
        graph_service=service,
        stdout=topology_stdout,
    )

    lineage_stdout = StringIO()
    lineage_exit = main(
        ["graph-lineage", run_id],
        graph_service=service,
        stdout=lineage_stdout,
    )

    assert topology_exit == 0
    assert lineage_exit == 0
    assert service.topology_calls == [run_id]
    assert service.lineage_calls == [run_id]

    topology_rendered = topology_stdout.getvalue()
    _assert_lines(topology_rendered, _topology_expected_lines(topology_view))

    lineage_rendered = lineage_stdout.getvalue()
    _assert_lines(lineage_rendered, _lineage_expected_lines(lineage_view))

    incomplete_service = _GraphSurfaceService(canonical_evidence=canonical_evidence)
    incomplete_service.incomplete_topology_calls.add(run_id)
    incomplete_stdout = StringIO()
    incomplete_exit = main(
        ["graph-topology", run_id],
        graph_service=incomplete_service,
        stdout=incomplete_stdout,
    )

    assert incomplete_exit == 0
    assert incomplete_service.topology_calls == [run_id]
    incomplete_view = replace(
        topology_view,
        completeness=ProjectionCompleteness(
            is_complete=False,
            missing_evidence_refs=(
                "graph:node_shape",
                "graph:runtime_node_order",
            ),
        ),
    )
    _assert_lines(
        incomplete_stdout.getvalue(),
        _topology_expected_lines(incomplete_view),
    )


def test_cli_graph_frontdoor_self_wires_default_observability_service(monkeypatch) -> None:
    run_id, canonical_evidence = _execute_successful_run()
    service = _GraphSurfaceService(canonical_evidence=canonical_evidence)
    stdout = StringIO()
    helper_calls: list[dict[str, str]] = []
    cli_main_module = importlib.import_module("surfaces.cli.main")

    def _fake_builder(*, env):
        helper_calls.append(dict(env or {}))
        return service

    monkeypatch.setattr(cli_main_module, "_build_default_observability_service", _fake_builder)

    exit_code = main(
        ["graph-topology", run_id],
        env={"WORKFLOW_DATABASE_URL": "postgresql://example/praxis"},
        stdout=stdout,
    )

    assert exit_code == 0
    assert helper_calls == [{"WORKFLOW_DATABASE_URL": "postgresql://example/praxis"}]
    assert service.topology_calls == [run_id]
    assert "kind: graph_topology" in stdout.getvalue()
