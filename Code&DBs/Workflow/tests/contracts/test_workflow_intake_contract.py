from __future__ import annotations

from contracts.domain import (
    MINIMAL_WORKFLOW_EDGE_TYPE,
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
    validate_workflow_request,
)


def _node(
    *,
    node_id: str,
    position_index: int,
    display_name: str,
    adapter_type: str = MINIMAL_WORKFLOW_NODE_TYPE,
    inputs: dict | None = None,
    expected_outputs: dict | None = None,
    template_owner_node_id: str | None = None,
) -> WorkflowNodeContract:
    workspace_ref = "workspace.alpha"
    runtime_profile_ref = "runtime_profile.alpha"
    return WorkflowNodeContract(
        node_id=node_id,
        node_type=MINIMAL_WORKFLOW_NODE_TYPE,
        adapter_type=adapter_type,
        display_name=display_name,
        inputs=inputs or {"task_name": display_name},
        expected_outputs=expected_outputs or {},
        success_condition={"status": "success"},
        failure_behavior={"status": "fail_closed"},
        authority_requirements={
            "workspace_ref": workspace_ref,
            "runtime_profile_ref": runtime_profile_ref,
        },
        execution_boundary={"workspace_ref": workspace_ref},
        position_index=position_index,
        template_owner_node_id=template_owner_node_id,
    )


def _edge(
    *,
    edge_id: str,
    from_node_id: str,
    to_node_id: str,
    position_index: int,
    edge_type: str = MINIMAL_WORKFLOW_EDGE_TYPE,
    release_condition: dict | None = None,
    payload_mapping: dict | None = None,
    template_owner_node_id: str | None = None,
) -> WorkflowEdgeContract:
    return WorkflowEdgeContract(
        edge_id=edge_id,
        edge_type=edge_type,
        from_node_id=from_node_id,
        to_node_id=to_node_id,
        release_condition=release_condition or {"upstream_result": "success"},
        payload_mapping=payload_mapping or {},
        position_index=position_index,
        template_owner_node_id=template_owner_node_id,
    )


def _valid_request() -> WorkflowRequest:
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
                inputs={"task_name": "prepare"},
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
                inputs={"task_name": "admit"},
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
                payload_mapping={},
                position_index=0,
            ),
        ),
    )


def test_validate_workflow_request_accepts_the_minimal_slice_graph() -> None:
    result = validate_workflow_request(_valid_request())

    assert result.is_valid is True
    assert result.reason_code == "request.valid"
    assert result.errors == ()
    assert result.normalized_request is not None
    assert result.normalized_request.nodes[0].node_id == "node_0"
    assert result.normalized_request.edges[0].edge_id == "edge_0"
    assert result.validation_result_ref.startswith("validation:")


def test_workflow_request_json_round_trips_cleanly() -> None:
    request = _valid_request()

    round_tripped = WorkflowRequest.from_json(request.to_json())

    assert round_tripped == request
    assert round_tripped.to_json() == request.to_json()


def test_validate_workflow_request_rejects_malformed_collection_shape() -> None:
    valid_request = _valid_request()
    malformed_request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:1111222233334444",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=None,
        edges=valid_request.edges,
    )

    result = validate_workflow_request(malformed_request)

    assert result.is_valid is False
    assert result.reason_code == "request.schema_invalid"
    assert result.errors == ("request.schema_invalid",)
    assert result.normalized_request is None


def test_validate_workflow_request_rejects_an_invalid_graph() -> None:
    valid_request = _valid_request()
    invalid_request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.alpha",
        request_id="request.alpha",
        workflow_definition_id="workflow_definition.alpha.v1",
        definition_hash="sha256:1111222233334444",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=valid_request.nodes,
        edges=(
            WorkflowEdgeContract(
                edge_id="edge_0",
                edge_type=MINIMAL_WORKFLOW_EDGE_TYPE,
                from_node_id="node_0",
                to_node_id="node_missing",
                release_condition={"upstream_result": "success"},
                payload_mapping={},
                position_index=0,
            ),
        ),
    )

    result = validate_workflow_request(invalid_request)

    assert result.is_valid is False
    assert result.reason_code == "request.graph_invalid"
    assert "request.graph_invalid" in result.errors
    assert result.normalized_request is None


def test_validate_workflow_request_accepts_foreach_operator_with_template_graph() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.foreach",
        request_id="request.foreach",
        workflow_definition_id="workflow_definition.foreach.v1",
        definition_hash="sha256:foreach",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"items": ["a", "b"]},
            ),
            _node(
                node_id="foreach",
                position_index=1,
                display_name="foreach",
                adapter_type="control_operator",
                inputs={
                    "task_name": "foreach",
                    "operator": {
                        "kind": "foreach",
                        "source_ref": {"from_node_id": "source", "output_key": "items"},
                        "max_items": 5,
                        "max_parallel": 2,
                        "aggregate_mode": "ordered_results",
                        "result_key": "results",
                    },
                },
            ),
            _node(
                node_id="foreach_body",
                position_index=2,
                display_name="foreach_body",
                template_owner_node_id="foreach",
            ),
        ),
        edges=(
            _edge(
                edge_id="edge_source_foreach",
                from_node_id="source",
                to_node_id="foreach",
                position_index=0,
            ),
        ),
    )

    result = validate_workflow_request(request)

    assert result.is_valid is True
    assert result.normalized_request is not None
    assert any(node.node_id == "foreach" for node in result.normalized_request.nodes)
    assert any(
        node.node_id == "foreach_body" and node.template_owner_node_id == "foreach"
        for node in result.normalized_request.nodes
    )


def test_validate_workflow_request_lowers_static_if_helper_to_conditional_edges() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.if",
        request_id="request.if",
        workflow_definition_id="workflow_definition.if.v1",
        definition_hash="sha256:if",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"flag": True},
            ),
            _node(
                node_id="route_if",
                position_index=1,
                display_name="route_if",
                adapter_type="control_operator",
                inputs={
                    "task_name": "route_if",
                    "operator": {
                        "kind": "if",
                        "predicate": {"field": "flag", "op": "equals", "value": True},
                    },
                },
            ),
            _node(node_id="then_node", position_index=2, display_name="then_node"),
            _node(node_id="else_node", position_index=3, display_name="else_node"),
        ),
        edges=(
            _edge(
                edge_id="edge_source_if",
                from_node_id="source",
                to_node_id="route_if",
                position_index=0,
            ),
            _edge(
                edge_id="edge_if_then",
                from_node_id="route_if",
                to_node_id="then_node",
                position_index=1,
                release_condition={"branch": "then"},
            ),
            _edge(
                edge_id="edge_if_else",
                from_node_id="route_if",
                to_node_id="else_node",
                position_index=2,
                release_condition={"branch": "else"},
            ),
        ),
    )

    result = validate_workflow_request(request)

    assert result.is_valid is True
    assert result.normalized_request is not None
    assert all(node.node_id != "route_if" for node in result.normalized_request.nodes)
    conditional_edges = [
        edge
        for edge in result.normalized_request.edges
        if edge.edge_type == "conditional"
    ]
    assert len(conditional_edges) == 2
    assert {edge.to_node_id for edge in conditional_edges} == {"then_node", "else_node"}


def test_validate_workflow_request_rejects_static_control_flow_payload_key_collisions() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.join.collision",
        request_id="request.join.collision",
        workflow_definition_id="workflow_definition.join.collision.v1",
        definition_hash="sha256:join-collision",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(node_id="left", position_index=0, display_name="left"),
            _node(node_id="right", position_index=1, display_name="right"),
            _node(
                node_id="join_all",
                position_index=2,
                display_name="join_all",
                adapter_type="control_operator",
                inputs={"task_name": "join_all", "operator": {"kind": "join_all"}},
            ),
            _node(node_id="sink", position_index=3, display_name="sink"),
        ),
        edges=(
            _edge(
                edge_id="edge_left_join",
                from_node_id="left",
                to_node_id="join_all",
                position_index=0,
                payload_mapping={"shared": "left_value"},
            ),
            _edge(
                edge_id="edge_right_join",
                from_node_id="right",
                to_node_id="join_all",
                position_index=1,
                payload_mapping={"shared": "right_value"},
            ),
            _edge(
                edge_id="edge_join_sink",
                from_node_id="join_all",
                to_node_id="sink",
                position_index=2,
                payload_mapping={"shared": "joined_value"},
            ),
        ),
    )

    result = validate_workflow_request(request)

    assert result.is_valid is False
    assert result.reason_code == "request.graph_invalid"
    assert result.normalized_request is None


def test_validate_workflow_request_rejects_malformed_control_operator_config() -> None:
    request = WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id="workflow.foreach.invalid",
        request_id="request.foreach.invalid",
        workflow_definition_id="workflow_definition.foreach.invalid.v1",
        definition_hash="sha256:foreach-invalid",
        workspace_ref="workspace.alpha",
        runtime_profile_ref="runtime_profile.alpha",
        nodes=(
            _node(
                node_id="source",
                position_index=0,
                display_name="source",
                expected_outputs={"items": ["a", "b"]},
            ),
            _node(
                node_id="foreach",
                position_index=1,
                display_name="foreach",
                adapter_type="control_operator",
                inputs={
                    "task_name": "foreach",
                    "operator": {
                        "kind": "foreach",
                        "source_ref": {"from_node_id": "source", "output_key": "items"},
                        "max_items": 5,
                        "max_parallel": 0,
                        "aggregate_mode": "ordered_results",
                        "result_key": "results",
                    },
                },
            ),
            _node(
                node_id="foreach_body",
                position_index=2,
                display_name="foreach_body",
                template_owner_node_id="foreach",
            ),
        ),
        edges=(
            _edge(
                edge_id="edge_source_foreach",
                from_node_id="source",
                to_node_id="foreach",
                position_index=0,
            ),
        ),
    )

    result = validate_workflow_request(request)

    assert result.is_valid is False
    assert result.reason_code == "request.graph_invalid"
    assert result.normalized_request is None
