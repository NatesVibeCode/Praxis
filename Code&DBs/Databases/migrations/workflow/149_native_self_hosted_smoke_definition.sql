-- Migration 149: seed the native self-hosted smoke definition template
--
-- The repo-local `workflow native-operator smoke` surface loads one canonical
-- template definition from DB authority before it isolates ids and submits a
-- live run. Fresh databases need this template row to exist; otherwise the
-- smoke front door fails closed before it can prove anything.

WITH smoke_definition AS (
    SELECT
        'workflow_definition.native_self_hosted_smoke.v1'::text AS workflow_definition_id,
        'workflow.native-self-hosted-smoke'::text AS workflow_id,
        'definition.native_self_hosted_smoke.v1'::text AS definition_hash,
        TIMESTAMPTZ '2026-04-16 00:00:00+00' AS created_at,
        jsonb_build_object(
            'schema_version', 1,
            'workflow_id', 'workflow.native-self-hosted-smoke',
            'request_id', 'request.native-self-hosted-smoke',
            'workflow_definition_id', 'workflow_definition.native_self_hosted_smoke.v1',
            'definition_version', 1,
            'definition_hash', 'definition.native_self_hosted_smoke.v1',
            'workspace_ref', 'praxis',
            'runtime_profile_ref', 'praxis',
            'nodes', jsonb_build_array(
                jsonb_build_object(
                    'workflow_definition_node_id', 'workflow_definition.native_self_hosted_smoke.v1:node_0',
                    'workflow_definition_id', 'workflow_definition.native_self_hosted_smoke.v1',
                    'node_id', 'node_0',
                    'node_type', 'deterministic_task',
                    'schema_version', 1,
                    'adapter_type', 'deterministic_task',
                    'display_name', 'prepare',
                    'inputs', jsonb_build_object(
                        'task_name', 'prepare',
                        'input_payload', jsonb_build_object(
                            'step', 0,
                            'allow_passthrough_echo', true
                        )
                    ),
                    'expected_outputs', jsonb_build_object('result', 'prepared'),
                    'success_condition', jsonb_build_object('kind', 'always'),
                    'failure_behavior', jsonb_build_object('kind', 'stop'),
                    'authority_requirements', jsonb_build_object(
                        'workspace_ref', 'praxis',
                        'runtime_profile_ref', 'praxis'
                    ),
                    'execution_boundary', jsonb_build_object('workspace_ref', 'praxis'),
                    'position_index', 0
                ),
                jsonb_build_object(
                    'workflow_definition_node_id', 'workflow_definition.native_self_hosted_smoke.v1:node_1',
                    'workflow_definition_id', 'workflow_definition.native_self_hosted_smoke.v1',
                    'node_id', 'node_1',
                    'node_type', 'deterministic_task',
                    'schema_version', 1,
                    'adapter_type', 'deterministic_task',
                    'display_name', 'persist',
                    'inputs', jsonb_build_object(
                        'task_name', 'persist',
                        'input_payload', jsonb_build_object(
                            'step', 1,
                            'allow_passthrough_echo', true
                        )
                    ),
                    'expected_outputs', jsonb_build_object('result', 'persisted'),
                    'success_condition', jsonb_build_object('kind', 'always'),
                    'failure_behavior', jsonb_build_object('kind', 'stop'),
                    'authority_requirements', jsonb_build_object(
                        'workspace_ref', 'praxis',
                        'runtime_profile_ref', 'praxis'
                    ),
                    'execution_boundary', jsonb_build_object('workspace_ref', 'praxis'),
                    'position_index', 1
                )
            ),
            'edges', jsonb_build_array(
                jsonb_build_object(
                    'workflow_definition_edge_id', 'workflow_definition.native_self_hosted_smoke.v1:edge_0',
                    'workflow_definition_id', 'workflow_definition.native_self_hosted_smoke.v1',
                    'edge_id', 'edge_0',
                    'edge_type', 'after_success',
                    'schema_version', 1,
                    'from_node_id', 'node_0',
                    'to_node_id', 'node_1',
                    'release_condition', jsonb_build_object('kind', 'always'),
                    'payload_mapping', jsonb_build_object('prepared_result', 'result'),
                    'position_index', 0
                )
            )
        ) AS request_envelope
)
INSERT INTO workflow_definitions (
    workflow_definition_id,
    workflow_id,
    schema_version,
    definition_version,
    definition_hash,
    status,
    request_envelope,
    normalized_definition,
    created_at
)
SELECT
    workflow_definition_id,
    workflow_id,
    1,
    1,
    definition_hash,
    'active',
    request_envelope,
    request_envelope,
    created_at
FROM smoke_definition
ON CONFLICT (workflow_definition_id) DO UPDATE
SET workflow_id = EXCLUDED.workflow_id,
    schema_version = EXCLUDED.schema_version,
    definition_version = EXCLUDED.definition_version,
    definition_hash = EXCLUDED.definition_hash,
    status = EXCLUDED.status,
    request_envelope = EXCLUDED.request_envelope,
    normalized_definition = EXCLUDED.normalized_definition;

WITH smoke_definition AS (
    SELECT jsonb_build_array(
        jsonb_build_object(
            'workflow_definition_node_id', 'workflow_definition.native_self_hosted_smoke.v1:node_0',
            'workflow_definition_id', 'workflow_definition.native_self_hosted_smoke.v1',
            'node_id', 'node_0',
            'node_type', 'deterministic_task',
            'schema_version', 1,
            'adapter_type', 'deterministic_task',
            'display_name', 'prepare',
            'inputs', jsonb_build_object(
                'task_name', 'prepare',
                'input_payload', jsonb_build_object(
                    'step', 0,
                    'allow_passthrough_echo', true
                )
            ),
            'expected_outputs', jsonb_build_object('result', 'prepared'),
            'success_condition', jsonb_build_object('kind', 'always'),
            'failure_behavior', jsonb_build_object('kind', 'stop'),
            'authority_requirements', jsonb_build_object(
                'workspace_ref', 'praxis',
                'runtime_profile_ref', 'praxis'
            ),
            'execution_boundary', jsonb_build_object('workspace_ref', 'praxis'),
            'position_index', 0
        ),
        jsonb_build_object(
            'workflow_definition_node_id', 'workflow_definition.native_self_hosted_smoke.v1:node_1',
            'workflow_definition_id', 'workflow_definition.native_self_hosted_smoke.v1',
            'node_id', 'node_1',
            'node_type', 'deterministic_task',
            'schema_version', 1,
            'adapter_type', 'deterministic_task',
            'display_name', 'persist',
            'inputs', jsonb_build_object(
                'task_name', 'persist',
                'input_payload', jsonb_build_object(
                    'step', 1,
                    'allow_passthrough_echo', true
                )
            ),
            'expected_outputs', jsonb_build_object('result', 'persisted'),
            'success_condition', jsonb_build_object('kind', 'always'),
            'failure_behavior', jsonb_build_object('kind', 'stop'),
            'authority_requirements', jsonb_build_object(
                'workspace_ref', 'praxis',
                'runtime_profile_ref', 'praxis'
            ),
            'execution_boundary', jsonb_build_object('workspace_ref', 'praxis'),
            'position_index', 1
        )
    ) AS nodes
),
node_rows AS (
    SELECT
        node->>'workflow_definition_node_id' AS workflow_definition_node_id,
        node->>'workflow_definition_id' AS workflow_definition_id,
        node->>'node_id' AS node_id,
        node->>'node_type' AS node_type,
        (node->>'schema_version')::integer AS schema_version,
        node->>'adapter_type' AS adapter_type,
        node->>'display_name' AS display_name,
        COALESCE(node->'inputs', '{}'::jsonb) AS inputs,
        COALESCE(node->'expected_outputs', '{}'::jsonb) AS expected_outputs,
        COALESCE(node->'success_condition', '{}'::jsonb) AS success_condition,
        COALESCE(node->'failure_behavior', '{}'::jsonb) AS failure_behavior,
        COALESCE(node->'authority_requirements', '{}'::jsonb) AS authority_requirements,
        COALESCE(node->'execution_boundary', '{}'::jsonb) AS execution_boundary,
        (node->>'position_index')::integer AS position_index
    FROM smoke_definition
    CROSS JOIN LATERAL jsonb_array_elements(nodes) AS node
)
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
)
SELECT
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
FROM node_rows
ON CONFLICT (workflow_definition_node_id) DO UPDATE
SET workflow_definition_id = EXCLUDED.workflow_definition_id,
    node_id = EXCLUDED.node_id,
    node_type = EXCLUDED.node_type,
    schema_version = EXCLUDED.schema_version,
    adapter_type = EXCLUDED.adapter_type,
    display_name = EXCLUDED.display_name,
    inputs = EXCLUDED.inputs,
    expected_outputs = EXCLUDED.expected_outputs,
    success_condition = EXCLUDED.success_condition,
    failure_behavior = EXCLUDED.failure_behavior,
    authority_requirements = EXCLUDED.authority_requirements,
    execution_boundary = EXCLUDED.execution_boundary,
    position_index = EXCLUDED.position_index;

WITH smoke_definition AS (
    SELECT jsonb_build_array(
        jsonb_build_object(
            'workflow_definition_edge_id', 'workflow_definition.native_self_hosted_smoke.v1:edge_0',
            'workflow_definition_id', 'workflow_definition.native_self_hosted_smoke.v1',
            'edge_id', 'edge_0',
            'edge_type', 'after_success',
            'schema_version', 1,
            'from_node_id', 'node_0',
            'to_node_id', 'node_1',
            'release_condition', jsonb_build_object('kind', 'always'),
            'payload_mapping', jsonb_build_object('prepared_result', 'result'),
            'position_index', 0
        )
    ) AS edges
),
edge_rows AS (
    SELECT
        edge->>'workflow_definition_edge_id' AS workflow_definition_edge_id,
        edge->>'workflow_definition_id' AS workflow_definition_id,
        edge->>'edge_id' AS edge_id,
        edge->>'edge_type' AS edge_type,
        (edge->>'schema_version')::integer AS schema_version,
        edge->>'from_node_id' AS from_node_id,
        edge->>'to_node_id' AS to_node_id,
        COALESCE(edge->'release_condition', '{}'::jsonb) AS release_condition,
        COALESCE(edge->'payload_mapping', '{}'::jsonb) AS payload_mapping,
        (edge->>'position_index')::integer AS position_index
    FROM smoke_definition
    CROSS JOIN LATERAL jsonb_array_elements(edges) AS edge
)
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
)
SELECT
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
FROM edge_rows
ON CONFLICT (workflow_definition_edge_id) DO UPDATE
SET workflow_definition_id = EXCLUDED.workflow_definition_id,
    edge_id = EXCLUDED.edge_id,
    edge_type = EXCLUDED.edge_type,
    schema_version = EXCLUDED.schema_version,
    from_node_id = EXCLUDED.from_node_id,
    to_node_id = EXCLUDED.to_node_id,
    release_condition = EXCLUDED.release_condition,
    payload_mapping = EXCLUDED.payload_mapping,
    position_index = EXCLUDED.position_index;
