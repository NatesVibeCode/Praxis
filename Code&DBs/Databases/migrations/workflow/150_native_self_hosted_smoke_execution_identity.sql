-- Migration 150: make the native self-hosted smoke definition executable through
-- the admitted graph runtime by restoring explicit provider/model identity in the
-- deterministic smoke nodes.

WITH smoke_definition AS (
    SELECT
        'workflow_definition.native_self_hosted_smoke.v1'::text AS workflow_definition_id,
        'workflow.native-self-hosted-smoke'::text AS workflow_id,
        'definition.native_self_hosted_smoke.v1'::text AS definition_hash,
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
                        'provider_slug', 'openai',
                        'model_slug', 'gpt-5.4-mini',
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
                        'provider_slug', 'openai',
                        'model_slug', 'gpt-5.4-mini',
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
UPDATE workflow_definitions AS definitions
SET request_envelope = smoke_definition.request_envelope,
    normalized_definition = smoke_definition.request_envelope,
    definition_hash = smoke_definition.definition_hash,
    workflow_id = smoke_definition.workflow_id
FROM smoke_definition
WHERE definitions.workflow_definition_id = smoke_definition.workflow_definition_id;

WITH smoke_nodes AS (
    SELECT jsonb_build_array(
        jsonb_build_object(
            'workflow_definition_node_id', 'workflow_definition.native_self_hosted_smoke.v1:node_0',
            'inputs', jsonb_build_object(
                'task_name', 'prepare',
                'provider_slug', 'openai',
                'model_slug', 'gpt-5.4-mini',
                'input_payload', jsonb_build_object(
                    'step', 0,
                    'allow_passthrough_echo', true
                )
            )
        ),
        jsonb_build_object(
            'workflow_definition_node_id', 'workflow_definition.native_self_hosted_smoke.v1:node_1',
            'inputs', jsonb_build_object(
                'task_name', 'persist',
                'provider_slug', 'openai',
                'model_slug', 'gpt-5.4-mini',
                'input_payload', jsonb_build_object(
                    'step', 1,
                    'allow_passthrough_echo', true
                )
            )
        )
    ) AS nodes
),
node_rows AS (
    SELECT
        node->>'workflow_definition_node_id' AS workflow_definition_node_id,
        COALESCE(node->'inputs', '{}'::jsonb) AS inputs
    FROM smoke_nodes
    CROSS JOIN LATERAL jsonb_array_elements(nodes) AS node
)
UPDATE workflow_definition_nodes AS nodes
SET inputs = node_rows.inputs
FROM node_rows
WHERE nodes.workflow_definition_node_id = node_rows.workflow_definition_node_id;
