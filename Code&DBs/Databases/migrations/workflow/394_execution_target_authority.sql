-- Migration 394: Execution Target Authority and dispatch-choice receipts.
--
-- Docker remains a supported target. It stops being the default mental model.
-- The authority is now target/profile + dispatch choice proof.

BEGIN;

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.execution_target',
    'praxis.engine',
    'stream.authority.execution_target',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

CREATE TABLE IF NOT EXISTS registry_execution_target_authority (
    execution_target_ref text PRIMARY KEY,
    execution_target_kind text NOT NULL,
    execution_lane text NOT NULL,
    isolation_level text NOT NULL,
    packaging_kind text NOT NULL,
    supported_transports jsonb NOT NULL DEFAULT '[]'::jsonb,
    artifact_mode text NOT NULL,
    credential_mode text NOT NULL,
    health_probe_ref text NOT NULL,
    resource_class text NOT NULL,
    admitted boolean NOT NULL DEFAULT TRUE,
    disabled_reason text,
    decision_ref text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_registry_execution_target_kind
    ON registry_execution_target_authority (execution_target_kind, admitted);

CREATE INDEX IF NOT EXISTS idx_registry_execution_target_transport
    ON registry_execution_target_authority USING gin (supported_transports);

CREATE TABLE IF NOT EXISTS registry_execution_profile_authority (
    execution_profile_ref text PRIMARY KEY,
    execution_target_ref text NOT NULL REFERENCES registry_execution_target_authority(execution_target_ref),
    network_policy text NOT NULL,
    workspace_materialization text NOT NULL,
    timeout_profile text NOT NULL,
    resource_limits_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    fallback_policy text NOT NULL,
    sandbox_profile_ref text,
    decision_ref text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_registry_execution_profile_target
    ON registry_execution_profile_authority (execution_target_ref);

CREATE TABLE IF NOT EXISTS execution_dispatch_choices (
    dispatch_choice_ref text PRIMARY KEY,
    dispatch_ref text,
    workload_kind text NOT NULL,
    task_slug text NOT NULL,
    candidate_set_hash text NOT NULL,
    selected_candidate_ref text NOT NULL,
    selected_target_ref text,
    selected_profile_ref text,
    selected_provider_slug text,
    selected_model_slug text,
    selected_transport_type text,
    selection_kind text NOT NULL CHECK (
        selection_kind IN ('default', 'explicit_click', 'programmatic_override', 'ask_all')
    ),
    selected_by text NOT NULL,
    surface text NOT NULL,
    conversation_id text,
    candidate_set_json jsonb NOT NULL,
    selected_candidate_json jsonb NOT NULL,
    ask_all_candidates_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    selected_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_execution_dispatch_choices_lookup
    ON execution_dispatch_choices (workload_kind, task_slug, selected_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_dispatch_choices_candidate_hash
    ON execution_dispatch_choices (candidate_set_hash, selected_candidate_ref);

INSERT INTO registry_execution_target_authority (
    execution_target_ref,
    execution_target_kind,
    execution_lane,
    isolation_level,
    packaging_kind,
    supported_transports,
    artifact_mode,
    credential_mode,
    health_probe_ref,
    resource_class,
    admitted,
    disabled_reason,
    decision_ref,
    metadata
) VALUES
    ('execution_target.control_plane_api', 'control_plane_api', 'control_plane', 'provider_api_boundary', 'none', '["API"]'::jsonb, 'provider_response', 'secret_authority_env', 'provider_route_health', 'external_api', TRUE, NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_target.docker_thin_cli', 'docker_thin_cli', 'local_container', 'container', 'thin_cli_image', '["CLI"]'::jsonb, 'workspace_delta', 'provider_scoped_auth_mount', 'docker_cli_smoke', 'local_cpu_memory', TRUE, NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_target.docker_empty', 'docker_empty', 'local_container', 'container', 'empty_container', '["CLI","MCP"]'::jsonb, 'workspace_delta', 'explicit_secret_allowlist', 'docker_empty_probe', 'local_cpu_memory', TRUE, NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_target.docker_full', 'docker_full', 'local_container', 'container', 'full_container', '["CLI","MCP"]'::jsonb, 'workspace_delta', 'explicit_secret_allowlist', 'docker_full_probe', 'local_cpu_memory', TRUE, NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_target.cloud_container', 'cloud_container', 'remote_container', 'remote_container', 'cloud_container', '["CLI","MCP"]'::jsonb, 'workspace_delta', 'remote_secret_binding', 'remote_container_probe', 'remote_cpu_memory', TRUE, NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_target.python_bundle_remote', 'python_bundle_remote', 'remote_bundle', 'remote_worker', 'python_bundle', '["API","BUNDLE"]'::jsonb, 'bundle_mount', 'remote_secret_binding', 'bundle_worker_probe', 'remote_cpu_gpu', TRUE, NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_target.existing_endpoint', 'existing_endpoint', 'remote_endpoint', 'provider_endpoint_boundary', 'existing_endpoint', '["API","HTTP"]'::jsonb, 'provider_response', 'endpoint_credential_binding', 'endpoint_health_probe', 'remote_cpu_gpu', TRUE, NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_target.native_trusted', 'native_trusted', 'host_process', 'none_trusted_dev', 'host_process', '["CLI","PROCESS"]'::jsonb, 'host_workspace_delta', 'host_environment', 'dev_only_operator_gate', 'host_cpu_memory', TRUE, NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{"dev_only":true}'::jsonb),
    ('execution_target.process_sandbox', 'process_sandbox', 'host_process', 'not_proven', 'process_sandbox', '["PROCESS"]'::jsonb, 'host_workspace_delta', 'host_environment', 'blocked_until_isolation_proof', 'host_cpu_memory', FALSE, 'process_sandbox.isolation_not_proven', 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{"admitted":false}'::jsonb)
ON CONFLICT (execution_target_ref) DO UPDATE SET
    execution_target_kind = EXCLUDED.execution_target_kind,
    execution_lane = EXCLUDED.execution_lane,
    isolation_level = EXCLUDED.isolation_level,
    packaging_kind = EXCLUDED.packaging_kind,
    supported_transports = EXCLUDED.supported_transports,
    artifact_mode = EXCLUDED.artifact_mode,
    credential_mode = EXCLUDED.credential_mode,
    health_probe_ref = EXCLUDED.health_probe_ref,
    resource_class = EXCLUDED.resource_class,
    admitted = EXCLUDED.admitted,
    disabled_reason = EXCLUDED.disabled_reason,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO registry_execution_profile_authority (
    execution_profile_ref,
    execution_target_ref,
    network_policy,
    workspace_materialization,
    timeout_profile,
    resource_limits_json,
    fallback_policy,
    sandbox_profile_ref,
    decision_ref,
    metadata
) VALUES
    ('execution_profile.praxis.control_plane_api', 'execution_target.control_plane_api', 'provider_api_only', 'none', 'interactive_api', '{"max_tokens_policy":"caller_bound"}'::jsonb, 'route_failover_allowed', NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_profile.praxis.docker_thin_cli', 'execution_target.docker_thin_cli', 'provider_api_plus_praxis_mcp', 'manifest_shard', 'interactive_cli', '{"docker_memory":"500m","docker_cpus":"2"}'::jsonb, 'none', 'sandbox_profile.praxis.default', 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_profile.praxis.docker_empty', 'execution_target.docker_empty', 'explicit', 'none', 'bounded', '{"docker_memory":"500m","docker_cpus":"2"}'::jsonb, 'none', 'sandbox_profile.praxis.default', 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_profile.praxis.docker_full', 'execution_target.docker_full', 'explicit', 'manifest_shard', 'bounded', '{"docker_memory":"1g","docker_cpus":"4"}'::jsonb, 'none', 'sandbox_profile.praxis.default', 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_profile.praxis.cloud_container', 'execution_target.cloud_container', 'remote_worker_policy', 'snapshot_upload', 'remote_bounded', '{"provider":"cloudflare_remote"}'::jsonb, 'none', 'sandbox_profile.praxis.legacy_copy_debug', 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_profile.praxis.python_bundle_remote', 'execution_target.python_bundle_remote', 'remote_worker_policy', 'python_bundle', 'remote_bounded', '{"artifact":"python_wheel_bundle"}'::jsonb, 'none', NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_profile.praxis.existing_endpoint', 'execution_target.existing_endpoint', 'endpoint_policy', 'none', 'interactive_api', '{"endpoint":"preprovisioned"}'::jsonb, 'route_failover_allowed', NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('execution_profile.praxis.native_trusted', 'execution_target.native_trusted', 'host_default', 'host_workspace', 'dev_only', '{"process":"host"}'::jsonb, 'none', NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{"dev_only":true}'::jsonb),
    ('execution_profile.praxis.process_sandbox', 'execution_target.process_sandbox', 'blocked', 'blocked', 'blocked', '{"admitted":false}'::jsonb, 'none', NULL, 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{"admitted":false}'::jsonb)
ON CONFLICT (execution_profile_ref) DO UPDATE SET
    execution_target_ref = EXCLUDED.execution_target_ref,
    network_policy = EXCLUDED.network_policy,
    workspace_materialization = EXCLUDED.workspace_materialization,
    timeout_profile = EXCLUDED.timeout_profile,
    resource_limits_json = EXCLUDED.resource_limits_json,
    fallback_policy = EXCLUDED.fallback_policy,
    sandbox_profile_ref = EXCLUDED.sandbox_profile_ref,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

CREATE OR REPLACE VIEW registry_sandbox_profile_execution_projection AS
SELECT
    COALESCE(p.sandbox_profile_ref, 'sandbox_profile.compat.' || replace(p.execution_profile_ref, '.', '_')) AS sandbox_profile_ref,
    CASE
        WHEN t.execution_target_kind IN ('docker_thin_cli', 'docker_empty', 'docker_full') THEN 'docker_local'
        WHEN t.execution_target_kind = 'cloud_container' THEN 'cloudflare_remote'
        WHEN t.execution_target_kind = 'control_plane_api' THEN 'control_plane'
        WHEN t.execution_target_kind = 'native_trusted' THEN 'native_trusted'
        ELSE t.execution_target_kind
    END AS sandbox_provider,
    NULL::text AS docker_image,
    p.resource_limits_json ->> 'docker_cpus' AS docker_cpus,
    p.resource_limits_json ->> 'docker_memory' AS docker_memory,
    p.network_policy,
    p.workspace_materialization,
    '[]'::jsonb AS secret_allowlist,
    t.credential_mode AS auth_mount_policy,
    p.timeout_profile,
    p.recorded_at
FROM registry_execution_profile_authority p
JOIN registry_execution_target_authority t
  ON t.execution_target_ref = p.execution_target_ref;

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    ('registry_execution_target_authority', 'Execution target authority', 'table', 'Authoritative execution target catalog: target kind, lane, isolation, packaging, transport support, credential mode, health probe, resource class, and admission.', '{"migration":"394_execution_target_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.execution_target"}'::jsonb),
    ('registry_execution_profile_authority', 'Execution profile authority', 'table', 'Execution profile bindings from target to network policy, workspace materialization, limits, and fallback policy.', '{"migration":"394_execution_target_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.execution_target"}'::jsonb),
    ('execution_dispatch_choices', 'Execution dispatch choices', 'table', 'Receipt-backed operator dispatch choices including candidate-set hash, selected candidate, target/profile, provider/model/transport, selected_by, and surface.', '{"migration":"394_execution_target_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.execution_target"}'::jsonb),
    ('registry_sandbox_profile_execution_projection', 'Sandbox profile compatibility projection', 'projection', 'Temporary compatibility read projection from Execution Target Authority into legacy sandbox-profile-shaped rows.', '{"migration":"394_execution_target_authority.sql"}'::jsonb, '{"authority_domain_ref":"authority.execution_target","compatibility_only":true}'::jsonb)
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_object_registry (
    object_ref,
    object_kind,
    object_name,
    schema_name,
    authority_domain_ref,
    data_dictionary_object_kind,
    lifecycle_status,
    write_model_kind,
    owner_ref,
    source_decision_ref,
    metadata
) VALUES
    ('table.public.registry_execution_target_authority', 'table', 'registry_execution_target_authority', 'public', 'authority.execution_target', 'registry_execution_target_authority', 'active', 'registry', 'praxis.engine', 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('table.public.registry_execution_profile_authority', 'table', 'registry_execution_profile_authority', 'public', 'authority.execution_target', 'registry_execution_profile_authority', 'active', 'registry', 'praxis.engine', 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('table.public.execution_dispatch_choices', 'table', 'execution_dispatch_choices', 'public', 'authority.execution_target', 'execution_dispatch_choices', 'active', 'registry', 'praxis.engine', 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{}'::jsonb),
    ('view.public.registry_sandbox_profile_execution_projection', 'projection', 'registry_sandbox_profile_execution_projection', 'public', 'authority.execution_target', 'registry_sandbox_profile_execution_projection', 'active', 'projection', 'praxis.engine', 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime', '{"compatibility_only":true}'::jsonb)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

INSERT INTO authority_event_contracts (
    event_contract_ref,
    event_type,
    authority_domain_ref,
    payload_schema_ref,
    aggregate_ref_policy,
    reducer_refs,
    projection_refs,
    receipt_required,
    replay_policy,
    enabled,
    decision_ref,
    metadata
) VALUES (
    'event_contract.execution.dispatch_choice.committed',
    'execution.dispatch_choice.committed',
    'authority.execution_target',
    'data_dictionary.object.execution_dispatch_choice_committed_event',
    'operation_ref',
    '[]'::jsonb,
    '[]'::jsonb,
    TRUE,
    'replayable',
    TRUE,
    'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime',
    '{"expected_payload_fields":["dispatch_choice_ref","candidate_set_hash","selected_candidate_ref","selected_target_ref","selected_profile_ref","selected_provider_slug","selected_model_slug","selected_transport_type","selection_kind","selected_by","surface"]}'::jsonb
)
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

SELECT register_operation_atomic(
    p_operation_ref         := 'execution.query.targets_list',
    p_operation_name        := 'execution.targets.list',
    p_handler_ref           := 'runtime.operations.queries.execution_targets.handle_query_execution_targets_list',
    p_input_model_ref       := 'runtime.operations.queries.execution_targets.QueryExecutionTargetsList',
    p_authority_domain_ref  := 'authority.execution_target',
    p_authority_ref         := 'authority.execution_target',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/execution/targets',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime',
    p_binding_revision      := 'binding.operation_catalog_registry.execution_targets_list.20260501',
    p_label                 := 'Execution Targets List',
    p_summary               := 'List first-class execution targets and profiles, including non-admitted targets when requested.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'execution.query.targets_resolve',
    p_operation_name        := 'execution.targets.resolve',
    p_handler_ref           := 'runtime.operations.queries.execution_targets.handle_query_execution_targets_resolve',
    p_input_model_ref       := 'runtime.operations.queries.execution_targets.QueryExecutionTargetsResolve',
    p_authority_domain_ref  := 'authority.execution_target',
    p_authority_ref         := 'authority.execution_target',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/execution/targets/resolve',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime',
    p_binding_revision      := 'binding.operation_catalog_registry.execution_targets_resolve.20260501',
    p_label                 := 'Execution Targets Resolve',
    p_summary               := 'Resolve the final execution target/profile from an explicit pin, profile, transport, and workload requirements. Fails closed unless fallback_allowed=true.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'execution.query.dispatch_options_list',
    p_operation_name        := 'execution.dispatch_options.list',
    p_handler_ref           := 'runtime.operations.queries.execution_targets.handle_query_dispatch_options_list',
    p_input_model_ref       := 'runtime.operations.queries.execution_targets.QueryDispatchOptionsList',
    p_authority_domain_ref  := 'authority.execution_target',
    p_authority_ref         := 'authority.execution_target',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/execution/dispatch-options',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime',
    p_binding_revision      := 'binding.operation_catalog_registry.execution_dispatch_options_list.20260501',
    p_label                 := 'Execution Dispatch Options List',
    p_summary               := 'Return selectable dispatch candidates with provider/model, transport, execution target/profile, health, cost metadata, disabled reasons, and a candidate-set hash.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'execution.command.dispatch_choice_commit',
    p_operation_name        := 'execution.dispatch_choice.commit',
    p_handler_ref           := 'runtime.operations.commands.execution_dispatch_choice.handle_commit_dispatch_choice',
    p_input_model_ref       := 'runtime.operations.commands.execution_dispatch_choice.CommitDispatchChoiceCommand',
    p_authority_domain_ref  := 'authority.execution_target',
    p_authority_ref         := 'authority.execution_target',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/execution/dispatch-choice',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'execution.dispatch_choice.committed',
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::execution-target-authority::execution-target-authority-supersedes-docker-only-runtime',
    p_binding_revision      := 'binding.operation_catalog_registry.execution_dispatch_choice_commit.20260501',
    p_label                 := 'Execution Dispatch Choice Commit',
    p_summary               := 'Commit one default, clicked, programmatic, or ask-all dispatch selection after validating the candidate-set hash and candidate admission state.'
);

COMMIT;
