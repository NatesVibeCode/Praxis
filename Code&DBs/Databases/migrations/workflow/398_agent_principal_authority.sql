-- Migration 398: Agent Principal Authority — durable LLM actors that ride the
-- existing trigger evaluator + workflow control bus.
--
-- The "always-on business agent" is not a new runtime. It is:
--   agent_principal      durable identity (this migration)
--   trigger_routes       generic (target_kind, target_ref, target_args) on
--                        workflow_triggers + webhook_endpoints, so chat,
--                        schedule, and webhook all funnel through
--                        runtime.triggers without bespoke columns
--   wake_runs            workflow runs with requested_by_kind='agent' and
--                        requested_by_ref=<agent_principal_ref>
--   delegation_broker    parent agent → bounded child run via one MCP tool
--                        (praxis_agent_delegate) that materialises an
--                        agent_delegation row + admits a scoped tool/network
--                        envelope on the child workflow
--   tool_gap_feedback    when a delegated worker fails because Praxis lacks a
--                        capability, it files an agent_tool_gap row that
--                        becomes roadmap fuel
--
-- Standing orders anchoring this migration (filed via operator authority
-- before the migration was authored, see roadmap_item.business.agent.
-- substrate.with.delegated.praxis.only.workers):
--
--   architecture-policy::business-agent-substrate::
--     delegated-workers-praxis-only-no-internet
--   architecture-policy::business-agent-substrate::
--     praxis-only-egress-is-distinct-from-network-disabled
--
-- The praxis_only egress value is registered here as data-dictionary
-- metadata; the validator add is in
-- runtime/workflow/execution_policy.py and the docker enforcement marker
-- is in runtime/sandbox_runtime.py.
--
-- Idempotent throughout (CREATE TABLE IF NOT EXISTS, ON CONFLICT DO UPDATE).

BEGIN;

-- ---------------------------------------------------------------------------
-- Authority domain
-- ---------------------------------------------------------------------------

INSERT INTO authority_domains (
    authority_domain_ref,
    owner_ref,
    event_stream_ref,
    current_projection_ref,
    storage_target_ref,
    enabled,
    decision_ref
) VALUES (
    'authority.agent_principal',
    'praxis.engine',
    'stream.authority.agent_principal',
    NULL,
    'praxis.primary_postgres',
    TRUE,
    'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet'
)
ON CONFLICT (authority_domain_ref) DO UPDATE SET
    owner_ref = EXCLUDED.owner_ref,
    event_stream_ref = EXCLUDED.event_stream_ref,
    storage_target_ref = EXCLUDED.storage_target_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

-- ---------------------------------------------------------------------------
-- agent_principals — durable actor identity
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agent_principals (
    agent_principal_ref   text PRIMARY KEY,
    title                 text NOT NULL,
    status                text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'paused', 'killed')),
    max_in_flight_wakes   integer NOT NULL DEFAULT 1
        CHECK (max_in_flight_wakes >= 1 AND max_in_flight_wakes <= 16),
    write_envelope        jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(write_envelope) = 'array'),
    capability_refs       jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(capability_refs) = 'array'),
    integration_refs      jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(integration_refs) = 'array'),
    standing_order_keys   jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(standing_order_keys) = 'array'),
    allowed_tools         jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(allowed_tools) = 'array'),
    network_policy        text NOT NULL DEFAULT 'praxis_only'
        CHECK (network_policy IN ('disabled', 'provider_only', 'praxis_only', 'enabled')),
    default_conversation_id text,
    routing_policy        jsonb,
    metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
    decision_ref          text NOT NULL DEFAULT
        'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agent_principals_status
    ON agent_principals (status, agent_principal_ref);

CREATE INDEX IF NOT EXISTS idx_agent_principals_default_conversation
    ON agent_principals (default_conversation_id)
    WHERE default_conversation_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- agent_wakes — append-only wake ledger
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agent_wakes (
    wake_id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_principal_ref   text NOT NULL REFERENCES agent_principals(agent_principal_ref),
    trigger_kind          text NOT NULL
        CHECK (trigger_kind IN ('chat', 'schedule', 'webhook', 'delegation', 'manual')),
    trigger_source_ref    text,
    trigger_event_id      bigint,
    payload               jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload_hash          text,
    received_at           timestamptz NOT NULL DEFAULT now(),
    dispatched_at         timestamptz,
    completed_at          timestamptz,
    run_id                text,
    closeout_receipt_id   uuid REFERENCES authority_operation_receipts(receipt_id) ON DELETE SET NULL,
    status                text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'dispatched', 'completed', 'failed', 'skipped')),
    skip_reason           text,
    metadata              jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_agent_wakes_agent
    ON agent_wakes (agent_principal_ref, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_wakes_run
    ON agent_wakes (run_id)
    WHERE run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_agent_wakes_pending
    ON agent_wakes (agent_principal_ref, status, received_at)
    WHERE status IN ('pending', 'dispatched');

CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_wakes_payload_dedup
    ON agent_wakes (agent_principal_ref, trigger_kind, payload_hash)
    WHERE payload_hash IS NOT NULL;

-- ---------------------------------------------------------------------------
-- agent_delegations — parent agent → bounded child run
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agent_delegations (
    delegation_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_agent_ref      text NOT NULL REFERENCES agent_principals(agent_principal_ref),
    parent_run_id         text,
    parent_job_id         text,
    parent_wake_id        uuid REFERENCES agent_wakes(wake_id) ON DELETE SET NULL,
    child_task            text NOT NULL,
    child_intent          text NOT NULL,
    child_run_id          text,
    admitted_tools        jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(admitted_tools) = 'array'),
    admitted_integrations jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(admitted_integrations) = 'array'),
    write_envelope        jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(write_envelope) = 'array'),
    network_policy        text NOT NULL DEFAULT 'praxis_only'
        CHECK (network_policy IN ('disabled', 'provider_only', 'praxis_only', 'enabled')),
    timeout_ms            integer NOT NULL DEFAULT 600000
        CHECK (timeout_ms BETWEEN 1000 AND 7200000),
    status                text NOT NULL DEFAULT 'requested'
        CHECK (status IN ('requested', 'launched', 'completed', 'failed', 'gap_blocked', 'timeout')),
    result                jsonb,
    gap_count             integer NOT NULL DEFAULT 0 CHECK (gap_count >= 0),
    error_code            text,
    error_message         text,
    requested_at          timestamptz NOT NULL DEFAULT now(),
    launched_at           timestamptz,
    completed_at          timestamptz
);

CREATE INDEX IF NOT EXISTS idx_agent_delegations_parent
    ON agent_delegations (parent_agent_ref, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_delegations_parent_run
    ON agent_delegations (parent_run_id)
    WHERE parent_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_agent_delegations_child_run
    ON agent_delegations (child_run_id)
    WHERE child_run_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- agent_tool_gaps — roadmap fuel from blocked workers
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agent_tool_gaps (
    gap_id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    reporter_agent_ref    text NOT NULL REFERENCES agent_principals(agent_principal_ref),
    reporter_run_id       text,
    reporter_delegation_id uuid REFERENCES agent_delegations(delegation_id) ON DELETE SET NULL,
    missing_capability    text NOT NULL,
    attempted_task        text NOT NULL,
    admitted_tools        jsonb NOT NULL DEFAULT '[]'::jsonb,
    blocked_action        text NOT NULL,
    suggested_tool_contract jsonb,
    evidence              jsonb NOT NULL DEFAULT '{}'::jsonb,
    severity              text NOT NULL DEFAULT 'medium'
        CHECK (severity IN ('low', 'medium', 'high', 'blocking')),
    status                text NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'triaged', 'planned', 'shipped', 'declined', 'duplicate')),
    roadmap_item_ref      text,
    duplicate_of_gap_id   uuid REFERENCES agent_tool_gaps(gap_id),
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agent_tool_gaps_open
    ON agent_tool_gaps (severity, created_at DESC)
    WHERE status = 'open';

CREATE INDEX IF NOT EXISTS idx_agent_tool_gaps_capability
    ON agent_tool_gaps (missing_capability, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_tool_gaps_reporter
    ON agent_tool_gaps (reporter_agent_ref, created_at DESC);

-- ---------------------------------------------------------------------------
-- Generic trigger target shape on workflow_triggers + webhook_endpoints
-- (existing workflow_id and integration_* columns remain; the evaluator
-- branches on target_kind first and falls back to legacy columns)
-- ---------------------------------------------------------------------------

ALTER TABLE workflow_triggers
    ADD COLUMN IF NOT EXISTS target_kind text NOT NULL DEFAULT 'workflow'
        CHECK (target_kind IN ('workflow', 'integration', 'agent_wake')),
    ADD COLUMN IF NOT EXISTS target_ref text,
    ADD COLUMN IF NOT EXISTS target_args jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_workflow_triggers_target_agent
    ON workflow_triggers (target_ref)
    WHERE target_kind = 'agent_wake' AND enabled = TRUE;

ALTER TABLE webhook_endpoints
    ADD COLUMN IF NOT EXISTS target_kind text DEFAULT 'workflow',
    ADD COLUMN IF NOT EXISTS target_ref text,
    ADD COLUMN IF NOT EXISTS target_args jsonb DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_webhook_endpoints_target_agent
    ON webhook_endpoints (target_ref)
    WHERE target_kind = 'agent_wake' AND enabled = TRUE;

-- ---------------------------------------------------------------------------
-- Network policy registration: praxis_only as a first-class value
-- ---------------------------------------------------------------------------

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'network_policy.praxis_only',
        'Praxis-only network policy',
        'definition',
        'Worker can reach the Praxis MCP bridge and admitted provider/proxy routes; cannot browse the open web, fetch packages, or call arbitrary external APIs. Distinct from network_policy=disabled (which also cuts the worker off from Praxis MCP) and from network_policy=provider_only (which is not a hard egress guarantee).',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        jsonb_build_object(
            'enum_field_ref', 'CLIExecutionPolicy.network_policy',
            'enum_module_ref', 'runtime.workflow.execution_policy',
            'decision_ref', 'architecture-policy::business-agent-substrate::praxis-only-egress-is-distinct-from-network-disabled',
            'docker_enforcement_marker', 'sandbox_runtime._docker_network_flags_for_policy',
            'allowed_destinations', jsonb_build_array(
                'praxis.mcp.bridge',
                'admitted.provider.routes',
                'admitted.proxy.routes'
            )
        )
    ),
    (
        'network_policy.disabled',
        'Disabled network policy',
        'definition',
        'Worker is launched with --network=none. No egress at all, including Praxis MCP. Useful only for fully self-contained deterministic jobs.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        jsonb_build_object(
            'enum_field_ref', 'CLIExecutionPolicy.network_policy',
            'enum_module_ref', 'runtime.workflow.execution_policy'
        )
    ),
    (
        'network_policy.provider_only',
        'Provider-only network policy',
        'definition',
        'Worker uses the default docker network and is expected to call only provider endpoints; not a hard egress guarantee. Use praxis_only for delegated agent workers that need a cage.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        jsonb_build_object(
            'enum_field_ref', 'CLIExecutionPolicy.network_policy',
            'enum_module_ref', 'runtime.workflow.execution_policy'
        )
    ),
    (
        'network_policy.enabled',
        'Open network policy',
        'definition',
        'Worker has unrestricted network egress. Used for setup/maintenance jobs only.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        jsonb_build_object(
            'enum_field_ref', 'CLIExecutionPolicy.network_policy',
            'enum_module_ref', 'runtime.workflow.execution_policy'
        )
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

-- ---------------------------------------------------------------------------
-- Data dictionary registrations for the new tables
-- ---------------------------------------------------------------------------

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'agent_principals',
        'Agent principals',
        'table',
        'Durable LLM-actor identities. Bind a status, scope envelope, integration set, capability set, allowed tools, default conversation, network policy, and standing-order keys. Wakes and delegations reference principals.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'agent_wakes',
        'Agent wakes',
        'table',
        'Append-only wake ledger. Each row is one trigger fired (chat | schedule | webhook | delegation | manual) for one principal, with the workflow run that handled it and a closeout receipt id when complete.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'agent_delegations',
        'Agent delegations',
        'table',
        'Parent agent / run / job → bounded child workflow. Captures admitted tool list, integration list, write envelope, network policy, child run id, result, and gap count produced by the child.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'agent_tool_gaps',
        'Agent tool gaps',
        'table',
        'When a delegated worker fails because Praxis lacks a tool, it files a gap row: missing_capability, attempted_task, admitted_tools, blocked_action, suggested_tool_contract, evidence, severity. Roadmap fuel.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'workflow_triggers.target_kind',
        'Workflow trigger target kind',
        'definition',
        'Generic target shape on workflow_triggers — workflow | integration | agent_wake. Replaces ad-hoc per-target-kind columns. Existing workflow_id and integration_* columns remain as the legacy fall-through path.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"parent_table_ref":"workflow_triggers"}'::jsonb
    ),
    (
        'webhook_endpoints.target_kind',
        'Webhook endpoint target kind',
        'definition',
        'Generic target shape on webhook_endpoints — workflow | integration | agent_wake. Existing target_workflow_id and target_integration_* columns remain.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"parent_table_ref":"webhook_endpoints"}'::jsonb
    )
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
    ('table.public.agent_principals', 'table', 'agent_principals', 'public', 'authority.agent_principal', 'agent_principals', 'active', 'registry', 'praxis.engine', 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet', '{}'::jsonb),
    ('table.public.agent_wakes', 'table', 'agent_wakes', 'public', 'authority.agent_principal', 'agent_wakes', 'active', 'command_model', 'praxis.engine', 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet', '{}'::jsonb),
    ('table.public.agent_delegations', 'table', 'agent_delegations', 'public', 'authority.agent_principal', 'agent_delegations', 'active', 'command_model', 'praxis.engine', 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet', '{}'::jsonb),
    ('table.public.agent_tool_gaps', 'table', 'agent_tool_gaps', 'public', 'authority.agent_principal', 'agent_tool_gaps', 'active', 'command_model', 'praxis.engine', 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet', '{}'::jsonb)
ON CONFLICT (object_ref) DO UPDATE SET
    authority_domain_ref = EXCLUDED.authority_domain_ref,
    data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
    lifecycle_status = EXCLUDED.lifecycle_status,
    write_model_kind = EXCLUDED.write_model_kind,
    owner_ref = EXCLUDED.owner_ref,
    source_decision_ref = EXCLUDED.source_decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

-- ---------------------------------------------------------------------------
-- Event contracts
-- ---------------------------------------------------------------------------

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'agent_wake_requested_event',
        'agent.wake.requested event payload',
        'event',
        'Emitted whenever a chat message, schedule firing, webhook, or delegation should wake an agent_principal. Trigger evaluator turns this into a workflow run.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"event_type":"agent.wake.requested","authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'agent_wake_dispatched_event',
        'agent.wake.dispatched event payload',
        'event',
        'Emitted when the trigger evaluator launches a workflow run for an agent wake. Carries wake_id and run_id.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"event_type":"agent.wake.dispatched","authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'agent_wake_completed_event',
        'agent.wake.completed event payload',
        'event',
        'Emitted when an agent wake run reaches terminal status with a closeout receipt.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"event_type":"agent.wake.completed","authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'agent_delegation_committed_event',
        'agent.delegation.committed event payload',
        'event',
        'Emitted when a parent agent delegates a bounded child task. Carries delegation_id and admitted-tool envelope.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"event_type":"agent.delegation.committed","authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'agent_tool_gap_filed_event',
        'agent.tool_gap.filed event payload',
        'event',
        'Emitted when a worker files a tool-gap. Carries gap_id, missing_capability, severity.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"event_type":"agent.tool_gap.filed","authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'agent_principal_registered_event',
        'agent_principal.registered event payload',
        'event',
        'Emitted when an agent_principal row is upserted. Carries agent_principal_ref and resulting status.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"event_type":"agent_principal.registered","authority_domain_ref":"authority.agent_principal"}'::jsonb
    ),
    (
        'agent_principal_status_updated_event',
        'agent_principal.status_updated event payload',
        'event',
        'Emitted when an agent_principal status flips between active|paused|killed.',
        '{"migration":"396_agent_principal_authority.sql"}'::jsonb,
        '{"event_type":"agent_principal.status_updated","authority_domain_ref":"authority.agent_principal"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
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
) VALUES
    (
        'event_contract.agent.wake.requested',
        'agent.wake.requested',
        'authority.agent_principal',
        'data_dictionary.object.agent_wake_requested_event',
        'operation_ref',
        '[]'::jsonb,
        '["projection.agent_wakes"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
        jsonb_build_object(
            'expected_payload_fields',
            jsonb_build_array(
                'wake_id',
                'agent_principal_ref',
                'trigger_kind',
                'trigger_source_ref',
                'payload_hash'
            )
        )
    ),
    (
        'event_contract.agent.wake.dispatched',
        'agent.wake.dispatched',
        'authority.agent_principal',
        'data_dictionary.object.agent_wake_dispatched_event',
        'operation_ref',
        '[]'::jsonb,
        '["projection.agent_wakes"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
        jsonb_build_object(
            'expected_payload_fields',
            jsonb_build_array('wake_id', 'agent_principal_ref', 'run_id')
        )
    ),
    (
        'event_contract.agent.wake.completed',
        'agent.wake.completed',
        'authority.agent_principal',
        'data_dictionary.object.agent_wake_completed_event',
        'operation_ref',
        '[]'::jsonb,
        '["projection.agent_wakes"]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
        jsonb_build_object(
            'expected_payload_fields',
            jsonb_build_array('wake_id', 'run_id', 'closeout_receipt_id', 'status')
        )
    ),
    (
        'event_contract.agent.delegation.committed',
        'agent.delegation.committed',
        'authority.agent_principal',
        'data_dictionary.object.agent_delegation_committed_event',
        'operation_ref',
        '[]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
        jsonb_build_object(
            'expected_payload_fields',
            jsonb_build_array(
                'delegation_id',
                'parent_agent_ref',
                'parent_run_id',
                'child_run_id',
                'admitted_tools_count',
                'network_policy'
            )
        )
    ),
    (
        'event_contract.agent.tool_gap.filed',
        'agent.tool_gap.filed',
        'authority.agent_principal',
        'data_dictionary.object.agent_tool_gap_filed_event',
        'operation_ref',
        '[]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
        jsonb_build_object(
            'expected_payload_fields',
            jsonb_build_array(
                'gap_id',
                'reporter_agent_ref',
                'missing_capability',
                'severity',
                'reporter_delegation_id'
            )
        )
    ),
    (
        'event_contract.agent_principal.registered',
        'agent_principal.registered',
        'authority.agent_principal',
        'data_dictionary.object.agent_principal_registered_event',
        'operation_ref',
        '[]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
        jsonb_build_object(
            'expected_payload_fields',
            jsonb_build_array('agent_principal_ref', 'status', 'title')
        )
    ),
    (
        'event_contract.agent_principal.status_updated',
        'agent_principal.status_updated',
        'authority.agent_principal',
        'data_dictionary.object.agent_principal_status_updated_event',
        'operation_ref',
        '[]'::jsonb,
        '[]'::jsonb,
        TRUE,
        'replayable',
        TRUE,
        'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
        jsonb_build_object(
            'expected_payload_fields',
            jsonb_build_array('agent_principal_ref', 'status', 'reason')
        )
    )
ON CONFLICT (authority_domain_ref, event_type) DO UPDATE SET
    payload_schema_ref = EXCLUDED.payload_schema_ref,
    aggregate_ref_policy = EXCLUDED.aggregate_ref_policy,
    reducer_refs = EXCLUDED.reducer_refs,
    projection_refs = EXCLUDED.projection_refs,
    receipt_required = EXCLUDED.receipt_required,
    replay_policy = EXCLUDED.replay_policy,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

-- ---------------------------------------------------------------------------
-- CQRS operations — reads first, then commands
-- ---------------------------------------------------------------------------

SELECT register_operation_atomic(
    p_operation_ref         := 'agent_principal.query.list',
    p_operation_name        := 'agent_principal.list',
    p_handler_ref           := 'runtime.operations.queries.agent_principals.handle_list_agent_principals',
    p_input_model_ref       := 'runtime.operations.queries.agent_principals.ListAgentPrincipalsQuery',
    p_authority_domain_ref  := 'authority.agent_principal',
    p_authority_ref         := 'authority.agent_principal',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/agent_principals',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    p_binding_revision      := 'binding.operation_catalog_registry.agent_principal_list.20260501',
    p_label                 := 'Agent Principal List',
    p_summary               := 'List durable agent principals filtered by status, with their scope and integration envelopes.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'agent_principal.query.describe',
    p_operation_name        := 'agent_principal.describe',
    p_handler_ref           := 'runtime.operations.queries.agent_principals.handle_describe_agent_principal',
    p_input_model_ref       := 'runtime.operations.queries.agent_principals.DescribeAgentPrincipalQuery',
    p_authority_domain_ref  := 'authority.agent_principal',
    p_authority_ref         := 'authority.agent_principal',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/agent_principals/describe',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    p_binding_revision      := 'binding.operation_catalog_registry.agent_principal_describe.20260501',
    p_label                 := 'Agent Principal Describe',
    p_summary               := 'Describe one agent principal — full scope, recent wakes, recent delegations, recent tool gaps.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'agent_wake.query.list',
    p_operation_name        := 'agent_wake.list',
    p_handler_ref           := 'runtime.operations.queries.agent_principals.handle_list_agent_wakes',
    p_input_model_ref       := 'runtime.operations.queries.agent_principals.ListAgentWakesQuery',
    p_authority_domain_ref  := 'authority.agent_principal',
    p_authority_ref         := 'authority.agent_principal',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/agent_wakes',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    p_binding_revision      := 'binding.operation_catalog_registry.agent_wake_list.20260501',
    p_label                 := 'Agent Wake List',
    p_summary               := 'List wake-ledger rows filtered by principal, status, trigger_kind, time window.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'agent_principal.command.register',
    p_operation_name        := 'agent_principal.register',
    p_handler_ref           := 'runtime.operations.commands.agent_principals.handle_register_agent_principal',
    p_input_model_ref       := 'runtime.operations.commands.agent_principals.RegisterAgentPrincipalCommand',
    p_authority_domain_ref  := 'authority.agent_principal',
    p_authority_ref         := 'authority.agent_principal',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/agent_principals/register',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'agent_principal.registered',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 10000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    p_binding_revision      := 'binding.operation_catalog_registry.agent_principal_register.20260501',
    p_label                 := 'Agent Principal Register',
    p_summary               := 'Upsert one durable agent_principal row with scope, integrations, capabilities, allowed tools, network policy, and standing-order keys.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'agent_principal.command.update_status',
    p_operation_name        := 'agent_principal.update_status',
    p_handler_ref           := 'runtime.operations.commands.agent_principals.handle_update_agent_principal_status',
    p_input_model_ref       := 'runtime.operations.commands.agent_principals.UpdateAgentPrincipalStatusCommand',
    p_authority_domain_ref  := 'authority.agent_principal',
    p_authority_ref         := 'authority.agent_principal',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/agent_principals/status',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'agent_principal.status_updated',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    p_binding_revision      := 'binding.operation_catalog_registry.agent_principal_update_status.20260501',
    p_label                 := 'Agent Principal Update Status',
    p_summary               := 'Flip an agent principal between active | paused | killed. Killed/paused principals are skipped by the trigger evaluator with skip_reason.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'agent_wake.command.request',
    p_operation_name        := 'agent_wake.request',
    p_handler_ref           := 'runtime.operations.commands.agent_principals.handle_request_agent_wake',
    p_input_model_ref       := 'runtime.operations.commands.agent_principals.RequestAgentWakeCommand',
    p_authority_domain_ref  := 'authority.agent_principal',
    p_authority_ref         := 'authority.agent_principal',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/agent_wakes/request',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'agent.wake.requested',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 10000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    p_binding_revision      := 'binding.operation_catalog_registry.agent_wake_request.20260501',
    p_label                 := 'Agent Wake Request',
    p_summary               := 'Insert one agent_wakes row in status=pending and emit an agent.wake.requested event so runtime.triggers picks it up.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'agent.command.delegate',
    p_operation_name        := 'agent.delegate',
    p_handler_ref           := 'runtime.operations.commands.agent_delegate.handle_agent_delegate',
    p_input_model_ref       := 'runtime.operations.commands.agent_delegate.AgentDelegateCommand',
    p_authority_domain_ref  := 'authority.agent_principal',
    p_authority_ref         := 'authority.agent_principal',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/agent_delegations',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'agent.delegation.committed',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 30000,
    p_execution_lane        := 'background',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    p_binding_revision      := 'binding.operation_catalog_registry.agent_delegate.20260501',
    p_label                 := 'Agent Delegate',
    p_summary               := 'Parent agent (or run/job) delegates a bounded child task. Materialises an agent_delegations row, launches a child workflow with a scoped tool list and praxis_only network, and stamps the child run id back.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'agent_tool_gap.command.file',
    p_operation_name        := 'agent_tool_gap.file',
    p_handler_ref           := 'runtime.operations.commands.agent_principals.handle_file_agent_tool_gap',
    p_input_model_ref       := 'runtime.operations.commands.agent_principals.FileAgentToolGapCommand',
    p_authority_domain_ref  := 'authority.agent_principal',
    p_authority_ref         := 'authority.agent_principal',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/agent_tool_gaps/file',
    p_posture               := 'operate',
    p_idempotency_policy    := 'idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'agent.tool_gap.filed',
    p_receipt_required      := TRUE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    p_binding_revision      := 'binding.operation_catalog_registry.agent_tool_gap_file.20260501',
    p_label                 := 'Agent Tool Gap File',
    p_summary               := 'Worker files a tool gap row when Praxis lacks a needed capability. Becomes roadmap fuel — surfaced via agent_tool_gap.list and the agent_principal.describe view.'
);

SELECT register_operation_atomic(
    p_operation_ref         := 'agent_tool_gap.query.list',
    p_operation_name        := 'agent_tool_gap.list',
    p_handler_ref           := 'runtime.operations.queries.agent_principals.handle_list_agent_tool_gaps',
    p_input_model_ref       := 'runtime.operations.queries.agent_principals.ListAgentToolGapsQuery',
    p_authority_domain_ref  := 'authority.agent_principal',
    p_authority_ref         := 'authority.agent_principal',
    p_operation_kind        := 'query',
    p_http_method           := 'GET',
    p_http_path             := '/api/agent_tool_gaps',
    p_posture               := 'observe',
    p_idempotency_policy    := 'read_only',
    p_event_required        := FALSE,
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::business-agent-substrate::delegated-workers-praxis-only-no-internet',
    p_binding_revision      := 'binding.operation_catalog_registry.agent_tool_gap_list.20260501',
    p_label                 := 'Agent Tool Gap List',
    p_summary               := 'Read open / triaged / shipped tool gaps filtered by reporter, severity, and capability — for roadmap triage.'
);

COMMIT;
