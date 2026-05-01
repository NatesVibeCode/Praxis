-- Migration 399: Paid-model soft-off and one-run lease authority.
--
-- P0 BUG-A2B1564D: paid provider/model routes must not become broadly usable
-- by accident. Hard-off remains private_provider_model_access_denials; this
-- migration adds presentation soft-off state plus exact one-run leases.

BEGIN;

CREATE TABLE IF NOT EXISTS private_provider_model_access_soft_offs (
    runtime_profile_ref TEXT NOT NULL,
    job_type TEXT NOT NULL,
    transport_type TEXT NOT NULL CHECK (transport_type IN ('CLI', 'API')),
    adapter_type TEXT NOT NULL,
    provider_slug TEXT NOT NULL,
    model_slug TEXT NOT NULL,
    presentation_state TEXT NOT NULL DEFAULT 'soft_off'
        CHECK (presentation_state IN ('soft_off')),
    reason_code TEXT NOT NULL DEFAULT 'paid_model.presentation_soft_off',
    operator_message TEXT,
    decision_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug
    )
);

COMMENT ON TABLE private_provider_model_access_soft_offs IS
    'Presentation-only model access state. Soft-off hides a paid model from default picker surfaces; it never grants or denies backend dispatch.';

CREATE TABLE IF NOT EXISTS private_paid_model_access_leases (
    lease_id TEXT PRIMARY KEY,
    runtime_profile_ref TEXT NOT NULL,
    job_type TEXT NOT NULL,
    transport_type TEXT NOT NULL CHECK (transport_type IN ('CLI', 'API')),
    adapter_type TEXT NOT NULL,
    provider_slug TEXT NOT NULL,
    model_slug TEXT NOT NULL,
    approval_ref TEXT NOT NULL,
    approved_by TEXT NOT NULL,
    approval_note TEXT,
    proposal_hash TEXT NOT NULL,
    workflow_id TEXT,
    bound_run_id TEXT,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'bound', 'consumed', 'revoked', 'expired')),
    max_runs INTEGER NOT NULL DEFAULT 1 CHECK (max_runs = 1),
    consumed_runs INTEGER NOT NULL DEFAULT 0 CHECK (consumed_runs >= 0 AND consumed_runs <= max_runs),
    expires_at TIMESTAMPTZ NOT NULL,
    bound_at TIMESTAMPTZ,
    consumed_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    cost_posture JSONB NOT NULL DEFAULT '{}'::jsonb,
    route_truth_ref TEXT,
    decision_ref TEXT NOT NULL DEFAULT 'architecture-policy::model-access-control::paid-model-use-requires-explicit-scoped-approval-and-hard-off',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE private_paid_model_access_leases IS
    'Exact one-workflow-run leases for paid model access. Leases do not delete hard-off denials; dispatch must prove a matching bound lease before calling a paid backend.';

CREATE INDEX IF NOT EXISTS private_paid_model_access_leases_selector_idx
    ON private_paid_model_access_leases (
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug,
        status,
        expires_at
    );

CREATE INDEX IF NOT EXISTS private_paid_model_access_leases_run_idx
    ON private_paid_model_access_leases (bound_run_id, status)
    WHERE bound_run_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS private_paid_model_access_leases_active_once_idx
    ON private_paid_model_access_leases (
        approval_ref,
        proposal_hash,
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug
    )
    WHERE status IN ('active', 'bound');

CREATE OR REPLACE VIEW private_paid_model_access_state AS
WITH latest_lease AS (
    SELECT DISTINCT ON (
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug
    )
        *
    FROM private_paid_model_access_leases
    ORDER BY
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug,
        created_at DESC
)
SELECT
    control.runtime_profile_ref,
    control.job_type,
    control.transport_type,
    control.adapter_type,
    control.access_method,
    control.provider_slug,
    control.model_slug,
    control.model_version,
    control.cost_structure,
    control.cost_metadata,
    control.control_enabled AS hard_control_enabled,
    control.control_state AS hard_control_state,
    control.control_scope AS hard_control_scope,
    control.control_is_explicit,
    control.control_reason_code AS hard_reason_code,
    control.control_operator_message AS hard_operator_message,
    control.control_decision_ref AS hard_decision_ref,
    COALESCE(soft.presentation_state, 'visible') AS soft_state,
    soft.reason_code AS soft_reason_code,
    soft.operator_message AS soft_operator_message,
    soft.decision_ref AS soft_decision_ref,
    lease.lease_id,
    lease.status AS lease_state,
    lease.approval_ref,
    lease.approved_by,
    lease.proposal_hash,
    lease.bound_run_id,
    lease.expires_at AS lease_expires_at,
    lease.consumed_at AS lease_consumed_at,
    lease.revoked_at AS lease_revoked_at,
    lease.cost_posture,
    lease.route_truth_ref,
    lease.created_at AS lease_created_at,
    (
        lease.lease_id IS NOT NULL
        AND lease.status IN ('active', 'bound')
        AND lease.consumed_at IS NULL
        AND lease.consumed_runs < lease.max_runs
        AND lease.expires_at > now()
    ) AS has_live_lease,
    jsonb_build_object(
        'soft_state', COALESCE(soft.presentation_state, 'visible'),
        'hard_state', control.control_state,
        'lease_state', COALESCE(lease.status, 'none'),
        'lease_scope', 'one_workflow_run',
        'lease_id', lease.lease_id,
        'lease_expires_at', lease.expires_at,
        'cost_posture', COALESCE(lease.cost_posture, jsonb_build_object(
            'cost_structure', control.cost_structure,
            'cost_metadata', control.cost_metadata
        ))
    ) AS paid_model_access
FROM private_model_access_control_matrix AS control
LEFT JOIN private_provider_model_access_soft_offs AS soft
  ON soft.runtime_profile_ref = control.runtime_profile_ref
 AND soft.job_type = control.job_type
 AND soft.transport_type = control.transport_type
 AND soft.adapter_type = control.adapter_type
 AND soft.provider_slug = control.provider_slug
 AND soft.model_slug = control.model_slug
LEFT JOIN latest_lease AS lease
  ON lease.runtime_profile_ref = control.runtime_profile_ref
 AND lease.job_type = control.job_type
 AND lease.transport_type = control.transport_type
 AND lease.adapter_type = control.adapter_type
 AND lease.provider_slug = control.provider_slug
 AND lease.model_slug = control.model_slug;

COMMENT ON VIEW private_paid_model_access_state IS
    'Composed paid-model access read model: existing hard control state, presentation soft-off, and latest one-run lease. Runtime dispatch still checks bound_run_id before provider calls.';

SELECT register_operation_atomic(
    p_operation_ref         := 'operator-paid-model-access',
    p_operation_name        := 'operator.paid_model_access',
    p_handler_ref           := 'runtime.operations.commands.paid_model_access.handle_paid_model_access',
    p_input_model_ref       := 'runtime.operations.commands.paid_model_access.PaidModelAccessCommand',
    p_authority_domain_ref  := 'authority.access_control',
    p_authority_ref         := 'authority.access_control',
    p_operation_kind        := 'command',
    p_http_method           := 'POST',
    p_http_path             := '/api/operator_paid_model_access',
    p_posture               := 'operate',
    p_idempotency_policy    := 'non_idempotent',
    p_event_required        := TRUE,
    p_event_type            := 'model_access_control.paid_model_lease_changed',
    p_timeout_ms            := 5000,
    p_execution_lane        := 'interactive',
    p_kickoff_required      := FALSE,
    p_decision_ref          := 'architecture-policy::model-access-control::paid-model-use-requires-explicit-scoped-approval-and-hard-off',
    p_binding_revision      := 'binding.operation_catalog_registry.paid_model_access.20260501',
    p_label                 := 'Paid Model Access',
    p_summary               := 'Grant, revoke, consume, inspect, and presentation-soft-off exact one-run paid model access leases. Backend hard-off remains private_provider_model_access_denials.'
);

INSERT INTO data_dictionary_objects (
    object_kind,
    label,
    category,
    summary,
    origin_ref,
    metadata
) VALUES
    (
        'table:private_paid_model_access_leases',
        'Private paid model access leases',
        'table',
        'Exact one-workflow-run paid model leases. They are scoped to runtime profile, task, transport, adapter, provider, model, and approval hash.',
        '{"source":"migration.399_paid_model_access_leases"}'::jsonb,
        '{"authority":"authority.access_control","lease_scope":"one_workflow_run"}'::jsonb
    ),
    (
        'table:private_provider_model_access_soft_offs',
        'Private provider model access soft-offs',
        'table',
        'Presentation-only soft-off state for model pickers. Does not grant or deny backend dispatch.',
        '{"source":"migration.399_paid_model_access_leases"}'::jsonb,
        '{"authority":"authority.access_control","dispatch_effect":"none"}'::jsonb
    ),
    (
        'view:private_paid_model_access_state',
        'Private paid model access state',
        'projection',
        'Composed read model joining hard access control, soft presentation state, and latest paid lease state.',
        '{"source":"migration.399_paid_model_access_leases"}'::jsonb,
        '{"authority":"authority.access_control"}'::jsonb
    )
ON CONFLICT (object_kind) DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    summary = EXCLUDED.summary,
    origin_ref = EXCLUDED.origin_ref,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;
