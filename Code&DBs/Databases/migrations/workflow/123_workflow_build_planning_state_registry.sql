BEGIN;

CREATE TABLE IF NOT EXISTS review_policy_definitions (
    review_policy_ref TEXT PRIMARY KEY,
    allowed_actor_types_json JSONB NOT NULL,
    required_approval_count INTEGER NOT NULL CHECK (required_approval_count > 0),
    sensitive_target_kinds_json JSONB NOT NULL,
    defer_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    widen_allowed BOOLEAN NOT NULL DEFAULT TRUE,
    proposal_request_allowed BOOLEAN NOT NULL DEFAULT TRUE,
    escalation_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    policy_scope TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT review_policy_definitions_allowed_actor_types_array
        CHECK (jsonb_typeof(allowed_actor_types_json) = 'array'),
    CONSTRAINT review_policy_definitions_sensitive_target_kinds_array
        CHECK (jsonb_typeof(sensitive_target_kinds_json) = 'array'),
    CONSTRAINT review_policy_definitions_escalation_policy_object
        CHECK (jsonb_typeof(escalation_policy_json) = 'object')
);

CREATE INDEX IF NOT EXISTS review_policy_definitions_status_scope_idx
    ON review_policy_definitions (status, policy_scope, review_policy_ref);

CREATE TABLE IF NOT EXISTS capability_bundle_definitions (
    bundle_ref TEXT PRIMARY KEY,
    family TEXT NOT NULL,
    intent_tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    allowed_mcp_tools_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    allowed_adapter_tools_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    verification_policy_template_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    submission_policy_template_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    review_policy_template_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    workflow_shape_affinities_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    policy_overlays_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT capability_bundle_definitions_intent_tags_array
        CHECK (jsonb_typeof(intent_tags_json) = 'array'),
    CONSTRAINT capability_bundle_definitions_allowed_mcp_tools_array
        CHECK (jsonb_typeof(allowed_mcp_tools_json) = 'array'),
    CONSTRAINT capability_bundle_definitions_allowed_adapter_tools_array
        CHECK (jsonb_typeof(allowed_adapter_tools_json) = 'array'),
    CONSTRAINT capability_bundle_definitions_verification_policy_object
        CHECK (jsonb_typeof(verification_policy_template_json) = 'object'),
    CONSTRAINT capability_bundle_definitions_submission_policy_object
        CHECK (jsonb_typeof(submission_policy_template_json) = 'object'),
    CONSTRAINT capability_bundle_definitions_review_policy_object
        CHECK (jsonb_typeof(review_policy_template_json) = 'object'),
    CONSTRAINT capability_bundle_definitions_shape_affinities_array
        CHECK (jsonb_typeof(workflow_shape_affinities_json) = 'array'),
    CONSTRAINT capability_bundle_definitions_policy_overlays_object
        CHECK (jsonb_typeof(policy_overlays_json) = 'object')
);

CREATE INDEX IF NOT EXISTS capability_bundle_definitions_status_family_idx
    ON capability_bundle_definitions (status, family, bundle_ref);

CREATE TABLE IF NOT EXISTS workflow_shape_family_definitions (
    shape_family_ref TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    shape_template_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    default_bundle_affinities_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    review_policy_ref TEXT REFERENCES review_policy_definitions(review_policy_ref),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT workflow_shape_family_definitions_shape_template_object
        CHECK (jsonb_typeof(shape_template_json) = 'object'),
    CONSTRAINT workflow_shape_family_definitions_default_bundle_affinities_array
        CHECK (jsonb_typeof(default_bundle_affinities_json) = 'array')
);

CREATE INDEX IF NOT EXISTS workflow_shape_family_definitions_status_policy_idx
    ON workflow_shape_family_definitions (status, review_policy_ref, shape_family_ref);

CREATE TABLE IF NOT EXISTS workflow_build_intents (
    intent_ref TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    definition_revision TEXT NOT NULL,
    source_mode TEXT NOT NULL,
    goal TEXT NOT NULL,
    desired_outcome TEXT NOT NULL,
    constraints_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    success_criteria_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    referenced_entities_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    uncertainty_markers_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    bootstrap_state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT workflow_build_intents_constraints_array
        CHECK (jsonb_typeof(constraints_json) = 'array'),
    CONSTRAINT workflow_build_intents_success_criteria_array
        CHECK (jsonb_typeof(success_criteria_json) = 'array'),
    CONSTRAINT workflow_build_intents_referenced_entities_array
        CHECK (jsonb_typeof(referenced_entities_json) = 'array'),
    CONSTRAINT workflow_build_intents_uncertainty_markers_array
        CHECK (jsonb_typeof(uncertainty_markers_json) = 'array'),
    CONSTRAINT workflow_build_intents_bootstrap_state_object
        CHECK (jsonb_typeof(bootstrap_state_json) = 'object'),
    CONSTRAINT workflow_build_intents_workflow_definition_unique
        UNIQUE (workflow_id, definition_revision)
);

CREATE INDEX IF NOT EXISTS workflow_build_intents_workflow_definition_idx
    ON workflow_build_intents (workflow_id, definition_revision, updated_at DESC);

CREATE TABLE IF NOT EXISTS workflow_build_candidate_manifests (
    manifest_ref TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    definition_revision TEXT NOT NULL,
    manifest_revision TEXT NOT NULL,
    intent_ref TEXT NOT NULL REFERENCES workflow_build_intents(intent_ref) ON DELETE CASCADE,
    review_group_ref TEXT NOT NULL,
    execution_readiness TEXT NOT NULL CHECK (execution_readiness IN ('ready', 'review_required', 'blocked')),
    projection_status_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    blocking_issues_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    required_confirmations_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT workflow_build_candidate_manifests_projection_status_object
        CHECK (jsonb_typeof(projection_status_json) = 'object'),
    CONSTRAINT workflow_build_candidate_manifests_blocking_issues_array
        CHECK (jsonb_typeof(blocking_issues_json) = 'array'),
    CONSTRAINT workflow_build_candidate_manifests_required_confirmations_array
        CHECK (jsonb_typeof(required_confirmations_json) = 'array')
);

CREATE INDEX IF NOT EXISTS workflow_build_candidate_manifests_workflow_definition_idx
    ON workflow_build_candidate_manifests (workflow_id, definition_revision, updated_at DESC);

CREATE TABLE IF NOT EXISTS workflow_build_candidate_slots (
    manifest_ref TEXT NOT NULL REFERENCES workflow_build_candidate_manifests(manifest_ref) ON DELETE CASCADE,
    slot_ref TEXT NOT NULL,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    definition_revision TEXT NOT NULL,
    manifest_revision TEXT NOT NULL,
    slot_kind TEXT NOT NULL,
    required BOOLEAN NOT NULL DEFAULT TRUE,
    candidate_resolution_state TEXT NOT NULL CHECK (candidate_resolution_state IN ('candidate_set', 'unresolved', 'deferred', 'blocked')),
    approval_state TEXT NOT NULL CHECK (approval_state IN ('unapproved', 'approved', 'rejected', 'deferred', 'blocked')),
    source_binding_ref TEXT,
    source_evidence_ref TEXT,
    top_ranked_ref TEXT,
    approved_ref TEXT,
    resolution_rationale TEXT,
    slot_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (manifest_ref, slot_ref),
    CONSTRAINT workflow_build_candidate_slots_slot_metadata_object
        CHECK (jsonb_typeof(slot_metadata_json) = 'object')
);

CREATE INDEX IF NOT EXISTS workflow_build_candidate_slots_manifest_kind_idx
    ON workflow_build_candidate_slots (manifest_ref, slot_kind, approval_state, required);

CREATE TABLE IF NOT EXISTS workflow_build_candidates (
    manifest_ref TEXT NOT NULL REFERENCES workflow_build_candidate_manifests(manifest_ref) ON DELETE CASCADE,
    slot_ref TEXT NOT NULL,
    candidate_ref TEXT NOT NULL,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    definition_revision TEXT NOT NULL,
    manifest_revision TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    rank INTEGER NOT NULL CHECK (rank > 0),
    fit_score DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    source_def_ref TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    candidate_approval_state TEXT NOT NULL CHECK (candidate_approval_state IN ('proposed', 'approved', 'rejected', 'superseded')),
    candidate_rationale TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (manifest_ref, slot_ref, candidate_ref),
    CONSTRAINT workflow_build_candidates_payload_object
        CHECK (jsonb_typeof(payload_json) = 'object'),
    CONSTRAINT workflow_build_candidates_slot_ref_fkey
        FOREIGN KEY (manifest_ref, slot_ref)
        REFERENCES workflow_build_candidate_slots(manifest_ref, slot_ref)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS workflow_build_candidates_manifest_slot_rank_idx
    ON workflow_build_candidates (manifest_ref, slot_ref, rank, candidate_ref);

CREATE TABLE IF NOT EXISTS workflow_build_review_sessions (
    review_group_ref TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    definition_revision TEXT NOT NULL,
    manifest_ref TEXT NOT NULL REFERENCES workflow_build_candidate_manifests(manifest_ref) ON DELETE CASCADE,
    review_policy_ref TEXT NOT NULL REFERENCES review_policy_definitions(review_policy_ref),
    status TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS workflow_build_review_sessions_workflow_definition_idx
    ON workflow_build_review_sessions (workflow_id, definition_revision, opened_at DESC);

CREATE TABLE IF NOT EXISTS workflow_build_execution_manifests (
    execution_manifest_ref TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    definition_revision TEXT NOT NULL,
    manifest_ref TEXT NOT NULL REFERENCES workflow_build_candidate_manifests(manifest_ref) ON DELETE CASCADE,
    review_group_ref TEXT NOT NULL REFERENCES workflow_build_review_sessions(review_group_ref) ON DELETE CASCADE,
    compiled_spec_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    resolved_bindings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    approved_bundle_refs_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    tool_allowlist_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    verify_refs_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    policy_gates_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    hardening_report_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT workflow_build_execution_manifests_compiled_spec_object
        CHECK (jsonb_typeof(compiled_spec_json) = 'object'),
    CONSTRAINT workflow_build_execution_manifests_resolved_bindings_array
        CHECK (jsonb_typeof(resolved_bindings_json) = 'array'),
    CONSTRAINT workflow_build_execution_manifests_approved_bundle_refs_array
        CHECK (jsonb_typeof(approved_bundle_refs_json) = 'array'),
    CONSTRAINT workflow_build_execution_manifests_tool_allowlist_object
        CHECK (jsonb_typeof(tool_allowlist_json) = 'object'),
    CONSTRAINT workflow_build_execution_manifests_verify_refs_array
        CHECK (jsonb_typeof(verify_refs_json) = 'array'),
    CONSTRAINT workflow_build_execution_manifests_policy_gates_object
        CHECK (jsonb_typeof(policy_gates_json) = 'object'),
    CONSTRAINT workflow_build_execution_manifests_hardening_report_object
        CHECK (jsonb_typeof(hardening_report_json) = 'object')
);

CREATE INDEX IF NOT EXISTS workflow_build_execution_manifests_workflow_definition_idx
    ON workflow_build_execution_manifests (workflow_id, definition_revision, created_at DESC);

INSERT INTO review_policy_definitions (
    review_policy_ref,
    allowed_actor_types_json,
    required_approval_count,
    sensitive_target_kinds_json,
    defer_allowed,
    widen_allowed,
    proposal_request_allowed,
    escalation_policy_json,
    policy_scope,
    status
) VALUES (
    'review_policy:workflow_build/default',
    '["model","human","policy"]'::jsonb,
    1,
    '["binding","import_snapshot","capability_bundle","workflow_shape"]'::jsonb,
    TRUE,
    TRUE,
    TRUE,
    '{"escalate_to":"human","reason":"policy.default"}'::jsonb,
    'workflow_build/default',
    'active'
)
ON CONFLICT (review_policy_ref) DO UPDATE SET
    allowed_actor_types_json = EXCLUDED.allowed_actor_types_json,
    required_approval_count = EXCLUDED.required_approval_count,
    sensitive_target_kinds_json = EXCLUDED.sensitive_target_kinds_json,
    defer_allowed = EXCLUDED.defer_allowed,
    widen_allowed = EXCLUDED.widen_allowed,
    proposal_request_allowed = EXCLUDED.proposal_request_allowed,
    escalation_policy_json = EXCLUDED.escalation_policy_json,
    policy_scope = EXCLUDED.policy_scope,
    status = EXCLUDED.status,
    updated_at = now();

INSERT INTO capability_bundle_definitions (
    bundle_ref,
    family,
    intent_tags_json,
    allowed_mcp_tools_json,
    allowed_adapter_tools_json,
    verification_policy_template_json,
    submission_policy_template_json,
    review_policy_template_json,
    workflow_shape_affinities_json,
    policy_overlays_json,
    status
) VALUES
    (
        'capability_bundle:email_triage',
        'support_triage',
        '["support","email","triage","inbox","reply"]'::jsonb,
        '["praxis_query","praxis_status","praxis_integration","praxis_review_submission"]'::jsonb,
        '[]'::jsonb,
        '{"verify_refs":["verify_ref.workflow.support_triage"]}'::jsonb,
        '{"submission_required":false}'::jsonb,
        '{"review_required":true,"review_policy_ref":"review_policy:workflow_build/default"}'::jsonb,
        '["workflow_shape_family:support_inbox"]'::jsonb,
        '{}'::jsonb,
        'active'
    ),
    (
        'capability_bundle:invoice_processing',
        'ap_invoice',
        '["invoice","ap","accounts","payable","vendor","payment","erp"]'::jsonb,
        '["praxis_query","praxis_status","praxis_integration","praxis_review_submission"]'::jsonb,
        '[]'::jsonb,
        '{"verify_refs":["verify_ref.workflow.ap_invoice"]}'::jsonb,
        '{"submission_required":false}'::jsonb,
        '{"review_required":true,"review_policy_ref":"review_policy:workflow_build/default"}'::jsonb,
        '["workflow_shape_family:ap_invoice"]'::jsonb,
        '{}'::jsonb,
        'active'
    )
ON CONFLICT (bundle_ref) DO UPDATE SET
    family = EXCLUDED.family,
    intent_tags_json = EXCLUDED.intent_tags_json,
    allowed_mcp_tools_json = EXCLUDED.allowed_mcp_tools_json,
    allowed_adapter_tools_json = EXCLUDED.allowed_adapter_tools_json,
    verification_policy_template_json = EXCLUDED.verification_policy_template_json,
    submission_policy_template_json = EXCLUDED.submission_policy_template_json,
    review_policy_template_json = EXCLUDED.review_policy_template_json,
    workflow_shape_affinities_json = EXCLUDED.workflow_shape_affinities_json,
    policy_overlays_json = EXCLUDED.policy_overlays_json,
    status = EXCLUDED.status,
    updated_at = now();

INSERT INTO workflow_shape_family_definitions (
    shape_family_ref,
    name,
    shape_template_json,
    default_bundle_affinities_json,
    review_policy_ref,
    status
) VALUES
    (
        'workflow_shape_family:support_inbox',
        'Support Inbox Review',
        '{"kind":"builder_shape","summary":"Support inbox triage with gated reply path."}'::jsonb,
        '["capability_bundle:email_triage"]'::jsonb,
        'review_policy:workflow_build/default',
        'active'
    ),
    (
        'workflow_shape_family:ap_invoice',
        'AP Invoice Processing',
        '{"kind":"builder_shape","summary":"Invoice ingestion, vendor match, approval, and payable update."}'::jsonb,
        '["capability_bundle:invoice_processing"]'::jsonb,
        'review_policy:workflow_build/default',
        'active'
    )
ON CONFLICT (shape_family_ref) DO UPDATE SET
    name = EXCLUDED.name,
    shape_template_json = EXCLUDED.shape_template_json,
    default_bundle_affinities_json = EXCLUDED.default_bundle_affinities_json,
    review_policy_ref = EXCLUDED.review_policy_ref,
    status = EXCLUDED.status,
    updated_at = now();

COMMIT;
