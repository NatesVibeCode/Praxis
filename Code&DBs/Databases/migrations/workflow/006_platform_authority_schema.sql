-- Canonical platform-authority tables that should not live only in Python.

CREATE TABLE context_bundles (
    context_bundle_id text PRIMARY KEY,
    workflow_id text NOT NULL,
    run_id text NOT NULL UNIQUE,
    workspace_ref text NOT NULL,
    runtime_profile_ref text NOT NULL,
    model_profile_id text NOT NULL,
    provider_policy_id text NOT NULL,
    bundle_version integer NOT NULL CHECK (bundle_version >= 1),
    bundle_hash text NOT NULL,
    bundle_payload jsonb NOT NULL,
    source_decision_refs jsonb NOT NULL,
    resolved_at timestamptz NOT NULL
);

CREATE INDEX context_bundles_workspace_runtime_idx
    ON context_bundles (workspace_ref, runtime_profile_ref);

CREATE INDEX context_bundles_bundle_hash_idx
    ON context_bundles (bundle_hash);

COMMENT ON TABLE context_bundles IS 'Snapshot of resolved workspace, runtime profile, and policy context for a run. Owned by registry/.';
COMMENT ON COLUMN context_bundles.run_id IS 'One immutable bundle per run in v1. Do not revise the bundle in place.';
COMMENT ON COLUMN context_bundles.bundle_version IS 'Bundle schema version. v1 bundles are expected to be version 1.';

CREATE TABLE context_bundle_anchors (
    context_bundle_anchor_id text PRIMARY KEY,
    context_bundle_id text NOT NULL,
    anchor_ref text NOT NULL,
    anchor_kind text NOT NULL,
    content_hash text NOT NULL,
    anchor_payload jsonb NOT NULL,
    position_index integer NOT NULL CHECK (position_index >= 0),
    anchored_at timestamptz NOT NULL,
    CONSTRAINT context_bundle_anchors_bundle_fkey
        FOREIGN KEY (context_bundle_id)
        REFERENCES context_bundles (context_bundle_id)
        ON DELETE CASCADE,
    CONSTRAINT context_bundle_anchors_unique_window
        UNIQUE (context_bundle_id, anchor_kind, anchor_ref),
    CONSTRAINT context_bundle_anchors_position_window
        UNIQUE (context_bundle_id, position_index)
);

CREATE INDEX context_bundle_anchors_bundle_idx
    ON context_bundle_anchors (context_bundle_id, anchored_at DESC);

CREATE INDEX context_bundle_anchors_ref_idx
    ON context_bundle_anchors (anchor_kind, anchor_ref);

COMMENT ON TABLE context_bundle_anchors IS 'Immutable anchor rows that explain how a stored context bundle was derived. Owned by registry/.';
COMMENT ON COLUMN context_bundle_anchors.position_index IS 'Deterministic anchor order inside one bundle snapshot.';

CREATE TABLE provider_model_candidates (
    candidate_ref text PRIMARY KEY,
    provider_ref text NOT NULL,
    provider_name text NOT NULL,
    provider_slug text NOT NULL,
    model_slug text NOT NULL,
    status text NOT NULL,
    priority integer NOT NULL CHECK (priority >= 0),
    balance_weight integer NOT NULL CHECK (balance_weight > 0),
    capability_tags jsonb NOT NULL,
    default_parameters jsonb NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX provider_model_candidates_provider_ref_status_idx
    ON provider_model_candidates (provider_ref, status, effective_from DESC);

CREATE INDEX provider_model_candidates_slug_idx
    ON provider_model_candidates (provider_slug, model_slug);

CREATE INDEX provider_model_candidates_decision_ref_idx
    ON provider_model_candidates (decision_ref);

COMMENT ON TABLE provider_model_candidates IS 'Canonical provider/model candidate catalog behind stable candidate refs. Owned by registry/.';
COMMENT ON COLUMN provider_model_candidates.decision_ref IS 'Decision reference that approved the candidate row. Do not treat code defaults as the authority.';

CREATE TABLE model_profile_candidate_bindings (
    model_profile_candidate_binding_id text PRIMARY KEY,
    model_profile_id text NOT NULL,
    candidate_ref text NOT NULL,
    binding_role text NOT NULL,
    position_index integer NOT NULL CHECK (position_index >= 0),
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    created_at timestamptz NOT NULL,
    CONSTRAINT model_profile_candidate_bindings_model_profile_fkey
        FOREIGN KEY (model_profile_id)
        REFERENCES model_profiles (model_profile_id)
        ON DELETE RESTRICT,
    CONSTRAINT model_profile_candidate_bindings_candidate_fkey
        FOREIGN KEY (candidate_ref)
        REFERENCES provider_model_candidates (candidate_ref)
        ON DELETE RESTRICT,
    CONSTRAINT model_profile_candidate_bindings_unique_window
        UNIQUE (model_profile_id, candidate_ref, effective_from)
);

CREATE INDEX model_profile_candidate_bindings_profile_idx
    ON model_profile_candidate_bindings (model_profile_id, position_index);

CREATE INDEX model_profile_candidate_bindings_candidate_idx
    ON model_profile_candidate_bindings (candidate_ref, effective_from DESC);

COMMENT ON TABLE model_profile_candidate_bindings IS 'Canonical bindings from model profiles to admitted provider/model candidates. Owned by registry/.';

CREATE TABLE event_subscriptions (
    subscription_id text PRIMARY KEY,
    subscription_name text NOT NULL,
    consumer_kind text NOT NULL,
    envelope_kind text NOT NULL,
    workflow_id text,
    run_id text,
    cursor_scope text NOT NULL,
    status text NOT NULL,
    delivery_policy jsonb NOT NULL,
    filter_policy jsonb NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX event_subscriptions_status_consumer_idx
    ON event_subscriptions (status, consumer_kind);

CREATE INDEX event_subscriptions_workflow_run_idx
    ON event_subscriptions (workflow_id, run_id);

COMMENT ON TABLE event_subscriptions IS 'Durable subscriber definitions over workflow outbox authority. Owned by runtime/.';

CREATE TABLE subscription_checkpoints (
    checkpoint_id text PRIMARY KEY,
    subscription_id text NOT NULL,
    run_id text NOT NULL,
    last_evidence_seq bigint,
    last_authority_id text,
    checkpoint_status text NOT NULL,
    checkpointed_at timestamptz NOT NULL,
    metadata jsonb NOT NULL,
    CONSTRAINT subscription_checkpoints_subscription_fkey
        FOREIGN KEY (subscription_id)
        REFERENCES event_subscriptions (subscription_id)
        ON DELETE CASCADE,
    CONSTRAINT subscription_checkpoints_subscription_run_key
        UNIQUE (subscription_id, run_id)
);

CREATE INDEX subscription_checkpoints_subscription_idx
    ON subscription_checkpoints (subscription_id, checkpointed_at DESC);

CREATE INDEX subscription_checkpoints_run_idx
    ON subscription_checkpoints (run_id, checkpointed_at DESC);

COMMENT ON TABLE subscription_checkpoints IS 'Durable consumer offsets/checkpoints for event subscriptions. Owned by runtime/.';

CREATE TABLE workflow_lanes (
    workflow_lane_id text PRIMARY KEY,
    lane_name text NOT NULL,
    lane_kind text NOT NULL,
    status text NOT NULL,
    concurrency_cap integer NOT NULL CHECK (concurrency_cap > 0),
    default_route_kind text NOT NULL,
    review_required boolean NOT NULL DEFAULT false,
    retry_policy jsonb NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    created_at timestamptz NOT NULL
);

CREATE INDEX workflow_lanes_name_status_idx
    ON workflow_lanes (lane_name, status);

CREATE INDEX workflow_lanes_kind_effective_idx
    ON workflow_lanes (lane_kind, effective_from DESC);

COMMENT ON TABLE workflow_lanes IS 'Canonical native workflow lane catalog. Owned by policy/.';

CREATE TABLE workflow_lane_policies (
    workflow_lane_policy_id text PRIMARY KEY,
    workflow_lane_id text NOT NULL,
    policy_scope text NOT NULL,
    work_kind text NOT NULL,
    match_rules jsonb NOT NULL,
    lane_parameters jsonb NOT NULL,
    decision_ref text NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    created_at timestamptz NOT NULL,
    CONSTRAINT workflow_lane_policies_lane_fkey
        FOREIGN KEY (workflow_lane_id)
        REFERENCES workflow_lanes (workflow_lane_id)
        ON DELETE CASCADE,
    CONSTRAINT workflow_lane_policies_unique_window
        UNIQUE (workflow_lane_id, policy_scope, work_kind, effective_from)
);

CREATE INDEX workflow_lane_policies_lane_idx
    ON workflow_lane_policies (workflow_lane_id, effective_from DESC);

CREATE INDEX workflow_lane_policies_scope_kind_idx
    ON workflow_lane_policies (policy_scope, work_kind, effective_from DESC);

CREATE INDEX workflow_lane_policies_decision_ref_idx
    ON workflow_lane_policies (decision_ref);

COMMENT ON TABLE workflow_lane_policies IS 'Canonical policy bindings that map work classes onto native workflow lanes. Owned by policy/.';
