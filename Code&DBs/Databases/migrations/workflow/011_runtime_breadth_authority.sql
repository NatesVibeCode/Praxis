-- Canonical runtime-breadth authority tables for provider failover, endpoint
-- bindings, persona bindings, and fork/worktree orchestration. These rows are
-- the storage authority with no shell-owned defaults or wrapper folklore.

CREATE TABLE provider_failover_bindings (
    provider_failover_binding_id text PRIMARY KEY,
    model_profile_id text NOT NULL,
    provider_policy_id text NOT NULL,
    candidate_ref text NOT NULL,
    binding_scope text NOT NULL,
    failover_role text NOT NULL,
    trigger_rule text NOT NULL,
    position_index integer NOT NULL CHECK (position_index >= 0),
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT provider_failover_bindings_model_profile_fkey
        FOREIGN KEY (model_profile_id)
        REFERENCES model_profiles (model_profile_id)
        ON DELETE RESTRICT,
    CONSTRAINT provider_failover_bindings_provider_policy_fkey
        FOREIGN KEY (provider_policy_id)
        REFERENCES provider_policies (provider_policy_id)
        ON DELETE RESTRICT,
    CONSTRAINT provider_failover_bindings_candidate_fkey
        FOREIGN KEY (candidate_ref)
        REFERENCES provider_model_candidates (candidate_ref)
        ON DELETE RESTRICT,
    CONSTRAINT provider_failover_bindings_unique_scope_position
        UNIQUE (model_profile_id, provider_policy_id, binding_scope, position_index, effective_from),
    CONSTRAINT provider_failover_bindings_unique_candidate_window
        UNIQUE (model_profile_id, provider_policy_id, binding_scope, candidate_ref, effective_from),
    CONSTRAINT provider_failover_bindings_effective_window
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX provider_failover_bindings_scope_idx
    ON provider_failover_bindings (model_profile_id, provider_policy_id, binding_scope, effective_from DESC);

CREATE INDEX provider_failover_bindings_candidate_idx
    ON provider_failover_bindings (candidate_ref, effective_from DESC);

CREATE INDEX provider_failover_bindings_decision_ref_idx
    ON provider_failover_bindings (decision_ref);

COMMENT ON TABLE provider_failover_bindings IS 'Canonical provider failover order over admitted model profile, policy, and candidate rows. Owned by policy/.';
COMMENT ON COLUMN provider_failover_bindings.trigger_rule IS 'Stored machine rule that explains when failover may advance to this candidate. Do not hide failover semantics in router code.';

CREATE TABLE provider_endpoint_bindings (
    provider_endpoint_binding_id text PRIMARY KEY,
    provider_policy_id text NOT NULL,
    candidate_ref text NOT NULL,
    binding_scope text NOT NULL,
    endpoint_ref text NOT NULL,
    endpoint_kind text NOT NULL,
    transport_kind text NOT NULL,
    endpoint_uri text NOT NULL,
    auth_ref text NOT NULL,
    binding_status text NOT NULL,
    request_policy jsonb NOT NULL,
    circuit_breaker_policy jsonb NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT provider_endpoint_bindings_provider_policy_fkey
        FOREIGN KEY (provider_policy_id)
        REFERENCES provider_policies (provider_policy_id)
        ON DELETE RESTRICT,
    CONSTRAINT provider_endpoint_bindings_candidate_fkey
        FOREIGN KEY (candidate_ref)
        REFERENCES provider_model_candidates (candidate_ref)
        ON DELETE RESTRICT,
    CONSTRAINT provider_endpoint_bindings_unique_window
        UNIQUE (provider_policy_id, candidate_ref, endpoint_ref, effective_from),
    CONSTRAINT provider_endpoint_bindings_effective_window
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX provider_endpoint_bindings_policy_status_idx
    ON provider_endpoint_bindings (provider_policy_id, binding_status, effective_from DESC);

CREATE INDEX provider_endpoint_bindings_candidate_endpoint_idx
    ON provider_endpoint_bindings (candidate_ref, endpoint_kind, endpoint_ref);

CREATE INDEX provider_endpoint_bindings_decision_ref_idx
    ON provider_endpoint_bindings (decision_ref);

COMMENT ON TABLE provider_endpoint_bindings IS 'Canonical endpoint bindings from admitted policy and candidate rows onto concrete provider endpoints. Owned by registry/.';
COMMENT ON COLUMN provider_endpoint_bindings.auth_ref IS 'Credential or auth authority reference for the endpoint binding. No shell-owned endpoint auth defaults.';

CREATE TABLE persona_profiles (
    persona_profile_id text PRIMARY KEY,
    persona_name text NOT NULL,
    persona_kind text NOT NULL,
    status text NOT NULL,
    instruction_contract text NOT NULL,
    response_contract jsonb NOT NULL,
    tool_policy jsonb NOT NULL,
    runtime_hints jsonb NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT persona_profiles_name_effective_key
        UNIQUE (persona_name, effective_from),
    CONSTRAINT persona_profiles_effective_window
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX persona_profiles_name_status_idx
    ON persona_profiles (persona_name, status);

CREATE INDEX persona_profiles_kind_effective_idx
    ON persona_profiles (persona_kind, effective_from DESC);

CREATE INDEX persona_profiles_decision_ref_idx
    ON persona_profiles (decision_ref);

COMMENT ON TABLE persona_profiles IS 'Canonical persona profile rows that define stored instruction and response contracts. Owned by registry/.';
COMMENT ON COLUMN persona_profiles.instruction_contract IS 'Stored persona instruction contract. Do not let operator wrappers become the only persona authority.';

CREATE TABLE persona_context_bindings (
    persona_context_binding_id text PRIMARY KEY,
    persona_profile_id text NOT NULL,
    binding_scope text NOT NULL,
    workspace_ref text,
    runtime_profile_ref text,
    model_profile_id text,
    provider_policy_id text,
    context_selector jsonb NOT NULL,
    binding_status text NOT NULL,
    position_index integer NOT NULL CHECK (position_index >= 0),
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT persona_context_bindings_persona_profile_fkey
        FOREIGN KEY (persona_profile_id)
        REFERENCES persona_profiles (persona_profile_id)
        ON DELETE RESTRICT,
    CONSTRAINT persona_context_bindings_model_profile_fkey
        FOREIGN KEY (model_profile_id)
        REFERENCES model_profiles (model_profile_id)
        ON DELETE RESTRICT,
    CONSTRAINT persona_context_bindings_provider_policy_fkey
        FOREIGN KEY (provider_policy_id)
        REFERENCES provider_policies (provider_policy_id)
        ON DELETE RESTRICT,
    CONSTRAINT persona_context_bindings_context_present
        CHECK (
            workspace_ref IS NOT NULL
            OR runtime_profile_ref IS NOT NULL
            OR model_profile_id IS NOT NULL
            OR provider_policy_id IS NOT NULL
        ),
    CONSTRAINT persona_context_bindings_unique_position
        UNIQUE (persona_profile_id, binding_scope, position_index, effective_from),
    CONSTRAINT persona_context_bindings_effective_window
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX persona_context_bindings_profile_idx
    ON persona_context_bindings (persona_profile_id, position_index);

CREATE INDEX persona_context_bindings_context_idx
    ON persona_context_bindings (workspace_ref, runtime_profile_ref, effective_from DESC);

CREATE INDEX persona_context_bindings_model_policy_idx
    ON persona_context_bindings (model_profile_id, provider_policy_id, effective_from DESC);

COMMENT ON TABLE persona_context_bindings IS 'Canonical bindings from persona profiles onto workspace, runtime, model, or provider-policy context. Owned by registry/.';
COMMENT ON COLUMN persona_context_bindings.context_selector IS 'Stored selector contract for persona activation. No shell-owned persona routing.';

CREATE TABLE fork_profiles (
    fork_profile_id text PRIMARY KEY,
    profile_name text NOT NULL,
    orchestration_kind text NOT NULL,
    status text NOT NULL,
    fork_mode text NOT NULL,
    worktree_strategy text NOT NULL,
    sandbox_policy jsonb NOT NULL,
    retention_policy jsonb NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT fork_profiles_name_effective_key
        UNIQUE (profile_name, effective_from),
    CONSTRAINT fork_profiles_effective_window
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX fork_profiles_name_status_idx
    ON fork_profiles (profile_name, status);

CREATE INDEX fork_profiles_kind_effective_idx
    ON fork_profiles (orchestration_kind, effective_from DESC);

CREATE INDEX fork_profiles_decision_ref_idx
    ON fork_profiles (decision_ref);

COMMENT ON TABLE fork_profiles IS 'Canonical fork/worktree orchestration profiles for bounded native execution. Owned by policy/.';
COMMENT ON COLUMN fork_profiles.sandbox_policy IS 'Stored sandbox/worktree policy contract. No shell-owned fork strategy may outrank this row.';

CREATE TABLE fork_worktree_bindings (
    fork_worktree_binding_id text PRIMARY KEY,
    fork_profile_id text NOT NULL,
    sandbox_session_id text NOT NULL,
    workflow_run_id text NOT NULL,
    binding_scope text NOT NULL,
    binding_status text NOT NULL,
    workspace_ref text NOT NULL,
    runtime_profile_ref text NOT NULL,
    base_ref text NOT NULL,
    fork_ref text NOT NULL,
    worktree_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    retired_at timestamptz,
    decision_ref text,
    CONSTRAINT fork_worktree_bindings_fork_profile_fkey
        FOREIGN KEY (fork_profile_id)
        REFERENCES fork_profiles (fork_profile_id)
        ON DELETE RESTRICT,
    CONSTRAINT fork_worktree_bindings_sandbox_session_fkey
        FOREIGN KEY (sandbox_session_id)
        REFERENCES sandbox_sessions (sandbox_session_id)
        ON DELETE RESTRICT,
    CONSTRAINT fork_worktree_bindings_workflow_run_fkey
        FOREIGN KEY (workflow_run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT,
    CONSTRAINT fork_worktree_bindings_unique_binding_scope
        UNIQUE (sandbox_session_id, workflow_run_id, binding_scope),
    CONSTRAINT fork_worktree_bindings_unique_worktree
        UNIQUE (workspace_ref, runtime_profile_ref, fork_ref, worktree_ref),
    CONSTRAINT fork_worktree_bindings_retired_window
        CHECK (retired_at IS NULL OR retired_at >= created_at)
);

CREATE INDEX fork_worktree_bindings_profile_status_idx
    ON fork_worktree_bindings (fork_profile_id, binding_status, created_at DESC);

CREATE INDEX fork_worktree_bindings_sandbox_idx
    ON fork_worktree_bindings (sandbox_session_id, created_at DESC);

CREATE INDEX fork_worktree_bindings_worktree_idx
    ON fork_worktree_bindings (workspace_ref, runtime_profile_ref, worktree_ref);

COMMENT ON TABLE fork_worktree_bindings IS 'Canonical bindings that explain which bounded run owns which fork/worktree authority surface. Owned by runtime/.';
COMMENT ON COLUMN fork_worktree_bindings.worktree_ref IS 'Stored worktree authority reference for one bounded orchestration path. Do not infer worktree ownership from shell state.';
