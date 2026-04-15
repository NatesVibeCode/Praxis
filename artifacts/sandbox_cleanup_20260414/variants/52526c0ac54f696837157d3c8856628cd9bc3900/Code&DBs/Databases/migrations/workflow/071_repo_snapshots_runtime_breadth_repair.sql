BEGIN;

CREATE TABLE IF NOT EXISTS repo_snapshots (
    repo_snapshot_ref TEXT PRIMARY KEY,
    repo_root TEXT NOT NULL,
    repo_fingerprint TEXT NOT NULL,
    git_head TEXT NOT NULL,
    git_branch TEXT NOT NULL,
    git_dirty BOOLEAN NOT NULL DEFAULT FALSE,
    git_status_hash TEXT NOT NULL,
    workspace_ref TEXT,
    runtime_profile_ref TEXT,
    packet_provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT repo_snapshots_repo_unique
        UNIQUE (repo_root, repo_fingerprint),
    CONSTRAINT repo_snapshots_packet_provenance_object_check
        CHECK (jsonb_typeof(packet_provenance) = 'object')
);

CREATE INDEX IF NOT EXISTS repo_snapshots_repo_fingerprint_idx
    ON repo_snapshots (repo_fingerprint, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS repo_snapshots_workspace_runtime_idx
    ON repo_snapshots (workspace_ref, runtime_profile_ref, last_seen_at DESC);

COMMENT ON TABLE repo_snapshots IS 'Canonical repo snapshot authority for receipt and proof provenance. Receipts should point at this row instead of duplicating full git state.';
COMMENT ON COLUMN repo_snapshots.repo_snapshot_ref IS 'Stable repo snapshot reference derived from repo_root and repo_fingerprint.';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'persona_profiles'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'persona_profiles'
          AND column_name = 'persona_profile_id'
    ) THEN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'persona_profiles_legacy_20260409'
        ) THEN
            ALTER TABLE persona_profiles RENAME TO persona_profiles_legacy_20260409;
        END IF;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'persona_profiles_legacy_20260409'
    ) AND EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'persona_profiles_pkey'
          AND conrelid = 'public.persona_profiles_legacy_20260409'::regclass
    ) THEN
        ALTER TABLE persona_profiles_legacy_20260409
            RENAME CONSTRAINT persona_profiles_pkey
            TO persona_profiles_legacy_20260409_pkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'persona_context_bindings'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'persona_context_bindings'
          AND column_name = 'persona_context_binding_id'
    ) THEN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'persona_context_bindings_legacy_20260409'
        ) THEN
            ALTER TABLE persona_context_bindings RENAME TO persona_context_bindings_legacy_20260409;
        END IF;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS provider_failover_bindings (
    provider_failover_binding_id TEXT PRIMARY KEY,
    model_profile_id TEXT NOT NULL,
    provider_policy_id TEXT NOT NULL,
    candidate_ref TEXT NOT NULL,
    binding_scope TEXT NOT NULL,
    failover_role TEXT NOT NULL,
    trigger_rule TEXT NOT NULL,
    position_index INTEGER NOT NULL CHECK (position_index >= 0),
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
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

CREATE INDEX IF NOT EXISTS provider_failover_bindings_scope_idx
    ON provider_failover_bindings (model_profile_id, provider_policy_id, binding_scope, effective_from DESC);

CREATE INDEX IF NOT EXISTS provider_failover_bindings_candidate_idx
    ON provider_failover_bindings (candidate_ref, effective_from DESC);

CREATE INDEX IF NOT EXISTS provider_failover_bindings_decision_ref_idx
    ON provider_failover_bindings (decision_ref);

CREATE TABLE IF NOT EXISTS provider_endpoint_bindings (
    provider_endpoint_binding_id TEXT PRIMARY KEY,
    provider_policy_id TEXT NOT NULL,
    candidate_ref TEXT NOT NULL,
    binding_scope TEXT NOT NULL,
    endpoint_ref TEXT NOT NULL,
    endpoint_kind TEXT NOT NULL,
    transport_kind TEXT NOT NULL,
    endpoint_uri TEXT NOT NULL,
    auth_ref TEXT NOT NULL,
    binding_status TEXT NOT NULL,
    request_policy JSONB NOT NULL,
    circuit_breaker_policy JSONB NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
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

CREATE INDEX IF NOT EXISTS provider_endpoint_bindings_policy_status_idx
    ON provider_endpoint_bindings (provider_policy_id, binding_status, effective_from DESC);

CREATE INDEX IF NOT EXISTS provider_endpoint_bindings_candidate_endpoint_idx
    ON provider_endpoint_bindings (candidate_ref, endpoint_kind, endpoint_ref);

CREATE INDEX IF NOT EXISTS provider_endpoint_bindings_decision_ref_idx
    ON provider_endpoint_bindings (decision_ref);

CREATE TABLE IF NOT EXISTS persona_profiles (
    persona_profile_id TEXT PRIMARY KEY,
    persona_name TEXT NOT NULL,
    persona_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    instruction_contract TEXT NOT NULL,
    response_contract JSONB NOT NULL,
    tool_policy JSONB NOT NULL,
    runtime_hints JSONB NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT persona_profiles_name_effective_key
        UNIQUE (persona_name, effective_from),
    CONSTRAINT persona_profiles_effective_window
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX IF NOT EXISTS persona_profiles_name_status_idx
    ON persona_profiles (persona_name, status);

CREATE INDEX IF NOT EXISTS persona_profiles_kind_effective_idx
    ON persona_profiles (persona_kind, effective_from DESC);

CREATE INDEX IF NOT EXISTS persona_profiles_decision_ref_idx
    ON persona_profiles (decision_ref);

CREATE TABLE IF NOT EXISTS persona_context_bindings (
    persona_context_binding_id TEXT PRIMARY KEY,
    persona_profile_id TEXT NOT NULL,
    binding_scope TEXT NOT NULL,
    workspace_ref TEXT,
    runtime_profile_ref TEXT,
    model_profile_id TEXT,
    provider_policy_id TEXT,
    context_selector JSONB NOT NULL,
    binding_status TEXT NOT NULL,
    position_index INTEGER NOT NULL CHECK (position_index >= 0),
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
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

CREATE INDEX IF NOT EXISTS persona_context_bindings_profile_idx
    ON persona_context_bindings (persona_profile_id, position_index);

CREATE INDEX IF NOT EXISTS persona_context_bindings_context_idx
    ON persona_context_bindings (workspace_ref, runtime_profile_ref, effective_from DESC);

CREATE INDEX IF NOT EXISTS persona_context_bindings_model_policy_idx
    ON persona_context_bindings (model_profile_id, provider_policy_id, effective_from DESC);

CREATE TABLE IF NOT EXISTS fork_profiles (
    fork_profile_id TEXT PRIMARY KEY,
    profile_name TEXT NOT NULL,
    orchestration_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    fork_mode TEXT NOT NULL,
    worktree_strategy TEXT NOT NULL,
    sandbox_policy JSONB NOT NULL,
    retention_policy JSONB NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    decision_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fork_profiles_name_effective_key
        UNIQUE (profile_name, effective_from),
    CONSTRAINT fork_profiles_effective_window
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE INDEX IF NOT EXISTS fork_profiles_name_status_idx
    ON fork_profiles (profile_name, status);

CREATE INDEX IF NOT EXISTS fork_profiles_kind_effective_idx
    ON fork_profiles (orchestration_kind, effective_from DESC);

CREATE INDEX IF NOT EXISTS fork_profiles_decision_ref_idx
    ON fork_profiles (decision_ref);

CREATE TABLE IF NOT EXISTS fork_worktree_bindings (
    fork_worktree_binding_id TEXT PRIMARY KEY,
    fork_profile_id TEXT NOT NULL,
    sandbox_session_id TEXT NOT NULL,
    workflow_run_id TEXT NOT NULL,
    binding_scope TEXT NOT NULL,
    binding_status TEXT NOT NULL,
    workspace_ref TEXT NOT NULL,
    runtime_profile_ref TEXT NOT NULL,
    base_ref TEXT NOT NULL,
    fork_ref TEXT NOT NULL,
    worktree_ref TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    retired_at TIMESTAMPTZ,
    decision_ref TEXT,
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

CREATE INDEX IF NOT EXISTS fork_worktree_bindings_profile_status_idx
    ON fork_worktree_bindings (fork_profile_id, binding_status, created_at DESC);

CREATE INDEX IF NOT EXISTS fork_worktree_bindings_sandbox_idx
    ON fork_worktree_bindings (sandbox_session_id, created_at DESC);

CREATE INDEX IF NOT EXISTS fork_worktree_bindings_worktree_idx
    ON fork_worktree_bindings (workspace_ref, runtime_profile_ref, worktree_ref);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'persona_profiles_legacy_20260409'
    ) THEN
        INSERT INTO persona_profiles (
            persona_profile_id,
            persona_name,
            persona_kind,
            status,
            instruction_contract,
            response_contract,
            tool_policy,
            runtime_hints,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        )
        SELECT
            legacy.persona_ref,
            legacy.persona_name,
            legacy.persona_kind,
            'legacy_imported',
            format(
                'Legacy persona imported from runtime_profile_ref=%s default_context_ref=%s',
                COALESCE(legacy.runtime_profile_ref, ''),
                COALESCE(legacy.default_context_ref, '')
            ),
            '{}'::jsonb,
            jsonb_build_object('legacy_import', true),
            jsonb_build_object(
                'legacy_runtime_profile_ref', legacy.runtime_profile_ref,
                'legacy_default_context_ref', legacy.default_context_ref,
                'legacy_source_table', 'persona_profiles_legacy_20260409'
            ),
            legacy.created_at,
            NULL::timestamptz,
            'decision.runtime_breadth_legacy_import.20260409',
            legacy.created_at
        FROM persona_profiles_legacy_20260409 AS legacy
        ON CONFLICT (persona_profile_id) DO UPDATE
        SET persona_name = EXCLUDED.persona_name,
            persona_kind = EXCLUDED.persona_kind,
            status = EXCLUDED.status,
            instruction_contract = EXCLUDED.instruction_contract,
            response_contract = EXCLUDED.response_contract,
            tool_policy = EXCLUDED.tool_policy,
            runtime_hints = EXCLUDED.runtime_hints,
            effective_from = EXCLUDED.effective_from,
            effective_to = EXCLUDED.effective_to,
            decision_ref = EXCLUDED.decision_ref,
            created_at = EXCLUDED.created_at;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'persona_context_bindings_legacy_20260409'
    ) THEN
        INSERT INTO persona_context_bindings (
            persona_context_binding_id,
            persona_profile_id,
            binding_scope,
            workspace_ref,
            runtime_profile_ref,
            model_profile_id,
            provider_policy_id,
            context_selector,
            binding_status,
            position_index,
            effective_from,
            effective_to,
            decision_ref,
            created_at
        )
        SELECT
            'persona_context_binding.legacy.'
                || substr(
                    md5(
                        legacy.persona_ref
                        || '|'
                        || legacy.context_ref
                        || '|'
                        || legacy.binding_role
                        || '|'
                        || legacy.position_index::text
                    ),
                    1,
                    16
                ),
            legacy.persona_ref,
            'legacy_import',
            'dag-project',
            'dag-project',
            NULL,
            NULL,
            jsonb_build_object(
                'legacy_context_ref', legacy.context_ref,
                'binding_role', legacy.binding_role,
                'operator_path', 'native_operator_surface',
                'legacy_import', true
            ),
            'legacy_imported',
            legacy.position_index,
            legacy.effective_from,
            legacy.effective_to,
            'decision.runtime_breadth_legacy_import.20260409',
            legacy.created_at
        FROM persona_context_bindings_legacy_20260409 AS legacy
        ON CONFLICT (persona_context_binding_id) DO UPDATE
        SET persona_profile_id = EXCLUDED.persona_profile_id,
            binding_scope = EXCLUDED.binding_scope,
            workspace_ref = EXCLUDED.workspace_ref,
            runtime_profile_ref = EXCLUDED.runtime_profile_ref,
            model_profile_id = EXCLUDED.model_profile_id,
            provider_policy_id = EXCLUDED.provider_policy_id,
            context_selector = EXCLUDED.context_selector,
            binding_status = EXCLUDED.binding_status,
            position_index = EXCLUDED.position_index,
            effective_from = EXCLUDED.effective_from,
            effective_to = EXCLUDED.effective_to,
            decision_ref = EXCLUDED.decision_ref,
            created_at = EXCLUDED.created_at;
    END IF;
END $$;

COMMIT;
