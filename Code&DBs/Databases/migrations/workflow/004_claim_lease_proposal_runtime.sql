-- Runtime-owned claim/lease/proposal route support rows.
-- workflow_runs remains the canonical lifecycle state authority.
-- These tables add the minimal lineage, concurrency, and sandbox support data
-- needed to make claim/lease/proposal mechanics real.

CREATE TABLE IF NOT EXISTS workflow_claim_lease_proposal_runtime (
    run_id text PRIMARY KEY,
    workflow_id text NOT NULL,
    request_id text NOT NULL,
    authority_context_ref text NOT NULL,
    authority_context_digest text NOT NULL,
    claim_id text NOT NULL,
    lease_id text,
    proposal_id text,
    promotion_decision_id text,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    transition_seq integer NOT NULL CHECK (transition_seq >= 0),
    sandbox_group_id text,
    sandbox_session_id text,
    share_mode text NOT NULL CHECK (share_mode IN ('exclusive', 'shared')),
    reuse_reason_code text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT workflow_claim_lease_proposal_runtime_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS workflow_claim_lease_proposal_runtime_claim_id_key
    ON workflow_claim_lease_proposal_runtime (claim_id);

CREATE UNIQUE INDEX IF NOT EXISTS workflow_claim_lease_proposal_runtime_lease_id_key
    ON workflow_claim_lease_proposal_runtime (lease_id)
    WHERE lease_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS workflow_claim_lease_proposal_runtime_proposal_id_key
    ON workflow_claim_lease_proposal_runtime (proposal_id)
    WHERE proposal_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS workflow_claim_lease_proposal_runtime_sandbox_session_idx
    ON workflow_claim_lease_proposal_runtime (sandbox_session_id)
    WHERE sandbox_session_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS sandbox_sessions (
    sandbox_session_id text PRIMARY KEY,
    sandbox_group_id text,
    workspace_ref text NOT NULL,
    runtime_profile_ref text NOT NULL,
    base_ref text NOT NULL,
    base_digest text NOT NULL,
    authority_context_digest text NOT NULL,
    shared_compatibility_key text,
    sandbox_root text NOT NULL,
    share_mode text NOT NULL CHECK (share_mode IN ('exclusive', 'shared')),
    opened_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    closed_at timestamptz,
    closed_reason_code text,
    owner_route_ref text NOT NULL,
    CONSTRAINT sandbox_sessions_owner_route_ref_fkey
        FOREIGN KEY (owner_route_ref)
        REFERENCES workflow_claim_lease_proposal_runtime (run_id)
        ON DELETE RESTRICT
);

ALTER TABLE IF EXISTS sandbox_sessions
    ADD COLUMN IF NOT EXISTS authority_context_digest text;

ALTER TABLE IF EXISTS sandbox_sessions
    ADD COLUMN IF NOT EXISTS shared_compatibility_key text;

UPDATE sandbox_sessions AS session
SET authority_context_digest = owner.authority_context_digest,
    shared_compatibility_key = CASE
        WHEN session.share_mode = 'shared' AND session.sandbox_group_id IS NOT NULL THEN
            concat_ws(
                '|',
                session.sandbox_group_id,
                session.workspace_ref,
                session.runtime_profile_ref,
                owner.authority_context_digest,
                session.base_ref,
                session.base_digest
            )
        ELSE NULL
    END
FROM workflow_claim_lease_proposal_runtime AS owner
WHERE owner.run_id = session.owner_route_ref
  AND (
      session.authority_context_digest IS NULL
      OR (
          session.share_mode = 'shared'
          AND session.shared_compatibility_key IS NULL
          AND session.sandbox_group_id IS NOT NULL
      )
  );

ALTER TABLE IF EXISTS sandbox_sessions
    ALTER COLUMN authority_context_digest SET NOT NULL;

CREATE INDEX IF NOT EXISTS sandbox_sessions_group_open_idx
    ON sandbox_sessions (sandbox_group_id, closed_at);

CREATE INDEX IF NOT EXISTS sandbox_sessions_profile_share_open_idx
    ON sandbox_sessions (workspace_ref, runtime_profile_ref, share_mode, closed_at);

CREATE UNIQUE INDEX IF NOT EXISTS sandbox_sessions_live_shared_compatibility_key_key
    ON sandbox_sessions (shared_compatibility_key)
    WHERE shared_compatibility_key IS NOT NULL
      AND closed_at IS NULL;

CREATE TABLE IF NOT EXISTS sandbox_bindings (
    sandbox_binding_id text PRIMARY KEY,
    sandbox_session_id text NOT NULL,
    workflow_id text NOT NULL,
    run_id text NOT NULL,
    claim_id text NOT NULL,
    lease_id text,
    proposal_id text,
    work_packet_id text,
    binding_role text NOT NULL,
    reuse_reason_code text,
    bound_at timestamptz NOT NULL,
    released_at timestamptz,
    CONSTRAINT sandbox_bindings_sandbox_session_id_fkey
        FOREIGN KEY (sandbox_session_id)
        REFERENCES sandbox_sessions (sandbox_session_id)
        ON DELETE RESTRICT,
    CONSTRAINT sandbox_bindings_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS sandbox_bindings_session_bound_idx
    ON sandbox_bindings (sandbox_session_id, bound_at);

CREATE INDEX IF NOT EXISTS sandbox_bindings_claim_id_idx
    ON sandbox_bindings (claim_id);

CREATE INDEX IF NOT EXISTS sandbox_bindings_proposal_id_idx
    ON sandbox_bindings (proposal_id)
    WHERE proposal_id IS NOT NULL;

ALTER TABLE workflow_claim_lease_proposal_runtime
    ADD CONSTRAINT workflow_claim_lease_proposal_runtime_sandbox_session_id_fkey
    FOREIGN KEY (sandbox_session_id)
    REFERENCES sandbox_sessions (sandbox_session_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;

COMMENT ON TABLE workflow_claim_lease_proposal_runtime IS 'Runtime-owned route lineage, transition cursor, and explicit sandbox support refs for claim/lease/proposal mechanics.';
COMMENT ON COLUMN workflow_claim_lease_proposal_runtime.transition_seq IS 'Monotonic compare-and-swap cursor owned by runtime/.';
COMMENT ON COLUMN workflow_claim_lease_proposal_runtime.sandbox_group_id IS 'Logical shared-work context for explicit sandbox reuse. Support data only.';
COMMENT ON COLUMN workflow_claim_lease_proposal_runtime.sandbox_session_id IS 'Concrete ephemeral sandbox instance used by the route. Support data only.';
COMMENT ON COLUMN workflow_claim_lease_proposal_runtime.share_mode IS 'Explicit sandbox reuse mode. Shared reuse never replaces claim, lease, or proposal truth.';
COMMENT ON COLUMN workflow_claim_lease_proposal_runtime.reuse_reason_code IS 'Machine-readable reason for sandbox reuse. Null means no reuse was claimed.';

COMMENT ON TABLE sandbox_sessions IS 'Runtime-owned explicit sandbox or worktree instances. Support state, never lifecycle truth.';
COMMENT ON COLUMN sandbox_sessions.authority_context_digest IS 'Canonical authority digest that scopes sandbox reuse compatibility.';
COMMENT ON COLUMN sandbox_sessions.shared_compatibility_key IS 'Canonical compatibility fingerprint for one live shared sandbox tuple.';
COMMENT ON COLUMN sandbox_sessions.share_mode IS 'Exclusive sandboxes never reuse. Shared sandboxes require explicit compatibility.';

COMMENT ON TABLE sandbox_bindings IS 'Explicit lineage-to-sandbox bindings that explain who used a sandbox session and why.';
COMMENT ON COLUMN sandbox_bindings.reuse_reason_code IS 'Machine-readable reason explaining why this route reused an existing sandbox session.';
