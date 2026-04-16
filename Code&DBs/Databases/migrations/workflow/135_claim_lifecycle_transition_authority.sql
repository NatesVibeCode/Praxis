-- DB-backed claim lifecycle transition authority.
-- This table stores the allowed transitions for the claim/lease/proposal slice.
-- Runtime code may project these rows into process-local maps, but the durable
-- authority lives here.

CREATE TABLE IF NOT EXISTS workflow_claim_lifecycle_transition_authority (
    workflow_claim_lifecycle_transition_id text PRIMARY KEY,
    from_state text NOT NULL,
    to_state text NOT NULL,
    rationale text NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT workflow_claim_lifecycle_transition_authority_state_check
        CHECK (
            from_state IN (
                'claim_received',
                'claim_validating',
                'claim_blocked',
                'claim_rejected',
                'claim_accepted',
                'lease_requested',
                'lease_blocked',
                'lease_active',
                'lease_expired',
                'proposal_submitted',
                'proposal_invalid'
            )
            AND to_state IN (
                'claim_received',
                'claim_validating',
                'claim_blocked',
                'claim_rejected',
                'claim_accepted',
                'lease_requested',
                'lease_blocked',
                'lease_active',
                'lease_expired',
                'proposal_submitted',
                'proposal_invalid'
            )
            AND from_state <> to_state
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS claim_lifecycle_transition_state_window_idx
    ON workflow_claim_lifecycle_transition_authority (from_state, to_state, effective_from);

CREATE INDEX IF NOT EXISTS claim_lifecycle_transition_from_state_active_idx
    ON workflow_claim_lifecycle_transition_authority (from_state, effective_from DESC, created_at DESC);

INSERT INTO workflow_claim_lifecycle_transition_authority (
    workflow_claim_lifecycle_transition_id,
    from_state,
    to_state,
    rationale,
    effective_from,
    effective_to,
    decision_ref,
    created_at
) VALUES
    (
        'claim_lifecycle_transition:claim_received:claim_validating:v1',
        'claim_received',
        'claim_validating',
        'Claims must validate before they can be accepted, blocked, or rejected.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:claim_validating:claim_accepted:v1',
        'claim_validating',
        'claim_accepted',
        'Validated claims may advance to accepted.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:claim_validating:claim_blocked:v1',
        'claim_validating',
        'claim_blocked',
        'Validated claims may pause in a blocked state when prerequisites are unmet.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:claim_validating:claim_rejected:v1',
        'claim_validating',
        'claim_rejected',
        'Validated claims may terminate as rejected.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:claim_blocked:claim_validating:v1',
        'claim_blocked',
        'claim_validating',
        'Blocked claims may re-enter validation when conditions change.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:claim_blocked:claim_rejected:v1',
        'claim_blocked',
        'claim_rejected',
        'Blocked claims may terminate as rejected.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:claim_accepted:lease_requested:v1',
        'claim_accepted',
        'lease_requested',
        'Accepted claims must request a lease before execution can continue.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:lease_requested:lease_active:v1',
        'lease_requested',
        'lease_active',
        'Lease requests may activate when capacity is granted.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:lease_requested:lease_blocked:v1',
        'lease_requested',
        'lease_blocked',
        'Lease requests may block when capacity is unavailable.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:lease_blocked:lease_requested:v1',
        'lease_blocked',
        'lease_requested',
        'Blocked lease requests may retry when capacity is reconsidered.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:lease_active:lease_expired:v1',
        'lease_active',
        'lease_expired',
        'Active leases may expire when their validity window closes.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:lease_active:proposal_submitted:v1',
        'lease_active',
        'proposal_submitted',
        'Active leases may advance to proposal submission.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    ),
    (
        'claim_lifecycle_transition:lease_active:proposal_invalid:v1',
        'lease_active',
        'proposal_invalid',
        'Active leases may fail closed with an invalid proposal result.',
        '2026-01-01T00:00:00+00:00',
        NULL,
        'migration.135_claim_lifecycle_transition_authority',
        '2026-01-01T00:00:00+00:00'
    )
ON CONFLICT (workflow_claim_lifecycle_transition_id) DO UPDATE
SET from_state = EXCLUDED.from_state,
    to_state = EXCLUDED.to_state,
    rationale = EXCLUDED.rationale,
    effective_from = EXCLUDED.effective_from,
    effective_to = EXCLUDED.effective_to,
    decision_ref = EXCLUDED.decision_ref,
    created_at = EXCLUDED.created_at;

COMMENT ON TABLE workflow_claim_lifecycle_transition_authority IS 'DB-backed authority for allowed claim/lease/proposal lifecycle transitions.';
COMMENT ON COLUMN workflow_claim_lifecycle_transition_authority.decision_ref IS 'Durable provenance reference for the transition authority row.';
