-- Migration 295: Policy Authority subsystem — schema half (P4.2.a).
--
-- Anchor decision:
--   architecture-policy::policy-authority::data-layer-teeth
--   (operator_decisions row recorded 2026-04-27)
--
-- Why this exists
--   The JIT surfacing layer (cursor rules, PreToolUse hooks, gateway-side
--   _standing_orders_surfaced) is advisory. Migration 295+296 close the
--   loop with data-layer enforcement so a confused agent or a future
--   harness we haven't integrated cannot silently disable enforcement
--   by accident or design.
--
--   This migration ships the schema only:
--     - policy_definitions  — projection from operator_decisions, FK-bound
--     - authority_compliance_receipts — companion to authority_operation_receipts
--
--   Migration 296 ships the actual BEFORE-DELETE triggers that consume
--   policy_definitions rows. Migration 297 registers the CQRS operations.
--
-- Idempotency
--   IF NOT EXISTS on every CREATE so re-running the migration is safe
--   (matches the rest of the workflow migration set's pattern).

BEGIN;

CREATE TABLE IF NOT EXISTS policy_definitions (
    policy_id text PRIMARY KEY CHECK (btrim(policy_id) <> ''),
    decision_key text NOT NULL,
    enforcement_kind text NOT NULL CHECK (enforcement_kind IN (
        'insert_reject',
        'update_reject',
        'delete_reject',
        'truncate_reject'
    )),
    target_table text NOT NULL CHECK (btrim(target_table) <> ''),
    target_column text,
    -- predicate_sql is intentionally optional. For naked rejects (e.g.
    -- BEFORE DELETE with no extra condition) the trigger logic doesn't
    -- need a predicate; the enforcement_kind is the whole rule. When
    -- present, the trigger appends `AND <predicate_sql>` to its match
    -- condition.
    predicate_sql text,
    rationale text NOT NULL CHECK (btrim(rationale) <> ''),
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT policy_definitions_decision_key_fk
        FOREIGN KEY (decision_key)
        REFERENCES operator_decisions (decision_key)
        ON DELETE RESTRICT,
    CONSTRAINT policy_definitions_window_valid
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

-- Single active row per (decision_key, target_table, enforcement_kind).
-- Supersession is set effective_to non-null on the old row, then INSERT
-- a new active row — same pattern as operator_decisions.
CREATE UNIQUE INDEX IF NOT EXISTS policy_definitions_active_unique_idx
    ON policy_definitions (decision_key, target_table, enforcement_kind)
    WHERE effective_to IS NULL;

CREATE INDEX IF NOT EXISTS policy_definitions_target_active_idx
    ON policy_definitions (target_table, enforcement_kind)
    WHERE effective_to IS NULL;

COMMENT ON TABLE policy_definitions IS
    'Projection from operator_decisions for data-layer policy enforcement. '
    'Each row binds a standing-order decision to one concrete trigger shape '
    'on one target table. The triggers in migration 296 read this table.';
COMMENT ON COLUMN policy_definitions.predicate_sql IS
    'Optional extra condition appended to the trigger match. NULL means the '
    'enforcement_kind is the entire rule (e.g. delete_reject blocks every DELETE).';

CREATE TABLE IF NOT EXISTS authority_compliance_receipts (
    compliance_receipt_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id text NOT NULL,
    decision_key text NOT NULL,
    target_table text NOT NULL,
    operation text NOT NULL CHECK (operation IN ('INSERT','UPDATE','DELETE','TRUNCATE')),
    outcome text NOT NULL CHECK (outcome IN ('admit','reject')),
    rejected_reason text,
    subject_pk jsonb,
    -- Best-effort linkage to the gateway operation that triggered the
    -- mutation. Filled when the trigger can read it from a session GUC
    -- (set by the gateway before any DML); NULL for direct DML and
    -- migration-time mutations.
    operation_receipt_id uuid,
    correlation_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT authority_compliance_receipts_policy_fk
        FOREIGN KEY (policy_id)
        REFERENCES policy_definitions (policy_id)
        ON DELETE RESTRICT,
    CONSTRAINT authority_compliance_receipts_reject_has_reason
        CHECK (
            outcome = 'admit' OR (rejected_reason IS NOT NULL AND btrim(rejected_reason) <> '')
        )
);

CREATE INDEX IF NOT EXISTS authority_compliance_receipts_policy_idx
    ON authority_compliance_receipts (policy_id, created_at DESC);

CREATE INDEX IF NOT EXISTS authority_compliance_receipts_target_outcome_idx
    ON authority_compliance_receipts (target_table, outcome, created_at DESC);

CREATE INDEX IF NOT EXISTS authority_compliance_receipts_correlation_idx
    ON authority_compliance_receipts (correlation_id)
    WHERE correlation_id IS NOT NULL;

COMMENT ON TABLE authority_compliance_receipts IS
    'Audit trail for policy_definitions enforcement. Companion to '
    'authority_operation_receipts: receipts answer "did the gateway run?", '
    'compliance answers "did this row hit a policy, and did it admit or reject?". '
    'A reject row records the attempted mutation BEFORE the trigger raises, '
    'using a sibling autonomous-transaction shape (see migration 296).';

COMMIT;

-- Verification (run manually after apply):
--   SELECT count(*) FROM policy_definitions;
--   SELECT count(*) FROM authority_compliance_receipts;
--   \d policy_definitions
--   \d authority_compliance_receipts
