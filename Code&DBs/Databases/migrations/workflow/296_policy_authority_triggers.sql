-- Migration 296: Policy Authority subsystem — triggers + initial policies (P4.2.b).
--
-- Anchor decision:
--   architecture-policy::policy-authority::data-layer-teeth
--
-- This migration is the second of two. Migration 295 ships the schema
-- (policy_definitions + authority_compliance_receipts). This one ships:
--
--   1. The initial operator_decisions rows that justify the triggers
--      (so the FK from policy_definitions to operator_decisions is
--      satisfied, and the rationale is queryable from /orient).
--   2. policy_definitions rows that bind those decisions to concrete
--      enforcement shapes on concrete tables.
--   3. The actual BEFORE-DELETE / BEFORE-TRUNCATE triggers that read
--      policy_definitions and reject violating mutations.
--
-- Compliance-receipt-on-reject is intentionally deferred. PostgreSQL has
-- no native autonomous transactions; recording a rejection row in the
-- same transaction that rolls back is wasted ink. The dblink-to-self
-- pattern that gives true audit-on-reject can ship in a follow-up
-- migration once the rest of the subsystem is exercised. For now, the
-- RAISE EXCEPTION message carries the decision_key and rationale so the
-- agent sees exactly which standing order they hit.
--
-- Idempotency
--   Triggers are CREATE OR REPLACE FUNCTION + DROP TRIGGER IF EXISTS
--   guards. operator_decisions rows are ON CONFLICT DO NOTHING.
--   policy_definitions rows are ON CONFLICT (decision_key, target_table,
--   enforcement_kind) WHERE effective_to IS NULL DO NOTHING — re-applying
--   the migration is a no-op.

BEGIN;

-- ============================================================
-- 1. Operator decisions justifying the triggers
-- ============================================================
INSERT INTO operator_decisions (
    operator_decision_id,
    decision_key,
    decision_kind,
    decision_status,
    title,
    rationale,
    decided_by,
    decision_source,
    effective_from,
    effective_to,
    decided_at,
    created_at,
    updated_at,
    decision_scope_kind,
    decision_scope_ref,
    scope_clamp
) VALUES
    (
        'operator_decision.architecture_policy.policy_authority.operator_decisions_not_deletable',
        'architecture-policy::policy-authority::operator-decisions-not-deletable',
        'architecture_policy',
        'decided',
        'operator_decisions rows are not deletable',
        'Standing orders are the durable history of operator authority. To retire a decision, set effective_to (supersession). Hard delete erases the audit trail and lets a confused agent rewrite history. Enforced at the data layer via a BEFORE DELETE trigger.',
        'praxis',
        'migration_296_policy_authority',
        '2026-04-27T00:00:00Z',
        NULL,
        '2026-04-27T00:00:00Z',
        now(),
        now(),
        'authority_domain',
        'policy_authority',
        '{"applies_to":["operator_decisions"],"does_not_apply_to":[]}'::jsonb
    ),
    (
        'operator_decision.architecture_policy.policy_authority.receipts_immutable',
        'architecture-policy::policy-authority::receipts-immutable',
        'architecture_policy',
        'decided',
        'authority_operation_receipts is immutable',
        'Receipts are the canonical audit trail for every gateway dispatch. Deleting them breaks replay, cause-walking, and idempotency caches. Enforced via BEFORE DELETE + BEFORE TRUNCATE triggers.',
        'praxis',
        'migration_296_policy_authority',
        '2026-04-27T00:00:00Z',
        NULL,
        '2026-04-27T00:00:00Z',
        now(),
        now(),
        'authority_domain',
        'policy_authority',
        '{"applies_to":["authority_operation_receipts"],"does_not_apply_to":[]}'::jsonb
    ),
    (
        'operator_decision.architecture_policy.policy_authority.events_immutable',
        'architecture-policy::policy-authority::events-immutable',
        'architecture_policy',
        'decided',
        'authority_events is immutable',
        'Events are the command-event ledger emitted by every gateway command with event_required=TRUE. Deleting them breaks downstream projections and event-sourced consumers. Enforced via BEFORE DELETE + BEFORE TRUNCATE triggers.',
        'praxis',
        'migration_296_policy_authority',
        '2026-04-27T00:00:00Z',
        NULL,
        '2026-04-27T00:00:00Z',
        now(),
        now(),
        'authority_domain',
        'policy_authority',
        '{"applies_to":["authority_events"],"does_not_apply_to":[]}'::jsonb
    )
ON CONFLICT (decision_key) DO NOTHING;

-- ============================================================
-- 2. policy_definitions rows binding decisions → triggers
-- ============================================================
INSERT INTO policy_definitions (
    policy_id,
    decision_key,
    enforcement_kind,
    target_table,
    rationale,
    effective_from
) VALUES
    (
        'policy.operator_decisions.delete_reject',
        'architecture-policy::policy-authority::operator-decisions-not-deletable',
        'delete_reject',
        'operator_decisions',
        'Standing orders are durable; supersede via effective_to instead of DELETE.',
        '2026-04-27T00:00:00Z'
    ),
    (
        'policy.operator_decisions.truncate_reject',
        'architecture-policy::policy-authority::operator-decisions-not-deletable',
        'truncate_reject',
        'operator_decisions',
        'TRUNCATE wipes the standing-order ledger. Use supersession instead.',
        '2026-04-27T00:00:00Z'
    ),
    (
        'policy.authority_operation_receipts.delete_reject',
        'architecture-policy::policy-authority::receipts-immutable',
        'delete_reject',
        'authority_operation_receipts',
        'Receipts are the gateway audit trail; never deletable.',
        '2026-04-27T00:00:00Z'
    ),
    (
        'policy.authority_operation_receipts.truncate_reject',
        'architecture-policy::policy-authority::receipts-immutable',
        'truncate_reject',
        'authority_operation_receipts',
        'Receipts are the gateway audit trail; never truncatable.',
        '2026-04-27T00:00:00Z'
    ),
    (
        'policy.authority_events.delete_reject',
        'architecture-policy::policy-authority::events-immutable',
        'delete_reject',
        'authority_events',
        'Events are the command-event ledger; never deletable.',
        '2026-04-27T00:00:00Z'
    ),
    (
        'policy.authority_events.truncate_reject',
        'architecture-policy::policy-authority::events-immutable',
        'truncate_reject',
        'authority_events',
        'Events are the command-event ledger; never truncatable.',
        '2026-04-27T00:00:00Z'
    )
ON CONFLICT (policy_id) DO NOTHING;

-- ============================================================
-- 3. Trigger function — generic delete reject
-- ============================================================
-- A single generic trigger function reads policy_definitions for the
-- target table + 'delete_reject' and raises with the decision_key +
-- rationale. Each protected table gets a thin trigger that calls this
-- function. Generic > one-function-per-table because it keeps the
-- enforcement logic in one place; the catalog of *what* is enforced
-- lives in policy_definitions, not in trigger SQL bodies.
CREATE OR REPLACE FUNCTION policy_authority_reject_delete()
RETURNS TRIGGER AS $$
DECLARE
    v_policy_id text;
    v_decision_key text;
    v_rationale text;
    v_target_table text := TG_TABLE_NAME;
BEGIN
    -- Operators sometimes need to bypass for emergency surgery (e.g.
    -- offline forensic cleanup). The bypass is a session GUC that an
    -- operator must set explicitly:
    --   SET LOCAL praxis.policy_bypass = 'on';
    -- Setting it requires DB-level privileges; agents don't have it.
    -- Set LOCAL means the bypass dies with the transaction.
    IF current_setting('praxis.policy_bypass', true) = 'on' THEN
        RETURN OLD;
    END IF;

    SELECT policy_id, decision_key, rationale
      INTO v_policy_id, v_decision_key, v_rationale
      FROM policy_definitions
     WHERE target_table = v_target_table
       AND enforcement_kind = 'delete_reject'
       AND effective_to IS NULL
     LIMIT 1;

    IF v_policy_id IS NULL THEN
        -- No active policy bound — admit. This shouldn't happen for
        -- tables that have a trigger attached, but degrade gracefully
        -- (better to allow than to raise without a reason).
        RETURN OLD;
    END IF;

    RAISE EXCEPTION
        'policy_authority: DELETE on % rejected by policy % (decision_key: %). Reason: %',
        v_target_table, v_policy_id, v_decision_key, v_rationale
        USING ERRCODE = 'check_violation',
              HINT = 'Supersede the row via effective_to instead, or set LOCAL praxis.policy_bypass = ''on'' for emergency surgery (operator privilege required).';
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 4. Trigger function — generic truncate reject (statement-level)
-- ============================================================
CREATE OR REPLACE FUNCTION policy_authority_reject_truncate()
RETURNS TRIGGER AS $$
DECLARE
    v_policy_id text;
    v_decision_key text;
    v_rationale text;
    v_target_table text := TG_TABLE_NAME;
BEGIN
    IF current_setting('praxis.policy_bypass', true) = 'on' THEN
        RETURN NULL;
    END IF;

    SELECT policy_id, decision_key, rationale
      INTO v_policy_id, v_decision_key, v_rationale
      FROM policy_definitions
     WHERE target_table = v_target_table
       AND enforcement_kind = 'truncate_reject'
       AND effective_to IS NULL
     LIMIT 1;

    IF v_policy_id IS NULL THEN
        RETURN NULL;
    END IF;

    RAISE EXCEPTION
        'policy_authority: TRUNCATE on % rejected by policy % (decision_key: %). Reason: %',
        v_target_table, v_policy_id, v_decision_key, v_rationale
        USING ERRCODE = 'check_violation',
              HINT = 'TRUNCATE wipes the audit trail. Set LOCAL praxis.policy_bypass = ''on'' for emergency surgery (operator privilege required).';
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 5. Attach triggers to each protected table
-- ============================================================
CREATE OR REPLACE TRIGGER policy_operator_decisions_no_delete
    BEFORE DELETE ON operator_decisions
    FOR EACH ROW
    EXECUTE FUNCTION policy_authority_reject_delete();

CREATE OR REPLACE TRIGGER policy_operator_decisions_no_truncate
    BEFORE TRUNCATE ON operator_decisions
    FOR EACH STATEMENT
    EXECUTE FUNCTION policy_authority_reject_truncate();

CREATE OR REPLACE TRIGGER policy_authority_operation_receipts_no_delete
    BEFORE DELETE ON authority_operation_receipts
    FOR EACH ROW
    EXECUTE FUNCTION policy_authority_reject_delete();

CREATE OR REPLACE TRIGGER policy_authority_operation_receipts_no_truncate
    BEFORE TRUNCATE ON authority_operation_receipts
    FOR EACH STATEMENT
    EXECUTE FUNCTION policy_authority_reject_truncate();

CREATE OR REPLACE TRIGGER policy_authority_events_no_delete
    BEFORE DELETE ON authority_events
    FOR EACH ROW
    EXECUTE FUNCTION policy_authority_reject_delete();

CREATE OR REPLACE TRIGGER policy_authority_events_no_truncate
    BEFORE TRUNCATE ON authority_events
    FOR EACH STATEMENT
    EXECUTE FUNCTION policy_authority_reject_truncate();

COMMIT;

-- Verification (run manually after apply):
--   -- Should fail with ERRCODE 23514 (check_violation):
--   BEGIN; DELETE FROM operator_decisions WHERE decision_key = 'nonexistent'; ROLLBACK;
--
--   -- Bypass works only with the GUC:
--   BEGIN;
--     SET LOCAL praxis.policy_bypass = 'on';
--     DELETE FROM operator_decisions WHERE decision_key = 'definitely_nonexistent';
--   ROLLBACK;
--
--   -- Active policies catalog:
--   SELECT target_table, enforcement_kind, decision_key
--     FROM policy_definitions
--    WHERE effective_to IS NULL
--    ORDER BY target_table, enforcement_kind;
