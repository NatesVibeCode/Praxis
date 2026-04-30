-- Migration 352: receipts UPDATE-immutable + transport_kind column.
--
-- Anchor decision:
--   architecture-policy::policy-authority::receipts-immutable
--   (operator_decisions row, registered in migration 296)
--
-- Why this exists
--   Migration 296 attached BEFORE-DELETE + BEFORE-TRUNCATE reject
--   triggers to authority_operation_receipts and authority_events.
--   It did NOT block UPDATE — at the time the gateway still relied on
--   a receipt_id stitching path on authority_events, and a symmetric
--   policy_authority_reject_update() function was deferred.
--
--   This migration:
--     1. Adds policy_authority_reject_update() (symmetric to
--        policy_authority_reject_delete from 296).
--     2. Registers a policy_definitions row binding update_reject to
--        authority_operation_receipts. Receipts are insert-only by
--        production-code convention (only the gateway writes them, and
--        only via INSERT) — this turns convention into data-layer teeth.
--        authority_events UPDATE remains permitted because the gateway's
--        post-insert receipt_id stitching path is still in use; that
--        relaxation will retire when the typed_gap and feedback writers
--        migrate to gateway dispatch.
--     3. Adds a transport_kind column on authority_operation_receipts so
--        every receipt records which surface (cli/mcp/http/workflow/...)
--        produced the call. The gateway today hardcodes caller_ref to
--        'authority_gateway' at the insert site even though entry-point
--        surfaces have the information. Filling this column unblocks
--        per-surface audit queries ("show me everything CLI did today").
--
-- Idempotency
--   ALTER TABLE ... IF NOT EXISTS, CREATE OR REPLACE FUNCTION,
--   ON CONFLICT DO NOTHING on policy_definitions, and
--   CREATE OR REPLACE TRIGGER on the trigger attachment.

BEGIN;

-- ============================================================
-- 1. transport_kind column on authority_operation_receipts
-- ============================================================
ALTER TABLE authority_operation_receipts
    ADD COLUMN IF NOT EXISTS transport_kind TEXT;

ALTER TABLE authority_operation_receipts
    DROP CONSTRAINT IF EXISTS authority_operation_receipts_transport_kind_check;

ALTER TABLE authority_operation_receipts
    ADD CONSTRAINT authority_operation_receipts_transport_kind_check
        CHECK (
            transport_kind IS NULL
            OR transport_kind IN (
                'cli',
                'mcp',
                'http',
                'workflow',
                'heartbeat',
                'internal',
                'sandbox',
                'test',
                'unknown'
            )
        ) NOT VALID;

-- VALIDATE separately so the table-level scan is a fast-path no-op when
-- there are no rows (e.g. fresh-clone bootstrap), and a forward-only
-- check on existing deployments. Existing rows have transport_kind NULL
-- (the column was just added), which the constraint admits.
ALTER TABLE authority_operation_receipts
    VALIDATE CONSTRAINT authority_operation_receipts_transport_kind_check;

CREATE INDEX IF NOT EXISTS authority_operation_receipts_transport_kind_idx
    ON authority_operation_receipts (transport_kind, created_at DESC)
    WHERE transport_kind IS NOT NULL;

COMMENT ON COLUMN authority_operation_receipts.transport_kind IS
    'Which surface dispatched this gateway call. Filled by entry-point '
    'surfaces (cli, mcp, http, workflow, heartbeat) via CallerContext. '
    'NULL means the column was unfilled (legacy rows, internal callers '
    'that did not propagate a context, or pre-migration receipts).';

-- ============================================================
-- 2. policy_authority_reject_update() — symmetric to delete/truncate
-- ============================================================
-- Generic trigger function: reads policy_definitions for the target
-- table + 'update_reject' enforcement and raises if a matching active
-- policy exists. Identical shape to policy_authority_reject_delete()
-- in migration 296; the catalog of *what* is enforced lives in
-- policy_definitions, not in this function body.
CREATE OR REPLACE FUNCTION policy_authority_reject_update()
RETURNS TRIGGER AS $$
DECLARE
    v_policy_id text;
    v_decision_key text;
    v_rationale text;
    v_target_table text := TG_TABLE_NAME;
BEGIN
    -- Operators can bypass for emergency surgery. Same GUC as the
    -- sibling reject functions.
    IF current_setting('praxis.policy_bypass', true) = 'on' THEN
        RETURN NEW;
    END IF;

    SELECT policy_id, decision_key, rationale
      INTO v_policy_id, v_decision_key, v_rationale
      FROM policy_definitions
     WHERE target_table = v_target_table
       AND enforcement_kind = 'update_reject'
       AND effective_to IS NULL
     LIMIT 1;

    IF v_policy_id IS NULL THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION
        'policy_authority: UPDATE on % rejected by policy % (decision_key: %). Reason: %',
        v_target_table, v_policy_id, v_decision_key, v_rationale
        USING ERRCODE = 'check_violation',
              HINT = 'Receipts are append-only; record a new receipt instead, or set LOCAL praxis.policy_bypass = ''on'' for emergency surgery (operator privilege required).';
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 3. policy_definitions row binding update_reject to receipts
-- ============================================================
INSERT INTO policy_definitions (
    policy_id,
    decision_key,
    enforcement_kind,
    target_table,
    rationale,
    effective_from
) VALUES (
    'policy.authority_operation_receipts.update_reject',
    'architecture-policy::policy-authority::receipts-immutable',
    'update_reject',
    'authority_operation_receipts',
    'Receipts are the gateway audit trail; record a new receipt rather than mutating an existing one.',
    '2026-04-29T00:00:00Z'
)
ON CONFLICT (policy_id) DO NOTHING;

-- ============================================================
-- 4. Attach the trigger to authority_operation_receipts
-- ============================================================
CREATE OR REPLACE TRIGGER policy_authority_operation_receipts_no_update
    BEFORE UPDATE ON authority_operation_receipts
    FOR EACH ROW
    EXECUTE FUNCTION policy_authority_reject_update();

COMMIT;

-- Verification (run manually after apply):
--   -- Should fail with ERRCODE 23514 (check_violation):
--   BEGIN;
--     UPDATE authority_operation_receipts
--        SET error_detail = 'tampered'
--      WHERE receipt_id = (SELECT receipt_id FROM authority_operation_receipts LIMIT 1);
--   ROLLBACK;
--
--   -- Bypass works only with the GUC:
--   BEGIN;
--     SET LOCAL praxis.policy_bypass = 'on';
--     UPDATE authority_operation_receipts
--        SET error_detail = error_detail
--      WHERE receipt_id = '00000000-0000-0000-0000-000000000000';
--   ROLLBACK;
--
--   -- transport_kind admits the documented values + NULL:
--   SELECT
--     transport_kind,
--     count(*) AS receipts
--   FROM authority_operation_receipts
--   GROUP BY transport_kind
--   ORDER BY transport_kind NULLS FIRST;
