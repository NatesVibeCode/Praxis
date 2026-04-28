-- Migration 298: Compliance receipts on the reject path (P4.2.e).
--
-- Anchor decision:
--   architecture-policy::policy-authority::data-layer-teeth
--
-- The gap closed by this migration
--   Migration 296 attached enforcement triggers that block bad mutations
--   with `RAISE EXCEPTION`. The exception ROLLS BACK the transaction —
--   so any INSERT we did into authority_compliance_receipts inside the
--   same transaction would also roll back. Result: rejections were
--   visible in application logs (and gateway failure receipts) but not
--   in the dedicated compliance ledger.
--
--   PostgreSQL has no native autonomous transactions. The portable
--   workaround is dblink-to-self: the trigger opens a separate
--   connection to the same database, writes the receipt in *that*
--   connection's transaction, closes, then raises. The receipt
--   commits independently of the parent's rollback.
--
-- What ships here
--   1. dblink extension (no-op if already installed by superuser).
--   2. policy_authority_record_compliance_receipt(...) — SECURITY
--      DEFINER helper that does the autonomous write. Caller passes
--      everything; the function never inspects parent transaction
--      state.
--   3. Updated reject triggers (delete + truncate) that call the helper
--      before raising. Receipt outcome='reject' with the decision_key
--      and rationale.
--   4. Graceful degradation: if dblink_connect_u fails (e.g. no socket
--      auth, no password available), the helper logs a NOTICE and
--      returns without writing. The trigger still raises — enforcement
--      is preserved; only the audit row is lost. Better to lose the
--      audit row than to block the action that caused it.
--
-- What does NOT ship here
--   - Admit-path compliance receipts — no admit rules exist yet.
--     Migration 299 ships the generic insert/update trigger machinery;
--     admit-path receipts ship as part of those triggers' built-in
--     wiring.
--   - Cross-db dblink. The helper assumes same-database self-link.
--
-- One-time superuser provisioning required
--   The dblink extension and the EXECUTE grant on dblink_connect_u
--   require superuser. This migration is safe to re-run as praxis but
--   the helper's autonomous-write path silently degrades to a NOTICE
--   when dblink_connect_u isn't grantable. Run once per database:
--       scripts/setup-dblink-for-policy-authority.sh
--   The script is idempotent. Without it, enforcement still works
--   (triggers still raise) — only the compliance receipt is lost.

BEGIN;

-- ============================================================
-- 1. Compliance-receipt helper (autonomous write)
-- ============================================================
-- SECURITY DEFINER so callers don't need dblink_connect_u privilege.
-- The function creator (whoever runs the migration — praxis or super)
-- needs CONNECT on the database. In Praxis dev/prod that's the
-- migration-running role; if extending to a more locked-down role
-- model later, file a follow-on policy.
CREATE OR REPLACE FUNCTION policy_authority_record_compliance_receipt(
    p_policy_id text,
    p_decision_key text,
    p_target_table text,
    p_operation text,
    p_outcome text,
    p_rejected_reason text,
    p_subject_pk jsonb DEFAULT NULL,
    p_correlation_id uuid DEFAULT NULL
) RETURNS void AS $body$
DECLARE
    v_conn_name text;
    v_dsn text;
    v_payload_pk text;
    v_payload_correlation text;
    v_payload_reason text;
BEGIN
    -- Each invocation gets a unique connection name so concurrent
    -- triggers in different sessions don't clash on the dblink slot.
    v_conn_name := 'policy_compliance_writer_' || replace(gen_random_uuid()::text, '-', '');

    -- Same-database self-link. Empty connstring uses libpq's defaults
    -- — same host/db/user as the current session, with peer or password
    -- auth depending on pg_hba.conf. dblink_connect_u (the unsafe form)
    -- skips the trust check; SECURITY DEFINER scopes that risk to this
    -- function's logic only.
    BEGIN
        PERFORM dblink_connect_u(v_conn_name, format('dbname=%s', current_database()));
    EXCEPTION
        WHEN OTHERS THEN
            -- dblink unavailable / auth failed. Log + return so the
            -- parent action's audit trail is at least visible to ops
            -- via the NOTICE log; the gateway's failure receipt
            -- remains the primary audit surface.
            RAISE NOTICE 'policy_authority_record_compliance_receipt: dblink connect failed (%); compliance row not written for policy %, target %, outcome %',
                SQLERRM, p_policy_id, p_target_table, p_outcome;
            RETURN;
    END;

    -- Build escaped payload pieces. dblink_exec parses the SQL on the
    -- remote side; literal-quote everything client-side to avoid SQL
    -- injection on values like rejected_reason that come from policy
    -- rationales (operator-authored text).
    v_payload_pk := COALESCE(quote_literal(p_subject_pk::text) || '::jsonb', 'NULL');
    v_payload_correlation := COALESCE(quote_literal(p_correlation_id::text) || '::uuid', 'NULL');
    v_payload_reason := COALESCE(quote_literal(p_rejected_reason), 'NULL');

    BEGIN
        PERFORM dblink_exec(
            v_conn_name,
            format(
                'INSERT INTO authority_compliance_receipts ('
                    'policy_id, decision_key, target_table, operation, '
                    'outcome, rejected_reason, subject_pk, correlation_id'
                ') VALUES ('
                    '%L, %L, %L, %L, '
                    '%L, %s, %s, %s'
                ')',
                p_policy_id, p_decision_key, p_target_table, p_operation,
                p_outcome, v_payload_reason, v_payload_pk, v_payload_correlation
            )
        );
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'policy_authority_record_compliance_receipt: dblink_exec failed (%); compliance row not written for policy %, target %, outcome %',
                SQLERRM, p_policy_id, p_target_table, p_outcome;
    END;

    BEGIN
        PERFORM dblink_disconnect(v_conn_name);
    EXCEPTION
        WHEN OTHERS THEN
            -- Best effort; the connection auto-closes when the parent
            -- session ends.
            NULL;
    END;
END;
$body$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION policy_authority_record_compliance_receipt IS
    'Autonomous-transaction helper: writes one authority_compliance_receipts row '
    'via dblink-to-self so the row survives parent transaction rollback. '
    'Used by reject-path triggers to record blocked mutations.';

-- ============================================================
-- 2. Replace reject-path trigger functions with receipt-aware versions
-- ============================================================
CREATE OR REPLACE FUNCTION policy_authority_reject_delete()
RETURNS TRIGGER AS $$
DECLARE
    v_policy_id text;
    v_decision_key text;
    v_rationale text;
    v_target_table text := TG_TABLE_NAME;
    v_subject_pk jsonb;
BEGIN
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
        RETURN OLD;
    END IF;

    -- Capture a best-effort subject_pk from OLD as JSONB. row_to_json
    -- handles arbitrary tables; the receipt's subject_pk lets auditors
    -- reconstruct what was about to be deleted.
    BEGIN
        v_subject_pk := to_jsonb(OLD);
    EXCEPTION WHEN OTHERS THEN
        v_subject_pk := NULL;
    END;

    -- Autonomous compliance-receipt write BEFORE the raise.
    PERFORM policy_authority_record_compliance_receipt(
        p_policy_id        := v_policy_id,
        p_decision_key     := v_decision_key,
        p_target_table     := v_target_table,
        p_operation        := 'DELETE',
        p_outcome          := 'reject',
        p_rejected_reason  := v_rationale,
        p_subject_pk       := v_subject_pk,
        p_correlation_id   := NULL
    );

    RAISE EXCEPTION
        'policy_authority: DELETE on % rejected by policy % (decision_key: %). Reason: %',
        v_target_table, v_policy_id, v_decision_key, v_rationale
        USING ERRCODE = 'check_violation',
              HINT = 'Supersede the row via effective_to instead, or set LOCAL praxis.policy_bypass = ''on'' for emergency surgery (operator privilege required).';
END;
$$ LANGUAGE plpgsql;

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

    -- TRUNCATE has no row context; subject_pk is NULL.
    PERFORM policy_authority_record_compliance_receipt(
        p_policy_id        := v_policy_id,
        p_decision_key     := v_decision_key,
        p_target_table     := v_target_table,
        p_operation        := 'TRUNCATE',
        p_outcome          := 'reject',
        p_rejected_reason  := v_rationale,
        p_subject_pk       := NULL,
        p_correlation_id   := NULL
    );

    RAISE EXCEPTION
        'policy_authority: TRUNCATE on % rejected by policy % (decision_key: %). Reason: %',
        v_target_table, v_policy_id, v_decision_key, v_rationale
        USING ERRCODE = 'check_violation',
              HINT = 'TRUNCATE wipes the audit trail. Set LOCAL praxis.policy_bypass = ''on'' for emergency surgery (operator privilege required).';
END;
$$ LANGUAGE plpgsql;

COMMIT;

-- Verification (run manually after apply):
--   -- 1. Trigger raises and writes a receipt:
--   BEGIN; INSERT INTO operator_decisions (...) VALUES (...); COMMIT;
--   BEGIN; DELETE FROM operator_decisions WHERE decision_key = '...'; ROLLBACK;
--   SELECT outcome, rejected_reason FROM authority_compliance_receipts
--     WHERE target_table = 'operator_decisions' ORDER BY created_at DESC LIMIT 1;
