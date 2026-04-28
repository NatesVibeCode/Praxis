-- Migration 300: Receipt writer bypasses its own enforcement (P4.2.f.fix).
--
-- Anchor decision:
--   architecture-policy::policy-authority::data-layer-teeth
--
-- The bug found during P4.2.f end-to-end testing
--   policy_authority_record_compliance_receipt opens an autonomous
--   dblink session and INSERTs into authority_compliance_receipts. If
--   any insert_reject policy is attached to that table — or any policy
--   whose predicate happens to match the receipt's content (e.g. a
--   rationale string mentioning a banned term) — the autonomous write
--   gets blocked by the very enforcement layer it's auditing.
--
--   Concrete trigger from the smoke test: a test policy with
--   `predicate_sql = NEW.rejected_reason LIKE '%BLOCK_ME%'` and a
--   rationale containing 'BLOCK_ME' — the receipt for the rejected
--   parent INSERT itself matched, recursively rejected, no row landed.
--
-- Fix
--   The autonomous receipt session is privileged audit-write context.
--   Before INSERTing, it sets praxis.policy_bypass = 'on' on the
--   dblink session (session-scope, not LOCAL — applies until the
--   dblink session disconnects). The check at the top of every
--   policy-authority trigger admits the audit write.
--
-- Risk model
--   The bypass GUC is set on a dblink session that:
--     - is opened by SECURITY DEFINER code,
--     - executes exactly one INSERT into authority_compliance_receipts,
--     - is disconnected before returning.
--   The bypass scope can't escape this function: the dblink session
--   has no other queries, and a regular client session never inherits
--   it. The widening matches the pattern already documented at the
--   trigger level: policy_bypass exists for explicit operator-tier
--   surgery; the receipt writer has the same authority by construction.

BEGIN;

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
    v_payload_pk text;
    v_payload_correlation text;
    v_payload_reason text;
BEGIN
    v_conn_name := 'policy_compliance_writer_' || replace(gen_random_uuid()::text, '-', '');

    BEGIN
        PERFORM dblink_connect_u(v_conn_name, format('dbname=%s', current_database()));
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'policy_authority_record_compliance_receipt: dblink connect failed (%); compliance row not written for policy %, target %, outcome %',
                SQLERRM, p_policy_id, p_target_table, p_outcome;
            RETURN;
    END;

    -- Bypass policy_authority enforcement for this dblink session.
    -- The session executes exactly one INSERT into
    -- authority_compliance_receipts and disconnects; the bypass cannot
    -- leak. See migration header for risk-model details.
    BEGIN
        PERFORM dblink_exec(v_conn_name, $$SET praxis.policy_bypass = 'on'$$);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'policy_authority_record_compliance_receipt: dblink SET bypass failed (%); aborting receipt write',
                SQLERRM;
            BEGIN PERFORM dblink_disconnect(v_conn_name); EXCEPTION WHEN OTHERS THEN NULL; END;
            RETURN;
    END;

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
            RAISE NOTICE 'policy_authority_record_compliance_receipt: dblink_exec INSERT failed (%); compliance row not written for policy %, target %, outcome %',
                SQLERRM, p_policy_id, p_target_table, p_outcome;
    END;

    BEGIN
        PERFORM dblink_disconnect(v_conn_name);
    EXCEPTION
        WHEN OTHERS THEN
            NULL;
    END;
END;
$body$ LANGUAGE plpgsql SECURITY DEFINER;

COMMIT;
