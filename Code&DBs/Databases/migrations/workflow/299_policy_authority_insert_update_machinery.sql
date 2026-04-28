-- Migration 299: Insert/update enforcement machinery (P4.2.f).
--
-- Anchor decision:
--   architecture-policy::policy-authority::data-layer-teeth
--
-- Migrations 295-298 shipped the schema, delete/truncate enforcement,
-- and reject-path compliance receipts. This migration extends the
-- subsystem to insert_reject and update_reject enforcement_kinds
-- (already declared valid in policy_definitions.enforcement_kind).
--
-- Why generation, not a generic-runtime trigger
--   Reading policy_definitions inside a row-level trigger and then
--   EXECUTE-ing predicate_sql against NEW is awkward in plpgsql:
--   NEW is a record, predicate_sql references NEW.<col> directly, and
--   passing NEW through dynamic SQL means a JSONB round-trip per row.
--   That's hot-path overhead for every INSERT.
--
--   Cleaner: at activation time, generate a trigger function specific
--   to the (table, policy) pair, with predicate_sql baked in. The
--   trigger is one EXECUTE per row (the predicate as a static IF), no
--   policy_definitions lookup, no JSONB conversion. Activation /
--   deactivation is the slow path; trigger fire is the fast path.
--
-- What ships
--   1. policy_authority_record_admit_receipt — companion to the
--      reject-path helper. Same dblink-to-self pattern, outcome='admit'.
--      Optional — admit receipts are nice-to-have, the trigger can opt
--      out by passing a NULL admit_receipt_outcome.
--   2. policy_authority_attach_table_policy(policy_id) — reads the
--      policy_definitions row, generates the appropriate trigger
--      function and CREATE OR REPLACE TRIGGER, attaches.
--   3. policy_authority_detach_table_policy(policy_id) — drops the
--      generated trigger when supersession sets effective_to.
--   4. Documented status of update_clamp — supported in the schema but
--      not yet wired. clamp needs an additional column (clamp_expression
--      text) that this migration intentionally doesn't add. When a
--      concrete clamp policy lands, file a follow-up migration.
--
-- What does NOT ship here
--   - Any actual insert_reject / update_reject policies. The machinery
--     is the deliverable; the policies that use it ship per-need.
--   - update_clamp implementation. See above.

BEGIN;

-- ============================================================
-- 1. Admit-receipt helper (companion to the reject helper)
-- ============================================================
CREATE OR REPLACE FUNCTION policy_authority_record_admit_receipt(
    p_policy_id text,
    p_decision_key text,
    p_target_table text,
    p_operation text,
    p_subject_pk jsonb DEFAULT NULL,
    p_correlation_id uuid DEFAULT NULL
) RETURNS void AS $body$
BEGIN
    -- Admit-path receipts use the same autonomous helper. Why
    -- autonomous on admit too? Consistency: the audit trail's stable
    -- across both outcomes. Cost is one extra dblink round-trip per
    -- mutation that fires a policy. If that becomes hot-path expensive,
    -- a future migration can specialize the admit path to write inside
    -- the parent transaction (which is fine — admit doesn't roll back).
    PERFORM policy_authority_record_compliance_receipt(
        p_policy_id        := p_policy_id,
        p_decision_key     := p_decision_key,
        p_target_table     := p_target_table,
        p_operation        := p_operation,
        p_outcome          := 'admit',
        p_rejected_reason  := NULL,
        p_subject_pk       := p_subject_pk,
        p_correlation_id   := p_correlation_id
    );
END;
$body$ LANGUAGE plpgsql;

COMMENT ON FUNCTION policy_authority_record_admit_receipt IS
    'Records outcome=admit compliance receipt for a successful mutation '
    'that matched a policy. Optional — generated triggers may skip the '
    'admit-path write when audit volume matters.';

-- ============================================================
-- 2. Attach activator
-- ============================================================
-- Generates a trigger function and CREATE OR REPLACE TRIGGER for the
-- given policy_id. Idempotent — re-running on an already-attached
-- policy refreshes the function (in case predicate_sql was edited).
--
-- Naming convention:
--   trigger function:   policy_<policy_slug>_check()
--   trigger:            policy_<policy_slug>_<insert|update>_check
-- where <policy_slug> is the policy_id with dots/colons replaced.
--
-- update_clamp returns an explicit "not yet implemented" so callers
-- see the gap clearly instead of silently doing nothing.
CREATE OR REPLACE FUNCTION policy_authority_attach_table_policy(
    p_policy_id text
) RETURNS text AS $body$
DECLARE
    v_policy RECORD;
    v_slug text;
    v_func_name text;
    v_trigger_name text;
    v_event text;
    v_predicate_clause text;
    v_subject_capture text;
    v_func_body text;
BEGIN
    SELECT *
      INTO v_policy
      FROM policy_definitions
     WHERE policy_id = p_policy_id
       AND effective_to IS NULL;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'policy_authority_attach_table_policy: no active policy with policy_id=%', p_policy_id
            USING ERRCODE = 'no_data_found';
    END IF;

    -- Slug: replace anything that's not [a-zA-Z0-9_] with _
    v_slug := regexp_replace(v_policy.policy_id, '[^a-zA-Z0-9_]+', '_', 'g');
    v_func_name := 'policy_' || v_slug || '_check';
    v_trigger_name := 'policy_' || v_slug;

    -- Validate enforcement_kind. delete_reject / truncate_reject are
    -- handled by the static migration-296 triggers — refuse to
    -- regenerate them here to avoid duplicate-trigger conflicts.
    IF v_policy.enforcement_kind IN ('delete_reject', 'truncate_reject') THEN
        RAISE EXCEPTION
            'policy_authority_attach_table_policy: enforcement_kind=% is owned by static triggers in migration 296; do not re-attach via this activator',
            v_policy.enforcement_kind
            USING ERRCODE = 'feature_not_supported';
    END IF;

    IF v_policy.enforcement_kind = 'update_clamp' THEN
        RAISE EXCEPTION
            'policy_authority_attach_table_policy: enforcement_kind=update_clamp is not yet implemented (needs clamp_expression column on policy_definitions). File a follow-up migration.'
            USING ERRCODE = 'feature_not_supported';
    END IF;

    -- Predicate: NULL means "always fires" (degenerate insert_reject /
    -- update_reject = block every row). Non-NULL is interpolated as a
    -- boolean expression that can reference NEW.<col>.
    IF v_policy.predicate_sql IS NULL THEN
        v_predicate_clause := 'TRUE';
    ELSE
        v_predicate_clause := '(' || v_policy.predicate_sql || ')';
    END IF;

    -- Pick event keyword.
    IF v_policy.enforcement_kind = 'insert_reject' THEN
        v_event := 'INSERT';
    ELSIF v_policy.enforcement_kind = 'update_reject' THEN
        v_event := 'UPDATE';
    ELSE
        RAISE EXCEPTION
            'policy_authority_attach_table_policy: unrecognized enforcement_kind %', v_policy.enforcement_kind;
    END IF;

    -- Function body. Captures NEW as JSONB for the receipt's subject_pk.
    v_subject_capture := 'BEGIN v_subject_pk := to_jsonb(NEW); EXCEPTION WHEN OTHERS THEN v_subject_pk := NULL; END;';

    v_func_body := format($func$
CREATE OR REPLACE FUNCTION %I() RETURNS TRIGGER AS $trigger$
DECLARE
    v_subject_pk jsonb;
BEGIN
    IF current_setting('praxis.policy_bypass', true) = 'on' THEN
        RETURN NEW;
    END IF;
    %s

    IF %s THEN
        PERFORM policy_authority_record_compliance_receipt(
            p_policy_id        := %L,
            p_decision_key     := %L,
            p_target_table     := %L,
            p_operation        := %L,
            p_outcome          := 'reject',
            p_rejected_reason  := %L,
            p_subject_pk       := v_subject_pk,
            p_correlation_id   := NULL
        );
        RAISE EXCEPTION
            'policy_authority: %s on %% rejected by policy %s (decision_key: %s). Reason: %s',
            TG_TABLE_NAME
            USING ERRCODE = 'check_violation',
                  HINT = 'Set LOCAL praxis.policy_bypass = ''on'' for emergency surgery (operator privilege required).';
    END IF;

    -- Admit-path receipt is intentionally OFF by default — high
    -- mutation rates would balloon authority_compliance_receipts. To
    -- enable for one mutation: SET LOCAL praxis.policy_admit_receipts = 'on'.
    IF current_setting('praxis.policy_admit_receipts', true) = 'on' THEN
        PERFORM policy_authority_record_admit_receipt(
            p_policy_id     := %L,
            p_decision_key  := %L,
            p_target_table  := %L,
            p_operation     := %L,
            p_subject_pk    := v_subject_pk
        );
    END IF;

    RETURN NEW;
END;
$trigger$ LANGUAGE plpgsql;
$func$,
        v_func_name,
        v_subject_capture,
        v_predicate_clause,
        v_policy.policy_id,
        v_policy.decision_key,
        v_policy.target_table,
        v_event,
        v_policy.rationale,
        v_event, v_policy.policy_id, v_policy.decision_key, v_policy.rationale,
        v_policy.policy_id,
        v_policy.decision_key,
        v_policy.target_table,
        v_event
    );

    EXECUTE v_func_body;

    -- Attach the trigger. CREATE OR REPLACE TRIGGER (PG14+) is
    -- idempotent for re-attach.
    EXECUTE format(
        'CREATE OR REPLACE TRIGGER %I BEFORE %s ON %I FOR EACH ROW EXECUTE FUNCTION %I()',
        v_trigger_name,
        v_event,
        v_policy.target_table,
        v_func_name
    );

    RETURN format('attached %s on %s as %s', v_event, v_policy.target_table, v_trigger_name);
END;
$body$ LANGUAGE plpgsql;

COMMENT ON FUNCTION policy_authority_attach_table_policy IS
    'Reads policy_definitions[policy_id] (active row only) and generates + '
    'attaches a BEFORE INSERT or BEFORE UPDATE trigger that enforces the '
    'policy. Idempotent. Refuses delete_reject / truncate_reject (handled '
    'by migration 296 static triggers) and update_clamp (not yet wired).';

-- ============================================================
-- 3. Detach
-- ============================================================
-- Drops the trigger + function for a policy. Used when a policy row
-- gets superseded (effective_to set non-null). The drop is wrapped to
-- silently succeed if nothing was attached — re-running detach is safe.
CREATE OR REPLACE FUNCTION policy_authority_detach_table_policy(
    p_policy_id text
) RETURNS text AS $body$
DECLARE
    v_target_table text;
    v_slug text;
    v_func_name text;
    v_trigger_name text;
BEGIN
    SELECT target_table INTO v_target_table
      FROM policy_definitions
     WHERE policy_id = p_policy_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'policy_authority_detach_table_policy: no policy_id=%', p_policy_id
            USING ERRCODE = 'no_data_found';
    END IF;

    v_slug := regexp_replace(p_policy_id, '[^a-zA-Z0-9_]+', '_', 'g');
    v_func_name := 'policy_' || v_slug || '_check';
    v_trigger_name := 'policy_' || v_slug;

    -- Drop the trigger first, then the function. IF EXISTS so detach
    -- of a never-attached policy is a no-op.
    BEGIN
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', v_trigger_name, v_target_table);  -- safety-bypass: detach mirrors the activator's own attach; not removing externally-authored policy enforcement
    EXCEPTION WHEN OTHERS THEN
        -- Table may have been dropped; carry on to function cleanup.
        NULL;
    END;
    EXECUTE format('DROP FUNCTION IF EXISTS %I()', v_func_name);

    RETURN format('detached policy %s from %s', p_policy_id, v_target_table);
END;
$body$ LANGUAGE plpgsql;

COMMENT ON FUNCTION policy_authority_detach_table_policy IS
    'Drops the trigger + function attached by policy_authority_attach_table_policy. '
    'Idempotent. Caller must update policy_definitions.effective_to separately.';

COMMIT;

-- Verification (run manually):
--   -- 1. Define a test policy:
--   INSERT INTO policy_definitions (
--       policy_id, decision_key, enforcement_kind, target_table,
--       predicate_sql, rationale, effective_from)
--   VALUES (
--       'policy.TEST.users_no_admin_email',
--       'architecture-policy::TEST::no-admin-email-creation',
--       'insert_reject', 'users',
--       $$NEW.email LIKE '%@admin.example.com'$$,
--       'No admins via signup',
--       now());
--   -- 2. Attach:
--   SELECT policy_authority_attach_table_policy('policy.TEST.users_no_admin_email');
--   -- 3. Test rejection:
--   INSERT INTO users (email) VALUES ('intruder@admin.example.com'); -- raises
--   -- 4. Detach:
--   SELECT policy_authority_detach_table_policy('policy.TEST.users_no_admin_email');
