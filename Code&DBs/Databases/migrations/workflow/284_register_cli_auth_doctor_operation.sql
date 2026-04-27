-- Migration 284: Register the cli_auth_doctor wizard as a CQRS query operation.
--
-- Two parts:
--
--   1. FIX register_operation_atomic — migrations 239 + 240 hardcoded
--      `query_model` for query-kind ops in authority_object_registry, but
--      the CHECK constraint on `write_model_kind` only accepts
--      command_model | read_model | event_stream | transport | feedback |
--      definition | registry | reference | projection. Result: every
--      register_operation_atomic call for a query op fails with a CHECK
--      violation. Same shape as BUG-2062CB3B that the helper was supposed
--      to PREVENT — the helper itself shipped with the bug. Re-CREATE OR
--      REPLACE the function with the correct mapping (`read_model`).
--
--   2. Register cli_auth_doctor — diagnostic wizard that probes claude /
--      codex / gemini CLI binaries with a trivial prompt, parses output
--      for auth-failure patterns, returns structured per-provider health
--      with concrete host-side remediation commands. Read-tier op,
--      `non_idempotent` so each call probes fresh (auth state is
--      time-windowed; replay would lie — same lesson the gateway
--      read_only-replay fix earlier today enforced).

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- (1) Fix register_operation_atomic: query → 'read_model', not 'query_model'
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION register_operation_atomic(
    p_operation_ref            TEXT,
    p_operation_name           TEXT,
    p_handler_ref              TEXT,
    p_input_model_ref          TEXT,
    p_authority_domain_ref     TEXT,
    p_authority_ref            TEXT DEFAULT NULL,
    p_operation_kind           TEXT DEFAULT 'command',
    p_source_kind              TEXT DEFAULT NULL,
    p_http_method              TEXT DEFAULT 'POST',
    p_http_path                TEXT DEFAULT NULL,
    p_posture                  TEXT DEFAULT 'operate',
    p_idempotency_policy       TEXT DEFAULT 'non_idempotent',
    p_event_type               TEXT DEFAULT NULL,
    p_event_required           BOOLEAN DEFAULT NULL,
    p_receipt_required         BOOLEAN DEFAULT TRUE,
    p_output_schema_ref        TEXT DEFAULT 'operation.output.default',
    p_input_schema_ref         TEXT DEFAULT NULL,
    p_decision_ref             TEXT DEFAULT 'decision.architecture_policy.platform_architecture.conceptual_events_register_through_operation_catalog_registry',
    p_binding_revision         TEXT DEFAULT NULL,
    p_storage_target_ref       TEXT DEFAULT 'praxis.primary_postgres',
    p_label                    TEXT DEFAULT NULL,
    p_summary                  TEXT DEFAULT NULL,
    p_owner_ref                TEXT DEFAULT 'praxis.engine'
)
RETURNS VOID AS $$
DECLARE
    v_object_kind        TEXT := 'operation.' || p_operation_name;
    v_authority_ref      TEXT := COALESCE(p_authority_ref, p_authority_domain_ref);
    v_source_kind        TEXT := COALESCE(
        p_source_kind,
        CASE WHEN p_operation_kind = 'query' THEN 'operation_query' ELSE 'operation_command' END
    );
    v_http_path          TEXT := COALESCE(p_http_path, '/api/' || p_operation_name);
    v_input_schema_ref   TEXT := COALESCE(p_input_schema_ref, p_input_model_ref);
    v_binding_revision   TEXT := COALESCE(
        p_binding_revision,
        'binding.operation_catalog_registry.' || replace(p_operation_name, '.', '_') || '.' || to_char(now(), 'YYYYMMDD')
    );
    v_label              TEXT := COALESCE(p_label, p_operation_name);
    v_summary            TEXT := COALESCE(
        p_summary,
        'Operation catalog entry owned by ' || p_authority_domain_ref
    );
    v_event_required     BOOLEAN := COALESCE(
        p_event_required,
        p_operation_kind = 'command'
    );
    -- Map operation_kind → write_model_kind that the
    -- authority_object_registry_write_model_kind_check constraint accepts.
    -- 'query' → 'read_model' (NOT 'query_model' — that's not in the enum).
    v_write_model_kind   TEXT := CASE
        WHEN p_operation_kind = 'query'   THEN 'read_model'
        WHEN p_operation_kind = 'command' THEN 'command_model'
        ELSE p_operation_kind  -- pass-through for already-canonical values
    END;
BEGIN
    -- Step 1: data_dictionary_objects
    INSERT INTO data_dictionary_objects (
        object_kind, label, category, summary, origin_ref, metadata
    ) VALUES (
        v_object_kind,
        v_label,
        p_operation_kind,
        v_summary,
        jsonb_build_object('source', 'operation_catalog_registry', 'operation_ref', p_operation_ref),
        jsonb_build_object(
            'operation_kind', p_operation_kind,
            'authority_domain_ref', p_authority_domain_ref,
            'event_type', p_event_type
        )
    )
    ON CONFLICT (object_kind) DO UPDATE SET
        label      = EXCLUDED.label,
        category   = EXCLUDED.category,
        summary    = EXCLUDED.summary,
        origin_ref = EXCLUDED.origin_ref,
        metadata   = EXCLUDED.metadata,
        updated_at = now();

    -- Step 2: authority_object_registry
    INSERT INTO authority_object_registry (
        object_ref,
        object_kind,
        object_name,
        schema_name,
        authority_domain_ref,
        data_dictionary_object_kind,
        lifecycle_status,
        write_model_kind,
        owner_ref,
        source_decision_ref,
        metadata
    ) VALUES (
        v_object_kind,
        p_operation_kind,
        p_operation_name,
        NULL,
        p_authority_domain_ref,
        v_object_kind,
        'active',
        v_write_model_kind,
        p_owner_ref,
        p_decision_ref,
        jsonb_build_object(
            'handler_ref', p_handler_ref,
            'source_kind', v_source_kind,
            'event_type', p_event_type
        )
    )
    ON CONFLICT (object_ref) DO UPDATE SET
        authority_domain_ref       = EXCLUDED.authority_domain_ref,
        data_dictionary_object_kind = EXCLUDED.data_dictionary_object_kind,
        lifecycle_status           = EXCLUDED.lifecycle_status,
        write_model_kind           = EXCLUDED.write_model_kind,
        owner_ref                  = EXCLUDED.owner_ref,
        source_decision_ref        = EXCLUDED.source_decision_ref,
        metadata                   = EXCLUDED.metadata,
        updated_at                 = now();

    -- Step 3: operation_catalog_registry
    INSERT INTO operation_catalog_registry (
        operation_ref,
        operation_name,
        source_kind,
        operation_kind,
        http_method,
        http_path,
        input_model_ref,
        handler_ref,
        authority_ref,
        posture,
        idempotency_policy,
        binding_revision,
        decision_ref,
        authority_domain_ref,
        storage_target_ref,
        input_schema_ref,
        output_schema_ref,
        receipt_required,
        event_required,
        event_type
    ) VALUES (
        p_operation_ref,
        p_operation_name,
        v_source_kind,
        p_operation_kind,
        p_http_method,
        v_http_path,
        p_input_model_ref,
        p_handler_ref,
        v_authority_ref,
        p_posture,
        p_idempotency_policy,
        v_binding_revision,
        p_decision_ref,
        p_authority_domain_ref,
        p_storage_target_ref,
        v_input_schema_ref,
        p_output_schema_ref,
        p_receipt_required,
        v_event_required,
        p_event_type
    )
    ON CONFLICT (operation_ref) DO UPDATE SET
        handler_ref          = EXCLUDED.handler_ref,
        input_model_ref      = EXCLUDED.input_model_ref,
        input_schema_ref     = EXCLUDED.input_schema_ref,
        authority_ref        = EXCLUDED.authority_ref,
        authority_domain_ref = EXCLUDED.authority_domain_ref,
        event_type           = EXCLUDED.event_type,
        event_required       = EXCLUDED.event_required,
        receipt_required     = EXCLUDED.receipt_required,
        posture              = EXCLUDED.posture,
        idempotency_policy   = EXCLUDED.idempotency_policy,
        binding_revision     = EXCLUDED.binding_revision,
        decision_ref         = EXCLUDED.decision_ref,
        output_schema_ref    = EXCLUDED.output_schema_ref,
        updated_at           = now();
END;
$$ LANGUAGE plpgsql;

-- ──────────────────────────────────────────────────────────────────────────
-- (1b) Fix enforce_operation_catalog_cqrs_contract: AOR row check hardcoded
--      `object_kind = 'command'`, blocking every query-kind op even with a
--      matching AOR row inserted. Loosen to accept either kind.
-- ──────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.enforce_operation_catalog_cqrs_contract()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    caller_count INTEGER;
BEGIN
    IF NEW.enabled IS NOT TRUE THEN
        RETURN NEW;
    END IF;

    IF NEW.operation_kind NOT IN ('command', 'query') THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must declare operation_kind command or query',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    IF NEW.authority_domain_ref IS NULL OR btrim(NEW.authority_domain_ref) = '' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must declare authority_domain_ref',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM authority_domains domains
        WHERE domains.authority_domain_ref = NEW.authority_domain_ref
          AND domains.enabled = TRUE
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation references unknown or disabled authority domain',
            DETAIL = jsonb_build_object(
                'operation_name', NEW.operation_name,
                'authority_domain_ref', NEW.authority_domain_ref
            )::text;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM authority_storage_targets targets
        WHERE targets.storage_target_ref = NEW.storage_target_ref
          AND targets.enabled = TRUE
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation references unknown or disabled storage target',
            DETAIL = jsonb_build_object(
                'operation_name', NEW.operation_name,
                'storage_target_ref', NEW.storage_target_ref
            )::text;
    END IF;

    IF NEW.input_schema_ref IS NULL OR btrim(NEW.input_schema_ref) = '' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must declare input_schema_ref',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    IF NEW.output_schema_ref IS NULL OR btrim(NEW.output_schema_ref) = '' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must declare output_schema_ref',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    IF jsonb_typeof(NEW.idempotency_key_fields) <> 'array' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must declare idempotency_key_fields as an array',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    IF jsonb_typeof(NEW.required_capabilities) <> 'object' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must declare required_capabilities as an object',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    IF jsonb_typeof(NEW.allowed_callers) <> 'array' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must declare allowed_callers as an array',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    SELECT count(*) INTO caller_count
    FROM jsonb_array_elements_text(NEW.allowed_callers) callers(value)
    WHERE btrim(callers.value) <> '';

    IF caller_count < 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must allow at least one caller',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    IF NEW.timeout_ms IS NULL OR NEW.timeout_ms <= 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must declare a positive timeout_ms',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    IF NEW.receipt_required IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation must require an authority receipt',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    IF NEW.operation_kind = 'command' THEN
        IF NEW.event_required IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'enabled command must require an authority event',
                DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
        END IF;
        IF NEW.event_type IS NULL OR btrim(NEW.event_type) = '' THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'enabled command must declare event_type',
                DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
        END IF;
    END IF;

    IF NEW.operation_kind = 'query'
       AND NEW.projection_ref IS NOT NULL
       AND btrim(NEW.projection_ref) <> ''
       AND (
           NEW.projection_freshness_policy_ref IS NULL
           OR btrim(NEW.projection_freshness_policy_ref) = ''
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'projection-backed query must declare projection_freshness_policy_ref',
            DETAIL = jsonb_build_object('operation_name', NEW.operation_name)::text;
    END IF;

    -- AOR-row check: was hardcoded `object_kind = 'command'` which made every
    -- query op fail. Loosen to match the operation's own kind so query ops
    -- pass when AOR has the matching `object_kind = 'query'` row.
    IF NOT EXISTS (
        SELECT 1
        FROM authority_object_registry registry
        WHERE registry.object_kind = NEW.operation_kind
          AND registry.object_ref = 'operation.' || NEW.operation_name
          AND registry.authority_domain_ref = NEW.authority_domain_ref
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation is missing authority object registry row',
            DETAIL = jsonb_build_object(
                'operation_name', NEW.operation_name,
                'operation_kind', NEW.operation_kind,
                'expected_object_ref', 'operation.' || NEW.operation_name,
                'authority_domain_ref', NEW.authority_domain_ref
            )::text;
    END IF;

    RETURN NEW;
END;
$function$;

-- ──────────────────────────────────────────────────────────────────────────
-- (2) Register cli_auth_doctor via the now-fixed helper
-- ──────────────────────────────────────────────────────────────────────────
SELECT register_operation_atomic(
    p_operation_ref         := 'cli-auth-doctor',
    p_operation_name        := 'cli_auth_doctor',
    p_handler_ref           := 'runtime.operations.commands.cli_auth_doctor.handle_cli_auth_doctor',
    p_input_model_ref       := 'runtime.operations.commands.cli_auth_doctor.CliAuthDoctorCommand',
    p_authority_domain_ref  := 'authority.provider_onboarding',
    p_operation_kind        := 'query',
    p_posture               := 'observe',
    p_idempotency_policy    := 'non_idempotent',
    p_label                 := 'Operation: cli_auth_doctor',
    p_summary               := 'Diagnose CLI auth state for claude / codex / gemini in one call. Probes each binary with a trivial prompt, parses output for auth-failure patterns ("Not logged in" / 401 / "authentication error"), and returns a structured per-provider report with concrete host-side remediation commands. Read-only — does not itself rehydrate auth (security CLI / login flows must run on the host).'
);

COMMIT;

-- Verification (run manually):
--   SELECT operation_ref, operation_kind, posture, handler_ref
--     FROM operation_catalog_registry
--    WHERE operation_ref = 'cli-auth-doctor';
--   curl -sS -X POST http://localhost:8420/api/operate -H 'Content-Type: application/json' \
--        -d '{"operation":"cli_auth_doctor","input":{}}' | jq .result.summary
