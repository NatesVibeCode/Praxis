-- Migration 240: register_operation_atomic — auto-resolve event_required default.
--
-- Migration 239 introduced register_operation_atomic with
-- p_event_required BOOLEAN DEFAULT FALSE. The operation_catalog_registry
-- CHECK trigger raises ('enabled command must require an authority event')
-- when a command lands with event_required=FALSE — so the FALSE default is
-- the wrong choice for commands. Callers had to remember to override.
--
-- This migration changes the default to NULL and resolves it inside the
-- function: TRUE for commands, FALSE for queries. Callers can still override
-- explicitly by passing the boolean.

BEGIN;

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
    -- Resolve event_required from operation_kind when caller did not set it.
    -- Commands must emit events (CHECK constraint); queries do not.
    v_event_required     BOOLEAN := COALESCE(
        p_event_required,
        p_operation_kind = 'command'
    );
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
        CASE WHEN p_operation_kind = 'query' THEN 'query_model' ELSE 'command_model' END,
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

COMMIT;
