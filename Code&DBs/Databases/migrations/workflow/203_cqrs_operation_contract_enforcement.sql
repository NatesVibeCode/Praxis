-- Migration 203: CQRS operation contract enforcement.
--
-- New enabled operation-catalog rows and authority objects must be complete
-- enough for future agents to reason from the database instead of guessing.

BEGIN;

CREATE OR REPLACE FUNCTION enforce_authority_object_dictionary_binding()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lifecycle_status IN ('draft', 'active', 'legacy') THEN
        IF NOT EXISTS (
            SELECT 1
            FROM data_dictionary_objects dictionary
            WHERE dictionary.object_kind = NEW.data_dictionary_object_kind
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'authority object is missing data dictionary binding',
                DETAIL = jsonb_build_object(
                    'object_ref', NEW.object_ref,
                    'data_dictionary_object_kind', NEW.data_dictionary_object_kind
                )::text;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_authority_object_dictionary_binding ON authority_object_registry;
CREATE CONSTRAINT TRIGGER trg_authority_object_dictionary_binding
    AFTER INSERT OR UPDATE ON authority_object_registry
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_authority_object_dictionary_binding();

CREATE OR REPLACE FUNCTION enforce_operation_catalog_cqrs_contract()
RETURNS trigger LANGUAGE plpgsql AS $$
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

    IF NOT EXISTS (
        SELECT 1
        FROM authority_object_registry registry
        WHERE registry.object_kind = 'command'
          AND registry.object_ref = 'operation.' || NEW.operation_name
          AND registry.authority_domain_ref = NEW.authority_domain_ref
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'enabled operation is missing authority object registry row',
            DETAIL = jsonb_build_object(
                'operation_name', NEW.operation_name,
                'expected_object_ref', 'operation.' || NEW.operation_name,
                'authority_domain_ref', NEW.authority_domain_ref
            )::text;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_operation_catalog_cqrs_contract ON operation_catalog_registry;
CREATE CONSTRAINT TRIGGER trg_operation_catalog_cqrs_contract
    AFTER INSERT OR UPDATE ON operation_catalog_registry
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_operation_catalog_cqrs_contract();

CREATE OR REPLACE VIEW authority_contract_validation_report AS
SELECT
    'operation_catalog'::text AS contract_area,
    operation_name AS object_ref,
    CASE
        WHEN authority_domain_ref IS NULL OR btrim(authority_domain_ref) = '' THEN 'missing_authority_domain'
        WHEN input_schema_ref IS NULL OR btrim(input_schema_ref) = '' THEN 'missing_input_schema'
        WHEN output_schema_ref IS NULL OR btrim(output_schema_ref) = '' THEN 'missing_output_schema'
        WHEN receipt_required IS DISTINCT FROM TRUE THEN 'receipt_not_required'
        WHEN operation_kind = 'command'
             AND (
                 event_required IS DISTINCT FROM TRUE
                 OR event_type IS NULL
                 OR btrim(event_type) = ''
             ) THEN 'command_event_not_declared'
        WHEN NOT EXISTS (
            SELECT 1
            FROM authority_object_registry registry
            WHERE registry.object_kind = 'command'
              AND registry.object_ref = 'operation.' || operations.operation_name
              AND registry.authority_domain_ref = operations.authority_domain_ref
        ) THEN 'missing_authority_object'
        ELSE 'ok'
    END AS validation_status,
    jsonb_build_object(
        'operation_ref', operation_ref,
        'operation_kind', operation_kind,
        'authority_domain_ref', authority_domain_ref,
        'projection_ref', projection_ref
    ) AS details
FROM operation_catalog_registry operations
WHERE enabled = TRUE
UNION ALL
SELECT
    'authority_object_registry'::text AS contract_area,
    object_ref,
    CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM data_dictionary_objects dictionary
            WHERE dictionary.object_kind = registry.data_dictionary_object_kind
        ) THEN 'missing_data_dictionary_object'
        ELSE 'ok'
    END AS validation_status,
    jsonb_build_object(
        'object_kind', object_kind,
        'authority_domain_ref', authority_domain_ref,
        'data_dictionary_object_kind', data_dictionary_object_kind
    ) AS details
FROM authority_object_registry registry
WHERE lifecycle_status IN ('draft', 'active', 'legacy');

COMMENT ON VIEW authority_contract_validation_report IS
    'CQRS contract validation report. New operation and object writes are enforced by deferred triggers; this view exposes current status for inspection.';

COMMIT;
