-- Migration 375: Remove task_type_routing rows with no matching provider transport admission.
--
-- Why:
--   task_type_routing declares per-task routes (provider_slug, model_slug, transport_type, rank).
--   provider_transport_admissions is the authority for which transports each provider actually
--   exposes. Rows where (provider_slug, transport_type) has no active admission are unreachable
--   garbage: they pollute pickers, failover chains, and best-route resolution (e.g. CLI rows for
--   HTTP-only providers like OpenRouter).
--
-- Authority link:
--   Deletions are gated solely on NOT EXISTS against provider_transport_admissions with
--   status = 'active'. Mapping: task_type_routing.transport_type 'API' -> transport_kind 'http',
--   'CLI' -> 'cli'.
--
-- Idempotent: re-run deletes zero rows once invalid rows are gone.

BEGIN;

DO $$
DECLARE
    pre_count bigint;
    deleted bigint;
    post_count bigint;
BEGIN
    SELECT COUNT(*) INTO STRICT pre_count
    FROM task_type_routing ttr
    WHERE NOT EXISTS (
        SELECT 1
        FROM provider_transport_admissions pta
        WHERE pta.provider_slug = ttr.provider_slug
          AND pta.status = 'active'
          AND (
              (ttr.transport_type = 'API' AND pta.transport_kind = 'http')
              OR (ttr.transport_type = 'CLI' AND pta.transport_kind = 'cli')
          )
    );

    RAISE NOTICE '375_cleanup_invalid_task_type_routing_transports: pre-delete invalid row count = %', pre_count;

    DELETE FROM task_type_routing ttr
    WHERE NOT EXISTS (
        SELECT 1
        FROM provider_transport_admissions pta
        WHERE pta.provider_slug = ttr.provider_slug
          AND pta.status = 'active'
          AND (
              (ttr.transport_type = 'API' AND pta.transport_kind = 'http')
              OR (ttr.transport_type = 'CLI' AND pta.transport_kind = 'cli')
          )
    );

    GET DIAGNOSTICS deleted = ROW_COUNT;
    RAISE NOTICE '375_cleanup_invalid_task_type_routing_transports: rows deleted = %', deleted;

    SELECT COUNT(*) INTO STRICT post_count
    FROM task_type_routing ttr
    WHERE NOT EXISTS (
        SELECT 1
        FROM provider_transport_admissions pta
        WHERE pta.provider_slug = ttr.provider_slug
          AND pta.status = 'active'
          AND (
              (ttr.transport_type = 'API' AND pta.transport_kind = 'http')
              OR (ttr.transport_type = 'CLI' AND pta.transport_kind = 'cli')
          )
    );

    RAISE NOTICE '375_cleanup_invalid_task_type_routing_transports: post-delete invalid row count = %', post_count;
END $$;

COMMIT;
