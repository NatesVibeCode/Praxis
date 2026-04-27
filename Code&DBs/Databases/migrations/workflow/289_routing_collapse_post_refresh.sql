-- Migration 289: Post-collapse refresh of private_provider_job_catalog.
--
-- Migration 287 deferred the manual refresh after rewriting the matrix view
-- because doing it inside 287's transaction surfaced a transient
-- "ON CONFLICT DO UPDATE command cannot affect row a second time" error.
-- Investigation post-bootstrap (this migration's commit message):
--
--   * Matrix view itself produces zero duplicate (runtime_profile_ref,
--     job_type, adapter_type, provider_slug, model_slug) tuples.
--   * runtime_profile_admitted_routes has zero (runtime_profile_ref,
--     candidate_ref) duplicates among admitted rows.
--   * provider_transport_admissions has zero (provider_slug, adapter_type)
--     duplicates.
--   * Manually invoking refresh_private_provider_job_catalog('praxis')
--     after bootstrap completes succeeds without error.
--
-- The transient failure was state-dependent inside 287's transaction —
-- most likely a plan-cache or visibility quirk with the freshly-DROP+CREATEd
-- matrix view executing inside the same TX that just inserted new
-- task_type_routing rows. Outside the TX, the refresh is clean. So this
-- migration is the formal post-collapse step: reapply the refresh per
-- runtime profile so private_provider_job_catalog reflects the
-- routing-derived API admission shape, and emit a sentinel row in
-- data_dictionary_objects so bootstrap can detect that 289 has applied
-- (the function call has no name-based existence signature).

BEGIN;

DO $$
DECLARE
    profile_ref text;
BEGIN
    FOR profile_ref IN
        SELECT runtime_profile_ref
        FROM registry_native_runtime_profile_authority
        ORDER BY runtime_profile_ref
    LOOP
        PERFORM refresh_private_provider_job_catalog(profile_ref);
    END LOOP;
END $$;

INSERT INTO data_dictionary_objects (
    object_kind, label, category, summary, origin_ref, metadata
) VALUES (
    'projection.private_provider_job_catalog.routing_collapse_v289',
    'Routing-collapse projection refresh marker (migration 289)',
    'projection',
    'Sentinel row signaling that the private_provider_job_catalog projection has been refreshed against the routing-derived matrix view introduced by migration 287. Used by bootstrap to detect that migration 289 has applied; without this marker, the migration has no name-based footprint that the readiness check can verify.',
    jsonb_build_object('source', 'migration.289_routing_collapse_post_refresh'),
    jsonb_build_object('marker_for', 'migration.289', 'depends_on', 'migration.287')
)
ON CONFLICT (object_kind) DO UPDATE SET
    summary = EXCLUDED.summary,
    metadata = EXCLUDED.metadata,
    updated_at = now();

COMMIT;

-- Verification (run manually):
--   SELECT object_kind FROM data_dictionary_objects
--    WHERE object_kind = 'projection.private_provider_job_catalog.routing_collapse_v289';
--
--   SELECT projection_ref, freshness_status, last_refreshed_at
--     FROM authority_projection_state
--    WHERE projection_ref = 'projection.private_provider_job_catalog';
--
--   SELECT runtime_profile_ref, COUNT(*) AS catalog_rows,
--          COUNT(*) FILTER (WHERE availability_state = 'available') AS available_rows
--     FROM private_provider_job_catalog
--    GROUP BY runtime_profile_ref
--    ORDER BY runtime_profile_ref;
