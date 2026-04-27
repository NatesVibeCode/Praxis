-- Migration 283: admit picker winners at the runtime_profile_admitted_routes layer
-- AND fix the audit view to surface the layer the resolver actually consults.
--
-- Migration 282 admitted the new picks at the API allowlist layer
-- (`private_provider_api_job_allowlist`) and built a `task_type_routing_admission_audit`
-- view. But that view queried `private_model_access_control_matrix`, which only
-- includes the allowlist + denial logic. The actual gate the resolver consults
-- is `effective_private_provider_job_catalog`, which adds another layer:
-- `runtime_profile_admitted_routes`. A candidate is dropped with
-- `runtime_profile_route.not_admitted` if it has no admitted row at the
-- profile layer.
--
-- This migration:
--   (1) adds admitted_routes rows for the picker winners for both runtime
--       profiles (praxis + scratch_agent),
--   (2) refreshes the snapshot so the resolver picks them up immediately,
--   (3) replaces the audit view with one that queries the SAME view the
--       resolver consults — so the audit tells the truth, not a partial view.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────
-- Layer 3: per-profile admission (runtime_profile_admitted_routes)
-- ──────────────────────────────────────────────────────────────────────────

INSERT INTO runtime_profile_admitted_routes (
    runtime_profile_ref, model_profile_id, provider_policy_id,
    candidate_ref, provider_ref, provider_slug, model_slug,
    eligibility_status, reason_code, source_window_refs,
    projected_at, projection_ref
)
SELECT
    profile.runtime_profile_ref,
    'model_profile.openrouter.compose.gemini-3-flash-preview',
    'provider_policy.openrouter.llm_task',
    'candidate.openrouter.google-gemini-3-flash-preview',
    'provider.openrouter',
    'openrouter',
    'google/gemini-3-flash-preview',
    'admitted',
    'compose_picker.empirical_pick_2026_04_26',
    '["migration.283_admit_picker_winners_runtime_profile_routes"]'::jsonb,
    now(),
    'projection.runtime_profile_admitted_routes'
FROM registry_native_runtime_profile_authority AS profile
ON CONFLICT (runtime_profile_ref, candidate_ref) DO UPDATE SET
    eligibility_status = EXCLUDED.eligibility_status,
    reason_code        = EXCLUDED.reason_code,
    source_window_refs = EXCLUDED.source_window_refs,
    projected_at       = EXCLUDED.projected_at,
    projection_ref     = EXCLUDED.projection_ref;

INSERT INTO runtime_profile_admitted_routes (
    runtime_profile_ref, model_profile_id, provider_policy_id,
    candidate_ref, provider_ref, provider_slug, model_slug,
    eligibility_status, reason_code, source_window_refs,
    projected_at, projection_ref
)
SELECT
    profile.runtime_profile_ref,
    'model_profile.openrouter.compose.gpt-5-4-mini',
    'provider_policy.openrouter.llm_task',
    'candidate.openrouter.openai-gpt-5-4-mini',
    'provider.openrouter',
    'openrouter',
    'openai/gpt-5.4-mini',
    'admitted',
    'compose_picker.empirical_pick_2026_04_26',
    '["migration.283_admit_picker_winners_runtime_profile_routes"]'::jsonb,
    now(),
    'projection.runtime_profile_admitted_routes'
FROM registry_native_runtime_profile_authority AS profile
ON CONFLICT (runtime_profile_ref, candidate_ref) DO UPDATE SET
    eligibility_status = EXCLUDED.eligibility_status,
    reason_code        = EXCLUDED.reason_code,
    source_window_refs = EXCLUDED.source_window_refs,
    projected_at       = EXCLUDED.projected_at,
    projection_ref     = EXCLUDED.projection_ref;

-- Refresh the catalog table FIRST (it rebuilds from runtime_profile_admitted_routes
-- — without this call, the catalog is stale and the snapshot inherits the
-- staleness), THEN refresh the snapshot.
SELECT refresh_private_provider_job_catalog('praxis');
SELECT refresh_private_provider_job_catalog('scratch_agent');
SELECT refresh_private_provider_control_plane_snapshot('praxis');
SELECT refresh_private_provider_control_plane_snapshot('scratch_agent');

-- ──────────────────────────────────────────────────────────────────────────
-- Replace the audit view with one that queries the gate the resolver
-- actually consults (effective_private_provider_job_catalog), instead of
-- private_model_access_control_matrix which only shows the allowlist layer.
-- ──────────────────────────────────────────────────────────────────────────

DROP VIEW IF EXISTS task_type_routing_admission_audit;

CREATE OR REPLACE VIEW task_type_routing_admission_audit AS
SELECT
    route.task_type,
    route.rank,
    route.provider_slug,
    route.model_slug,
    rp.runtime_profile_ref,
    catalog.reason_code AS catalog_reason_code,
    CASE
        WHEN catalog.runtime_profile_ref IS NULL THEN 'route_not_in_resolver_gate'
        ELSE 'admitted'
    END AS admission_status
FROM task_type_routing AS route
CROSS JOIN registry_native_runtime_profile_authority AS rp
LEFT JOIN effective_private_provider_job_catalog AS catalog
    ON catalog.runtime_profile_ref = rp.runtime_profile_ref
   AND catalog.job_type            = route.task_type
   AND catalog.transport_type      = 'API'
   AND catalog.adapter_type        = 'llm_task'
   AND catalog.provider_slug       = route.provider_slug
   AND catalog.model_slug          = route.model_slug
WHERE route.permitted IS TRUE
ORDER BY route.task_type, route.rank, rp.runtime_profile_ref;

COMMENT ON VIEW task_type_routing_admission_audit IS
'Surfaces task_type_routing rows that the production resolver (resolve_matrix_gated_route_configs) will actually drop at the JOIN. A rank-1 routing row only takes effect if the (runtime_profile, task_type, adapter_type=llm_task, provider, model) combination is admitted across THREE layers: (1) private_provider_api_job_allowlist, (2) runtime_profile_admitted_routes, (3) effective_private_provider_control_plane (snapshot). Querying this view filtered to admission_status<>''admitted'' surfaces all three failure modes. After authoring a routing change, query this view BEFORE shipping to confirm the new picks are admitted.';

COMMIT;
