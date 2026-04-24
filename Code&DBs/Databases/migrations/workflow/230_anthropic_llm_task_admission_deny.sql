-- Migration 230: Strip the migration-078 "legacy provider_cli_profiles backfill"
-- rows from provider_transport_admissions and re-derive admissions from the
-- authoritative provider_lane_policy.
--
-- Migration 078 seeded provider_transport_admissions with admit=true rows for
-- every (provider_slug, adapter_type) where the provider had a binary or an
-- api_endpoint. Migrations 168/175/182 superseded specific rows with proper
-- decision_refs, but the legacy backfill rows for anthropic, deepseek, google,
-- and openai were never cleaned up. They still admit transport paths that the
-- current provider_lane_policy denies — most importantly:
--
--   anthropic + llm_task (HTTP API path) admitted_by_policy=true
--   while provider_lane_policy.anthropic = {cli_llm} only.
--
-- The router's _apply_provider_transport_admission_filter passes any candidate
-- whose admission row says admitted=true. So the legacy backfill row admits
-- anthropic/claude-opus-4-7 via the HTTP API, the worker hits api.anthropic.com
-- with no key, and the run dies on 401.
--
-- This migration:
--   1. Deletes every legacy backfill admission row.
--   2. Re-inserts clean admission rows derived from provider_lane_policy:
--        for every (provider, adapter_type) in allowed_adapter_types, write a
--        fresh row with admitted_by_policy=true and a real decision_ref.
--   3. Leaves rows whose policy_reason has already been superseded with a
--        non-legacy reason (cursor, cursor_local, openrouter) untouched.
--
-- Net effect: admissions mirror lane policy, no provider can route through an
-- adapter type its lane policy forbids, and the API-401 silent collapse is
-- closed. Aligns with decision.2026-04-20.anthropic-cli-only-restored.

BEGIN;

DELETE FROM public.provider_transport_admissions
WHERE policy_reason = 'Admitted via legacy provider_cli_profiles backfill.';

INSERT INTO public.provider_transport_admissions (
    provider_transport_admission_id,
    provider_slug,
    adapter_type,
    transport_kind,
    execution_topology,
    admitted_by_policy,
    policy_reason,
    lane_id,
    docs_urls,
    credential_sources,
    probe_contract,
    decision_ref,
    status
)
SELECT
    'provider_transport_admission.' || lp.provider_slug || '.' || adapter_type,
    lp.provider_slug,
    adapter_type,
    CASE adapter_type WHEN 'cli_llm' THEN 'cli' ELSE 'http' END,
    CASE adapter_type WHEN 'cli_llm' THEN 'local_cli' ELSE 'direct_http' END,
    true,
    'Admitted per provider_lane_policy ' || lp.decision_ref,
    lp.provider_slug || ':' || adapter_type,
    '{}'::jsonb,
    '[]'::jsonb,
    jsonb_build_object('derived_from', 'provider_lane_policy'),
    lp.decision_ref,
    'active'
FROM public.provider_lane_policy lp
CROSS JOIN LATERAL unnest(lp.allowed_adapter_types) AS adapter_type
WHERE NOT EXISTS (
    SELECT 1
    FROM public.provider_transport_admissions a
    WHERE a.provider_slug = lp.provider_slug
      AND a.adapter_type = adapter_type
)
ON CONFLICT (provider_slug, adapter_type) DO NOTHING;

COMMIT;

-- Verification (run manually):
--   SELECT provider_slug, adapter_type, admitted_by_policy, policy_reason
--   FROM provider_transport_admissions
--   ORDER BY provider_slug, adapter_type;
--     -> no rows with policy_reason ILIKE '%legacy%backfill%'
--     -> for every (provider, adapter_type) in provider_lane_policy.
--        allowed_adapter_types: a row exists with admitted_by_policy=true
--     -> anthropic has only cli_llm row (no llm_task admission)
