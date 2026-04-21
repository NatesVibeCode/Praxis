-- Migration 197: Remove synthetic Anthropic API-key authority.
--
-- Anthropic is CLI-only per decision.2026-04-20.anthropic-cli-only-restored.
-- Runtime now fails closed when API transport auth env names are not declared
-- by provider_cli_profiles, so the DB must not advertise the forbidden
-- ANTHROPIC_API_KEY through neighboring admission or sandbox rows.

BEGIN;

UPDATE provider_transport_admissions
   SET credential_sources = '[]'::jsonb,
       probe_contract = CASE
           WHEN adapter_type = 'llm_task' THEN '{}'::jsonb
           ELSE probe_contract
       END,
       policy_reason = CASE
           WHEN adapter_type = 'cli_llm'
               THEN 'CLI auth is resolved by the provider CLI session, not provider API-key env vars.'
           ELSE policy_reason
       END,
       decision_ref = 'decision.2026-04-20.anthropic-cli-only-restored',
       updated_at = now()
 WHERE provider_slug = 'anthropic'
   AND credential_sources ? 'ANTHROPIC_API_KEY';

UPDATE registry_sandbox_profile_authority AS sandbox
   SET secret_allowlist = stripped.secret_allowlist,
       recorded_at = now()
  FROM (
      SELECT sandbox_profile_ref,
             COALESCE(jsonb_agg(to_jsonb(secret_name)) FILTER (
                 WHERE secret_name <> 'ANTHROPIC_API_KEY'
             ), '[]'::jsonb) AS secret_allowlist
        FROM registry_sandbox_profile_authority
        LEFT JOIN LATERAL jsonb_array_elements_text(secret_allowlist) AS secret_name
          ON true
       GROUP BY sandbox_profile_ref
  ) AS stripped
 WHERE sandbox.sandbox_profile_ref = stripped.sandbox_profile_ref
   AND sandbox.secret_allowlist ? 'ANTHROPIC_API_KEY';

UPDATE provider_cli_profiles AS profile
   SET sandbox_env_overrides = CASE
           WHEN stripped.strip_values = '[]'::jsonb
               THEN COALESCE(profile.sandbox_env_overrides, '{}'::jsonb) - 'strip'
           ELSE jsonb_set(
               COALESCE(profile.sandbox_env_overrides, '{}'::jsonb),
               '{strip}',
               stripped.strip_values,
               true
           )
       END,
       updated_at = now()
  FROM (
      SELECT provider_slug,
             COALESCE(jsonb_agg(to_jsonb(strip_name)) FILTER (
                 WHERE strip_name <> 'ANTHROPIC_API_KEY'
             ), '[]'::jsonb) AS strip_values
        FROM provider_cli_profiles
        LEFT JOIN LATERAL jsonb_array_elements_text(sandbox_env_overrides->'strip') AS strip_name
          ON true
       WHERE provider_slug = 'anthropic'
       GROUP BY provider_slug
  ) AS stripped
 WHERE profile.provider_slug = stripped.provider_slug
   AND COALESCE(profile.sandbox_env_overrides, '{}'::jsonb)->'strip' ? 'ANTHROPIC_API_KEY';

COMMIT;
