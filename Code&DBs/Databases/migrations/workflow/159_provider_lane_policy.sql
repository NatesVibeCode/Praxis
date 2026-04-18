-- Provider lane policy: per-provider authority for which adapter types
-- (cli_llm, llm_task) may be admitted at the router, and whether specs/jobs
-- may override that default.
--
-- The admission path (runtime/routing_economics.admit_adapter_type) refuses
-- any route whose adapter_type is not in allowed_adapter_types for that
-- provider. This is the first gate of the lane-control hierarchy described
-- in the debate under artifacts/ — it narrows, never widens.
--
-- Seed policy:
--   anthropic -> {cli_llm}, locked  (CLI is sunk cost; no silent paid fallback)
--   cursor    -> {cli_llm}, locked  (CLI-only provider)
--   openai    -> {cli_llm, llm_task}, overridable (mechanical fan-out OK)
--   google    -> {cli_llm, llm_task}, overridable (mechanical fan-out OK)
--   deepseek  -> {llm_task}, locked  (direct API provider, no CLI)

BEGIN;

CREATE TABLE provider_lane_policy (
    provider_slug text PRIMARY KEY,
    allowed_adapter_types text[] NOT NULL,
    overridable boolean NOT NULL DEFAULT false,
    decision_ref text NOT NULL,
    effective_from timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_provider_lane_policy_adapter_types_nonempty
        CHECK (array_length(allowed_adapter_types, 1) >= 1),
    CONSTRAINT ck_provider_lane_policy_adapter_types_known
        CHECK (allowed_adapter_types <@ ARRAY['cli_llm', 'llm_task']::text[])
);

INSERT INTO provider_lane_policy
    (provider_slug, allowed_adapter_types, overridable, decision_ref)
VALUES
    ('anthropic', ARRAY['cli_llm'],            false, 'migration:159:anthropic_cli_locked'),
    ('cursor',    ARRAY['cli_llm'],            false, 'migration:159:cursor_cli_only'),
    ('openai',    ARRAY['cli_llm','llm_task'], true,  'migration:159:openai_both'),
    ('google',    ARRAY['cli_llm','llm_task'], true,  'migration:159:google_both'),
    ('deepseek',  ARRAY['llm_task'],           false, 'migration:159:deepseek_api_only');

COMMIT;
