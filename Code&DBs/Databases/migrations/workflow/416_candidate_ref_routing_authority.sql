-- Migration 416: collapse routing identity onto provider_model_candidates.candidate_ref.
--
-- The runtime now carries candidate_ref through router -> plan manifest ->
-- claim -> execution. The schema still allowed task_type_routing to point at
-- loose provider/model/transport tuples, which meant a rename, fan-out, or
-- broker-host ambiguity could orphan a route without a hard database edge.
--
-- This migration makes candidate_ref the routing authority:
--   * provider_model_candidates gets explicit transport/host/variant/effort
--     columns for inspection and deterministic lookup.
--   * task_type_routing gets candidate_ref plus a direct FK.
--   * a trigger rejects candidate_ref values that do not match the route's
--     provider/model/transport.
--   * refresh_private_provider_job_catalog resolves through route.candidate_ref
--     first, then an exact transport-aware fallback for still-unbound routes.
--   * task_type_routing_orphans exposes remaining rows that cannot materialize.

BEGIN;

ALTER TABLE provider_model_candidates
    ADD COLUMN IF NOT EXISTS transport_type TEXT,
    ADD COLUMN IF NOT EXISTS host_provider_slug TEXT,
    ADD COLUMN IF NOT EXISTS variant TEXT,
    ADD COLUMN IF NOT EXISTS effort_slug TEXT;

ALTER TABLE task_type_routing
    ADD COLUMN IF NOT EXISTS candidate_ref TEXT,
    ADD COLUMN IF NOT EXISTS host_provider_slug TEXT,
    ADD COLUMN IF NOT EXISTS variant TEXT,
    ADD COLUMN IF NOT EXISTS effort_slug TEXT;

-- Retire the legacy composite tuple authority before backfill. It cannot
-- represent candidate fan-out safely because route identity is temporarily
-- incomplete while candidate_ref is being resolved.
ALTER TABLE task_type_routing
    DROP CONSTRAINT IF EXISTS task_type_routing_candidate_fkey;

-- Candidate transport backfill. Explicit .cli. candidate refs stay CLI. Any
-- candidate already represented by an exact route transport will be fanned out
-- below if the opposite transport is needed.
UPDATE provider_model_candidates
   SET transport_type = 'CLI'
 WHERE (transport_type IS NULL OR btrim(transport_type) = '')
   AND candidate_ref LIKE '%.cli.%';

WITH single_lane AS (
    SELECT provider_slug,
           bool_or(adapter_economics ? 'cli_llm') AS supports_cli,
           bool_or(adapter_economics ? 'llm_task') AS supports_api
      FROM provider_cli_profiles
     WHERE status = 'active'
     GROUP BY provider_slug
)
UPDATE provider_model_candidates AS c
   SET transport_type = CASE
       WHEN lane.supports_cli AND NOT lane.supports_api THEN 'CLI'
       WHEN lane.supports_api AND NOT lane.supports_cli THEN 'API'
       ELSE c.transport_type
   END
  FROM single_lane AS lane
 WHERE c.provider_slug = lane.provider_slug
   AND (c.transport_type IS NULL OR btrim(c.transport_type) = '')
   AND (
       (lane.supports_cli AND NOT lane.supports_api)
       OR (lane.supports_api AND NOT lane.supports_cli)
   );

UPDATE provider_model_candidates
   SET transport_type = 'API'
 WHERE transport_type IS NULL OR btrim(transport_type) = '';

UPDATE provider_model_candidates
   SET variant = CASE
       WHEN candidate_ref LIKE '%-picker' THEN 'picker'
       ELSE ''
   END
 WHERE variant IS NULL;

UPDATE provider_model_candidates
   SET effort_slug = ''
 WHERE effort_slug IS NULL;

-- Some live beta databases have retired duplicate candidates that only become
-- visible after this migration fills host_provider_slug. Keep their history,
-- but move non-winning duplicates onto an explicit legacy variant before the
-- existing identity constraint can reject the host backfill.
WITH openrouter_hosts(candidate_suffix, host_provider_slug) AS (
    VALUES
        ('anthropic-claude-haiku-4-5', 'anthropic'),
        ('anthropic.claude-sonnet-4-6', 'anthropic'),
        ('auto', 'openrouter'),
        ('deepseek-r1', 'deepseek'),
        ('deepseek-r1-picker', 'deepseek'),
        ('deepseek-v3', 'deepseek'),
        ('deepseek-v3.2', 'deepseek'),
        ('deepseek-v4-flash', 'deepseek'),
        ('deepseek-v4-flash-picker', 'deepseek'),
        ('deepseek-v4-pro', 'deepseek'),
        ('gemini-2.5-flash', 'google'),
        ('google-gemini-3-flash-preview', 'google'),
        ('google/gemini-3.1-pro-preview', 'google'),
        ('gemini-3-flash-preview', 'google'),
        ('llama-3.3-70b-instruct', 'meta-llama'),
        ('mistral-medium-3.1', 'mistralai'),
        ('mistral-small-3.2-24b', 'mistralai'),
        ('moonshotai/kimi-k2.6', 'moonshotai'),
        ('openai-gpt-5-1-codex-mini', 'openai'),
        ('openai-gpt-5-4-mini', 'openai'),
        ('openai-gpt-5-mini', 'openai'),
        ('gpt-5.4-mini', 'openai'),
        ('qwen/qwen3.6-plus', 'qwen'),
        ('qwen3-235b-a22b-2507', 'qwen'),
        ('qwen3-30b-a3b-thinking-2507', 'qwen'),
        ('qwen3-max', 'qwen'),
        ('x-ai/grok-4.1-fast', 'x-ai'),
        ('xiaomi/mimo-v2.5-pro', 'xiaomi'),
        ('z-ai/glm-5.1', 'z-ai')
),
normalized AS (
    SELECT
        c.candidate_ref,
        c.transport_type,
        c.provider_slug,
        COALESCE(
            NULLIF(c.host_provider_slug, ''),
            CASE
                WHEN c.provider_slug NOT IN ('openrouter', 'fireworks', 'together') THEN c.provider_slug
                WHEN c.provider_slug = 'together' AND c.model_slug LIKE '%/%'
                    THEN split_part(c.model_slug, '/', 1)
                WHEN c.provider_slug = 'together' THEN 'together'
                WHEN c.provider_slug = 'fireworks' THEN 'fireworks'
                WHEN c.provider_slug = 'openrouter' THEN hosts.host_provider_slug
                ELSE c.provider_slug
            END,
            c.provider_slug
        ) AS target_host_provider_slug,
        c.model_slug,
        COALESCE(NULLIF(c.variant, ''), '') AS target_variant,
        COALESCE(NULLIF(c.effort_slug, ''), '') AS target_effort_slug,
        c.status,
        c.priority,
        c.created_at
    FROM provider_model_candidates AS c
    LEFT JOIN openrouter_hosts AS hosts
      ON c.provider_slug = 'openrouter'
     AND c.candidate_ref = 'candidate.openrouter.' || hosts.candidate_suffix
),
ranked AS (
    SELECT
        candidate_ref,
        row_number() OVER (
            PARTITION BY
                transport_type,
                provider_slug,
                target_host_provider_slug,
                model_slug,
                target_variant,
                target_effort_slug
            ORDER BY
                CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                priority ASC,
                created_at DESC,
                candidate_ref ASC
        ) AS duplicate_rank
    FROM normalized
)
UPDATE provider_model_candidates AS c
   SET variant = 'legacy-' || substr(md5(c.candidate_ref), 1, 12)
  FROM ranked
 WHERE c.candidate_ref = ranked.candidate_ref
   AND ranked.duplicate_rank > 1
   AND COALESCE(NULLIF(c.variant, ''), '') = '';

UPDATE provider_model_candidates
   SET host_provider_slug = provider_slug
 WHERE (host_provider_slug IS NULL OR btrim(host_provider_slug) = '')
   AND provider_slug NOT IN ('openrouter', 'fireworks', 'together');

UPDATE provider_model_candidates
   SET host_provider_slug = split_part(model_slug, '/', 1)
 WHERE (host_provider_slug IS NULL OR btrim(host_provider_slug) = '')
   AND provider_slug = 'together'
   AND model_slug LIKE '%/%';

UPDATE provider_model_candidates
   SET host_provider_slug = 'together'
 WHERE (host_provider_slug IS NULL OR btrim(host_provider_slug) = '')
   AND provider_slug = 'together';

UPDATE provider_model_candidates
   SET host_provider_slug = 'fireworks'
 WHERE (host_provider_slug IS NULL OR btrim(host_provider_slug) = '')
   AND provider_slug = 'fireworks';

WITH openrouter_hosts(candidate_suffix, host_provider_slug) AS (
    VALUES
        ('anthropic-claude-haiku-4-5', 'anthropic'),
        ('anthropic.claude-sonnet-4-6', 'anthropic'),
        ('auto', 'openrouter'),
        ('deepseek-r1', 'deepseek'),
        ('deepseek-r1-picker', 'deepseek'),
        ('deepseek-v3', 'deepseek'),
        ('deepseek-v3.2', 'deepseek'),
        ('deepseek-v4-flash', 'deepseek'),
        ('deepseek-v4-flash-picker', 'deepseek'),
        ('deepseek-v4-pro', 'deepseek'),
        ('gemini-2.5-flash', 'google'),
        ('google-gemini-3-flash-preview', 'google'),
        ('google/gemini-3.1-pro-preview', 'google'),
        ('gemini-3-flash-preview', 'google'),
        ('llama-3.3-70b-instruct', 'meta-llama'),
        ('mistral-medium-3.1', 'mistralai'),
        ('mistral-small-3.2-24b', 'mistralai'),
        ('moonshotai/kimi-k2.6', 'moonshotai'),
        ('openai-gpt-5-1-codex-mini', 'openai'),
        ('openai-gpt-5-4-mini', 'openai'),
        ('openai-gpt-5-mini', 'openai'),
        ('gpt-5.4-mini', 'openai'),
        ('qwen/qwen3.6-plus', 'qwen'),
        ('qwen3-235b-a22b-2507', 'qwen'),
        ('qwen3-30b-a3b-thinking-2507', 'qwen'),
        ('qwen3-max', 'qwen'),
        ('x-ai/grok-4.1-fast', 'x-ai'),
        ('xiaomi/mimo-v2.5-pro', 'xiaomi'),
        ('z-ai/glm-5.1', 'z-ai')
)
UPDATE provider_model_candidates AS c
   SET host_provider_slug = hosts.host_provider_slug
  FROM openrouter_hosts AS hosts
 WHERE (c.host_provider_slug IS NULL OR btrim(c.host_provider_slug) = '')
   AND c.provider_slug = 'openrouter'
   AND c.candidate_ref = 'candidate.openrouter.' || hosts.candidate_suffix;

UPDATE provider_model_candidates
   SET host_provider_slug = provider_slug
 WHERE host_provider_slug IS NULL OR btrim(host_provider_slug) = '';

-- Route-declared transports are explicit authority. If a route uses a
-- transport that has no concrete candidate yet, clone the nearest candidate
-- into a deterministic transport-specific candidate_ref. This preserves
-- existing declared capability without broad-enabling new routes.
WITH needs AS (
    SELECT DISTINCT
           r.provider_slug,
           r.model_slug,
           r.transport_type
      FROM task_type_routing AS r
      LEFT JOIN provider_model_candidates AS c
        ON c.provider_slug = r.provider_slug
       AND c.model_slug = r.model_slug
       AND c.transport_type = r.transport_type
     WHERE c.candidate_ref IS NULL
),
sources AS (
    SELECT DISTINCT ON (provider_slug, model_slug)
           candidate_ref,
           provider_slug,
           model_slug
      FROM provider_model_candidates
     ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
),
fanout_refs AS (
    SELECT
        sources.candidate_ref || '.' || lower(needs.transport_type) AS candidate_ref
      FROM needs
      JOIN sources
        ON sources.provider_slug = needs.provider_slug
       AND sources.model_slug = needs.model_slug
)
DELETE FROM model_profile_candidate_bindings AS binding
 USING fanout_refs
 WHERE binding.model_profile_candidate_binding_id = 'binding.auto.' || fanout_refs.candidate_ref
   AND binding.candidate_ref IS DISTINCT FROM fanout_refs.candidate_ref;

WITH needs AS (
    SELECT DISTINCT
           r.provider_slug,
           r.model_slug,
           r.transport_type
      FROM task_type_routing AS r
      LEFT JOIN provider_model_candidates AS c
        ON c.provider_slug = r.provider_slug
       AND c.model_slug = r.model_slug
       AND c.transport_type = r.transport_type
     WHERE c.candidate_ref IS NULL
),
sources AS (
    SELECT DISTINCT ON (provider_slug, model_slug)
           candidate_ref,
           provider_ref,
           provider_name,
           provider_slug,
           model_slug,
           status,
           priority,
           balance_weight,
           capability_tags,
           default_parameters,
           effective_to,
           decision_ref,
           cli_config,
           route_tier,
           route_tier_rank,
           latency_class,
           latency_rank,
           reasoning_control,
           task_affinities,
           benchmark_profile,
           cap_language_high,
           cap_analysis_architecture_research,
           cap_build_high,
           cap_review,
           cap_tool_use,
           cap_build_med,
           cap_language_low,
           cap_build_low,
           cap_research_fan,
           cap_image,
           host_provider_slug,
           variant,
           effort_slug
      FROM provider_model_candidates
     ORDER BY provider_slug, model_slug, priority ASC, created_at DESC
)
INSERT INTO provider_model_candidates (
    candidate_ref,
    provider_ref,
    provider_name,
    provider_slug,
    model_slug,
    transport_type,
    host_provider_slug,
    variant,
    effort_slug,
    status,
    priority,
    balance_weight,
    capability_tags,
    default_parameters,
    effective_from,
    effective_to,
    decision_ref,
    created_at,
    cli_config,
    route_tier,
    route_tier_rank,
    latency_class,
    latency_rank,
    reasoning_control,
    task_affinities,
    benchmark_profile,
    cap_language_high,
    cap_analysis_architecture_research,
    cap_build_high,
    cap_review,
    cap_tool_use,
    cap_build_med,
    cap_language_low,
    cap_build_low,
    cap_research_fan,
    cap_image
)
SELECT
    sources.candidate_ref || '.' || lower(needs.transport_type),
    sources.provider_ref,
    sources.provider_name,
    sources.provider_slug,
    sources.model_slug,
    needs.transport_type,
    sources.host_provider_slug,
    sources.variant,
    sources.effort_slug,
    sources.status,
    sources.priority,
    sources.balance_weight,
    sources.capability_tags,
    sources.default_parameters,
    now(),
    sources.effective_to,
    sources.decision_ref,
    now(),
    sources.cli_config,
    sources.route_tier,
    sources.route_tier_rank,
    sources.latency_class,
    sources.latency_rank,
    sources.reasoning_control,
    sources.task_affinities,
    sources.benchmark_profile,
    sources.cap_language_high,
    sources.cap_analysis_architecture_research,
    sources.cap_build_high,
    sources.cap_review,
    sources.cap_tool_use,
    sources.cap_build_med,
    sources.cap_language_low,
    sources.cap_build_low,
    sources.cap_research_fan,
    sources.cap_image
  FROM needs
  JOIN sources
    ON sources.provider_slug = needs.provider_slug
   AND sources.model_slug = needs.model_slug
ON CONFLICT (candidate_ref) DO NOTHING;

-- Migration 378 made provider_transport_admissions the hard authority for
-- task_type_routing transports. Some scratch/live beta databases can still
-- carry stale rows inserted before that trigger (for example direct DeepSeek
-- with transport_type='CLI'). Clean them before the candidate_ref UPDATE below,
-- because even a metadata-only UPDATE must satisfy the trigger.
DELETE FROM task_type_routing AS r
 WHERE NOT EXISTS (
     SELECT 1
       FROM provider_transport_admissions AS admission
      WHERE admission.provider_slug = r.provider_slug
        AND admission.status = 'active'
        AND (
            (r.transport_type = 'API' AND admission.transport_kind = 'http')
            OR (r.transport_type = 'CLI' AND admission.transport_kind = 'cli')
        )
 );

UPDATE task_type_routing
   SET host_provider_slug = COALESCE(host_provider_slug, ''),
       variant = COALESCE(variant, ''),
       effort_slug = COALESCE(effort_slug, '');

WITH selected_candidate AS (
    SELECT DISTINCT ON (
        r.task_type,
        r.sub_task_type,
        r.provider_slug,
        r.model_slug,
        r.transport_type
    )
        r.task_type,
        r.sub_task_type,
        r.provider_slug,
        r.model_slug,
        r.transport_type,
        c.candidate_ref,
        c.host_provider_slug,
        c.variant,
        c.effort_slug
      FROM task_type_routing AS r
      JOIN provider_model_candidates AS c
        ON c.provider_slug = r.provider_slug
       AND c.model_slug = r.model_slug
       AND c.transport_type = r.transport_type
       AND c.status = 'active'
       AND c.effective_from <= now()
       AND (c.effective_to IS NULL OR c.effective_to > now())
     ORDER BY
        r.task_type,
        r.sub_task_type,
        r.provider_slug,
        r.model_slug,
        r.transport_type,
        CASE WHEN c.variant = '' THEN 0 ELSE 1 END,
        CASE WHEN c.effort_slug = '' THEN 0 ELSE 1 END,
        c.priority ASC,
        c.created_at DESC
)
UPDATE task_type_routing AS r
   SET candidate_ref = COALESCE(r.candidate_ref, selected_candidate.candidate_ref),
       host_provider_slug = selected_candidate.host_provider_slug,
       variant = selected_candidate.variant,
       effort_slug = selected_candidate.effort_slug
  FROM selected_candidate
 WHERE r.task_type = selected_candidate.task_type
   AND r.sub_task_type = selected_candidate.sub_task_type
   AND r.provider_slug = selected_candidate.provider_slug
   AND r.model_slug = selected_candidate.model_slug
   AND r.transport_type = selected_candidate.transport_type
   AND (
       r.candidate_ref IS NULL
       OR r.host_provider_slug IS DISTINCT FROM selected_candidate.host_provider_slug
       OR r.variant IS DISTINCT FROM selected_candidate.variant
       OR r.effort_slug IS DISTINCT FROM selected_candidate.effort_slug
   );

-- Existing bad values should not block the FK; they become visible in the
-- orphan view and stay non-runnable until repaired.
UPDATE task_type_routing AS r
   SET candidate_ref = NULL
 WHERE candidate_ref IS NOT NULL
   AND NOT EXISTS (
       SELECT 1
         FROM provider_model_candidates AS c
        WHERE c.candidate_ref = r.candidate_ref
   );

ALTER TABLE provider_model_candidates
    ALTER COLUMN transport_type SET DEFAULT 'API',
    ALTER COLUMN transport_type SET NOT NULL,
    ALTER COLUMN host_provider_slug SET DEFAULT '',
    ALTER COLUMN host_provider_slug SET NOT NULL,
    ALTER COLUMN variant SET DEFAULT '',
    ALTER COLUMN variant SET NOT NULL,
    ALTER COLUMN effort_slug SET DEFAULT '',
    ALTER COLUMN effort_slug SET NOT NULL;

ALTER TABLE task_type_routing
    ALTER COLUMN host_provider_slug SET DEFAULT '',
    ALTER COLUMN host_provider_slug SET NOT NULL,
    ALTER COLUMN variant SET DEFAULT '',
    ALTER COLUMN variant SET NOT NULL,
    ALTER COLUMN effort_slug SET DEFAULT '',
    ALTER COLUMN effort_slug SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'provider_model_candidates'::regclass
           AND conname = 'provider_model_candidates_transport_type_check'
    ) THEN
        ALTER TABLE provider_model_candidates
            ADD CONSTRAINT provider_model_candidates_transport_type_check
            CHECK (transport_type IN ('CLI', 'API'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'task_type_routing'::regclass
           AND conname = 'task_type_routing_candidate_ref_nonblank'
    ) THEN
        ALTER TABLE task_type_routing
            ADD CONSTRAINT task_type_routing_candidate_ref_nonblank
            CHECK (candidate_ref IS NULL OR btrim(candidate_ref) <> '');
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'task_type_routing'::regclass
           AND conname = 'task_type_routing_candidate_ref_fkey'
    ) THEN
        ALTER TABLE task_type_routing
            ADD CONSTRAINT task_type_routing_candidate_ref_fkey
            FOREIGN KEY (candidate_ref)
            REFERENCES provider_model_candidates (candidate_ref)
            ON UPDATE CASCADE
            ON DELETE RESTRICT;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS provider_model_candidates_identity_lookup_idx
    ON provider_model_candidates (
        transport_type,
        provider_slug,
        host_provider_slug,
        model_slug,
        variant,
        effort_slug
    );

CREATE INDEX IF NOT EXISTS task_type_routing_candidate_ref_idx
    ON task_type_routing (candidate_ref)
    WHERE candidate_ref IS NOT NULL;

CREATE OR REPLACE FUNCTION validate_task_type_routing_candidate_ref()
RETURNS trigger AS $$
DECLARE
    candidate RECORD;
BEGIN
    IF NEW.candidate_ref IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT provider_slug, model_slug, transport_type, host_provider_slug, variant, effort_slug
      INTO candidate
      FROM provider_model_candidates
     WHERE candidate_ref = NEW.candidate_ref;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'task_type_routing.candidate_ref % does not exist',
            NEW.candidate_ref
            USING ERRCODE = '23503';
    END IF;

    IF candidate.provider_slug <> NEW.provider_slug
       OR candidate.model_slug <> NEW.model_slug
       OR candidate.transport_type <> NEW.transport_type THEN
        RAISE EXCEPTION
            'task_type_routing.candidate_ref % mismatches route provider/model/transport (%/%/%)',
            NEW.candidate_ref,
            NEW.provider_slug,
            NEW.model_slug,
            NEW.transport_type
            USING ERRCODE = '23514';
    END IF;

    NEW.host_provider_slug := candidate.host_provider_slug;
    NEW.variant := candidate.variant;
    NEW.effort_slug := candidate.effort_slug;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS task_type_routing_candidate_ref_identity_check ON task_type_routing;
CREATE TRIGGER task_type_routing_candidate_ref_identity_check
    BEFORE INSERT OR UPDATE OF candidate_ref, provider_slug, model_slug, transport_type
    ON task_type_routing
    FOR EACH ROW
    EXECUTE FUNCTION validate_task_type_routing_candidate_ref();

DROP VIEW IF EXISTS task_type_routing_admission_audit;
DROP VIEW IF EXISTS task_type_routing_orphans;

CREATE OR REPLACE VIEW task_type_routing_orphans AS
SELECT
    r.task_type,
    r.sub_task_type,
    r.rank,
    r.provider_slug,
    r.host_provider_slug,
    r.model_slug,
    r.transport_type,
    r.variant,
    r.effort_slug,
    r.candidate_ref,
    r.permitted,
    CASE
        WHEN r.candidate_ref IS NULL THEN 'task_type_routing.candidate_ref_missing'
        WHEN c.candidate_ref IS NULL THEN 'provider_model_candidate.missing'
        WHEN c.status <> 'active' THEN 'provider_model_candidate.inactive'
        WHEN c.effective_from > now()
          OR (c.effective_to IS NOT NULL AND c.effective_to <= now())
            THEN 'provider_model_candidate.outside_effective_window'
        WHEN c.provider_slug <> r.provider_slug
          OR c.model_slug <> r.model_slug
          OR c.transport_type <> r.transport_type
            THEN 'task_type_routing.candidate_ref_identity_mismatch'
        WHEN transport.provider_transport_admission_id IS NULL THEN 'transport.not_admitted'
        WHEN transport.admitted_by_policy IS NOT TRUE THEN 'transport.policy_denied'
        WHEN transport.status <> 'active' THEN 'transport.inactive'
        ELSE 'unknown'
    END AS orphan_reason
  FROM task_type_routing AS r
  LEFT JOIN provider_model_candidates AS c
    ON c.candidate_ref = r.candidate_ref
  LEFT JOIN provider_transport_admissions AS transport
    ON transport.provider_slug = r.provider_slug
   AND transport.adapter_type = CASE
       WHEN r.transport_type = 'CLI' THEN 'cli_llm'
       WHEN r.transport_type = 'API' THEN 'llm_task'
       ELSE NULL
   END
 WHERE r.candidate_ref IS NULL
    OR c.candidate_ref IS NULL
    OR c.status <> 'active'
    OR c.effective_from > now()
    OR (c.effective_to IS NOT NULL AND c.effective_to <= now())
    OR c.provider_slug <> r.provider_slug
    OR c.model_slug <> r.model_slug
    OR c.transport_type <> r.transport_type
    OR transport.provider_transport_admission_id IS NULL
    OR transport.admitted_by_policy IS NOT TRUE
    OR transport.status <> 'active';

COMMENT ON VIEW task_type_routing_orphans IS
    'Routes that cannot materialize through candidate_ref authority. Non-empty rows explain why task_type_routing will not project as runnable.';

CREATE OR REPLACE FUNCTION refresh_private_provider_job_catalog(
    p_runtime_profile_ref text
)
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    IF p_runtime_profile_ref IS NULL OR btrim(p_runtime_profile_ref) = '' THEN
        RAISE EXCEPTION 'runtime_profile_ref must be a non-empty string';
    END IF;

    DELETE FROM private_provider_job_catalog
    WHERE runtime_profile_ref = btrim(p_runtime_profile_ref);

    WITH economics AS (
        SELECT
            profile.provider_slug,
            entry.key AS adapter_type,
            CASE
                WHEN entry.key = 'cli_llm' THEN 'CLI'
                WHEN entry.key = 'llm_task' THEN 'API'
                ELSE 'API'
            END AS transport_type,
            COALESCE(NULLIF(entry.value ->> 'billing_mode', ''), 'unspecified') AS cost_structure,
            entry.value AS cost_metadata
        FROM provider_cli_profiles AS profile
        CROSS JOIN LATERAL jsonb_each(
            COALESCE(profile.adapter_economics, '{}'::jsonb)
        ) AS entry(key, value)
        WHERE profile.status = 'active'
          AND entry.key IN ('cli_llm', 'llm_task')
    ),
    matrix_rows AS (
        SELECT DISTINCT ON (
            btrim(p_runtime_profile_ref),
            route.task_type,
            economics.adapter_type,
            route.provider_slug,
            route.model_slug
        )
            btrim(p_runtime_profile_ref) AS runtime_profile_ref,
            route.task_type AS job_type,
            economics.transport_type,
            economics.adapter_type,
            route.provider_slug,
            route.model_slug,
            COALESCE(
                NULLIF(active_candidates.default_parameters ->> 'model_version', ''),
                NULLIF(active_candidates.default_parameters ->> 'version', ''),
                route.model_slug
            ) AS model_version,
            economics.cost_structure,
            economics.cost_metadata,
            CASE
                WHEN route.permitted IS NOT TRUE THEN 'disabled'
                WHEN route.candidate_ref IS NULL THEN 'disabled'
                WHEN active_candidates.candidate_ref IS NULL THEN 'disabled'
                WHEN route_window.eligibility_status = 'rejected' THEN 'disabled'
                WHEN transport.provider_transport_admission_id IS NULL THEN 'disabled'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'disabled'
                WHEN transport.status <> 'active' THEN 'disabled'
                WHEN admitted.candidate_ref IS NOT NULL THEN 'available'
                WHEN route_window.eligibility_status = 'eligible' THEN 'available'
                ELSE 'disabled'
            END AS availability_state,
            CASE
                WHEN route.permitted IS NOT TRUE THEN 'task_type_routing.denied'
                WHEN route.candidate_ref IS NULL THEN 'task_type_routing.candidate_ref_missing'
                WHEN active_candidates.candidate_ref IS NULL THEN 'provider_model_candidate.missing'
                WHEN route_window.eligibility_status = 'rejected'
                    THEN COALESCE(NULLIF(route_window.reason_code, ''), 'task_route_eligibility.rejected')
                WHEN transport.provider_transport_admission_id IS NULL THEN 'transport.not_admitted'
                WHEN transport.admitted_by_policy IS NOT TRUE THEN 'transport.policy_denied'
                WHEN transport.status <> 'active' THEN 'transport.inactive'
                WHEN admitted.candidate_ref IS NOT NULL THEN 'available'
                WHEN route_window.eligibility_status = 'eligible'
                    THEN COALESCE(NULLIF(route_window.reason_code, ''), 'task_route_eligibility.eligible')
                ELSE 'runtime_profile.not_admitted'
            END AS reason_code,
            active_candidates.candidate_ref,
            active_candidates.provider_ref,
            (
                jsonb_build_array(
                    'table.task_type_routing',
                    'table.provider_model_candidates',
                    'table.runtime_profile_admitted_routes',
                    'table.provider_transport_admissions',
                    'table.provider_cli_profiles',
                    'table.private_provider_transport_control_policy'
                )
                || CASE
                    WHEN route_window.task_route_eligibility_id IS NULL THEN '[]'::jsonb
                    ELSE jsonb_build_array('table.task_type_route_eligibility')
                END
            ) AS source_refs
        FROM task_type_routing AS route
        JOIN economics
          ON economics.provider_slug = route.provider_slug
         AND economics.transport_type = route.transport_type
        LEFT JOIN LATERAL (
            SELECT
                candidate_ref,
                provider_ref,
                provider_slug,
                model_slug,
                transport_type,
                host_provider_slug,
                variant,
                effort_slug,
                default_parameters,
                priority,
                created_at
            FROM provider_model_candidates AS c
            WHERE c.status = 'active'
              AND c.effective_from <= now()
              AND (c.effective_to IS NULL OR c.effective_to > now())
              AND (
                  (
                      route.candidate_ref IS NOT NULL
                      AND c.candidate_ref = route.candidate_ref
                      AND c.provider_slug = route.provider_slug
                      AND c.model_slug = route.model_slug
                      AND c.transport_type = route.transport_type
                  )
                  OR (
                      route.candidate_ref IS NULL
                      AND c.provider_slug = route.provider_slug
                      AND c.model_slug = route.model_slug
                      AND c.transport_type = route.transport_type
                      AND c.host_provider_slug IS NOT DISTINCT FROM route.host_provider_slug
                      AND c.variant IS NOT DISTINCT FROM route.variant
                      AND c.effort_slug IS NOT DISTINCT FROM route.effort_slug
                  )
              )
            ORDER BY
                CASE WHEN c.candidate_ref = route.candidate_ref THEN 0 ELSE 1 END,
                CASE WHEN c.variant = '' THEN 0 ELSE 1 END,
                CASE WHEN c.effort_slug = '' THEN 0 ELSE 1 END,
                c.priority ASC,
                c.created_at DESC
            LIMIT 1
        ) AS active_candidates ON TRUE
        LEFT JOIN runtime_profile_admitted_routes AS admitted
          ON admitted.runtime_profile_ref = btrim(p_runtime_profile_ref)
         AND admitted.candidate_ref = active_candidates.candidate_ref
         AND admitted.eligibility_status = 'admitted'
        LEFT JOIN provider_transport_admissions AS transport
          ON transport.provider_slug = route.provider_slug
         AND transport.adapter_type = economics.adapter_type
        LEFT JOIN LATERAL (
            SELECT
                eligibility.task_route_eligibility_id,
                eligibility.eligibility_status,
                eligibility.reason_code
            FROM task_type_route_eligibility AS eligibility
            WHERE eligibility.provider_slug = route.provider_slug
              AND (eligibility.task_type = route.task_type OR eligibility.task_type IS NULL)
              AND (eligibility.model_slug = route.model_slug OR eligibility.model_slug IS NULL)
              AND eligibility.effective_from <= now()
              AND (eligibility.effective_to IS NULL OR eligibility.effective_to > now())
            ORDER BY
                CASE WHEN eligibility.task_type = route.task_type THEN 1 ELSE 0 END DESC,
                CASE WHEN eligibility.model_slug = route.model_slug THEN 1 ELSE 0 END DESC,
                eligibility.effective_from DESC,
                eligibility.decision_ref DESC,
                eligibility.task_route_eligibility_id DESC
            LIMIT 1
        ) AS route_window ON TRUE
        ORDER BY
            btrim(p_runtime_profile_ref),
            route.task_type,
            economics.adapter_type,
            route.provider_slug,
            route.model_slug,
            CASE WHEN route.sub_task_type = '*' THEN 1 ELSE 0 END,
            route.rank ASC
    )
    INSERT INTO private_provider_job_catalog (
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug,
        model_version,
        cost_structure,
        cost_metadata,
        availability_state,
        reason_code,
        candidate_ref,
        provider_ref,
        source_refs,
        projected_at
    )
    SELECT
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug,
        model_version,
        cost_structure,
        cost_metadata,
        availability_state,
        reason_code,
        candidate_ref,
        provider_ref,
        source_refs,
        now()
    FROM matrix_rows
    ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
    DO UPDATE SET
        transport_type = EXCLUDED.transport_type,
        model_version = EXCLUDED.model_version,
        cost_structure = EXCLUDED.cost_structure,
        cost_metadata = EXCLUDED.cost_metadata,
        availability_state = EXCLUDED.availability_state,
        reason_code = EXCLUDED.reason_code,
        candidate_ref = EXCLUDED.candidate_ref,
        provider_ref = EXCLUDED.provider_ref,
        source_refs = EXCLUDED.source_refs,
        projected_at = EXCLUDED.projected_at;
END
$fn$;

CREATE OR REPLACE VIEW task_type_routing_admission_audit AS
SELECT
    route.task_type,
    route.sub_task_type,
    route.rank,
    route.provider_slug,
    route.model_slug,
    route.transport_type,
    route.candidate_ref,
    rp.runtime_profile_ref,
    catalog.availability_state,
    catalog.reason_code AS catalog_reason_code,
    CASE
        WHEN orphan.orphan_reason IS NOT NULL THEN orphan.orphan_reason
        WHEN catalog.runtime_profile_ref IS NULL THEN 'route_not_in_resolver_gate'
        WHEN catalog.availability_state = 'available' THEN 'admitted'
        ELSE catalog.reason_code
    END AS admission_status
FROM task_type_routing AS route
CROSS JOIN registry_native_runtime_profile_authority AS rp
LEFT JOIN private_provider_job_catalog AS catalog
  ON catalog.runtime_profile_ref = rp.runtime_profile_ref
 AND catalog.job_type = route.task_type
 AND catalog.transport_type = route.transport_type
 AND catalog.provider_slug = route.provider_slug
 AND catalog.model_slug = route.model_slug
 AND (
     route.candidate_ref IS NULL
     OR catalog.candidate_ref IS NOT DISTINCT FROM route.candidate_ref
 )
LEFT JOIN task_type_routing_orphans AS orphan
  ON orphan.task_type = route.task_type
 AND orphan.sub_task_type = route.sub_task_type
 AND orphan.provider_slug = route.provider_slug
 AND orphan.model_slug = route.model_slug
 AND orphan.transport_type = route.transport_type
 AND orphan.candidate_ref IS NOT DISTINCT FROM route.candidate_ref
WHERE route.permitted IS TRUE
ORDER BY route.task_type, route.rank, rp.runtime_profile_ref;

COMMENT ON VIEW task_type_routing_admission_audit IS
    'Candidate-ref aware audit for task_type_routing rows. Shows whether each route materializes into private_provider_job_catalog and names candidate/orphan reasons when it does not.';

-- Retire the unregistered Phase-B schema command if a local catalog had
-- learned it. Structural schema authority is this migration now, not a
-- callable operation that can drift away from the manifest.
DELETE FROM operation_catalog_registry
 WHERE operation_name = 'candidate_identity_phase_b'
    OR operation_ref = 'candidate_identity_phase_b'
    OR http_path = '/api/candidate_identity_phase_b'
    OR input_model_ref LIKE '%candidate_identity_phase_b%'
    OR handler_ref LIKE '%candidate_identity_phase_b%';

DO $$
DECLARE
    profile_ref text;
BEGIN
    FOR profile_ref IN
        SELECT runtime_profile_ref
        FROM registry_native_runtime_profile_authority
    LOOP
        PERFORM refresh_private_provider_job_catalog(profile_ref);
        PERFORM refresh_private_provider_control_plane_snapshot(profile_ref);
    END LOOP;
END $$;

COMMIT;
