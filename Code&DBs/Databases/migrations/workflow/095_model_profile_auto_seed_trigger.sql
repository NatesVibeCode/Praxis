-- Migration 095: Auto-seed model_profiles and bindings for new provider_model_candidates
--
-- Installs a trigger that fires after every INSERT into provider_model_candidates and:
--   1. Seeds a model_profiles row if no profile yet exists for that provider+model pair.
--   2. Seeds a model_profile_candidate_bindings row linking the candidate to its profile.
--
-- ON CONFLICT DO NOTHING — hand-crafted profiles and bindings always take precedence.
-- The trigger uses SELECT … WHERE NOT EXISTS so it is a true no-op when a profile
-- already exists for the provider+model pair (avoids polluting the profile table with
-- auto-generated duplicates).
--
-- Backfill at the bottom of this file seeds any existing candidates that were
-- onboarded before this trigger existed.

BEGIN;

-- -----------------------------------------------------------------------
-- 1. Trigger function
-- -----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION seed_model_profile_for_candidate()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v_profile_id  text;
    v_binding_id  text;
    v_routing_pol jsonb;
    v_defaults    jsonb;
BEGIN
    v_profile_id  := 'model_profile.' || NEW.provider_slug || '.' || NEW.model_slug;
    v_binding_id  := 'binding.auto.' || NEW.candidate_ref;

    v_routing_pol := jsonb_build_object(
        'selection',     'direct_candidate',
        'route_tier',    NEW.route_tier,
        'latency_class', NEW.latency_class,
        'seeded_from',   'provider_model_candidates'
    );

    -- Merge provider/model slugs into default_parameters for AgentRegistry resolution.
    v_defaults := NEW.default_parameters
        || jsonb_build_object(
               'provider_slug', NEW.provider_slug,
               'model_slug',    NEW.model_slug
           );

    -- Seed profile only when no profile for this provider+model exists yet.
    -- Existing hand-crafted profiles (potentially with different IDs) win.
    INSERT INTO model_profiles (
        model_profile_id,
        profile_name,
        provider_name,
        model_name,
        schema_version,
        status,
        budget_policy,
        routing_policy,
        default_parameters,
        effective_from,
        created_at
    )
    SELECT
        v_profile_id,
        NEW.provider_slug || '.' || NEW.model_slug,
        NEW.provider_slug,
        NEW.model_slug,
        1,
        NEW.status,
        '{}'::jsonb,
        v_routing_pol,
        v_defaults,
        NEW.effective_from,
        NEW.created_at
    WHERE NOT EXISTS (
        SELECT 1 FROM model_profiles mp
        WHERE mp.provider_name = NEW.provider_slug
          AND mp.model_name    = NEW.model_slug
    )
    ON CONFLICT (model_profile_id) DO NOTHING;

    -- Bind the candidate to its profile (prefer older/hand-crafted profile if multiple exist).
    INSERT INTO model_profile_candidate_bindings (
        model_profile_candidate_binding_id,
        model_profile_id,
        candidate_ref,
        binding_role,
        position_index,
        effective_from,
        created_at
    )
    SELECT
        v_binding_id,
        mp.model_profile_id,
        NEW.candidate_ref,
        'primary',
        0,
        NEW.effective_from,
        NEW.created_at
    FROM model_profiles mp
    WHERE mp.provider_name = NEW.provider_slug
      AND mp.model_name    = NEW.model_slug
    ORDER BY mp.created_at ASC
    LIMIT 1
    ON CONFLICT ON CONSTRAINT model_profile_candidate_bindings_unique_window DO NOTHING;

    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION seed_model_profile_for_candidate() IS
    'After-insert trigger on provider_model_candidates. Seeds model_profiles (when none '
    'exists for the provider+model pair) and a primary binding. ON CONFLICT DO NOTHING '
    'preserves hand-crafted profiles and bindings.';

-- -----------------------------------------------------------------------
-- 2. Trigger
-- -----------------------------------------------------------------------

CREATE OR REPLACE TRIGGER provider_model_candidates_profile_seed
    AFTER INSERT ON provider_model_candidates
    FOR EACH ROW
    EXECUTE FUNCTION seed_model_profile_for_candidate();

-- -----------------------------------------------------------------------
-- 3. Backfill — seed profiles for candidates with no matching profile yet
-- -----------------------------------------------------------------------

INSERT INTO model_profiles (
    model_profile_id,
    profile_name,
    provider_name,
    model_name,
    schema_version,
    status,
    budget_policy,
    routing_policy,
    default_parameters,
    effective_from,
    created_at
)
SELECT
    'model_profile.' || c.provider_slug || '.' || c.model_slug,
    c.provider_slug || '.' || c.model_slug,
    c.provider_slug,
    c.model_slug,
    1,
    c.status,
    '{}'::jsonb,
    jsonb_build_object(
        'selection',     'direct_candidate',
        'route_tier',    c.route_tier,
        'latency_class', c.latency_class,
        'seeded_from',   'provider_model_candidates'
    ),
    c.default_parameters || jsonb_build_object(
        'provider_slug', c.provider_slug,
        'model_slug',    c.model_slug
    ),
    c.effective_from,
    c.created_at
FROM provider_model_candidates c
WHERE NOT EXISTS (
    SELECT 1 FROM model_profiles mp
    WHERE mp.provider_name = c.provider_slug
      AND mp.model_name    = c.model_slug
)
ON CONFLICT (model_profile_id) DO NOTHING;

-- -----------------------------------------------------------------------
-- 4. Backfill — seed bindings for all unbound candidates
--    After step 3 every candidate has at least one matching profile.
--    Prefer the oldest profile (hand-crafted entries predate auto-seeded ones).
-- -----------------------------------------------------------------------

INSERT INTO model_profile_candidate_bindings (
    model_profile_candidate_binding_id,
    model_profile_id,
    candidate_ref,
    binding_role,
    position_index,
    effective_from,
    created_at
)
SELECT DISTINCT ON (c.candidate_ref)
    'binding.auto.' || c.candidate_ref,
    mp.model_profile_id,
    c.candidate_ref,
    'primary',
    0,
    c.effective_from,
    c.created_at
FROM provider_model_candidates c
JOIN model_profiles mp
    ON  mp.provider_name = c.provider_slug
    AND mp.model_name    = c.model_slug
WHERE NOT EXISTS (
    SELECT 1 FROM model_profile_candidate_bindings b
    WHERE b.candidate_ref = c.candidate_ref
)
ORDER BY c.candidate_ref, mp.created_at ASC
ON CONFLICT ON CONSTRAINT model_profile_candidate_bindings_unique_window DO NOTHING;

COMMIT;
