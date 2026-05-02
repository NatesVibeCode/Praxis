-- Migration 420: Keep provider reasoning-effort matrix in sync with candidates.
--
-- Migration 327 seeded provider_reasoning_effort_matrix from the active
-- provider_model_candidates that existed at that moment. Later candidate
-- registration must not create a hidden second authority where task routing can
-- see a model but effort routing cannot. Candidate authority now fills missing
-- generic effort rows at write time; explicit/custom matrix rows still win.

BEGIN;

CREATE OR REPLACE FUNCTION refresh_provider_reasoning_effort_matrix_for_candidate(
    p_candidate_ref text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
    WITH effort_axis(effort_slug, cost_multiplier, latency_multiplier, quality_bias) AS (
        VALUES
            ('instant', 0.75::numeric, 0.50::numeric, -0.15::numeric),
            ('low',     0.90::numeric, 0.75::numeric, -0.05::numeric),
            ('medium',  1.00::numeric, 1.00::numeric,  0.00::numeric),
            ('high',    1.35::numeric, 1.50::numeric,  0.15::numeric),
            ('max',     1.75::numeric, 2.25::numeric,  0.25::numeric)
    ),
    active_candidates AS (
        SELECT DISTINCT ON (provider_slug, model_slug, transport_type)
               provider_slug,
               model_slug,
               transport_type
          FROM provider_model_candidates
         WHERE status = 'active'
           AND effective_from <= now()
           AND (effective_to IS NULL OR effective_to > now())
           AND (p_candidate_ref IS NULL OR candidate_ref = p_candidate_ref)
         ORDER BY provider_slug, model_slug, transport_type, priority ASC, created_at DESC
    )
    INSERT INTO provider_reasoning_effort_matrix (
        effort_matrix_ref,
        provider_slug,
        model_slug,
        transport_type,
        effort_slug,
        supported,
        provider_payload,
        cost_multiplier,
        latency_multiplier,
        quality_bias,
        decision_ref,
        metadata
    )
    SELECT
        'reasoning_effort.'
            || candidate.provider_slug || '.'
            || regexp_replace(candidate.model_slug, '[^a-zA-Z0-9]+', '-', 'g') || '.'
            || lower(candidate.transport_type) || '.'
            || effort.effort_slug AS effort_matrix_ref,
        candidate.provider_slug,
        candidate.model_slug,
        lower(candidate.transport_type) AS transport_type,
        effort.effort_slug,
        true AS supported,
        CASE
            WHEN candidate.provider_slug = 'openai' THEN
                jsonb_build_object(
                    'provider', 'openai',
                    'reasoning_effort', CASE effort.effort_slug
                        WHEN 'instant' THEN 'low'
                        WHEN 'low' THEN 'low'
                        WHEN 'medium' THEN 'medium'
                        ELSE 'high'
                    END
                )
            WHEN candidate.provider_slug = 'anthropic' THEN
                jsonb_build_object(
                    'provider', 'anthropic',
                    'thinking', jsonb_build_object(
                        'type', CASE
                            WHEN effort.effort_slug IN ('instant', 'low') THEN 'disabled'
                            ELSE 'enabled'
                        END,
                        'budget_tokens', CASE effort.effort_slug
                            WHEN 'instant' THEN 0
                            WHEN 'low' THEN 0
                            WHEN 'medium' THEN 4096
                            WHEN 'high' THEN 12000
                            ELSE 24000
                        END
                    )
                )
            WHEN candidate.provider_slug = 'google' THEN
                jsonb_build_object(
                    'provider', 'google',
                    'thinking_budget', CASE effort.effort_slug
                        WHEN 'instant' THEN 0
                        WHEN 'low' THEN 1024
                        WHEN 'medium' THEN 4096
                        WHEN 'high' THEN 12000
                        ELSE 24000
                    END
                )
            ELSE
                jsonb_build_object(
                    'provider', candidate.provider_slug,
                    'internal_effort_slug', effort.effort_slug
                )
        END AS provider_payload,
        effort.cost_multiplier,
        effort.latency_multiplier,
        effort.quality_bias,
        'operator_decision.architecture_policy.provider_routing.reasoning_effort_is_first_class_route_dimension',
        jsonb_build_object(
            'source', 'migration.420_provider_reasoning_effort_candidate_projection',
            'projection', 'provider_model_candidates.active'
        )
      FROM active_candidates AS candidate
      CROSS JOIN effort_axis AS effort
    ON CONFLICT (provider_slug, model_slug, transport_type, effort_slug) DO NOTHING;
END
$fn$;

CREATE OR REPLACE FUNCTION fill_provider_reasoning_effort_matrix_from_candidate()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
    PERFORM refresh_provider_reasoning_effort_matrix_for_candidate(NEW.candidate_ref);
    RETURN NEW;
END
$fn$;

DROP TRIGGER IF EXISTS provider_model_candidates_reasoning_effort_matrix_fill
    ON provider_model_candidates;
CREATE TRIGGER provider_model_candidates_reasoning_effort_matrix_fill
    AFTER INSERT OR UPDATE OF provider_slug, model_slug, transport_type, status, effective_from, effective_to
    ON provider_model_candidates
    FOR EACH ROW
    EXECUTE FUNCTION fill_provider_reasoning_effort_matrix_from_candidate();

SELECT refresh_provider_reasoning_effort_matrix_for_candidate(NULL);

COMMIT;
