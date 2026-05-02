-- Migration 417: repair route rows surfaced by candidate_ref authority.
--
-- Migration 416 made task_type_routing.candidate_ref the enforced route edge.
-- The live beta DB then surfaced a small set of legacy routes whose model slugs
-- were display aliases rather than exact candidate slugs. Bind the active
-- Gemini OpenRouter routes to their exact candidate and close GPT/OpenAI routes
-- that should not be runnable while GPT usage is intentionally off.

BEGIN;

UPDATE provider_model_candidates
   SET host_provider_slug = split_part(model_slug, '/', 1)
 WHERE provider_slug = 'openrouter'
   AND model_slug LIKE '%/%'
   AND (host_provider_slug IS NULL OR host_provider_slug IN ('', 'openrouter'));

WITH active_alias_candidates AS (
    SELECT DISTINCT ON (
        route.task_type,
        route.sub_task_type,
        route.provider_slug,
        route.model_slug,
        route.transport_type
    )
        route.task_type,
        route.sub_task_type,
        route.provider_slug,
        route.model_slug AS route_model_slug,
        route.transport_type,
        candidate.model_slug AS candidate_model_slug
    FROM task_type_routing AS route
    JOIN provider_model_candidates AS candidate
      ON candidate.provider_slug = route.provider_slug
     AND candidate.transport_type = route.transport_type
     AND candidate.status = 'active'
     AND candidate.effective_from <= now()
     AND (candidate.effective_to IS NULL OR candidate.effective_to > now())
     AND candidate.model_slug = 'google/' || route.model_slug
    WHERE route.candidate_ref IS NULL
      AND route.provider_slug = 'openrouter'
      AND route.permitted IS TRUE
    ORDER BY
        route.task_type,
        route.sub_task_type,
        route.provider_slug,
        route.model_slug,
        route.transport_type,
        candidate.priority ASC,
        candidate.created_at DESC
)
UPDATE task_type_routing AS route
   SET permitted = false
  FROM active_alias_candidates
 WHERE route.task_type = active_alias_candidates.task_type
   AND route.sub_task_type = active_alias_candidates.sub_task_type
   AND route.provider_slug = active_alias_candidates.provider_slug
   AND route.model_slug = active_alias_candidates.route_model_slug
   AND route.transport_type = active_alias_candidates.transport_type
   AND EXISTS (
       SELECT 1
         FROM task_type_routing AS canonical
        WHERE canonical.task_type = route.task_type
          AND canonical.sub_task_type = route.sub_task_type
          AND canonical.provider_slug = route.provider_slug
          AND canonical.model_slug = active_alias_candidates.candidate_model_slug
   );

WITH active_alias_candidates AS (
    SELECT DISTINCT ON (
        route.task_type,
        route.sub_task_type,
        route.provider_slug,
        route.model_slug,
        route.transport_type
    )
        route.task_type,
        route.sub_task_type,
        route.provider_slug,
        route.model_slug AS route_model_slug,
        route.transport_type,
        candidate.candidate_ref,
        candidate.model_slug AS candidate_model_slug,
        candidate.host_provider_slug,
        candidate.variant,
        candidate.effort_slug
    FROM task_type_routing AS route
    JOIN provider_model_candidates AS candidate
      ON candidate.provider_slug = route.provider_slug
     AND candidate.transport_type = route.transport_type
     AND candidate.status = 'active'
     AND candidate.effective_from <= now()
     AND (candidate.effective_to IS NULL OR candidate.effective_to > now())
     AND (
         candidate.model_slug = route.model_slug
         OR candidate.model_slug = 'google/' || route.model_slug
     )
    WHERE route.candidate_ref IS NULL
      AND route.provider_slug = 'openrouter'
      AND route.permitted IS TRUE
    ORDER BY
        route.task_type,
        route.sub_task_type,
        route.provider_slug,
        route.model_slug,
        route.transport_type,
        CASE WHEN candidate.model_slug = route.model_slug THEN 0 ELSE 1 END,
        candidate.priority ASC,
        candidate.created_at DESC
)
UPDATE task_type_routing AS route
   SET model_slug = active_alias_candidates.candidate_model_slug,
       candidate_ref = active_alias_candidates.candidate_ref,
       host_provider_slug = active_alias_candidates.host_provider_slug,
       variant = active_alias_candidates.variant,
       effort_slug = active_alias_candidates.effort_slug
  FROM active_alias_candidates
 WHERE route.task_type = active_alias_candidates.task_type
   AND route.sub_task_type = active_alias_candidates.sub_task_type
   AND route.provider_slug = active_alias_candidates.provider_slug
   AND route.model_slug = active_alias_candidates.route_model_slug
   AND route.transport_type = active_alias_candidates.transport_type
   AND route.candidate_ref IS NULL;

-- User-directed usage brake: GPT/OpenAI routes must not remain eligible just
-- because they are present in task_type_routing. Paid-model access control and
-- hard-off denials remain the backend authority; this closes stale route rows.
UPDATE task_type_routing
   SET permitted = false
 WHERE permitted IS TRUE
   AND provider_slug IN ('openai', 'anthropic');

UPDATE task_type_routing
   SET permitted = false
 WHERE permitted IS TRUE
   AND provider_slug = 'openrouter'
   AND (
       lower(model_slug) LIKE '%gpt%'
       OR lower(model_slug) LIKE 'openai/%'
       OR lower(host_provider_slug) = 'openai'
   );

DROP VIEW IF EXISTS task_type_routing_admission_audit;
DROP VIEW IF EXISTS task_type_routing_orphans;

CREATE VIEW task_type_routing_orphans AS
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
 WHERE r.permitted IS TRUE
   AND (
       r.candidate_ref IS NULL
       OR c.candidate_ref IS NULL
       OR c.status <> 'active'
       OR c.effective_from > now()
       OR (c.effective_to IS NOT NULL AND c.effective_to <= now())
       OR c.provider_slug <> r.provider_slug
       OR c.model_slug <> r.model_slug
       OR c.transport_type <> r.transport_type
       OR transport.provider_transport_admission_id IS NULL
       OR transport.admitted_by_policy IS NOT TRUE
       OR transport.status <> 'active'
   );

COMMENT ON VIEW task_type_routing_orphans IS
    'Permitted routes that cannot materialize through candidate_ref authority. Disabled routes are not orphans; they are intentionally non-runnable.';

CREATE VIEW task_type_routing_admission_audit AS
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
        WHEN route.permitted IS NOT TRUE THEN 'task_type_routing.denied'
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
ORDER BY route.task_type, route.rank, rp.runtime_profile_ref;

COMMENT ON VIEW task_type_routing_admission_audit IS
    'Candidate-ref aware audit for task_type_routing rows. Shows whether each route materializes into private_provider_job_catalog and names candidate/orphan reasons when it does not.';

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
