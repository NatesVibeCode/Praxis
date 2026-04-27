-- Migration 282: admit the compose-picker winners to the API access allowlist.
--
-- Migration 280 promoted the empirical winners in `task_type_routing`:
--   - plan_synthesis    rank-1 -> openrouter/google/gemini-3-flash-preview
--   - plan_fork_author  rank-1 -> openrouter/google/gemini-3-flash-preview
--   - plan_pill_match   rank-1 -> openrouter/openai/gpt-5.4-mini
--
-- But routing rank is only the ORDERING. The ON/OFF gate lives in
-- `private_model_access_control_matrix` (a view derived from the API
-- allowlist + transport policy + denial table). The API transport posture
-- is `deny_unless_allowlisted`, so a rank-1 row that has no matching
-- allowlist row is silently dropped at the JOIN inside
-- `resolve_matrix_gated_route_configs`.
--
-- That's exactly what happened: 280 added the new rank-1 rows but did
-- nothing about the allowlist. Production calls fell through the JOIN to
-- the only admitted row (together/V4-Pro), which loops at temperature=0
-- and returns empty content.
--
-- This migration:
--   (1) admits the picker winners to the allowlist for both active
--       runtime profiles (praxis + scratch_agent),
--   (2) creates `task_type_routing_admission_audit` — a view that surfaces
--       any task_type_routing row whose (provider, model) is NOT admitted by
--       the access matrix. After future routing edits, query this view
--       filtered to `admission_status <> 'admitted'` to confirm the gate
--       agrees with the rank ordering. This is the durable exposure fix
--       for the hidden coupling that broke 280.

BEGIN;

INSERT INTO private_provider_api_job_allowlist
    (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug,
     allowed, reason_code, decision_ref)
VALUES
    ('praxis',        'plan_synthesis',   'llm_task',
     'openrouter',    'google/gemini-3-flash-preview', TRUE,
     'compose_picker.empirical_pick_2026_04_26',
     'decision.2026-04-26.compose-picker-matrix'),
    ('scratch_agent', 'plan_synthesis',   'llm_task',
     'openrouter',    'google/gemini-3-flash-preview', TRUE,
     'compose_picker.empirical_pick_2026_04_26',
     'decision.2026-04-26.compose-picker-matrix'),
    ('praxis',        'plan_fork_author', 'llm_task',
     'openrouter',    'google/gemini-3-flash-preview', TRUE,
     'compose_picker.empirical_pick_2026_04_26',
     'decision.2026-04-26.compose-picker-matrix'),
    ('scratch_agent', 'plan_fork_author', 'llm_task',
     'openrouter',    'google/gemini-3-flash-preview', TRUE,
     'compose_picker.empirical_pick_2026_04_26',
     'decision.2026-04-26.compose-picker-matrix'),
    ('praxis',        'plan_pill_match',  'llm_task',
     'openrouter',    'openai/gpt-5.4-mini',           TRUE,
     'compose_picker.empirical_pick_2026_04_26',
     'decision.2026-04-26.compose-picker-matrix'),
    ('scratch_agent', 'plan_pill_match',  'llm_task',
     'openrouter',    'openai/gpt-5.4-mini',           TRUE,
     'compose_picker.empirical_pick_2026_04_26',
     'decision.2026-04-26.compose-picker-matrix')
ON CONFLICT (runtime_profile_ref, job_type, adapter_type, provider_slug, model_slug)
DO UPDATE SET
    allowed      = EXCLUDED.allowed,
    reason_code  = EXCLUDED.reason_code,
    decision_ref = EXCLUDED.decision_ref,
    updated_at   = NOW();

-- ──────────────────────────────────────────────────────────────────────────
-- Exposure fix: surface the routing-vs-matrix gap as a queryable view.
-- A task_type_routing row only takes effect when the (runtime_profile,
-- task_type, adapter_type=llm_task, provider, model) combination is
-- ALSO admitted in private_model_access_control_matrix with control_state='on'.
-- After authoring a routing change, query this view filtered to
-- `admission_status <> 'admitted'` to catch the hidden gap.
-- ──────────────────────────────────────────────────────────────────────────

-- DROP first so re-application after a column-shape change succeeds. Postgres
-- forbids column renames via CREATE OR REPLACE VIEW; without this, every
-- bootstrap restart on a database that already has the older view shape fails
-- with "cannot change name of view column ... to ...". Migration 283 also
-- carries this DROP for the same reason; mirroring it here lets 282 itself
-- be idempotent on existing deployments where the column-renamed view was
-- never re-created cleanly.
DROP VIEW IF EXISTS task_type_routing_admission_audit CASCADE;

CREATE OR REPLACE VIEW task_type_routing_admission_audit AS
SELECT
    route.task_type,
    route.rank,
    route.provider_slug,
    route.model_slug,
    rp.runtime_profile_ref,
    matrix.control_state,
    matrix.control_reason_code,
    matrix.control_decision_ref,
    CASE
        WHEN matrix.control_state IS NULL THEN 'route_not_in_matrix'
        WHEN matrix.control_state = 'off' THEN 'route_present_but_off'
        ELSE 'admitted'
    END AS admission_status
FROM task_type_routing AS route
CROSS JOIN registry_native_runtime_profile_authority AS rp
LEFT JOIN private_model_access_control_matrix AS matrix
    ON matrix.runtime_profile_ref = rp.runtime_profile_ref
   AND matrix.job_type            = route.task_type
   AND matrix.transport_type      = 'API'
   AND matrix.adapter_type        = 'llm_task'
   AND matrix.provider_slug       = route.provider_slug
   AND matrix.model_slug          = route.model_slug
WHERE route.permitted IS TRUE
ORDER BY route.task_type, route.rank, rp.runtime_profile_ref;

COMMENT ON VIEW task_type_routing_admission_audit IS
'Surfaces task_type_routing rows that are NOT admitted by private_model_access_control_matrix. A rank-1 routing row only takes effect if the (runtime_profile, task_type, adapter_type=llm_task, provider, model) combination is also admitted by the access matrix. After authoring a routing change, query this view filtered to admission_status<>''admitted'' to catch the hidden allowlist coupling.';

COMMIT;
