-- Migration 246: DeepSeek is only reachable through the UI compile path
--
-- Operator direction (2026-04-25, nate): "make sure we are only using
-- Deepseek for compile from the UI and not any other time".
--
-- Context:
--   Migration 243 promoted openrouter/deepseek-v4-pro to task_type='build'
--   rank=1. Migration 245 split compile out as its own task_type but kept
--   v4-pro as the rank=1 API code/build route. That keeps DeepSeek
--   reachable for any caller resolving 'auto/build' through the API lane,
--   which is broader than the operator's "UI compile only" boundary.
--
--   This migration removes the build → deepseek route and narrows the
--   v4-pro candidate's affinities/capability tags so auto-resolution
--   cannot reintroduce a deepseek route for any task_type other than
--   compile. Compile routing (task_type='compile' rank=1 v4-flash, rank=2
--   v4-pro) is preserved — that is the UI "Describe it" path.
--
-- Scope:
--   - task_type_routing: drop the build → openrouter/deepseek-v4-pro row.
--     Sonnet 4.6 (rank=2 from migration 243) becomes the only API build
--     route, in line with provider-routing::cli-default-api-exception.
--   - provider_model_candidates: re-narrow v4-pro task_affinities and
--     capability_tags to compile-only. Toggle cap_build_high / cap_build_med
--     off so capability-based matching cannot pull v4-pro into build work.
--   - operator_decisions: file a new architecture-policy decision that
--     supersedes the build portion of decision.2026-04-25.compile-task-type.
--
-- DeepSeek direct-API (provider_slug='deepseek', model 'deepseek-r3') is
-- untouched. That route stays research-only under the existing
-- feedback_deepseek_research_only memory; this migration does not address it.

BEGIN;

-- -----------------------------------------------------------------------
-- 1. Remove every build → deepseek route.
--    Two rows currently leak:
--      build / rank=1  / openrouter / deepseek/deepseek-v4-pro    (explicit, migration 245)
--      build / rank=27 / openrouter / deepseek/deepseek-v4-flash  (derived from cap_build_med=true)
-- -----------------------------------------------------------------------
DELETE FROM task_type_routing
 WHERE task_type = 'build'
   AND provider_slug = 'openrouter'
   AND model_slug IN ('deepseek/deepseek-v4-pro', 'deepseek/deepseek-v4-flash');

-- -----------------------------------------------------------------------
-- 2. Narrow v4-pro candidate to compile-only affinities + capabilities.
--    Anything that previously pulled it in for build, coding, review,
--    chat, etc. via auto-resolution now sees the candidate as
--    explicitly-avoid for those buckets.
-- -----------------------------------------------------------------------
UPDATE provider_model_candidates
   SET capability_tags = '["compile","structured-output","workflow-definition","schema-normalization","long-context","primary-engine"]'::jsonb,
       task_affinities = '{
         "primary": ["compile","structured-output","workflow-definition","schema-normalization"],
         "secondary": [],
         "specialized": ["long-context","brokered-routing"],
         "fallback": [],
         "avoid": ["build","tool-use","agentic-coding","coding","review","analysis","debug","architecture","chat","refactor","test","wiring"]
       }'::jsonb,
       cap_build_high = false,
       cap_build_med = false,
       cap_review = false,
       cap_tool_use = false,
       cap_analysis_architecture_research = false,
       benchmark_profile = jsonb_set(
           COALESCE(benchmark_profile, '{}'::jsonb),
           '{positioning}',
           to_jsonb('DeepSeek V4-Pro via OpenRouter — compile-only fallback for the Moon UI Describe-it path. Migration 246 narrowed scope: not a build/code/review engine, only a compile fallback when V4-Flash degrades.'::text),
           true
       )
 WHERE candidate_ref = 'candidate.openrouter.deepseek-v4-pro';

-- Lock v4-flash to the compile lane only.
-- The affinity filter is what _materialize_derived_rows actually consults
-- (cap_build_* alone is not enough — TaskTypeRouter._build_profile_task_rows
-- skips candidates by `affinity_bucket == 'avoid'` or `task_type in avoid`).
-- Add the full set of non-compile labels to task_affinities.avoid so the
-- build/chat/review/etc. profiles cannot pull v4-flash in as "unclassified".
UPDATE provider_model_candidates
   SET cap_build_med = false,
       cap_build_high = false,
       task_affinities = jsonb_set(
           jsonb_set(
               COALESCE(task_affinities, '{}'::jsonb),
               '{avoid}',
               '["build","coding","review","analysis","tool-use","agentic-coding","debug","architecture","chat","refactor","test","wiring","final-build-authority"]'::jsonb,
               true
           ),
           '{primary}',
           '["compile","structured-output","schema-normalization","json-repair","data-extraction","classification"]'::jsonb,
           true
       ),
       benchmark_profile = jsonb_set(
           COALESCE(benchmark_profile, '{}'::jsonb),
           '{positioning}',
           to_jsonb('DeepSeek V4-Flash via OpenRouter — compile lane only (Moon UI Describe-it). Migration 246 added build/coding/review/etc. to task_affinities.avoid so derived routing cannot reintroduce a build row.'::text),
           true
       )
 WHERE candidate_ref = 'candidate.openrouter.deepseek-v4-flash';

-- Drop any already-derived row that snuck in before this migration.
DELETE FROM task_type_routing
 WHERE provider_slug = 'openrouter'
   AND model_slug LIKE 'deepseek/%'
   AND task_type <> 'compile'
   AND route_source = 'derived';

-- -----------------------------------------------------------------------
-- 3. File the policy decision and supersede the build-engine portion of
--    earlier decisions.
-- -----------------------------------------------------------------------
UPDATE operator_decisions
   SET decision_status = 'superseded',
       effective_to = now(),
       updated_at = now(),
       rationale = rationale || E'\n\nSuperseded 2026-04-25 by architecture-policy::provider-routing::deepseek-compile-only-no-build-route: build no longer routes to deepseek; compile-from-UI is the only DeepSeek lane via the OpenRouter broker.'
 WHERE decision_key IN (
       'decision.2026-04-25.openrouter-deepseek-v4-pro-build-engine',
       'architecture-policy::provider-routing::compile-task-type'
   )
   AND effective_to IS NULL;

INSERT INTO operator_decisions (
    operator_decision_id,
    decision_key,
    decision_kind,
    decision_status,
    title,
    rationale,
    decided_by,
    decision_source,
    effective_from,
    decided_at,
    created_at,
    updated_at,
    decision_scope_kind,
    decision_scope_ref
) VALUES (
    'operator_decision.deepseek-compile-only-no-build-route.2026-04-25',
    'architecture-policy::provider-routing::deepseek-compile-only-no-build-route',
    'architecture_policy',
    'decided',
    'DeepSeek (via OpenRouter) is only reachable from the UI compile path',
    $DEC$
DeepSeek through the OpenRouter broker is reachable only via task_type='compile', which is the Moon "Describe it" UI compile lane. No other task_type may resolve to a deepseek model.

Concretely:
- task_type='compile' rank=1: openrouter/deepseek-v4-flash (preserved).
- task_type='compile' rank=2: openrouter/deepseek-v4-pro  (preserved as compile fallback only).
- task_type='build' → deepseek-v4-pro: removed (was rank=1 explicit).
- task_type='build' → deepseek-v4-flash: removed (was rank=27 derived from cap_build_med).
- candidate.openrouter.deepseek-v4-pro: task_affinities narrowed to compile-only; build/coding/review/analysis/architecture/chat moved to "avoid"; cap_build_high / cap_build_med / cap_review / cap_tool_use / cap_analysis_architecture_research toggled off so capability matching cannot reroute it back into build work.
- candidate.openrouter.deepseek-v4-flash: cap_build_high / cap_build_med toggled off so derived task_type_routing cannot reintroduce a build row.
- Build now resolves through CLI candidates by default; the surviving API build candidates (sonnet-4.6, gpt-5.x, gemini-3.x, etc.) cover any required API fallback.

Untouched (out of scope for this decision):
- provider_slug='deepseek' direct API (model deepseek-r3) remains the research-only lane per feedback_deepseek_research_only.
- Sonnet 4.6 stays at task_type='build' rank=2 as the API build fallback.

Rationale: the operator's standing boundary is "API is opt-in, CLI is the default". The compile lane is an explicit API exception for the Describe-it UI surface. Allowing deepseek to resolve for build broadened that exception beyond the UI compile path. This decision restores the boundary: deepseek-via-OpenRouter is a UI-compile-only lane.

Rollback path: re-INSERT the build → openrouter/deepseek-v4-pro task_type_routing row at rank=1; restore v4-pro candidate task_affinities and cap_* flags from migration 245.
    $DEC$,
    'nate',
    'conversation',
    now(),
    now(),
    now(),
    now(),
    'authority_domain',
    'provider_routing'
) ON CONFLICT (decision_key) DO UPDATE SET
    decision_kind = EXCLUDED.decision_kind,
    decision_status = EXCLUDED.decision_status,
    title = EXCLUDED.title,
    rationale = EXCLUDED.rationale,
    decided_by = EXCLUDED.decided_by,
    decision_source = EXCLUDED.decision_source,
    effective_from = EXCLUDED.effective_from,
    decided_at = EXCLUDED.decided_at,
    updated_at = now(),
    decision_scope_kind = EXCLUDED.decision_scope_kind,
    decision_scope_ref = EXCLUDED.decision_scope_ref;

COMMIT;

-- Verification (run manually):
--   SELECT task_type, rank, provider_slug, model_slug, permitted, route_source
--     FROM task_type_routing
--    WHERE provider_slug='openrouter' AND model_slug LIKE 'deepseek%'
--    ORDER BY task_type, rank;
--     -> expect ONLY two rows:
--          compile | rank=1 | openrouter | deepseek/deepseek-v4-flash | explicit
--          compile | rank=2 | openrouter | deepseek/deepseek-v4-pro   | explicit
--        (no build → deepseek rows)
--
--   SELECT candidate_ref, task_affinities->'primary',
--          cap_build_high, cap_build_med, cap_review, cap_tool_use
--     FROM provider_model_candidates
--    WHERE candidate_ref IN (
--      'candidate.openrouter.deepseek-v4-pro',
--      'candidate.openrouter.deepseek-v4-flash'
--    );
--     -> expect cap_build_high=false and cap_build_med=false on both;
--        v4-pro primary affinities are compile-focused with build in avoid.
