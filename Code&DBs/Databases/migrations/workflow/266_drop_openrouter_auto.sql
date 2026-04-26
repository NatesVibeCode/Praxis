-- Migration 266: Drop the openrouter/auto pseudo-router slug.
--
-- Operator direction (2026-04-26, nate): "openrouter/auto delete please" +
-- "Only thing we use is router slugs". openrouter/auto is OpenRouter's
-- meta-route ("let OpenRouter pick"); not a specific model, so it doesn't
-- belong in the catalog where every other slug names an explicit model.
--
-- Removes the row from task_type_routing (rank 26 on analysis + chat) and
-- the provider_model_candidates row, plus any provider_endpoint_bindings
-- that reference it.

BEGIN;

DELETE FROM task_type_routing
 WHERE provider_slug = 'openrouter'
   AND model_slug = 'openrouter/auto';

DELETE FROM provider_endpoint_bindings
 WHERE candidate_ref IN (
        SELECT candidate_ref FROM provider_model_candidates
         WHERE provider_slug = 'openrouter'
           AND model_slug = 'openrouter/auto'
       );

DELETE FROM model_profile_candidate_bindings
 WHERE candidate_ref IN (
        SELECT candidate_ref FROM provider_model_candidates
         WHERE provider_slug = 'openrouter'
           AND model_slug = 'openrouter/auto'
       );

DELETE FROM provider_model_candidates
 WHERE provider_slug = 'openrouter'
   AND model_slug = 'openrouter/auto';

COMMIT;
