-- Structured scope_clamp on operator_decisions.
--
-- Anchored by:
--   architecture-policy::decision-authority::scope-clamp-preserved-verbatim
--   (operator_decision.architecture_policy.decision_authority.scope_clamp_preserved_verbatim,
--    filed 2026-04-26 by nate)
--
-- Why this column exists: decisions get distorted through summarization.
-- Canonical failure: "DeepSeek added for compile-task primary only" got
-- paraphrased to "DeepSeek added to the pool" and the broader (false) claim
-- spread to other call sites. Free-text scope buried in rationale is
-- invisible to downstream LLMs and agents — they paraphrase the body and
-- the clamp evaporates. This migration makes scope a queryable JSONB clamp
-- so it survives every summary, plan, and orient envelope.
--
-- Shape: {"applies_to": [string, ...], "does_not_apply_to": [string, ...]}.
-- Existing rows backfill with the placeholder applies_to=["pending_review"];
-- the Moon Decisions panel surfaces these so the operator can fill them in
-- without paraphrasing the rationale automatically. The anchor row for this
-- policy installs its real clamp inline below.

BEGIN;

ALTER TABLE operator_decisions
    ADD COLUMN scope_clamp JSONB NOT NULL
        DEFAULT '{"applies_to":["pending_review"],"does_not_apply_to":[]}'::jsonb;

ALTER TABLE operator_decisions
    ADD CONSTRAINT operator_decisions_scope_clamp_shape
        CHECK (
            jsonb_typeof(scope_clamp) = 'object'
            AND jsonb_typeof(scope_clamp -> 'applies_to') = 'array'
            AND jsonb_typeof(scope_clamp -> 'does_not_apply_to') = 'array'
        );

CREATE INDEX operator_decisions_scope_clamp_gin_idx
    ON operator_decisions USING GIN (scope_clamp jsonb_path_ops);

CREATE INDEX operator_decisions_scope_clamp_pending_review_idx
    ON operator_decisions ((scope_clamp -> 'applies_to'))
    WHERE scope_clamp -> 'applies_to' @> '["pending_review"]'::jsonb;

COMMENT ON COLUMN operator_decisions.scope_clamp IS
    'Structured scope clamp ({applies_to, does_not_apply_to}). Surfaces that summarize, quote, or reference this decision must reproduce the clamp verbatim — never paraphrase. See architecture-policy::decision-authority::scope-clamp-preserved-verbatim.';

-- Install the real clamp on the anchor architecture-policy row (filed via
-- praxis_operator_decisions earlier in the same conversation). All other
-- existing rows keep the pending_review placeholder until the operator
-- reviews them through the Moon Decisions panel.
UPDATE operator_decisions
SET scope_clamp = jsonb_build_object(
        'applies_to', jsonb_build_array(
            'All operator_decisions rows across every decision_kind (architecture_policy, delivery_plan, dataset_promotion, etc.)',
            'All surfaces that reference, summarize, or quote decisions: praxis_orient, compose_plan, launch_plan, plan_lifecycle, agent summaries, plan receipts, closeout receipts, Moon Decisions surface, status snapshots',
            'Agents and LLMs producing plans, summaries, recommendations, or roadmap proposals that lean on decision authority'
        ),
        'does_not_apply_to', jsonb_build_array(
            'Conversation context that has not yet been promoted to a decision row',
            'User feedback in chat that has not gone through praxis_operator_write or praxis_operator_decisions(action=record)',
            'Auto-memory entries — those have their own scope discipline (the user''s MEMORY.md index)',
            'Raw query receipts (run_status, query results) that surface a decision_ref pointer without summarizing it'
        )
    ),
    updated_at = NOW()
WHERE operator_decision_id = 'operator_decision.architecture_policy.decision_authority.scope_clamp_preserved_verbatim';

COMMIT;
