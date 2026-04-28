-- Migration 302: Operator decisions are drillable — provenance + why.
--
-- Anchor decision (filed in this session):
--   architecture-policy::operator-decisions::drillable-with-provenance-and-why
--
-- Why this exists
--   Today every operator_decisions row is treated equally regardless of how
--   it landed in the table. A decision the operator unequivocally made
--   ("turn off Anthropic", "no Gemini 2.5 for real work") sits next to a
--   decision the model GUESSED was operator intent during conversation
--   parsing. Same weight at surface time, same weight in conflict
--   resolution, same weight in compliance enforcement. That's wrong.
--
--   The operator wants two things:
--     1. Every decision drillable as an object — title + rationale + why
--        + provenance + scope_clamp + history.
--     2. A first-class discriminator between inferred (model guessed)
--        and explicit (operator unequivocally said) decisions, so
--        downstream consumers (trigger registry, friction surface,
--        compliance receipts) can weight them differently.
--
-- What ships
--   1. decision_provenance text — 'inferred' | 'explicit'.
--      NOT NULL, default 'inferred' (conservative — model writes start as
--      inferred until promoted by an explicit operator action).
--   2. decision_why text — separate from rationale. Rationale captures
--      WHAT the rule is; why captures WHY it exists (the deeper
--      motivation). Nullable; populated when known.
--   3. Backfill heuristic for existing rows based on decision_source:
--      explicit when the source is operator-direct
--      (cto.guidance, operator, fresh_install_seed, migration_*, memory_file_migration);
--      inferred for everything else (conversation*, praxis-debate*, claude_code*, etc.).
--      Heuristic is conservative — false-negative on explicit is fine, false-positive
--      is not.
--
-- What does NOT ship here
--   - Weighting logic. That lives in the consumers (trigger_check render,
--     compliance receipt scoring, conflict resolution). This migration
--     gives them the column to read; consumers ship in follow-ups.
--   - Splitting current rationale strings into rationale + why. Operator
--     authors fill in `why` going forward; existing rows keep rationale
--     unchanged.

BEGIN;

ALTER TABLE operator_decisions
    ADD COLUMN IF NOT EXISTS decision_provenance text NOT NULL DEFAULT 'inferred',
    ADD COLUMN IF NOT EXISTS decision_why text;

ALTER TABLE operator_decisions
    DROP CONSTRAINT IF EXISTS operator_decisions_provenance_check;

ALTER TABLE operator_decisions
    ADD CONSTRAINT operator_decisions_provenance_check
    CHECK (decision_provenance IN ('inferred', 'explicit'));

CREATE INDEX IF NOT EXISTS operator_decisions_provenance_kind_idx
    ON operator_decisions (decision_provenance, decision_kind, decided_at DESC);

COMMENT ON COLUMN operator_decisions.decision_provenance IS
    'How this decision landed in the table. ''explicit'' = operator unequivocally said so. '
    '''inferred'' = model guessed during conversation/debate parsing. Consumers (trigger '
    'registry, friction surface, compliance scoring) should weight explicit higher.';

COMMENT ON COLUMN operator_decisions.decision_why IS
    'Deeper motivation for the decision (separate from rationale, which captures the rule). '
    'Optional; populated when known. Drillable surfaces should expose why alongside rationale.';

-- ============================================================
-- Backfill heuristic
-- ============================================================
-- Operator-direct sources → explicit. Conservative list — only sources
-- that map cleanly to operator authorship.
UPDATE operator_decisions
   SET decision_provenance = 'explicit'
 WHERE decision_provenance = 'inferred'
   AND (
        decision_source = 'cto.guidance'
        OR decision_source = 'operator'
        OR decision_source = 'fresh_install_seed'
        OR decision_source = 'memory_file_migration'
        OR decision_source LIKE 'migration\_%' ESCAPE '\'
        OR decision_source LIKE 'migration:%'
   );

-- Everything else stays at the default 'inferred', including:
--   conversation*, claude_code*, praxis-debate*, praxis-review*, debate.*,
--   workflow-review:*, p4_2_*, sessions, ad-hoc.
-- These rows can be promoted to explicit through a future operator-action
-- after review; not auto-promoting protects the trust signal.

COMMIT;

-- Verification (run manually):
--   SELECT decision_provenance, count(*)
--     FROM operator_decisions
--    GROUP BY decision_provenance
--    ORDER BY 2 DESC;
