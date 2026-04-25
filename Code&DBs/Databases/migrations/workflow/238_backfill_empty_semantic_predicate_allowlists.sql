-- Migration 238: Backfill empty subject_kind_allowlist / object_kind_allowlist
-- on semantic_predicates rows whose validators got tightened.
--
-- Context: BUG-C0E701DC. The semantic_assertion validator at
-- runtime/semantic_assertions.py:189 raises on empty subject_kind_allowlist
-- ("must contain at least one kind token"). Seven semantic_predicates rows
-- shipped with empty allowlists in JSONB and now block every operation that
-- writes assertions through that predicate. Surfaces affected:
--   * praxis_operator_architecture_policy   (uses 'architecture_policy')
--   * praxis_operator_decisions(record)     (uses 'architecture_policy' / 'delivery_plan')
--   * praxis_operator_relations             (uses 'is_part_of')
--   * roadmap semantic-bridge auto-emission (uses 'sourced_from_bug' /
--                                            'sourced_from_idea' /
--                                            'governed_by_decision_ref' /
--                                            'touches_repo_path')
--
-- The actual allowlists are already encoded in code at
-- surfaces/api/operator_write.py: _ROADMAP_SEMANTIC_SUBJECT_ALLOWLIST and
-- _ROADMAP_SEMANTIC_PREDICATE_SPECS. This migration ports those values into
-- the DB rows so the validator passes and the surfaces unblock.
--
-- For 'architecture_policy' and 'delivery_plan' predicates (decision-kind
-- classifications, no other object), subject is operator_decision; the
-- "object" is the decision_scope (authority_domain), so we list that.

BEGIN;

UPDATE semantic_predicates
   SET subject_kind_allowlist = '["roadmap_item","bug","operator_idea"]'::jsonb,
       object_kind_allowlist  = '["bug"]'::jsonb,
       updated_at             = now()
 WHERE predicate_slug = 'sourced_from_bug';

UPDATE semantic_predicates
   SET subject_kind_allowlist = '["roadmap_item"]'::jsonb,
       object_kind_allowlist  = '["operator_idea"]'::jsonb,
       updated_at             = now()
 WHERE predicate_slug = 'sourced_from_idea';

UPDATE semantic_predicates
   SET subject_kind_allowlist = '["roadmap_item","bug","operator_decision","operator_idea"]'::jsonb,
       object_kind_allowlist  = '["decision_ref"]'::jsonb,
       updated_at             = now()
 WHERE predicate_slug = 'governed_by_decision_ref';

UPDATE semantic_predicates
   SET subject_kind_allowlist = '["roadmap_item","bug","operator_decision"]'::jsonb,
       object_kind_allowlist  = '["repo_path"]'::jsonb,
       updated_at             = now()
 WHERE predicate_slug = 'touches_repo_path';

UPDATE semantic_predicates
   SET subject_kind_allowlist = '["roadmap_item","bug","operator_idea","operator_decision"]'::jsonb,
       object_kind_allowlist  = '["roadmap_item","bug","operator_idea","operator_decision","functional_area"]'::jsonb,
       updated_at             = now()
 WHERE predicate_slug = 'is_part_of';

UPDATE semantic_predicates
   SET subject_kind_allowlist = '["operator_decision"]'::jsonb,
       object_kind_allowlist  = '["authority_domain","operator_decision"]'::jsonb,
       updated_at             = now()
 WHERE predicate_slug = 'architecture_policy';

UPDATE semantic_predicates
   SET subject_kind_allowlist = '["operator_decision"]'::jsonb,
       object_kind_allowlist  = '["authority_domain","operator_decision"]'::jsonb,
       updated_at             = now()
 WHERE predicate_slug = 'delivery_plan';

COMMIT;
