-- Migration 121: enrich workflow build review decisions with shared contract fields
--
-- Review decisions are the shared approval surface for bindings, imports,
-- capability bundles, and workflow shapes. The DB contract needs slot/group
-- scope, authority scope, and supersession metadata so later compiler stages
-- can trust one auditable approval system.

BEGIN;

ALTER TABLE workflow_build_review_decisions
    ADD COLUMN IF NOT EXISTS review_group_ref text;

ALTER TABLE workflow_build_review_decisions
    ADD COLUMN IF NOT EXISTS slot_ref text;

ALTER TABLE workflow_build_review_decisions
    ADD COLUMN IF NOT EXISTS authority_scope text;

ALTER TABLE workflow_build_review_decisions
    ADD COLUMN IF NOT EXISTS supersedes_decision_ref text;

UPDATE workflow_build_review_decisions
SET review_group_ref = concat('workflow_build:', workflow_id, ':', definition_revision)
WHERE review_group_ref IS NULL OR btrim(review_group_ref) = '';

UPDATE workflow_build_review_decisions
SET slot_ref = target_ref
WHERE slot_ref IS NULL
  AND target_kind IN ('binding', 'import_snapshot', 'capability_bundle', 'workflow_shape');

UPDATE workflow_build_review_decisions
SET authority_scope = concat('workflow_build/', target_kind)
WHERE authority_scope IS NULL OR btrim(authority_scope) = '';

ALTER TABLE workflow_build_review_decisions
    ALTER COLUMN review_group_ref SET NOT NULL;

ALTER TABLE workflow_build_review_decisions
    ALTER COLUMN authority_scope SET NOT NULL;

ALTER TABLE workflow_build_review_decisions
    DROP CONSTRAINT IF EXISTS workflow_build_review_decisions_supersedes_decision_ref_fkey;

ALTER TABLE workflow_build_review_decisions
    ADD CONSTRAINT workflow_build_review_decisions_supersedes_decision_ref_fkey
    FOREIGN KEY (supersedes_decision_ref)
    REFERENCES workflow_build_review_decisions(review_decision_id)
    ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_workflow_build_review_decisions_group_target
    ON workflow_build_review_decisions (
        workflow_id,
        definition_revision,
        review_group_ref,
        target_kind,
        target_ref,
        decided_at DESC,
        created_at DESC
    );

COMMENT ON COLUMN workflow_build_review_decisions.review_group_ref IS 'Stable review grouping key for one workflow build revision review session.';
COMMENT ON COLUMN workflow_build_review_decisions.slot_ref IS 'Optional slot identifier when the review target belongs to a wider candidate slot.';
COMMENT ON COLUMN workflow_build_review_decisions.authority_scope IS 'Authority lane that accepted the decision (for example workflow_build/binding).';
COMMENT ON COLUMN workflow_build_review_decisions.supersedes_decision_ref IS 'Previous effective review decision superseded by this record for the same target.';

COMMIT;
