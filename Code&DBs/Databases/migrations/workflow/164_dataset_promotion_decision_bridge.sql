-- Typed operator decisions for dataset refinery promotions, rejections, and
-- supersedes.
--
-- Before this migration, dataset_promotions referenced operator_decisions
-- through an optional free-text decision_ref column and rejections/supersedes
-- had no typed decision row at all. That meant "why was this candidate
-- promoted?" and "who signed off?" could only be answered by reading the
-- rationale field and hoping it was populated.
--
-- This migration extends the operator_decisions_kind_scope_policy CHECK to
-- recognize three new decision kinds, each bound to a typed scope:
--   dataset_promotion           -> scope_kind=dataset_specialist (slm/...)
--   dataset_rejection           -> scope_kind=dataset_candidate   (candidate_id)
--   dataset_promotion_supersede -> scope_kind=dataset_promotion   (promotion_id)
--
-- The kinds are queryable as typed rows via operator_decisions, which means
-- the eligibility journal, the promotion record, and the governing decision
-- line up behind one decision-table authority surface.
--
-- Authority-policy rows documenting the bridge are upserted below so
-- operator_decisions answers "where does dataset promotion authority live?"
-- without a separate runbook.

BEGIN;

-- Legacy rows: a small number of dataset_promotion decisions were written
-- before this migration with no scope. Backfill them with a sentinel
-- dataset_specialist scope so the new CHECK holds. The canonical source
-- going forward is operator_write.arecord_dataset_promotion, which always
-- sets decision_scope_ref to the specialist_target.
UPDATE operator_decisions
   SET decision_scope_kind = 'dataset_specialist',
       decision_scope_ref  = 'legacy:unscoped',
       updated_at          = now()
 WHERE decision_kind = 'dataset_promotion'
   AND (decision_scope_kind IS NULL OR decision_scope_ref IS NULL);

UPDATE operator_decisions
   SET decision_scope_kind = 'dataset_candidate',
       decision_scope_ref  = 'legacy:unscoped',
       updated_at          = now()
 WHERE decision_kind = 'dataset_rejection'
   AND (decision_scope_kind IS NULL OR decision_scope_ref IS NULL);

UPDATE operator_decisions
   SET decision_scope_kind = 'dataset_promotion',
       decision_scope_ref  = 'legacy:unscoped',
       updated_at          = now()
 WHERE decision_kind = 'dataset_promotion_supersede'
   AND (decision_scope_kind IS NULL OR decision_scope_ref IS NULL);

ALTER TABLE operator_decisions
    DROP CONSTRAINT IF EXISTS operator_decisions_kind_scope_policy;

ALTER TABLE operator_decisions
    ADD CONSTRAINT operator_decisions_kind_scope_policy
        CHECK (
            CASE
                WHEN decision_kind IN (
                    'circuit_breaker_force_open',
                    'circuit_breaker_force_closed',
                    'circuit_breaker_reset'
                ) THEN (
                    decision_scope_kind = 'provider'
                    AND decision_scope_ref IS NOT NULL
                )
                WHEN decision_kind IN (
                    'native_primary_cutover',
                    'cutover_gate'
                ) THEN (
                    decision_scope_kind IN (
                        'roadmap_item',
                        'workflow_class',
                        'schedule_definition'
                    )
                    AND decision_scope_ref IS NOT NULL
                )
                WHEN decision_kind IN (
                    'architecture_policy'
                ) THEN (
                    decision_scope_kind = 'authority_domain'
                    AND decision_scope_ref IS NOT NULL
                )
                WHEN decision_kind = 'dataset_promotion' THEN (
                    decision_scope_kind = 'dataset_specialist'
                    AND decision_scope_ref IS NOT NULL
                )
                WHEN decision_kind = 'dataset_rejection' THEN (
                    decision_scope_kind = 'dataset_candidate'
                    AND decision_scope_ref IS NOT NULL
                )
                WHEN decision_kind = 'dataset_promotion_supersede' THEN (
                    decision_scope_kind = 'dataset_promotion'
                    AND decision_scope_ref IS NOT NULL
                )
                WHEN decision_kind IN (
                    'binding',
                    'query',
                    'operator_graph'
                ) THEN (
                    decision_scope_kind IS NULL
                    AND decision_scope_ref IS NULL
                )
                ELSE TRUE
            END
        );

COMMENT ON CONSTRAINT operator_decisions_kind_scope_policy ON operator_decisions IS 'Known decision kinds carry one explicit scope model. Scoped kinds must be queryable; unscoped kinds must not fake scope. Dataset promotion/rejection/supersede kinds use dataset-scoped scope refs so the refinery has typed decision rows alongside the free-text rationale.';

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
    effective_to,
    decided_at,
    created_at,
    updated_at,
    decision_scope_kind,
    decision_scope_ref
) VALUES (
    'operator_decision.architecture_policy.decision_tables.dataset_refinery_authority',
    'architecture-policy::decision-tables::dataset-refinery-authority',
    'architecture_policy',
    'decided',
    'Dataset refinery promotions are typed operator decisions',
    'Every dataset_promotion, dataset_rejection, and dataset_promotion_supersede must write a typed operator_decisions row with dataset-scoped scope_ref. Free-text rationale and legacy decision_ref columns on dataset_promotions are supplemental; the decision row is the authority.',
    'nate',
    'cto.guidance',
    TIMESTAMPTZ '2026-04-18T00:00:00Z',
    NULL,
    TIMESTAMPTZ '2026-04-18T00:00:00Z',
    TIMESTAMPTZ '2026-04-18T00:00:00Z',
    TIMESTAMPTZ '2026-04-18T00:00:00Z',
    'authority_domain',
    'decision_tables'
)
ON CONFLICT (operator_decision_id) DO UPDATE SET
    decision_key = EXCLUDED.decision_key,
    decision_kind = EXCLUDED.decision_kind,
    decision_status = EXCLUDED.decision_status,
    title = EXCLUDED.title,
    rationale = EXCLUDED.rationale,
    decided_by = EXCLUDED.decided_by,
    decision_source = EXCLUDED.decision_source,
    effective_from = EXCLUDED.effective_from,
    effective_to = EXCLUDED.effective_to,
    decided_at = EXCLUDED.decided_at,
    updated_at = EXCLUDED.updated_at,
    decision_scope_kind = EXCLUDED.decision_scope_kind,
    decision_scope_ref = EXCLUDED.decision_scope_ref;

COMMIT;
