-- Canonical operator-decision scope policy backfill and enforcement.

UPDATE operator_decisions AS decision
SET
    decision_scope_kind = target.scope_kind,
    decision_scope_ref = target.scope_ref
FROM (
    SELECT
        gate.opened_by_decision_id AS operator_decision_id,
        CASE
            WHEN gate.roadmap_item_id IS NOT NULL THEN 'roadmap_item'
            WHEN gate.workflow_class_id IS NOT NULL THEN 'workflow_class'
            WHEN gate.schedule_definition_id IS NOT NULL THEN 'schedule_definition'
            ELSE NULL
        END AS scope_kind,
        COALESCE(
            gate.roadmap_item_id,
            gate.workflow_class_id,
            gate.schedule_definition_id
        ) AS scope_ref
    FROM cutover_gates AS gate
    UNION
    SELECT
        gate.closed_by_decision_id AS operator_decision_id,
        CASE
            WHEN gate.roadmap_item_id IS NOT NULL THEN 'roadmap_item'
            WHEN gate.workflow_class_id IS NOT NULL THEN 'workflow_class'
            WHEN gate.schedule_definition_id IS NOT NULL THEN 'schedule_definition'
            ELSE NULL
        END AS scope_kind,
        COALESCE(
            gate.roadmap_item_id,
            gate.workflow_class_id,
            gate.schedule_definition_id
        ) AS scope_ref
    FROM cutover_gates AS gate
    WHERE gate.closed_by_decision_id IS NOT NULL
) AS target
WHERE decision.operator_decision_id = target.operator_decision_id
  AND decision.decision_kind IN ('cutover_gate', 'native_primary_cutover')
  AND target.scope_kind IS NOT NULL
  AND target.scope_ref IS NOT NULL
  AND decision.decision_scope_kind IS NULL
  AND decision.decision_scope_ref IS NULL;

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

COMMENT ON CONSTRAINT operator_decisions_kind_scope_policy ON operator_decisions IS 'Known decision kinds carry one explicit scope model. Scoped kinds must be queryable; unscoped kinds must not fake scope.';
