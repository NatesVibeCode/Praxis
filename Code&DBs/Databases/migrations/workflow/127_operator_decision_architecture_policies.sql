-- Canonical architecture-policy decisions for decision-table authority.

BEGIN;

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

COMMENT ON CONSTRAINT operator_decisions_kind_scope_policy ON operator_decisions IS 'Known decision kinds carry one explicit scope model. Scoped kinds must be queryable; unscoped kinds must not fake scope. Architecture policy decisions use typed authority-domain scope.';

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
) VALUES
    (
        'operator_decision.architecture_policy.decision_tables.db_native_authority',
        'architecture-policy::decision-tables::db-native-authority',
        'architecture_policy',
        'decided',
        'Decision tables are DB-native authority',
        'Do not script around architecture that should live in the database. Authority, durable state, orchestration, event relationships, registry metadata, and runtime coordination must live in durable DB primitives rather than ad hoc scripts.',
        'nate',
        'cto.guidance',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        NULL,
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        'authority_domain',
        'decision_tables'
    ),
    (
        'operator_decision.architecture_policy.decision_tables.scripts_support_only',
        'architecture-policy::decision-tables::scripts-support-only',
        'architecture_policy',
        'decided',
        'Scripts support decision tables; they do not replace them',
        'Use scripts for tooling and support tasks such as migrations, smoke checks, and maintenance helpers. Do not implement core system behavior in ad hoc scripts just because it is faster in the moment.',
        'nate',
        'cto.guidance',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        NULL,
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        'authority_domain',
        'decision_tables'
    ),
    (
        'operator_decision.architecture_policy.decision_tables.queryable_control',
        'architecture-policy::decision-tables::queryable-control',
        'architecture_policy',
        'decided',
        'Decision-table control must stay inspectable and queryable',
        'The runtime, event bus, and registry should be backed by durable, inspectable, queryable DB primitives where appropriate so control over state is real and observable.',
        'nate',
        'cto.guidance',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        NULL,
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        'authority_domain',
        'decision_tables'
    ),
    (
        'operator_decision.architecture_policy.decision_tables.use_operator_decisions_table',
        'architecture-policy::decision-tables::use-operator-decisions-table',
        'architecture_policy',
        'decided',
        'Use operator_decisions as the decision table authority',
        'If the decision-table shape is too complex, simplify or improve operator_decisions itself. Do not introduce a parallel decision store for architecture policy or operator control guidance.',
        'nate',
        'cto.guidance',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        NULL,
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        'authority_domain',
        'decision_tables'
    ),
    (
        'operator_decision.architecture_policy.decision_tables.operator_decisions_authority',
        'architecture-policy::decision-tables::operator-decisions-authority',
        'architecture_policy',
        'decided',
        'Operator decisions is the authority table',
        'Cross-cutting architecture policy guidance belongs in operator_decisions under the typed architecture_policy kind. If the decision table shape needs cleanup, improve operator_decisions rather than creating a parallel decision store.',
        'nate',
        'cto.guidance',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        NULL,
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
        TIMESTAMPTZ '2026-04-15T00:00:00Z',
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
