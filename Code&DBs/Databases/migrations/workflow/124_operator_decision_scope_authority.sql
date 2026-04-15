-- Canonical typed scope for operator decisions.

ALTER TABLE operator_decisions
    ADD COLUMN decision_scope_kind text,
    ADD COLUMN decision_scope_ref text;

UPDATE operator_decisions
SET
    decision_scope_kind = 'provider',
    decision_scope_ref = lower(split_part(substr(decision_key, length('circuit-breaker::') + 1), '::', 1))
WHERE decision_kind IN (
        'circuit_breaker_reset',
        'circuit_breaker_force_open',
        'circuit_breaker_force_closed'
    )
  AND decision_key LIKE 'circuit-breaker::%'
  AND decision_scope_kind IS NULL
  AND decision_scope_ref IS NULL;

ALTER TABLE operator_decisions
    ADD CONSTRAINT operator_decisions_scope_pair
        CHECK (
            (decision_scope_kind IS NULL AND decision_scope_ref IS NULL)
            OR (
                decision_scope_kind IS NOT NULL
                AND decision_scope_ref IS NOT NULL
                AND btrim(decision_scope_kind) <> ''
                AND btrim(decision_scope_ref) <> ''
            )
        );

CREATE INDEX operator_decisions_scope_decided_idx
    ON operator_decisions (decision_scope_kind, decision_scope_ref, decided_at DESC);

COMMENT ON COLUMN operator_decisions.decision_scope_kind IS 'Typed authority scope for the decision, for example provider or roadmap_item. Keep control scope out of opaque key strings.';
COMMENT ON COLUMN operator_decisions.decision_scope_ref IS 'Canonical reference inside the typed decision scope. Query this instead of parsing decision_key.';
