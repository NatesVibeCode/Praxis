CREATE TABLE IF NOT EXISTS task_type_route_eligibility (
    task_route_eligibility_id text PRIMARY KEY,
    task_type text,
    provider_slug text NOT NULL,
    model_slug text,
    eligibility_status text NOT NULL,
    reason_code text NOT NULL,
    rationale text NOT NULL DEFAULT '',
    effective_from timestamptz NOT NULL,
    effective_to timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (eligibility_status IN ('eligible', 'rejected')),
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);

CREATE INDEX IF NOT EXISTS task_type_route_eligibility_provider_window_idx
    ON task_type_route_eligibility (provider_slug, effective_from, effective_to);

CREATE INDEX IF NOT EXISTS task_type_route_eligibility_scope_idx
    ON task_type_route_eligibility (task_type, provider_slug, model_slug, effective_from);

CREATE INDEX IF NOT EXISTS task_type_route_eligibility_decision_ref_idx
    ON task_type_route_eligibility (decision_ref);
