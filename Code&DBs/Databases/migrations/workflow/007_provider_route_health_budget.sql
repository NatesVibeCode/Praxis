-- Canonical routing control-tower tables for provider health, budgets, and
-- route eligibility. These rows are authority for routing state, not shell
-- folklore or Python-only fixtures.

CREATE TABLE provider_route_health_windows (
    provider_route_health_window_id text PRIMARY KEY,
    candidate_ref text NOT NULL,
    provider_ref text NOT NULL,
    health_status text NOT NULL,
    health_score double precision NOT NULL CHECK (health_score >= 0 AND health_score <= 1),
    sample_count integer NOT NULL CHECK (sample_count >= 0),
    failure_rate double precision NOT NULL CHECK (failure_rate >= 0 AND failure_rate <= 1),
    latency_p95_ms integer,
    observed_window_started_at timestamptz NOT NULL,
    observed_window_ended_at timestamptz NOT NULL,
    observation_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT provider_route_health_windows_candidate_fkey
        FOREIGN KEY (candidate_ref)
        REFERENCES provider_model_candidates (candidate_ref)
        ON DELETE RESTRICT,
    CONSTRAINT provider_route_health_windows_window_range
        CHECK (observed_window_ended_at >= observed_window_started_at),
    CONSTRAINT provider_route_health_windows_unique_window
        UNIQUE (candidate_ref, observed_window_started_at, observed_window_ended_at)
);

CREATE INDEX provider_route_health_windows_provider_status_idx
    ON provider_route_health_windows (provider_ref, health_status, observed_window_ended_at DESC);

CREATE INDEX provider_route_health_windows_candidate_window_idx
    ON provider_route_health_windows (candidate_ref, observed_window_started_at DESC);

COMMENT ON TABLE provider_route_health_windows IS 'Canonical provider-route health windows behind routing decisions. Owned by registry/.';
COMMENT ON COLUMN provider_route_health_windows.observation_ref IS 'Observation or receipt reference that justifies the stored route health window.';

CREATE TABLE provider_budget_windows (
    provider_budget_window_id text PRIMARY KEY,
    provider_policy_id text NOT NULL,
    provider_ref text NOT NULL,
    budget_scope text NOT NULL,
    budget_status text NOT NULL,
    window_started_at timestamptz NOT NULL,
    window_ended_at timestamptz NOT NULL,
    request_limit bigint,
    requests_used bigint NOT NULL CHECK (requests_used >= 0),
    token_limit bigint,
    tokens_used bigint NOT NULL CHECK (tokens_used >= 0),
    spend_limit_usd numeric(18,6),
    spend_used_usd numeric(18,6) NOT NULL CHECK (spend_used_usd >= 0),
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT provider_budget_windows_policy_fkey
        FOREIGN KEY (provider_policy_id)
        REFERENCES provider_policies (provider_policy_id)
        ON DELETE RESTRICT,
    CONSTRAINT provider_budget_windows_window_range
        CHECK (window_ended_at >= window_started_at),
    CONSTRAINT provider_budget_windows_unique_window
        UNIQUE (provider_policy_id, budget_scope, window_started_at)
);

CREATE INDEX provider_budget_windows_provider_scope_status_idx
    ON provider_budget_windows (provider_ref, budget_scope, budget_status, window_ended_at DESC);

CREATE INDEX provider_budget_windows_policy_window_idx
    ON provider_budget_windows (provider_policy_id, window_started_at DESC);

COMMENT ON TABLE provider_budget_windows IS 'Canonical provider budget windows for routing and admission control. Owned by policy/.';
COMMENT ON COLUMN provider_budget_windows.decision_ref IS 'Decision reference that admitted the budget window as routing authority.';

CREATE TABLE route_eligibility_states (
    route_eligibility_state_id text PRIMARY KEY,
    model_profile_id text NOT NULL,
    provider_policy_id text NOT NULL,
    candidate_ref text NOT NULL,
    eligibility_status text NOT NULL,
    reason_code text NOT NULL,
    source_window_refs jsonb NOT NULL,
    evaluated_at timestamptz NOT NULL,
    expires_at timestamptz,
    decision_ref text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT route_eligibility_states_model_profile_fkey
        FOREIGN KEY (model_profile_id)
        REFERENCES model_profiles (model_profile_id)
        ON DELETE RESTRICT,
    CONSTRAINT route_eligibility_states_policy_fkey
        FOREIGN KEY (provider_policy_id)
        REFERENCES provider_policies (provider_policy_id)
        ON DELETE RESTRICT,
    CONSTRAINT route_eligibility_states_candidate_fkey
        FOREIGN KEY (candidate_ref)
        REFERENCES provider_model_candidates (candidate_ref)
        ON DELETE RESTRICT,
    CONSTRAINT route_eligibility_states_unique_evaluation
        UNIQUE (model_profile_id, provider_policy_id, candidate_ref, evaluated_at)
);

CREATE INDEX route_eligibility_states_profile_candidate_status_idx
    ON route_eligibility_states (model_profile_id, candidate_ref, eligibility_status);

CREATE INDEX route_eligibility_states_decision_ref_idx
    ON route_eligibility_states (decision_ref);

COMMENT ON TABLE route_eligibility_states IS 'Canonical route eligibility state derived from admitted provider health and budget authority. Owned by policy/.';
COMMENT ON COLUMN route_eligibility_states.source_window_refs IS 'Explicit refs to the health and budget windows that justified the eligibility decision.';
