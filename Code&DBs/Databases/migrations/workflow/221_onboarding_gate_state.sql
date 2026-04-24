-- Migration 221: onboarding gate-probe state cache.
--
-- One row per gate_ref from runtime.onboarding.ONBOARDING_GRAPH. The table is
-- a freshness cache over probe evaluation: surfaces read the cached result
-- when evaluated_at + cache_ttl_s is still in the future, and re-run the
-- probe otherwise. Apply handlers (Packet 2b) upsert a fresh row when they
-- mutate state and re-probe.
--
-- Deliberately a purpose-built table rather than reusing capability_grants.
-- Mobile v1 (archived 2026-04-24) previously owned that table and its
-- grant_kind enum was tied to mobile concepts (device_session, plan, etc.).
-- Gating onboarding readiness belongs to its own authority with its own
-- schema.

BEGIN;

CREATE TABLE IF NOT EXISTS onboarding_gate_state (
    gate_ref TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    status TEXT NOT NULL,
    observed_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    remediation_hint TEXT,
    remediation_doc_url TEXT,
    apply_ref TEXT,
    platform TEXT,
    cache_ttl_s INTEGER NOT NULL DEFAULT 300,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_at TIMESTAMPTZ,
    applied_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT onboarding_gate_state_gate_ref_nonblank_check
        CHECK (btrim(gate_ref) <> ''),
    CONSTRAINT onboarding_gate_state_status_valid_check
        CHECK (status IN ('ok', 'missing', 'blocked', 'unknown')),
    CONSTRAINT onboarding_gate_state_domain_valid_check
        CHECK (domain IN ('platform', 'runtime', 'provider', 'mcp', 'legacy')),
    CONSTRAINT onboarding_gate_state_cache_ttl_nonnegative_check
        CHECK (cache_ttl_s >= 0),
    CONSTRAINT onboarding_gate_state_observed_state_object_check
        CHECK (jsonb_typeof(observed_state) = 'object')
);

CREATE INDEX IF NOT EXISTS onboarding_gate_state_domain_status_idx
    ON onboarding_gate_state (domain, status);

CREATE INDEX IF NOT EXISTS onboarding_gate_state_evaluated_at_idx
    ON onboarding_gate_state (evaluated_at DESC);

CREATE INDEX IF NOT EXISTS onboarding_gate_state_apply_ref_idx
    ON onboarding_gate_state (apply_ref)
    WHERE apply_ref IS NOT NULL;

COMMIT;
