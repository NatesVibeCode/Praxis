-- Migration 181: Wiring-audit snapshot history.
--
-- Every heartbeat records a small summary row here with the current
-- count of hard-path, unreferenced-decision, and code-orphan findings.
-- That gives the scorecard / trend queries a time series to read
-- without re-scanning the source tree on every request — and lets the
-- operator see whether audit numbers are trending up or down over
-- time.
--
-- The row is ~200 bytes; retention handled by a cheap DELETE in the
-- projector (keep 60 days).
--
-- Decision: operator_decision.architecture_policy.data_dictionary.wiring_audit_in_platform
-- Scope:    authority_domain=data_dictionary.wiring_audit

CREATE TABLE IF NOT EXISTS data_dictionary_wiring_audit_snapshots (
    snapshot_id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    taken_at              timestamptz NOT NULL DEFAULT now(),
    triggered_by          text NOT NULL DEFAULT 'heartbeat',
    hard_path_total       integer NOT NULL DEFAULT 0 CHECK (hard_path_total >= 0),
    absolute_user_paths   integer NOT NULL DEFAULT 0 CHECK (absolute_user_paths >= 0),
    hardcoded_localhost   integer NOT NULL DEFAULT 0 CHECK (hardcoded_localhost >= 0),
    hardcoded_ports       integer NOT NULL DEFAULT 0 CHECK (hardcoded_ports >= 0),
    unreferenced_decisions integer NOT NULL DEFAULT 0 CHECK (unreferenced_decisions >= 0),
    code_orphan_tables    integer NOT NULL DEFAULT 0 CHECK (code_orphan_tables >= 0),
    duration_ms           integer NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    metadata              jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_data_dictionary_wiring_audit_snapshots_taken
    ON data_dictionary_wiring_audit_snapshots (taken_at DESC);
