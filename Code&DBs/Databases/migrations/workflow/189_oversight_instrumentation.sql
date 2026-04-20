-- Migration 189: Oversight instrumentation — debate verdicts on Pillars 3 & 4.
--
-- Source decision: debate_mobile_access_architecture_20260420
-- Roadmap items:
--   * roadmap_item.mobile.access.plan.webauthn.pwa.capability.ledger.oversight.metrics.policy.drift.count.autonomy.depth.debate.pillar.3
--   * roadmap_item.mobile.access.plan.webauthn.pwa.capability.ledger.session.blast.radius.tracker.gate.debate.pillar.4
--
-- The debate identified two gaps in the mobile access plan:
--
-- PILLAR 3 — Metrics complete-for-ops but thin-for-oversight.
--   Five-panel set (Flow/Economy/Providers/Quality/System) is ops hygiene,
--   not oversight. Two signals correspond directly to the autonomous-builder
--   threat model and are missing:
--     (a) policy_drift_count — runtime actions observed that violated an
--         active operator_decisions policy (semantic drift).
--     (b) autonomy_depth — longest chain of auto-approved actions since
--         last human tap.
--
-- PILLAR 4 — Per-action risk tagging is chain-bypassable.
--   Even inside a plan-hash-covered envelope, thousands of in-scope
--   primitives can compound damage. A session-scoped working-set counter
--   (files_mutated, bytes_written, external_calls, spend_cents) that gates
--   when any dimension crosses its ceiling catches the damage shape the
--   plan-envelope approves in aggregate but wouldn't have approved at size.
--
-- This migration adds four tables. Runtime instrumentation and mobile
-- surface reads are implemented by spec
-- config/cascade/specs/W_mobile_access_plan_20260420.queue.json waves.
--
-- No FKs to the mobile-access tables (approval_requests / capability_grants
-- from migrations 185-188) — those migrations are sequenced by the spec's
-- build waves; this migration is authority-only and must apply standalone.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────
-- policy_drift_events — inverse check on operator_decisions authority.
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS policy_drift_events (
    drift_event_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    detected_at       timestamptz NOT NULL DEFAULT now(),
    policy_id         text NOT NULL,         -- operator_decisions.policy_id
    decision_ref      text,
    detector_kind     text NOT NULL,         -- e.g. 'anthropic_api_call_attempt'
    violator_path     text,                  -- runtime path where observed
    violator_run_id   text,
    action_summary    text NOT NULL,
    remediation_hint  text,
    resolved_at       timestamptz,
    resolved_by       text,
    CONSTRAINT policy_drift_events_policy_id_nonblank
        CHECK (btrim(policy_id) <> ''),
    CONSTRAINT policy_drift_events_detector_kind_nonblank
        CHECK (btrim(detector_kind) <> ''),
    CONSTRAINT policy_drift_events_action_summary_nonblank
        CHECK (btrim(action_summary) <> '')
);

CREATE INDEX IF NOT EXISTS idx_policy_drift_events_detected_at
    ON policy_drift_events (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_policy_drift_events_unresolved
    ON policy_drift_events (policy_id, detected_at DESC)
    WHERE resolved_at IS NULL;

COMMENT ON TABLE policy_drift_events IS
    'Inverse check on operator_decisions. Each row records a runtime action '
    'that violated an active architecture policy. '
    'policy_drift_count_24h on the mobile canvas reads '
    'count(*) WHERE resolved_at IS NULL AND detected_at > now() - interval ''24 hours''. '
    'First detector: anthropic CLI-only enforcement (api.anthropic.com HTTP call attempt).';

-- ──────────────────────────────────────────────────────────────────────
-- autonomy_chain_ledger — chain length of auto-approved actions.
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS autonomy_chain_ledger (
    chain_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at          timestamptz NOT NULL DEFAULT now(),
    last_updated        timestamptz NOT NULL DEFAULT now(),
    last_human_tap_at   timestamptz,
    depth_counter       integer NOT NULL DEFAULT 0,
    current_run_id      text,
    terminated_at       timestamptz,
    termination_reason  text,
    CONSTRAINT autonomy_chain_ledger_depth_nonnegative
        CHECK (depth_counter >= 0),
    CONSTRAINT autonomy_chain_ledger_terminated_has_reason
        CHECK ((terminated_at IS NULL) = (termination_reason IS NULL))
);

-- At most one active chain at a time (human tap closes the previous, opens the next).
CREATE UNIQUE INDEX IF NOT EXISTS uniq_autonomy_chain_ledger_active_singleton
    ON autonomy_chain_ledger ((terminated_at IS NULL))
    WHERE terminated_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_autonomy_chain_ledger_history
    ON autonomy_chain_ledger (started_at DESC);

COMMENT ON TABLE autonomy_chain_ledger IS
    'Tracks chain length of auto-approved actions since last human tap. '
    'At most one active row (terminated_at IS NULL) at any time. '
    'autonomy_depth on the mobile canvas reads depth_counter from that active row.';

-- ──────────────────────────────────────────────────────────────────────
-- session_blast_radius — per-session working-set counter (Pillar 4).
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS session_blast_radius (
    session_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at          timestamptz NOT NULL DEFAULT now(),
    last_updated        timestamptz NOT NULL DEFAULT now(),
    current_run_id      text,
    files_mutated       integer NOT NULL DEFAULT 0,
    bytes_written       bigint  NOT NULL DEFAULT 0,
    external_calls      integer NOT NULL DEFAULT 0,
    spend_cents         integer NOT NULL DEFAULT 0,
    gate_triggered_at   timestamptz,
    gate_triggered_dim  text,   -- which dimension tripped the gate
    gate_approval_ref   text,   -- approval_requests.request_id (no FK; cross-migration)
    closed_at           timestamptz,
    closed_reason       text,
    CONSTRAINT session_blast_radius_counters_nonnegative CHECK (
        files_mutated  >= 0 AND
        bytes_written  >= 0 AND
        external_calls >= 0 AND
        spend_cents    >= 0
    ),
    CONSTRAINT session_blast_radius_gate_dim_valid CHECK (
        gate_triggered_dim IS NULL OR gate_triggered_dim IN (
            'files_mutated', 'bytes_written', 'external_calls', 'spend_cents'
        )
    ),
    CONSTRAINT session_blast_radius_gate_fields_paired CHECK (
        (gate_triggered_at IS NULL) = (gate_triggered_dim IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_session_blast_radius_active
    ON session_blast_radius (last_updated DESC)
    WHERE closed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_session_blast_radius_run
    ON session_blast_radius (current_run_id)
    WHERE current_run_id IS NOT NULL AND closed_at IS NULL;

COMMENT ON TABLE session_blast_radius IS
    'Per-session working-set counter. Updated on every mutation and external '
    'call. When any dimension crosses its ceiling in session_blast_radius_policy, '
    'the runtime opens an approval_request regardless of grant coverage. '
    'The mobile canvas BurnRateHeader reads the active row inline with burn rate.';

-- ──────────────────────────────────────────────────────────────────────
-- session_blast_radius_policy — threshold ceilings, editable row-by-row.
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS session_blast_radius_policy (
    policy_id                  text PRIMARY KEY,
    threshold_files_mutated    integer,
    threshold_bytes_written    bigint,
    threshold_external_calls   integer,
    threshold_spend_cents      integer,
    active                     boolean NOT NULL DEFAULT true,
    decision_ref               text,
    rationale                  text,
    created_at                 timestamptz NOT NULL DEFAULT now(),
    updated_at                 timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT session_blast_radius_policy_policy_id_nonblank
        CHECK (btrim(policy_id) <> ''),
    CONSTRAINT session_blast_radius_policy_any_threshold CHECK (
        threshold_files_mutated  IS NOT NULL OR
        threshold_bytes_written  IS NOT NULL OR
        threshold_external_calls IS NOT NULL OR
        threshold_spend_cents    IS NOT NULL
    ),
    CONSTRAINT session_blast_radius_policy_thresholds_positive CHECK (
        (threshold_files_mutated  IS NULL OR threshold_files_mutated  > 0) AND
        (threshold_bytes_written  IS NULL OR threshold_bytes_written  > 0) AND
        (threshold_external_calls IS NULL OR threshold_external_calls > 0) AND
        (threshold_spend_cents    IS NULL OR threshold_spend_cents    > 0)
    )
);

COMMENT ON TABLE session_blast_radius_policy IS
    'Threshold ceilings per blast-radius dimension. Editable row-by-row; '
    'gate fires when an active session crosses any ceiling, regardless of '
    'grant coverage. Dialing is a row-edit, not a code change.';

-- Seed a conservative starter policy. Any dimension crossing fires the gate.
--   files_mutated   = 50     (typical refactor-scale changeset)
--   bytes_written   = 5 MiB  (5 * 1024 * 1024)
--   external_calls  = 20     (beyond an orient-scale run)
--   spend_cents     = 1000   ($10 — tuned to a single wave's budget)
INSERT INTO session_blast_radius_policy (
    policy_id,
    threshold_files_mutated,
    threshold_bytes_written,
    threshold_external_calls,
    threshold_spend_cents,
    decision_ref,
    rationale
) VALUES (
    'default',
    50,
    5242880,
    20,
    1000,
    'debate_mobile_access_architecture_20260420',
    'Conservative starter thresholds. Tighten or relax via UPDATE as behavior data accumulates. Any dimension crossing triggers mid-plan approval_request — gate fires regardless of grant coverage.'
) ON CONFLICT (policy_id) DO NOTHING;

COMMIT;

-- Verification:
--   SELECT count(*) FROM policy_drift_events;                 -- expect 0
--   SELECT count(*) FROM autonomy_chain_ledger;               -- expect 0
--   SELECT count(*) FROM session_blast_radius;                -- expect 0
--   SELECT policy_id FROM session_blast_radius_policy;        -- expect 'default'
