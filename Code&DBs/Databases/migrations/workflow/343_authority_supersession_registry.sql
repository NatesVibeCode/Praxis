-- Migration 343: authority_supersession_registry — explicit successor → predecessor
-- relationships at the authority-unit level.
--
-- code_change_candidate_payloads.superseded_by tracks candidate-to-candidate
-- replacement. candidate_authority_impacts (migration 341) tracks per-candidate
-- impact rows. Neither answers the compose-time question:
--
--     "I am about to compose work that targets authority unit X. What is the
--      canonical successor of X today, and what predecessor obligations must
--      be carried into the new write scope?"
--
-- This table is the canonical join between the impact-row event ("candidate
-- N declared replace(X -> Y) and was validated by preflight + materialized")
-- and the live dispatch surface. compose_authority_binding queries it.
--
-- Population: candidate materialization writes one row per validated
-- candidate_authority_impacts row whose intent IN ('replace', 'retire').
-- Operators may also insert rows manually for legacy supersessions that
-- predate the impact contract.

BEGIN;

CREATE TYPE authority_supersession_status AS ENUM (
    'compat',          -- predecessor still callable; new work should target successor
    'pending_retire',  -- predecessor scheduled for retirement; emits deprecation
    'retired',         -- predecessor no longer dispatchable (quarantine vault)
    'rolled_back'      -- supersession reverted; predecessor regained canonical status
);

CREATE TABLE IF NOT EXISTS authority_supersession_registry (
    supersession_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    successor_unit_kind candidate_authority_unit_kind NOT NULL,
    successor_unit_ref text NOT NULL,
    predecessor_unit_kind candidate_authority_unit_kind NOT NULL,
    predecessor_unit_ref text NOT NULL,
    supersession_status authority_supersession_status NOT NULL DEFAULT 'compat',
    obligation_summary text,
    obligation_evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_candidate_id uuid,
    source_impact_id uuid,
    source_decision_ref text,
    rolled_back_at timestamptz,
    rollback_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT authority_supersession_self_reference CHECK (
        NOT (successor_unit_kind = predecessor_unit_kind
             AND successor_unit_ref = predecessor_unit_ref)
    ),
    CONSTRAINT authority_supersession_rolled_back_consistency CHECK (
        (supersession_status = 'rolled_back') = (rolled_back_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS authority_supersession_registry_pair_uq
    ON authority_supersession_registry (
        successor_unit_kind,
        successor_unit_ref,
        predecessor_unit_kind,
        predecessor_unit_ref
    )
    WHERE supersession_status <> 'rolled_back';

CREATE INDEX IF NOT EXISTS authority_supersession_registry_successor_idx
    ON authority_supersession_registry (successor_unit_kind, successor_unit_ref, supersession_status);

CREATE INDEX IF NOT EXISTS authority_supersession_registry_predecessor_idx
    ON authority_supersession_registry (predecessor_unit_kind, predecessor_unit_ref, supersession_status);

CREATE INDEX IF NOT EXISTS authority_supersession_registry_source_candidate_idx
    ON authority_supersession_registry (source_candidate_id)
    WHERE source_candidate_id IS NOT NULL;

-- Active obligations view: predecessors that new compose work must read but
-- not extend (compat or pending_retire). Joins to source candidate / impact
-- so the obligation_summary can be enriched at query time.
CREATE OR REPLACE VIEW authority_active_predecessor_obligations AS
SELECT supersession_id,
       successor_unit_kind,
       successor_unit_ref,
       predecessor_unit_kind,
       predecessor_unit_ref,
       supersession_status,
       obligation_summary,
       obligation_evidence,
       source_candidate_id,
       source_impact_id,
       source_decision_ref,
       created_at,
       updated_at
  FROM authority_supersession_registry
 WHERE supersession_status IN ('compat', 'pending_retire');

-- Canonical-successor lookup view: given a predecessor unit, which canonical
-- unit has taken over? Latest-non-rolledback wins.
CREATE OR REPLACE VIEW authority_canonical_successor_for AS
SELECT DISTINCT ON (predecessor_unit_kind, predecessor_unit_ref)
       predecessor_unit_kind,
       predecessor_unit_ref,
       successor_unit_kind,
       successor_unit_ref,
       supersession_status,
       supersession_id,
       updated_at
  FROM authority_supersession_registry
 WHERE supersession_status <> 'rolled_back'
 ORDER BY predecessor_unit_kind,
          predecessor_unit_ref,
          updated_at DESC,
          supersession_id DESC;

COMMENT ON TABLE authority_supersession_registry IS
    'Authority-unit supersession (successor -> predecessor) at the live dispatch level. Compose-time canonical resolution joins this so new work targets the successor and reads the predecessor only as a read-only obligation pack.';
COMMENT ON COLUMN authority_supersession_registry.supersession_status IS
    'compat: predecessor still dispatchable, new work must use successor. pending_retire: deprecation emitted on dispatch. retired: not dispatchable, kept for restore. rolled_back: supersession reverted, predecessor canonical again.';
COMMENT ON COLUMN authority_supersession_registry.obligation_summary IS
    'Human-readable summary of what the predecessor was doing that the successor must preserve. Surfaced to compose-time agents in the read-only obligation pack.';
COMMENT ON COLUMN authority_supersession_registry.source_impact_id IS
    'When populated by code-change candidate materialization, points at the candidate_authority_impacts row whose intent=replace/retire produced this supersession.';

COMMIT;
