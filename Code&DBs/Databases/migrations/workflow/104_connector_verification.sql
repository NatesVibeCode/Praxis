-- Migration 104: Connector capability verification
--
-- Adds verification_spec (declarative test cases) to connector_registry,
-- expands verification_runs/healing_runs target_kind to include 'connector',
-- and registers a builtin verifier for connector capability testing.

BEGIN;

-- 1. Expand target_kind CHECK on verification_runs and healing_runs
ALTER TABLE verification_runs
  DROP CONSTRAINT IF EXISTS verification_runs_target_kind_check;
ALTER TABLE verification_runs
  ADD CONSTRAINT verification_runs_target_kind_check
  CHECK (target_kind IN ('platform', 'receipt', 'run', 'path', 'connector'));

ALTER TABLE healing_runs
  DROP CONSTRAINT IF EXISTS healing_runs_target_kind_check;
ALTER TABLE healing_runs
  ADD CONSTRAINT healing_runs_target_kind_check
  CHECK (target_kind IN ('platform', 'receipt', 'run', 'path', 'connector'));

-- 2. Add verification columns to connector_registry
ALTER TABLE connector_registry
  ADD COLUMN IF NOT EXISTS verification_spec JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE connector_registry
  ADD COLUMN IF NOT EXISTS verification_status TEXT NOT NULL DEFAULT 'unverified';
ALTER TABLE connector_registry
  ADD COLUMN IF NOT EXISTS last_verified_at TIMESTAMPTZ;

-- 3. Register builtin verifier for connector capabilities
INSERT INTO verifier_registry (
    verifier_ref, display_name, description, verifier_kind,
    builtin_ref, default_inputs, enabled, decision_ref
) VALUES (
    'verifier.connector.capability',
    'Connector Capability Verification',
    'Verify that a registered connector can execute its declared capabilities against a live API.',
    'builtin',
    'connector_capability',
    '{}'::jsonb,
    TRUE,
    'decision.connector_verification.bootstrap.20260412'
) ON CONFLICT (verifier_ref) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    builtin_ref = EXCLUDED.builtin_ref,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
