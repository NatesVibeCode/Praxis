-- Migration 192: Allow `model_retirement` as a heartbeat probe_kind.
--
-- Migration 191 introduced ``provider_model_retirement_ledger`` and the
-- automatic retirement detector. The detector reuses the daily heartbeat
-- pipeline so its findings show up alongside provider/connector/credential/mcp
-- probes in ``heartbeat_probe_snapshots``. Migration 179's CHECK constraint
-- only listed the original four probe kinds, so we widen it here.
--
-- Status semantics for ``probe_kind = 'model_retirement'`` snapshots:
--   ok       — model present in live discovery; row stays active
--   warning  — model is in the curated ledger as `sunset_warning`/`deprecating`
--              but not yet past its effective date (advisory only)
--   degraded — provider could not be probed (discovery error / safety abort);
--              no rows acted on for that provider this cycle
--   failed   — model is missing from live discovery and was retired this cycle
--              (or would be, in dry_run mode)

BEGIN;

ALTER TABLE heartbeat_probe_snapshots
    DROP CONSTRAINT IF EXISTS heartbeat_probe_snapshots_probe_kind_check;

ALTER TABLE heartbeat_probe_snapshots
    ADD CONSTRAINT heartbeat_probe_snapshots_probe_kind_check
    CHECK (probe_kind IN (
        'provider_usage',
        'connector_liveness',
        'credential_expiry',
        'mcp_liveness',
        'model_retirement'
    ));

-- The run-level scope check needs the same widening so a heartbeat_run row
-- can record scope='model_retirement' (either as the explicit scope of a
-- targeted run, or rolled into scope='all').

ALTER TABLE heartbeat_runs
    DROP CONSTRAINT IF EXISTS heartbeat_runs_scope_check;

ALTER TABLE heartbeat_runs
    ADD CONSTRAINT heartbeat_runs_scope_check
    CHECK (scope IN (
        'providers',
        'connectors',
        'credentials',
        'mcp',
        'model_retirement',
        'all'
    ));

COMMIT;
