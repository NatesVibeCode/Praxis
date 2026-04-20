-- Migration 177: Governance scan audit trail.
--
-- Every governance scan / enforce cycle writes one row here, capturing
-- (a) what violations were seen at that moment in time, and
-- (b) which bugs were filed / skipped / errored as a result.
--
-- Each governance bug is linked back to the scan that found it via
-- `bug_evidence_links` with evidence_kind='governance_scan' and
-- evidence_ref=<scan_id>, so any bug viewer can answer "which scan
-- discovered this?" without re-running the scan.
--
-- Decision: operator_decision.architecture_policy.data_dictionary.governance_audit_trail
-- Scope:    authority_domain=data_dictionary.governance

CREATE TABLE IF NOT EXISTS data_dictionary_governance_scans (
    scan_id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    scanned_at        timestamptz NOT NULL DEFAULT now(),
    triggered_by      text NOT NULL DEFAULT 'heartbeat',
    dry_run           boolean NOT NULL DEFAULT true,
    total_violations  integer NOT NULL DEFAULT 0
        CHECK (total_violations >= 0),
    bugs_filed        integer NOT NULL DEFAULT 0
        CHECK (bugs_filed >= 0),
    bugs_skipped      integer NOT NULL DEFAULT 0
        CHECK (bugs_skipped >= 0),
    bugs_errored      integer NOT NULL DEFAULT 0
        CHECK (bugs_errored >= 0),
    by_policy         jsonb NOT NULL DEFAULT '{}'::jsonb,
    violations        jsonb NOT NULL DEFAULT '[]'::jsonb,
    filed_bug_ids     text[] NOT NULL DEFAULT ARRAY[]::text[],
    metadata          jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT data_dictionary_governance_scans_triggered_by_nonblank
        CHECK (btrim(triggered_by) <> '')
);

CREATE INDEX IF NOT EXISTS idx_data_dictionary_governance_scans_scanned
    ON data_dictionary_governance_scans (scanned_at DESC);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_governance_scans_triggered
    ON data_dictionary_governance_scans (triggered_by, scanned_at DESC);


-- pg_notify so subscribers (UI, log aggregators) can react to new scans.
CREATE OR REPLACE FUNCTION notify_data_dictionary_governance_scans() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(
        'data_dictionary_governance_scans',
        json_build_object(
            'scan_id',          NEW.scan_id::text,
            'scanned_at',       NEW.scanned_at,
            'triggered_by',     NEW.triggered_by,
            'dry_run',          NEW.dry_run,
            'total_violations', NEW.total_violations,
            'bugs_filed',       NEW.bugs_filed
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_data_dictionary_governance_scans_notify
    ON data_dictionary_governance_scans;
CREATE TRIGGER trg_data_dictionary_governance_scans_notify
    AFTER INSERT ON data_dictionary_governance_scans
    FOR EACH ROW
    EXECUTE FUNCTION notify_data_dictionary_governance_scans();
