-- Migration 178: Change-feed ledger for incremental governance.
--
-- Every mutation on the three governance-relevant authority tables
-- (classifications / stewardship / quality_rules) appends a row here,
-- keyed by the affected object_kind. A heartbeat module drains
-- unprocessed rows and runs a focused governance scan on exactly those
-- objects, instead of waiting for the next full scheduled scan.
--
-- Unlike pg_notify (which is ephemeral and requires a live LISTEN
-- connection), this ledger is durable: rows survive restarts and
-- cross-process coordination is just a `WHERE processed_at IS NULL`
-- SELECT.
--
-- Decision: operator_decision.architecture_policy.data_dictionary.incremental_governance
-- Scope:    authority_domain=data_dictionary.governance

CREATE TABLE IF NOT EXISTS data_dictionary_governance_change_ledger (
    change_id            bigserial PRIMARY KEY,
    affected_object_kind text NOT NULL,
    source_table         text NOT NULL,
    change_kind          text NOT NULL,
    payload              jsonb NOT NULL DEFAULT '{}'::jsonb,
    observed_at          timestamptz NOT NULL DEFAULT now(),
    processed_at         timestamptz,
    processed_scan_id    uuid,
    CONSTRAINT data_dictionary_governance_change_ledger_object_nonblank
        CHECK (btrim(affected_object_kind) <> ''),
    CONSTRAINT data_dictionary_governance_change_ledger_change_kind_check
        CHECK (change_kind IN ('insert', 'update', 'delete'))
);

-- Critical index for drain performance — only unprocessed rows matter.
CREATE INDEX IF NOT EXISTS idx_gov_change_ledger_pending
    ON data_dictionary_governance_change_ledger (observed_at)
    WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_gov_change_ledger_object
    ON data_dictionary_governance_change_ledger (affected_object_kind, observed_at DESC);


-- Trigger function shared by all three authority tables.
CREATE OR REPLACE FUNCTION gov_change_ledger_append() RETURNS trigger AS $$
DECLARE
    affected text;
BEGIN
    affected := COALESCE(
        NEW.object_kind,
        OLD.object_kind
    );
    IF affected IS NULL OR btrim(affected) = '' THEN
        RETURN COALESCE(NEW, OLD);
    END IF;

    INSERT INTO data_dictionary_governance_change_ledger
        (affected_object_kind, source_table, change_kind, payload)
    VALUES (
        affected,
        TG_TABLE_NAME,
        LOWER(TG_OP),
        COALESCE(
            to_jsonb(NEW),
            to_jsonb(OLD),
            '{}'::jsonb
        )
    );
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;


-- Attach to each authority table. DROP IF EXISTS first so the migration
-- is idempotent across re-runs.
DROP TRIGGER IF EXISTS trg_gov_change_ledger_classifications
    ON data_dictionary_classifications;
CREATE TRIGGER trg_gov_change_ledger_classifications
    AFTER INSERT OR UPDATE OR DELETE ON data_dictionary_classifications
    FOR EACH ROW EXECUTE FUNCTION gov_change_ledger_append();

DROP TRIGGER IF EXISTS trg_gov_change_ledger_stewardship
    ON data_dictionary_stewardship;
CREATE TRIGGER trg_gov_change_ledger_stewardship
    AFTER INSERT OR UPDATE OR DELETE ON data_dictionary_stewardship
    FOR EACH ROW EXECUTE FUNCTION gov_change_ledger_append();

DROP TRIGGER IF EXISTS trg_gov_change_ledger_quality_rules
    ON data_dictionary_quality_rules;
CREATE TRIGGER trg_gov_change_ledger_quality_rules
    AFTER INSERT OR UPDATE OR DELETE ON data_dictionary_quality_rules
    FOR EACH ROW EXECUTE FUNCTION gov_change_ledger_append();
