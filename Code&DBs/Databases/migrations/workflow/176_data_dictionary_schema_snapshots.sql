-- Migration 176: Schema snapshots for drift detection.
--
-- Captures the field inventory of `data_dictionary_entries` at a point
-- in time, so the heartbeat drift projector can diff successive
-- snapshots and surface schema changes (added / dropped columns, type
-- changes, nullability changes) along with cross-axis impact.
--
-- Two tables:
--   data_dictionary_schema_snapshots         (one row per snapshot)
--   data_dictionary_schema_snapshot_fields   (field inventory per snapshot)
--
-- The fields table is the bulk store; (snapshot_id, object_kind,
-- field_path) is the natural key. A `fingerprint` on the parent row
-- (sha256 over the sorted field set) lets the diff loop short-circuit
-- when nothing has changed.
--
-- Decision: operator_decision.architecture_policy.data_dictionary.drift_detection
-- Scope:    authority_domain=data_dictionary.drift

CREATE TABLE IF NOT EXISTS data_dictionary_schema_snapshots (
    snapshot_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    taken_at          timestamptz NOT NULL DEFAULT now(),
    fingerprint       text NOT NULL,
    object_count      integer NOT NULL DEFAULT 0
        CHECK (object_count >= 0),
    field_count       integer NOT NULL DEFAULT 0
        CHECK (field_count >= 0),
    triggered_by      text NOT NULL DEFAULT 'heartbeat',
    metadata          jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT data_dictionary_schema_snapshots_fingerprint_nonblank
        CHECK (btrim(fingerprint) <> '')
);

CREATE INDEX IF NOT EXISTS idx_data_dictionary_schema_snapshots_taken
    ON data_dictionary_schema_snapshots (taken_at DESC);
CREATE INDEX IF NOT EXISTS idx_data_dictionary_schema_snapshots_fingerprint
    ON data_dictionary_schema_snapshots (fingerprint);


CREATE TABLE IF NOT EXISTS data_dictionary_schema_snapshot_fields (
    snapshot_id       uuid NOT NULL REFERENCES data_dictionary_schema_snapshots
                            ON DELETE CASCADE,
    object_kind       text NOT NULL,
    field_path        text NOT NULL DEFAULT '',
    field_kind        text NOT NULL DEFAULT 'text',
    required          boolean NOT NULL DEFAULT false,
    sources           text[] NOT NULL DEFAULT ARRAY[]::text[],
    PRIMARY KEY (snapshot_id, object_kind, field_path),
    CONSTRAINT data_dictionary_schema_snapshot_fields_object_nonblank
        CHECK (btrim(object_kind) <> '')
);

CREATE INDEX IF NOT EXISTS idx_data_dictionary_schema_snapshot_fields_object
    ON data_dictionary_schema_snapshot_fields (snapshot_id, object_kind);


-- pg_notify trigger so subscribers can react to new snapshots.
CREATE OR REPLACE FUNCTION notify_data_dictionary_schema_snapshots() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(
        'data_dictionary_schema_snapshots',
        json_build_object(
            'snapshot_id', NEW.snapshot_id::text,
            'taken_at',    NEW.taken_at,
            'fingerprint', NEW.fingerprint,
            'object_count', NEW.object_count,
            'field_count', NEW.field_count
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_data_dictionary_schema_snapshots_notify
    ON data_dictionary_schema_snapshots;
CREATE TRIGGER trg_data_dictionary_schema_snapshots_notify
    AFTER INSERT ON data_dictionary_schema_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION notify_data_dictionary_schema_snapshots();
