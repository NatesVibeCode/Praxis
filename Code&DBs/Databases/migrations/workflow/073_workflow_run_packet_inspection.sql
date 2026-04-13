BEGIN;

ALTER TABLE workflow_runs
    ADD COLUMN IF NOT EXISTS packet_inspection JSONB;

ALTER TABLE workflow_runs
    DROP CONSTRAINT IF EXISTS workflow_runs_packet_inspection_object_check;

ALTER TABLE workflow_runs
    ADD CONSTRAINT workflow_runs_packet_inspection_object_check
    CHECK (
        packet_inspection IS NULL
        OR jsonb_typeof(packet_inspection) = 'object'
    );

COMMENT ON COLUMN workflow_runs.packet_inspection IS
    'Materialized run-level execution packet inspection derived from persisted execution packets.';

COMMIT;
