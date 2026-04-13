-- Derived workflow outbox over committed authority evidence.
-- workflow_events and receipts remain the source of truth.
-- This table exists so downstream readers can consume committed facts through
-- one boring seam without polling multiple authority tables directly.

CREATE TABLE IF NOT EXISTS workflow_outbox (
    authority_table text NOT NULL CHECK (authority_table IN ('workflow_events', 'receipts')),
    authority_id text NOT NULL,
    envelope_kind text NOT NULL CHECK (envelope_kind IN ('workflow_event', 'receipt')),
    workflow_id text NOT NULL,
    run_id text NOT NULL,
    request_id text NOT NULL,
    evidence_seq bigint NOT NULL CHECK (evidence_seq > 0),
    transition_seq bigint NOT NULL CHECK (transition_seq > 0),
    authority_recorded_at timestamptz NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT now(),
    envelope jsonb NOT NULL,
    CONSTRAINT workflow_outbox_pkey PRIMARY KEY (authority_table, authority_id),
    CONSTRAINT workflow_outbox_run_id_fkey
        FOREIGN KEY (run_id)
        REFERENCES workflow_runs (run_id)
        ON DELETE RESTRICT,
    CONSTRAINT workflow_outbox_run_id_evidence_seq_key UNIQUE (run_id, evidence_seq)
);

CREATE INDEX IF NOT EXISTS workflow_outbox_workflow_id_run_id_evidence_seq_idx
    ON workflow_outbox (workflow_id, run_id, evidence_seq);

CREATE INDEX IF NOT EXISTS workflow_outbox_envelope_kind_run_id_evidence_seq_idx
    ON workflow_outbox (envelope_kind, run_id, evidence_seq);

COMMENT ON TABLE workflow_outbox IS 'Derived committed-evidence outbox. Subscribers consume this seam, but workflow_events and receipts remain authority.';
COMMENT ON COLUMN workflow_outbox.evidence_seq IS 'Run-scoped replay cursor copied from the authority evidence row.';
COMMENT ON COLUMN workflow_outbox.transition_seq IS 'Authoritative transition lineage copied from the authority evidence row.';
COMMENT ON COLUMN workflow_outbox.envelope IS 'Snapshot of the committed authority row for replay and projection readers.';

CREATE OR REPLACE FUNCTION workflow_outbox_capture_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO workflow_outbox (
        authority_table,
        authority_id,
        envelope_kind,
        workflow_id,
        run_id,
        request_id,
        evidence_seq,
        transition_seq,
        authority_recorded_at,
        envelope
    ) VALUES (
        'workflow_events',
        NEW.event_id,
        'workflow_event',
        NEW.workflow_id,
        NEW.run_id,
        NEW.request_id,
        NEW.evidence_seq,
        (NEW.payload->>'transition_seq')::bigint,
        NEW.occurred_at,
        to_jsonb(NEW)
    )
    ON CONFLICT (authority_table, authority_id) DO NOTHING;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION workflow_outbox_capture_receipt()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO workflow_outbox (
        authority_table,
        authority_id,
        envelope_kind,
        workflow_id,
        run_id,
        request_id,
        evidence_seq,
        transition_seq,
        authority_recorded_at,
        envelope
    ) VALUES (
        'receipts',
        NEW.receipt_id,
        'receipt',
        NEW.workflow_id,
        NEW.run_id,
        NEW.request_id,
        NEW.evidence_seq,
        (NEW.inputs->>'transition_seq')::bigint,
        NEW.finished_at,
        to_jsonb(NEW)
    )
    ON CONFLICT (authority_table, authority_id) DO NOTHING;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS workflow_outbox_capture_event_after_insert ON workflow_events;

CREATE TRIGGER workflow_outbox_capture_event_after_insert
AFTER INSERT ON workflow_events
FOR EACH ROW
EXECUTE FUNCTION workflow_outbox_capture_event();

DROP TRIGGER IF EXISTS workflow_outbox_capture_receipt_after_insert ON receipts;

CREATE TRIGGER workflow_outbox_capture_receipt_after_insert
AFTER INSERT ON receipts
FOR EACH ROW
EXECUTE FUNCTION workflow_outbox_capture_receipt();

INSERT INTO workflow_outbox (
    authority_table,
    authority_id,
    envelope_kind,
    workflow_id,
    run_id,
    request_id,
    evidence_seq,
    transition_seq,
    authority_recorded_at,
    envelope
)
SELECT
    'workflow_events' AS authority_table,
    event_row.event_id AS authority_id,
    'workflow_event' AS envelope_kind,
    event_row.workflow_id,
    event_row.run_id,
    event_row.request_id,
    event_row.evidence_seq,
    (event_row.payload->>'transition_seq')::bigint AS transition_seq,
    event_row.occurred_at AS authority_recorded_at,
    to_jsonb(event_row) AS envelope
FROM workflow_events AS event_row
ON CONFLICT (authority_table, authority_id) DO NOTHING;

INSERT INTO workflow_outbox (
    authority_table,
    authority_id,
    envelope_kind,
    workflow_id,
    run_id,
    request_id,
    evidence_seq,
    transition_seq,
    authority_recorded_at,
    envelope
)
SELECT
    'receipts' AS authority_table,
    receipt_row.receipt_id AS authority_id,
    'receipt' AS envelope_kind,
    receipt_row.workflow_id,
    receipt_row.run_id,
    receipt_row.request_id,
    receipt_row.evidence_seq,
    (receipt_row.inputs->>'transition_seq')::bigint AS transition_seq,
    receipt_row.finished_at AS authority_recorded_at,
    to_jsonb(receipt_row) AS envelope
FROM receipts AS receipt_row
ON CONFLICT (authority_table, authority_id) DO NOTHING;
