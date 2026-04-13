CREATE TABLE IF NOT EXISTS idempotency_ledger (
    surface           TEXT NOT NULL,
    idempotency_key   TEXT NOT NULL,
    payload_hash      TEXT NOT NULL,
    run_id            TEXT,
    response_snapshot JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ NOT NULL DEFAULT now() + interval '14 days',
    PRIMARY KEY (surface, idempotency_key)
);

CREATE INDEX idx_idempotency_expires ON idempotency_ledger (expires_at);
CREATE INDEX idx_idempotency_run ON idempotency_ledger (run_id) WHERE run_id IS NOT NULL;

CREATE OR REPLACE FUNCTION reap_expired_idempotency_keys() RETURNS INT AS $$
DECLARE
    reaped INT;
BEGIN
    DELETE FROM idempotency_ledger WHERE expires_at < now();
    GET DIAGNOSTICS reaped = ROW_COUNT;
    RETURN reaped;
END;
$$ LANGUAGE plpgsql;
