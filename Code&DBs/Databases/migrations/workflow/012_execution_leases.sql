-- Execution lease table for distributed resource locking.
-- Holders acquire time-bounded leases on named resources; expired leases
-- are reaped lazily on the next acquire attempt.

CREATE TABLE execution_leases (
    lease_id text PRIMARY KEY,
    holder_id text NOT NULL,
    resource_key text UNIQUE NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    renewed_at timestamptz
);

CREATE INDEX execution_leases_expires_at_idx
    ON execution_leases (expires_at);

COMMENT ON TABLE execution_leases IS 'Short-lived execution leases for distributed resource coordination. Owned by runtime/.';
COMMENT ON COLUMN execution_leases.resource_key IS 'Unique resource identifier that the lease protects. Only one active lease per resource_key at a time.';
