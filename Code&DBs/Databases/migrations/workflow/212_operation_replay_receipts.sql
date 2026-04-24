-- Allow every idempotent replay to leave its own durable receipt.
--
-- The original index treated both completed and replayed rows as the one
-- idempotent success record. That made replay receipts impossible to insert
-- with the same operation_ref/idempotency_key as the canonical completed row.
-- Keep uniqueness scoped to completed commands only: read-only query receipts
-- are observations, so repeated identical queries must be allowed to leave
-- their own audit rows.
DROP INDEX IF EXISTS authority_operation_receipts_idempotency_success_idx;

CREATE UNIQUE INDEX IF NOT EXISTS authority_operation_receipts_idempotency_success_idx
    ON authority_operation_receipts (operation_ref, idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND execution_status = 'completed'
      AND operation_kind = 'command';
