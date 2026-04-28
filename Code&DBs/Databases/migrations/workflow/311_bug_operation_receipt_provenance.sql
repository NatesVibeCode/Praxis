-- Migration 311: Let bug discovery point at workflow or operation receipts.
--
-- bugs.discovered_in_receipt_id originally referenced only workflow job
-- receipts. CQRS operator tools now write their proof to
-- authority_operation_receipts, so the FK rejects valid operator-surface
-- provenance. Validation moves to runtime.bug_tracker, which checks the id
-- against either receipts or authority_operation_receipts before insert.

BEGIN;

ALTER TABLE bugs
    DROP CONSTRAINT IF EXISTS bugs_discovered_in_receipt_fkey;

COMMENT ON COLUMN bugs.discovered_in_receipt_id IS
    'Discovery receipt id. May refer to receipts.receipt_id or authority_operation_receipts.receipt_id; validated by runtime.bug_tracker before insert.';

COMMIT;
