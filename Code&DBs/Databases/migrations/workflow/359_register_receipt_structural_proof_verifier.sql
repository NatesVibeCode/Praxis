-- Migration 359: Register the per-receipt structural proof verifier.
--
-- The platform-level verifier verifier.platform.receipt_provenance scans the
-- whole receipts table and emits one pass/fail per scan. That metric stays
-- low because most receipts never get a verification block attached at write
-- time. This migration registers a lightweight per-receipt verifier that
-- runs inline in write_receipt() and adds a verification block to each
-- receipt's outputs, raising verification_status_coverage / verification_coverage
-- as new receipts land. The check is a pure function over receipt fields
-- (no DB I/O), so it does not contend on the connection pool.

BEGIN;

INSERT INTO verifier_registry (
    verifier_ref,
    display_name,
    description,
    verifier_kind,
    verification_ref,
    builtin_ref,
    default_inputs,
    enabled,
    decision_ref
) VALUES (
    'verifier.receipt.structural_proof',
    'Receipt Structural Proof',
    'Per-receipt structural proof check that validates one receipt has the provenance fields the platform aggregate verifier scans for. Runs inline at write_receipt time so every new workflow receipt carries a verification block.',
    'builtin',
    NULL,
    'receipt_structural_proof',
    '{}'::jsonb,
    TRUE,
    'decision.verifier_registry.receipt_structural_proof.20260430'
)
ON CONFLICT (verifier_ref) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    verifier_kind = EXCLUDED.verifier_kind,
    verification_ref = EXCLUDED.verification_ref,
    builtin_ref = EXCLUDED.builtin_ref,
    default_inputs = EXCLUDED.default_inputs,
    enabled = EXCLUDED.enabled,
    decision_ref = EXCLUDED.decision_ref,
    updated_at = now();

COMMIT;
